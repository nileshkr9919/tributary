// Package sub implements subscriptions: leases, acknowledgement, retries, dead letters,
// ordering and dispatch scheduling for one topic.
package sub

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/settings"
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/topic"
)

// Deps are a subscription's collaborators.
type Deps struct {
	Store  *store.Store
	Topics *topic.Manager
	Clock  clock.Clock
	Gate   Gate
}

type config struct {
	Name     string            `json:"name"`
	Topic    string            `json:"topic"`
	Settings settings.Settings `json:"settings"`
	Paused   map[string]bool   `json:"paused,omitempty"` // "" = whole subscription, else fairness value
}

// Subscription is one named consumer of a topic.
type Subscription struct {
	mu          sync.Mutex
	cfg         config
	dlq         string
	d           Deps
	entries     map[pos]*entry
	floors      map[int]uint64
	terminal    map[int]map[uint64]bool
	sched       *scheduler
	delays      timerHeap
	leases      timerHeap
	leased      int
	deadLetters map[string]*DeadLetter
	changed     chan struct{}
	gateRetry   time.Time
	busyKeys    map[string]int
	keyQueue    map[string][]*entry
	inFlightVal map[string]int
	pauseUntil  map[string]time.Time             // throttle pauses: "" subscription-wide, "v:<value>", "k:<key>"
	usage       map[[2]string]float64            // reported usage totals by (field, fairness value)
	nextOff     map[int]uint64                   // next offset expected per shard from OnAppend
	arrivals    map[int]map[uint64]topic.Message // out-of-order arrivals waiting for a gap to fill
}

// Name returns the subscription's name.
func (s *Subscription) Name() string { return s.cfg.Name }

// Topic returns the subscription's topic.
func (s *Subscription) Topic() string { return s.cfg.Topic }

// Settings returns the effective settings.
func (s *Subscription) Settings() settings.Settings { return s.cfg.Settings }

// DeadLetterTopic returns the name of the dead-letter topic.
func DeadLetterTopic(topicName, sub string) string { return topicName + ".dlq." + sub }

// New creates and persists a subscription.
func New(name, topicName string, st settings.Settings, d Deps) (*Subscription, error) {
	shards, err := d.Topics.Shards(topicName)
	if err != nil {
		return nil, ErrTopicNotFound
	}
	dlq := DeadLetterTopic(topicName, name)
	if err := d.Topics.Create(dlq, shards); err != nil && err != topic.ErrExists {
		return nil, err
	}
	cfg := config{Name: name, Topic: topicName, Settings: st}
	if err := saveConfig(d.Store, cfg); err != nil {
		return nil, err
	}
	return open(cfg, d)
}

// Load opens a persisted subscription.
func Load(name string, d Deps) (*Subscription, error) {
	v, ok, err := d.Store.Get(store.Key(store.TagSub, name))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("subscription %q not found", name)
	}
	var cfg config
	if err := json.Unmarshal(v, &cfg); err != nil {
		return nil, err
	}
	return open(cfg, d)
}

// Names lists persisted subscription names.
func Names(st *store.Store) ([]string, error) {
	var out []string
	err := st.Scan([]byte{store.TagSub}, func(k, v []byte) error {
		var cfg config
		if err := json.Unmarshal(v, &cfg); err != nil {
			return err
		}
		out = append(out, cfg.Name)
		return nil
	})
	return out, err
}

func saveConfig(st *store.Store, cfg config) error {
	b := st.NewBatch()
	v, _ := json.Marshal(cfg)
	b.Set(store.Key(store.TagSub, cfg.Name), v)
	return b.Commit()
}

func open(cfg config, d Deps) (*Subscription, error) {
	if d.Gate == nil {
		d.Gate = Unlimited{}
	}
	if cfg.Paused == nil {
		cfg.Paused = map[string]bool{}
	}
	s := &Subscription{
		cfg: cfg, dlq: DeadLetterTopic(cfg.Topic, cfg.Name), d: d,
		entries: map[pos]*entry{}, floors: map[int]uint64{}, terminal: map[int]map[uint64]bool{},
		sched: newScheduler(cfg.Settings), deadLetters: map[string]*DeadLetter{},
		changed: make(chan struct{}), busyKeys: map[string]int{}, keyQueue: map[string][]*entry{},
		inFlightVal: map[string]int{}, pauseUntil: map[string]time.Time{}, usage: map[[2]string]float64{},
		nextOff: map[int]uint64{}, arrivals: map[int]map[uint64]topic.Message{},
	}
	// Listen before scanning, so nothing produced during recovery is missed; arrivals wait
	// for the lock and duplicates of scanned messages are dropped by offset.
	s.mu.Lock()
	d.Topics.OnAppend(s.OnAppend)
	err := s.recover()
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Subscription) stateKey(p pos) []byte {
	return store.Key(store.TagState, s.cfg.Name, uint64(p.shard), p.offset)
}

func (s *Subscription) recover() error {
	shards, err := s.d.Topics.Shards(s.cfg.Topic)
	if err != nil {
		return ErrTopicNotFound
	}
	now := s.d.Clock.Now()
	for sh := 0; sh < shards; sh++ {
		s.terminal[sh] = map[uint64]bool{}
		if v, ok, err := s.d.Store.Get(store.Key(store.TagFloor, s.cfg.Name, uint64(sh))); err != nil {
			return err
		} else if ok {
			s.floors[sh] = binary.BigEndian.Uint64(v)
		}
		recs := map[uint64]record{}
		prefix := store.Prefix(store.TagState, s.cfg.Name, uint64(sh))
		err := s.d.Store.Scan(prefix, func(k, v []byte) error {
			var r record
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			recs[store.Uint64At(k, len(prefix))] = r
			return nil
		})
		if err != nil {
			return err
		}
		floor := s.floors[sh]
		s.nextOff[sh] = floor
		err = s.d.Topics.Scan(s.cfg.Topic, sh, floor, func(m topic.Message) error {
			s.nextOff[sh] = m.Offset + 1
			r, has := recs[m.Offset]
			delete(recs, m.Offset)
			if has && terminal(r.State) {
				s.terminal[sh][m.Offset] = true
				return nil
			}
			e := s.newEntry(m, now)
			if has {
				e.rec = r
			}
			s.track(e)
			return nil
		})
		if err != nil {
			return err
		}
		// records left over: requeued messages below the floor
		for off, r := range recs {
			if terminal(r.State) || r.Ref == "" {
				continue
			}
			src, ok, err := s.loadRef(r.Ref)
			if err != nil || !ok {
				continue
			}
			e := s.newEntry(src, now)
			e.p = pos{sh, off}
			e.ref = r.Ref
			e.rec = r
			s.track(e)
		}
	}
	err = s.d.Store.Scan(store.Prefix(store.TagDeadLetter, s.cfg.Name), func(k, v []byte) error {
		var dl DeadLetter
		if err := json.Unmarshal(v, &dl); err != nil {
			return err
		}
		s.deadLetters[dl.SourceID] = &dl
		return nil
	})
	if err != nil {
		return err
	}
	b := s.d.Store.NewBatch()
	for sh := range s.terminal {
		s.advanceFloor(b, sh)
	}
	if !b.Empty() {
		return b.Commit()
	}
	return nil
}

func (s *Subscription) loadRef(id string) (topic.Message, bool, error) {
	t, sh, off, err := topic.ParseID(id)
	if err != nil {
		return topic.Message{}, false, err
	}
	return s.d.Topics.Get(t, sh, off)
}

func (s *Subscription) newEntry(m topic.Message, now time.Time) *entry {
	e := &entry{
		p: pos{m.Shard, m.Offset}, ref: m.ID, key: m.Key, headers: m.Headers,
		priority: m.Priority, producedAt: m.ProducedAt,
	}
	if e.priority == "" && m.Headers != nil {
		e.priority = m.Headers["trib-priority"]
	}
	eligible := m.ProducedAt
	if m.DeliverAt.After(eligible) {
		eligible = m.DeliverAt
	}
	e.rec = record{State: statePending, Eligible: eligible}
	if m.DeliverAt.After(now) {
		e.rec.State = stateDelayed
		e.rec.NotBefore = m.DeliverAt
	}
	return e
}

// track adds an entry to the in-memory structures according to its state.
func (s *Subscription) track(e *entry) {
	s.entries[e.p] = e
	switch e.rec.State {
	case statePending:
		s.enqueue(e)
	case stateDelayed:
		s.delays.push(timerItem{at: e.rec.NotBefore, p: e.p})
	case stateLeased:
		s.leaseOn(e)
		s.leases.push(timerItem{at: e.rec.Deadline, p: e.p, tag: e.rec.LeaseID})
	case stateHeld:
		s.holdKey(e)
	}
	if e.rec.Attempt > 0 {
		s.holdKey(e) // a message that has been delivered is its key's head until it ends
	}
}

func (s *Subscription) enqueue(e *entry) { s.sched.push(e) }

// leaseOn counts an entry as in flight and makes it its key's head.
func (s *Subscription) leaseOn(e *entry) {
	s.leased++
	if v := e.fairness(s.cfg.Settings.FairnessHeader); v != "" {
		s.inFlightVal[v]++
	}
	s.holdKey(e)
}

// leaseOff stops counting an entry as in flight. The key stays held until the entry ends,
// so a head in back-off or held still blocks its key.
func (s *Subscription) leaseOff(e *entry) {
	s.leased--
	if v := e.fairness(s.cfg.Settings.FairnessHeader); v != "" {
		if s.inFlightVal[v]--; s.inFlightVal[v] <= 0 {
			delete(s.inFlightVal, v)
		}
	}
}

func (s *Subscription) holdKey(e *entry) {
	if e.key == "" || s.cfg.Settings.Ordering != "per-key" || e.holdsKey {
		return
	}
	e.holdsKey = true
	s.busyKeys[e.key]++
}

func (s *Subscription) releaseKey(e *entry) {
	if !e.holdsKey {
		return
	}
	e.holdsKey = false
	if s.busyKeys[e.key]--; s.busyKeys[e.key] <= 0 {
		delete(s.busyKeys, e.key)
	}
}

// Changed returns a channel closed at the next change that may make a message available.
func (s *Subscription) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

func (s *Subscription) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// OnAppend tracks newly produced messages.
func (s *Subscription) OnAppend(ms []topic.Message) {
	if len(ms) == 0 || ms[0].Topic != s.cfg.Topic {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.d.Clock.Now()
	b := s.d.Store.NewBatch()
	// Listeners can run out of commit order under concurrent produce; apply arrivals per
	// shard in offset order, holding any that arrive before a gap is filled.
	touched := map[int]bool{}
	for _, m := range ms {
		if m.Offset < s.nextOff[m.Shard] {
			continue // already tracked
		}
		if s.arrivals[m.Shard] == nil {
			s.arrivals[m.Shard] = map[uint64]topic.Message{}
		}
		s.arrivals[m.Shard][m.Offset] = m
		touched[m.Shard] = true
	}
	for sh := range touched {
		for {
			m, ok := s.arrivals[sh][s.nextOff[sh]]
			if !ok {
				break
			}
			delete(s.arrivals[sh], s.nextOff[sh])
			s.nextOff[sh]++
			if _, exists := s.entries[pos{m.Shard, m.Offset}]; exists {
				continue
			}
			e := s.newEntry(m, now)
			s.onArrival(b, e)
			s.track(e)
		}
	}
	if err := b.Commit(); err != nil {
		log.Printf("sub %s: arrival commit failed: %v", s.cfg.Name, err)
	}
	s.notify()
}

func (s *Subscription) available(now time.Time) func(*entry) bool {
	st := s.cfg.Settings
	return func(e *entry) bool {
		if s.cfg.Paused[""] {
			return false
		}
		if st.Ordering == "per-key" && e.key != "" && s.busyKeys[e.key] > 0 && !e.holdsKey {
			return false
		}
		v := e.fairness(st.FairnessHeader)
		if v != "" && s.cfg.Paused["v:"+v] {
			return false
		}
		if v != "" && st.MaxInFlightPerValue > 0 && s.inFlightVal[v] >= st.MaxInFlightPerValue {
			return false
		}
		if !s.unpaused(now, e) {
			return false
		}
		return true
	}
}

// Receive leases up to max messages to member. Lease grants are committed before returning.
func (s *Subscription) Receive(member string, max int) []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.d.Clock.Now()
	if max <= 0 {
		return nil
	}
	b := s.d.Store.NewBatch()
	type undo struct {
		e      *entry
		rec    record
		hadKey bool
	}
	var picked []undo
	base := s.available(now)
	denied := map[int]bool{} // bands the gate refused during this call
	avail := func(e *entry) bool { return !denied[e.band] && base(e) }
	for len(picked) < max && s.leased < s.cfg.Settings.MaxInFlight {
		e := s.sched.next(now, avail)
		if e == nil {
			break
		}
		charge, ok, retryAt := s.d.Gate.Reserve(s.cfg.Name, s.meta(e))
		if !ok {
			// A refusal (for example headroom kept for the top band) must not stop other
			// bands from being tried: exclude this band for the rest of the call.
			if s.gateRetry.IsZero() || retryAt.Before(s.gateRetry) {
				s.gateRetry = retryAt
			}
			denied[e.band] = true
			continue
		}
		picked = append(picked, undo{e, e.rec, e.holdsKey})
		s.sched.remove(e)
		r := &e.rec
		r.Attempt++
		if r.Holder != "" {
			r.PrevHolder = r.Holder
		}
		r.Holder = member
		r.LeaseID = rand.Uint64() | 1
		r.LeaseStart = now
		r.Deadline = now.Add(s.cfg.Settings.Lease.Initial)
		r.State = stateLeased
		r.Charged = charge
		r.Notified = ""
		s.leaseOn(e)
		s.putRecord(b, e)
	}
	if len(picked) == 0 {
		b.Commit()
		return nil
	}
	if err := b.Commit(); err != nil {
		log.Printf("sub %s: lease commit failed: %v", s.cfg.Name, err)
		for _, u := range picked {
			s.leaseOff(u.e)
			if !u.hadKey {
				s.releaseKey(u.e)
			}
			u.e.rec = u.rec
			s.enqueue(u.e)
		}
		return nil
	}
	out := make([]Delivery, 0, len(picked))
	for _, u := range picked {
		e := u.e
		s.leases.push(timerItem{at: e.rec.Deadline, p: e.p, tag: e.rec.LeaseID})
		out = append(out, s.delivery(e))
	}
	return out
}

func (s *Subscription) meta(e *entry) topic.Message {
	return topic.Message{ID: topic.FormatID(s.cfg.Topic, e.p.shard, e.p.offset), Topic: s.cfg.Topic,
		Shard: e.p.shard, Offset: e.p.offset, Key: e.key, Headers: e.headers, Priority: e.priority}
}

func (s *Subscription) delivery(e *entry) Delivery {
	id := topic.FormatID(s.cfg.Topic, e.p.shard, e.p.offset)
	d := Delivery{
		ID: id, Token: encodeToken(s.cfg.Name, e.p, e.rec.Attempt, e.rec.LeaseID), Key: e.key,
		Headers: e.headers, Attempt: e.rec.Attempt, OperationID: id, Priority: e.priority,
		Checkpoint: e.rec.Checkpoint, PreviousHolder: e.rec.PrevHolder,
		WaitMs: e.rec.LeaseStart.Sub(e.rec.Eligible).Milliseconds(),
	}
	if op := e.headers["trib-operation-id"]; op != "" {
		d.OperationID = op
	}
	if m, ok, err := s.loadRef(e.ref); err == nil && ok {
		d.Payload = m.Payload
	}
	return d
}

func (s *Subscription) putRecord(b *store.Batch, e *entry) {
	v, _ := json.Marshal(e.rec)
	b.Set(s.stateKey(e.p), v)
}

// lookup validates a token and returns its entry, or the status to report.
func (s *Subscription) lookup(tok string) (*entry, Status) {
	tp, ok := decodeToken(tok)
	if !ok || tp.sub != s.cfg.Name {
		return nil, StatusLost
	}
	e, ok := s.entries[tp.p]
	if !ok {
		return nil, StatusLost
	}
	if e.rec.State != stateLeased || e.rec.Attempt != tp.attempt || e.rec.LeaseID != tp.leaseID {
		return nil, StatusLost
	}
	if e.rec.Notified != "" {
		return e, Status(e.rec.Notified)
	}
	return e, StatusOK
}

func (s *Subscription) checkCheckpoint(c json.RawMessage) error {
	if len(c) > s.cfg.Settings.Lease.CheckpointLimit {
		return ErrCheckpointTooLarge
	}
	return nil
}

// Ack completes a delivery and settles reported usage.
func (s *Subscription) Ack(tok string, usage map[string]float64) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, st := s.lookup(tok)
	if e == nil {
		return st, nil
	}
	b := s.d.Store.NewBatch()
	if usage == nil {
		usage = map[string]float64{} // completed without a report: the charge stands
	}
	s.d.Gate.Settle(s.cfg.Name, e.rec.Charged, e.rec.LeaseStart, usage)
	s.addCost(e, usage)
	s.leaseOff(e)
	final := stateAcked
	if e.rec.Notified != "" {
		final = e.rec.Notified
	}
	s.finish(b, e, final, "")
	if err := b.Commit(); err != nil {
		return StatusLost, err
	}
	s.afterRelease(e)
	s.notify()
	return st, nil
}

func (s *Subscription) addCost(e *entry, usage map[string]float64) {
	label := e.fairness(s.cfg.Settings.FairnessHeader)
	for k, v := range usage {
		s.usage[[2]string{k, label}] += v
	}
	if c, ok := usage["cost"]; ok {
		e.rec.Cost += c
	} else if t, ok := usage["tokens"]; ok {
		e.rec.Cost += t
	}
}

// Extend renews a lease and optionally stores a progress checkpoint.
func (s *Subscription) Extend(tok string, d time.Duration, checkpoint json.RawMessage) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCheckpoint(checkpoint); err != nil {
		return "", err
	}
	e, st := s.lookup(tok)
	if e == nil || st != StatusOK {
		return st, nil
	}
	now := s.d.Clock.Now()
	if !s.cfg.Settings.Lease.Extendable && d > 0 {
		return st, ErrNotExtendable
	}
	if d <= 0 {
		d = s.cfg.Settings.Lease.Initial
	}
	deadline := now.Add(d)
	if limit := e.rec.LeaseStart.Add(s.cfg.Settings.Lease.MaxTotal); deadline.After(limit) {
		deadline = limit
	}
	e.rec.Deadline = deadline
	if len(checkpoint) > 0 {
		e.rec.Checkpoint = append(json.RawMessage(nil), checkpoint...)
	}
	b := s.d.Store.NewBatch()
	s.putRecord(b, e)
	if err := b.Commit(); err != nil {
		return StatusLost, err
	}
	s.leases.push(timerItem{at: deadline, p: e.p, tag: e.rec.LeaseID})
	if st == StatusOK && s.pausedFor(e) {
		return StatusPaused, nil
	}
	return st, nil
}

// Nack fails a delivery.
func (s *Subscription) Nack(tok string, n NackRequest) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCheckpoint(n.Checkpoint); err != nil {
		return "", err
	}
	e, st := s.lookup(tok)
	if e == nil {
		return st, nil
	}
	now := s.d.Clock.Now()
	b := s.d.Store.NewBatch()
	if len(n.Checkpoint) > 0 {
		e.rec.Checkpoint = append(json.RawMessage(nil), n.Checkpoint...)
	}
	e.rec.Expiries = 0
	s.leaseOff(e)
	s.d.Gate.Settle(s.cfg.Name, e.rec.Charged, e.rec.LeaseStart, n.Usage)
	s.addCost(e, n.Usage)
	switch {
	case e.rec.Notified != "":
		s.finish(b, e, e.rec.Notified, "")
	case s.cfg.Settings.Failure.MaxCost > 0 && e.rec.Cost > s.cfg.Settings.Failure.MaxCost:
		s.exhaust(b, e, "max cost exceeded", now)
	default:
		s.handleNack(b, e, n, now)
	}
	if err := b.Commit(); err != nil {
		return StatusLost, err
	}
	s.afterRelease(e)
	s.notify()
	return st, nil
}

// failAttempt retries with back-off or exhausts the message.
func (s *Subscription) failAttempt(b *store.Batch, e *entry, reason string, now time.Time) {
	if reason != "" {
		e.rec.Reason = reason
	}
	if e.rec.Attempt >= s.cfg.Settings.Failure.MaxAttempts {
		if e.rec.Reason == "" {
			e.rec.Reason = "max attempts exceeded"
		}
		s.exhaust(b, e, e.rec.Reason, now)
		return
	}
	s.delay(b, e, now.Add(s.backoff(e.rec.Attempt)))
}

func (s *Subscription) delay(b *store.Batch, e *entry, until time.Time) {
	e.rec.State = stateDelayed
	e.rec.NotBefore = until
	s.putRecord(b, e)
	s.delays.push(timerItem{at: until, p: e.p})
}

func (s *Subscription) backoff(attempt int) time.Duration {
	f := s.cfg.Settings.Failure
	d := float64(f.BackoffMin) * math.Pow(2, float64(attempt-1))
	if d > float64(f.BackoffMax) {
		d = float64(f.BackoffMax)
	}
	if f.Jitter {
		d *= 0.5 + 0.5*rand.Float64()
	}
	return time.Duration(d)
}

// exhaust applies on_exhausted.
func (s *Subscription) exhaust(b *store.Batch, e *entry, reason string, now time.Time) {
	e.rec.Reason = reason
	switch s.cfg.Settings.Failure.OnExhausted {
	case "hold":
		e.rec.State = stateHeld
		s.holdKey(e)
		s.putRecord(b, e)
	case "drop":
		s.finish(b, e, stateDropped, reason)
	default:
		s.deadLetter(b, e, reason, now)
	}
}

func (s *Subscription) deadLetter(b *store.Batch, e *entry, reason string, now time.Time) {
	src, _, _ := s.loadRef(e.ref)
	id := topic.FormatID(s.cfg.Topic, e.p.shard, e.p.offset)
	op := id
	if v := e.headers["trib-operation-id"]; v != "" {
		op = v
	}
	h := map[string]string{}
	for k, v := range e.headers {
		h[k] = v
	}
	h["trib-failure-reason"] = reason
	h["trib-attempts"] = strconv.Itoa(e.rec.Attempt)
	h["trib-source"] = id
	h["trib-operation-id"] = op
	h["trib-subscription"] = s.cfg.Name
	if len(e.rec.Checkpoint) > 0 {
		h["trib-checkpoint"] = string(e.rec.Checkpoint)
	}
	copies, err := s.d.Topics.Produce(s.dlq, []topic.Input{{
		Key: e.key, Headers: h, Payload: src.Payload, Priority: e.priority,
		IdempotencyKey: fmt.Sprintf("%s#dl#%d", id, e.rec.Attempt),
	}})
	copyID := ""
	if err == nil && len(copies) == 1 {
		copyID = copies[0].ID
	} else {
		log.Printf("sub %s: dead-letter copy failed: %v", s.cfg.Name, err)
	}
	dl := &DeadLetter{SourceID: id, CopyID: copyID, Reason: reason, Attempts: e.rec.Attempt, At: now, State: "dead"}
	s.deadLetters[id] = dl
	v, _ := json.Marshal(dl)
	b.Set(store.Key(store.TagDeadLetter, s.cfg.Name, id), v)
	s.finish(b, e, stateDeadLettered, reason)
}

// finish moves an entry to a terminal state and advances the floor.
func (s *Subscription) finish(b *store.Batch, e *entry, state, reason string) {
	s.releaseKey(e)
	e.rec.State = state
	if reason != "" {
		e.rec.Reason = reason
	}
	if e.queued {
		s.sched.remove(e)
	}
	delete(s.entries, e.p)
	sh := e.p.shard
	if e.p.offset < s.floors[sh] {
		b.Delete(s.stateKey(e.p))
		return
	}
	s.putRecord(b, e)
	s.terminal[sh][e.p.offset] = true
	s.advanceFloor(b, sh)
}

func (s *Subscription) advanceFloor(b *store.Batch, sh int) {
	f := s.floors[sh]
	start := f
	for s.terminal[sh][f] {
		delete(s.terminal[sh], f)
		b.Delete(s.stateKey(pos{sh, f}))
		f++
	}
	if f != start {
		s.floors[sh] = f
		b.Set(store.Key(store.TagFloor, s.cfg.Name, uint64(sh)), binary.BigEndian.AppendUint64(nil, f))
	}
}

// Floor returns the ack floor of a shard.
func (s *Subscription) Floor(shard int) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.floors[shard]
}

// Tick expires leases, releases due delays and wakes waiters.
func (s *Subscription) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.d.Clock.Now()
	b := s.d.Store.NewBatch()
	woke := false
	for {
		it, ok := s.leases.peek()
		if !ok || it.at.After(now) {
			break
		}
		s.leases.pop()
		e, ok := s.entries[it.p]
		if !ok || e.rec.State != stateLeased || e.rec.LeaseID != it.tag || !e.rec.Deadline.Equal(it.at) {
			continue
		}
		s.leaseOff(e)
		if e.rec.Notified != "" {
			// superseded or cancelled while leased: the lease ending is its end
			s.finish(b, e, e.rec.Notified, "")
			s.afterRelease(e)
			woke = true
			continue
		}
		e.rec.Expiries++
		s.d.Gate.Settle(s.cfg.Name, e.rec.Charged, e.rec.LeaseStart, nil)
		if e.rec.Expiries >= s.cfg.Settings.Failure.QuarantineAfter {
			s.exhaust(b, e, "consumer crash loop", now)
		} else {
			s.failAttempt(b, e, "lease expired", now)
		}
		s.afterRelease(e)
		woke = true
	}
	for {
		it, ok := s.delays.peek()
		if !ok || it.at.After(now) {
			break
		}
		s.delays.pop()
		e, ok := s.entries[it.p]
		if !ok || e.rec.State != stateDelayed || !e.rec.NotBefore.Equal(it.at) {
			continue
		}
		e.rec.State = statePending
		e.rec.Eligible = it.at
		s.putRecord(b, e)
		s.enqueue(e)
		woke = true
	}
	if !s.gateRetry.IsZero() && !now.Before(s.gateRetry) {
		s.gateRetry = time.Time{}
		woke = true
	}
	if s.pausesExpired(now) {
		woke = true
	}
	if !b.Empty() {
		if err := b.Commit(); err != nil {
			log.Printf("sub %s: tick commit failed: %v", s.cfg.Name, err)
		}
	} else {
		b.Commit()
	}
	if woke {
		s.notify()
	}
}

// UsageTotals returns reported usage summed by (field, fairness value).
func (s *Subscription) UsageTotals() map[[2]string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[[2]string]float64, len(s.usage))
	for k, v := range s.usage {
		out[k] = v
	}
	return out
}

// Counts returns the number of messages in each non-terminal state, plus dead letters.
func (s *Subscription) Counts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := map[string]int{statePending: 0, stateDelayed: 0, stateLeased: 0, stateHeld: 0, stateDeadLettered: 0}
	for _, e := range s.entries {
		c[e.rec.State]++
	}
	for _, dl := range s.deadLetters {
		if dl.State == "dead" {
			c[stateDeadLettered]++
		}
	}
	return c
}

// DeadLetters lists messages currently dead-lettered, oldest first.
func (s *Subscription) DeadLetters() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DeadLetter
	for _, dl := range s.deadLetters {
		if dl.State == "dead" {
			out = append(out, *dl)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

// Requeue returns dead-lettered messages to pending with a fresh attempt count.
func (s *Subscription) Requeue(ids []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.d.Clock.Now()
	b := s.d.Store.NewBatch()
	var added []*entry
	for _, id := range ids {
		dl, ok := s.deadLetters[id]
		if !ok || dl.State != "dead" || dl.CopyID == "" {
			continue
		}
		cp, ok, err := s.loadRef(dl.CopyID)
		if err != nil || !ok {
			continue
		}
		_, sh, off, err := topic.ParseID(id)
		if err != nil {
			continue
		}
		e := s.newEntry(cp, now)
		e.p = pos{sh, off}
		e.ref = dl.CopyID
		e.rec = record{State: statePending, Eligible: now, Ref: dl.CopyID}
		if cpj := cp.Headers["trib-checkpoint"]; cpj != "" {
			e.rec.Checkpoint = json.RawMessage(cpj)
		}
		delete(s.terminal[sh], off)
		s.putRecord(b, e)
		dl.State = "requeued"
		v, _ := json.Marshal(dl)
		b.Set(store.Key(store.TagDeadLetter, s.cfg.Name, id), v)
		added = append(added, e)
	}
	if len(added) == 0 {
		b.Commit()
		return 0
	}
	if err := b.Commit(); err != nil {
		return 0
	}
	for _, e := range added {
		s.track(e)
	}
	s.notify()
	return len(added)
}
