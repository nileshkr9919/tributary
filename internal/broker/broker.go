// Package broker wires topics, quota pools and subscriptions into one running node.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/quota"
	"github.com/nileshkr9919/tributary/internal/settings"
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/sub"
	"github.com/nileshkr9919/tributary/internal/topic"
)

// MaxPayload is the largest accepted message payload.
const MaxPayload = 8 << 20

// MaxWait caps a long-poll receive.
const MaxWait = 30 * time.Second

var (
	ErrSubExists     = errors.New("subscription already exists")
	ErrSubNotFound   = errors.New("subscription not found")
	ErrPoolNotFound  = errors.New("pool not found")
	ErrPayloadTooBig = errors.New("payload exceeds 8 MiB")
	ErrEmptyName     = errors.New("name is required")
	ErrBatchTooLarge = errors.New("at most 1000 messages per request")
)

// Counters holds metric counters.
type Counters struct {
	mu sync.Mutex
	CounterData
}

// CounterData is a copy of the counters.
type CounterData struct {
	Deliveries map[string]float64    // by subscription
	Nacks      map[[2]string]float64 // by subscription, reason
	WaitSum    map[[2]string]float64 // by subscription, band (seconds)
	WaitCount  map[[2]string]float64
	WaitBucket map[[3]string]float64 // by subscription, band, le
}

// WaitBuckets are the histogram bounds for dispatch wait, in seconds.
var WaitBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 600}

// Broker is one running node.
type Broker struct {
	mu      sync.RWMutex
	st      *store.Store
	c       clock.Clock
	Topics  *topic.Manager
	Pools   *quota.Registry
	subs    map[string]*sub.Subscription
	stop    chan struct{}
	wg      sync.WaitGroup
	Metrics *Counters
}

// Open opens a node's data directory and starts its ticker.
func Open(dir string, c clock.Clock) (*Broker, error) {
	st, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	b := &Broker{st: st, c: c, subs: map[string]*sub.Subscription{}, stop: make(chan struct{}),
		Metrics: &Counters{CounterData: newCounterData()}}
	if b.Topics, err = topic.NewManager(st, c); err != nil {
		st.Close()
		return nil, err
	}
	if b.Pools, err = quota.NewRegistry(st, c); err != nil {
		st.Close()
		return nil, err
	}
	names, err := sub.Names(st)
	if err != nil {
		st.Close()
		return nil, err
	}
	for _, n := range names {
		s, err := sub.Load(n, b.deps())
		if err != nil {
			st.Close()
			return nil, err
		}
		b.attach(s)
	}
	b.wg.Add(1)
	go b.loop()
	return b, nil
}

func (b *Broker) deps() sub.Deps {
	return sub.Deps{Store: b.st, Topics: b.Topics, Clock: b.c, Gate: b.Pools.Gate()}
}

func (b *Broker) attach(s *sub.Subscription) {
	st := s.Settings()
	top := ""
	if len(st.Priority) > 0 {
		top = st.Priority[0].Name
	}
	b.Pools.Bind(s.Name(), st.Pool, st.EstimateHeader, st.DefaultEstimate, top)
	b.subs[s.Name()] = s
}

func (b *Broker) loop() {
	defer b.wg.Done()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	lastFlush := time.Now()
	for {
		select {
		case <-b.stop:
			return
		case <-tick.C:
			b.mu.RLock()
			subs := make([]*sub.Subscription, 0, len(b.subs))
			for _, s := range b.subs {
				subs = append(subs, s)
			}
			b.mu.RUnlock()
			for _, s := range subs {
				s.Tick()
			}
			if time.Since(lastFlush) > time.Second {
				if err := b.Pools.Flush(); err != nil {
					log.Printf("pool flush: %v", err)
				}
				lastFlush = time.Now()
			}
		}
	}
}

// Close stops the ticker, persists pools and closes the store.
func (b *Broker) Close() error {
	select {
	case <-b.stop:
		return nil
	default:
	}
	close(b.stop)
	b.wg.Wait()
	b.Pools.Flush()
	return b.st.Close()
}

// CreateTopic creates a topic.
func (b *Broker) CreateTopic(name string, shards int) error {
	if name == "" {
		return ErrEmptyName
	}
	return b.Topics.Create(name, shards)
}

// Produce appends messages to a topic.
func (b *Broker) Produce(topicName string, in []topic.Input) ([]topic.Message, error) {
	if len(in) > 1000 {
		return nil, ErrBatchTooLarge
	}
	for _, x := range in {
		if len(x.Payload) > MaxPayload {
			return nil, ErrPayloadTooBig
		}
	}
	return b.Topics.Produce(topicName, in)
}

// CreateSubscription resolves settings and creates a subscription.
func (b *Broker) CreateSubscription(name, topicName, profile string, params map[string]string, overrides json.RawMessage) (*sub.Subscription, error) {
	if !topic.ValidName(name) {
		return nil, fmt.Errorf("%w: subscription %q", topic.ErrBadName, name)
	}
	st, err := settings.Resolve(profile, params, overrides)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[name]; ok {
		return nil, ErrSubExists
	}
	if !b.Topics.Exists(topicName) {
		return nil, topic.ErrNotFound
	}
	if st.Pool != "" && !b.Pools.Exists(st.Pool) {
		return nil, ErrPoolNotFound
	}
	s, err := sub.New(name, topicName, st, b.deps())
	if err != nil {
		return nil, err
	}
	b.attach(s)
	return s, nil
}

// Sub returns a subscription by name.
func (b *Broker) Sub(name string) (*sub.Subscription, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	s, ok := b.subs[name]
	if !ok {
		return nil, ErrSubNotFound
	}
	return s, nil
}

// Subs returns every subscription, sorted by name.
func (b *Broker) Subs() []*sub.Subscription {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*sub.Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// SubForToken returns the subscription a token belongs to, or nil.
func (b *Broker) SubForToken(tok string) *sub.Subscription {
	s, err := b.Sub(sub.TokenSubscription(tok))
	if err != nil {
		return nil
	}
	return s
}

// Receive long-polls a subscription for up to wait.
func (b *Broker) Receive(ctx context.Context, name, member string, max int, wait time.Duration) ([]sub.Delivery, error) {
	s, err := b.Sub(name)
	if err != nil {
		return nil, err
	}
	if wait > MaxWait {
		wait = MaxWait
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for {
		ch := s.Changed()
		if ds := s.Receive(member, max); len(ds) > 0 {
			b.record(name, ds)
			return ds, nil
		}
		if wait <= 0 {
			return nil, nil
		}
		select {
		case <-ch:
		case <-deadline.C:
			return nil, nil
		case <-ctx.Done():
			return nil, nil
		}
	}
}

func (b *Broker) record(name string, ds []sub.Delivery) {
	m := b.Metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Deliveries[name] += float64(len(ds))
	for _, d := range ds {
		band := d.Priority
		w := float64(d.WaitMs) / 1000
		k := [2]string{name, band}
		m.WaitSum[k] += w
		m.WaitCount[k]++
		for _, le := range WaitBuckets {
			if w <= le {
				m.WaitBucket[[3]string{name, band, BucketKey(le)}]++
			}
		}
	}
}

// RecordNack counts a nack by reason.
func (b *Broker) RecordNack(name, reason string) {
	if reason == "" {
		reason = "retry"
	}
	b.Metrics.mu.Lock()
	b.Metrics.Nacks[[2]string{name, reason}]++
	b.Metrics.mu.Unlock()
}

func newCounterData() CounterData {
	return CounterData{Deliveries: map[string]float64{}, Nacks: map[[2]string]float64{}, WaitSum: map[[2]string]float64{},
		WaitCount: map[[2]string]float64{}, WaitBucket: map[[3]string]float64{}}
}

// Snapshot copies the counters for rendering.
func (m *Counters) Snapshot() CounterData {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := newCounterData()
	for k, v := range m.Deliveries {
		cp.Deliveries[k] = v
	}
	for k, v := range m.Nacks {
		cp.Nacks[k] = v
	}
	for k, v := range m.WaitSum {
		cp.WaitSum[k] = v
	}
	for k, v := range m.WaitCount {
		cp.WaitCount[k] = v
	}
	for k, v := range m.WaitBucket {
		cp.WaitBucket[k] = v
	}
	return cp
}

// BucketKey formats a histogram bound as a label value.
func BucketKey(le float64) string { return strconv.FormatFloat(le, 'g', -1, 64) }
