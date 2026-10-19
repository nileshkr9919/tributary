// Package topic stores topics and their messages, and routes produced records to shards.
package topic

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/store"
)

// IdempotencyWindow is how long an idempotency key deduplicates produces.
const IdempotencyWindow = 15 * time.Minute

var (
	ErrExists   = errors.New("topic already exists")
	ErrNotFound = errors.New("topic not found")
	ErrShards   = errors.New("shards must be between 1 and 256")
	ErrBadID    = errors.New("malformed message id")
	ErrBadName  = errors.New("invalid name: use 1-200 characters, no '~'")
)

// Message is a stored record.
type Message struct {
	ID         string            `json:"id"`
	Topic      string            `json:"topic"`
	Shard      int               `json:"shard"`
	Offset     uint64            `json:"offset"`
	Key        string            `json:"key,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Payload    json.RawMessage   `json:"payload,omitempty"`
	Priority   string            `json:"priority,omitempty"`
	DeliverAt  time.Time         `json:"deliver_at,omitempty"`
	ProducedAt time.Time         `json:"produced_at"`
}

// Input is a record to produce.
type Input struct {
	Key            string
	Headers        map[string]string
	Payload        json.RawMessage
	Priority       string
	DeliverAt      time.Time
	IdempotencyKey string
}

type topicMeta struct {
	Name   string `json:"name"`
	Shards int    `json:"shards"`
}

type topicState struct {
	meta topicMeta
	next []uint64
	rr   int
}

type idemEntry struct {
	ID      string    `json:"id"`
	Expires time.Time `json:"expires"`
}

// Manager owns all topics.
type Manager struct {
	mu        sync.Mutex
	s         *store.Store
	c         clock.Clock
	topics    map[string]*topicState
	listeners []func([]Message)
}

// NewManager loads existing topics from s.
func NewManager(s *store.Store, c clock.Clock) (*Manager, error) {
	m := &Manager{s: s, c: c, topics: map[string]*topicState{}}
	err := s.Scan([]byte{store.TagTopic}, func(k, v []byte) error {
		var meta topicMeta
		if err := json.Unmarshal(v, &meta); err != nil {
			return err
		}
		ts := &topicState{meta: meta, next: make([]uint64, meta.Shards)}
		for sh := 0; sh < meta.Shards; sh++ {
			if b, ok, err := s.Get(store.Key(store.TagOffset, meta.Name, uint64(sh))); err != nil {
				return err
			} else if ok {
				ts.next[sh] = binary.BigEndian.Uint64(b)
			}
		}
		m.topics[meta.Name] = ts
		return nil
	})
	return m, err
}

// Create makes a topic. shards 0 means 4.
func (m *Manager) Create(name string, shards int) error {
	if shards == 0 {
		shards = 4
	}
	if shards < 1 || shards > 256 {
		return ErrShards
	}
	if !ValidName(name) {
		return fmt.Errorf("%w: topic %q", ErrBadName, name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[name]; ok {
		return ErrExists
	}
	meta := topicMeta{Name: name, Shards: shards}
	b := m.s.NewBatch()
	v, _ := json.Marshal(meta)
	b.Set(store.Key(store.TagTopic, name), v)
	if err := b.Commit(); err != nil {
		return err
	}
	m.topics[name] = &topicState{meta: meta, next: make([]uint64, shards)}
	return nil
}

// Exists reports whether a topic exists.
func (m *Manager) Exists(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.topics[name]
	return ok
}

// Shards returns the topic's shard count.
func (m *Manager) Shards(name string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.topics[name]
	if !ok {
		return 0, ErrNotFound
	}
	return t.meta.Shards, nil
}

// OnAppend registers a listener called, in order, after each produce commits.
func (m *Manager) OnAppend(fn func([]Message)) {
	m.mu.Lock()
	m.listeners = append(m.listeners, fn)
	m.mu.Unlock()
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// Produce appends records atomically in one synced batch and returns them.
// Duplicates of a live idempotency key return the original message and append nothing.
func (m *Manager) Produce(topic string, in []Input) ([]Message, error) {
	m.mu.Lock()
	t, ok := m.topics[topic]
	if !ok {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	now := m.c.Now()
	b := m.s.NewBatch()
	out := make([]Message, len(in))
	var appended []Message
	next := append([]uint64(nil), t.next...)
	seenIdem := map[string]int{}
	for i, x := range in {
		if x.IdempotencyKey != "" {
			if j, dup := seenIdem[x.IdempotencyKey]; dup {
				out[i] = out[j]
				continue
			}
			if v, ok, err := m.s.Get(store.Key(store.TagIdem, topic, x.IdempotencyKey)); err != nil {
				m.mu.Unlock()
				return nil, err
			} else if ok {
				var e idemEntry
				if json.Unmarshal(v, &e) == nil && now.Before(e.Expires) {
					if _, sh, off, err := ParseID(e.ID); err == nil {
						if orig, ok, _ := m.getLocked(topic, sh, off); ok {
							out[i] = orig
							seenIdem[x.IdempotencyKey] = i
							continue
						}
					}
				}
			}
		}
		var shard int
		switch {
		case x.Key != "":
			shard = int(hash(x.Key) % uint64(t.meta.Shards))
		case x.IdempotencyKey != "":
			shard = int(hash(x.IdempotencyKey) % uint64(t.meta.Shards))
		default:
			shard = t.rr % t.meta.Shards
			t.rr++
		}
		off := next[shard]
		next[shard]++
		msg := Message{
			ID: FormatID(topic, shard, off), Topic: topic, Shard: shard, Offset: off,
			Key: x.Key, Headers: x.Headers, Payload: x.Payload, Priority: x.Priority,
			DeliverAt: x.DeliverAt, ProducedAt: now,
		}
		v, _ := json.Marshal(msg)
		b.Set(store.Key(store.TagMessage, topic, uint64(shard), off), v)
		if x.IdempotencyKey != "" {
			e, _ := json.Marshal(idemEntry{ID: msg.ID, Expires: now.Add(IdempotencyWindow)})
			b.Set(store.Key(store.TagIdem, topic, x.IdempotencyKey), e)
			seenIdem[x.IdempotencyKey] = i
		}
		out[i] = msg
		appended = append(appended, msg)
	}
	for sh := range next {
		if next[sh] != t.next[sh] {
			b.Set(store.Key(store.TagOffset, topic, uint64(sh)), binary.BigEndian.AppendUint64(nil, next[sh]))
		}
	}
	if err := b.Commit(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	t.next = next
	listeners := make([]func([]Message), len(m.listeners))
	copy(listeners, m.listeners)
	m.mu.Unlock()
	if len(appended) > 0 {
		for _, fn := range listeners {
			fn(appended)
		}
	}
	return out, nil
}

// Get returns one message.
func (m *Manager) Get(topic string, shard int, offset uint64) (Message, bool, error) {
	return m.getLocked(topic, shard, offset)
}

func (m *Manager) getLocked(topic string, shard int, offset uint64) (Message, bool, error) {
	v, ok, err := m.s.Get(store.Key(store.TagMessage, topic, uint64(shard), offset))
	if err != nil || !ok {
		return Message{}, ok, err
	}
	var msg Message
	err = json.Unmarshal(v, &msg)
	return msg, err == nil, err
}

// Scan calls fn for messages in a shard from offset from, in order.
func (m *Manager) Scan(topic string, shard int, from uint64, fn func(Message) error) error {
	prefix := store.Prefix(store.TagMessage, topic, uint64(shard))
	start := store.Key(store.TagMessage, topic, uint64(shard), from)
	return m.s.ScanFrom(start, prefix, func(k, v []byte) error {
		var msg Message
		if err := json.Unmarshal(v, &msg); err != nil {
			return err
		}
		return fn(msg)
	})
}

// DeleteBelow removes a shard's messages with offsets below off, in batch b.
func (m *Manager) DeleteBelow(b *store.Batch, topic string, shard int, from, below uint64) {
	for o := from; o < below; o++ {
		b.Delete(store.Key(store.TagMessage, topic, uint64(shard), o))
	}
}

// ValidName reports whether a topic or subscription name is acceptable. '~' separates the
// parts of message IDs, so it may not appear in names.
func ValidName(name string) bool {
	return name != "" && len(name) <= 200 && !strings.ContainsAny(name, "~/")
}

// FormatID encodes a message ID.
func FormatID(topic string, shard int, offset uint64) string {
	return topic + "~" + strconv.Itoa(shard) + "~" + strconv.FormatUint(offset, 10)
}

// ParseID decodes a message ID.
func ParseID(id string) (string, int, uint64, error) {
	i := strings.LastIndex(id, "~")
	if i <= 0 {
		return "", 0, 0, ErrBadID
	}
	j := strings.LastIndex(id[:i], "~")
	if j <= 0 {
		return "", 0, 0, ErrBadID
	}
	sh, err1 := strconv.Atoi(id[j+1 : i])
	off, err2 := strconv.ParseUint(id[i+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return "", 0, 0, ErrBadID
	}
	return id[:j], sh, off, nil
}
