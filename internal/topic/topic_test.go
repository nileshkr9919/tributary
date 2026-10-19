package topic

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/store"
)

func newMgr(t *testing.T, dir string, c clock.Clock) (*Manager, *store.Store) {
	t.Helper()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(s, c)
	if err != nil {
		t.Fatal(err)
	}
	return m, s
}

func in(key string) Input { return Input{Key: key, Payload: json.RawMessage(`{"x":1}`)} }

func TestProduceAssignsSequentialOffsetsPerShard(t *testing.T) {
	m, s := newMgr(t, t.TempDir(), clock.NewFake(time.Unix(0, 0)))
	defer s.Close()
	if err := m.Create("t", 0); err != nil {
		t.Fatal(err)
	}
	msgs, err := m.Produce("t", []Input{in("k"), in("k"), in("k")})
	if err != nil {
		t.Fatal(err)
	}
	for i, msg := range msgs {
		if msg.Offset != uint64(i) || msg.Shard != msgs[0].Shard {
			t.Fatalf("msg %d: shard %d offset %d", i, msg.Shard, msg.Offset)
		}
	}
	if n, _ := m.Shards("t"); n != 4 {
		t.Fatalf("default shards %d", n)
	}
}

func TestCreateTwiceIsErrExists(t *testing.T) {
	m, s := newMgr(t, t.TempDir(), clock.Real())
	defer s.Close()
	m.Create("t", 2)
	if err := m.Create("t", 2); err != ErrExists {
		t.Fatalf("got %v", err)
	}
}

func TestKeyRoutingIsStable(t *testing.T) {
	dir := t.TempDir()
	m, s := newMgr(t, dir, clock.Real())
	m.Create("t", 8)
	a, _ := m.Produce("t", []Input{in("user-42")})
	s.Close()
	m2, s2 := newMgr(t, dir, clock.Real())
	defer s2.Close()
	b, _ := m2.Produce("t", []Input{in("user-42")})
	if a[0].Shard != b[0].Shard {
		t.Fatal("routing changed across reopen")
	}
}

func TestIdempotencyWithinWindow(t *testing.T) {
	c := clock.NewFake(time.Unix(1000, 0))
	m, s := newMgr(t, t.TempDir(), c)
	defer s.Close()
	m.Create("t", 1)
	x := in("")
	x.IdempotencyKey = "req-1"
	a, _ := m.Produce("t", []Input{x})
	c.Advance(14 * time.Minute)
	b, _ := m.Produce("t", []Input{x})
	if a[0].ID != b[0].ID {
		t.Fatalf("duplicate appended: %s vs %s", a[0].ID, b[0].ID)
	}
	n := 0
	m.Scan("t", 0, 0, func(Message) error { n++; return nil })
	if n != 1 {
		t.Fatalf("shard holds %d", n)
	}
}

func TestIdempotencyExpires(t *testing.T) {
	c := clock.NewFake(time.Unix(1000, 0))
	m, s := newMgr(t, t.TempDir(), c)
	defer s.Close()
	m.Create("t", 1)
	x := in("")
	x.IdempotencyKey = "req-1"
	a, _ := m.Produce("t", []Input{x})
	c.Advance(16 * time.Minute)
	b, _ := m.Produce("t", []Input{x})
	if a[0].ID == b[0].ID {
		t.Fatal("expired key still deduplicated")
	}
}

func TestProduceSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	m, s := newMgr(t, dir, clock.Real())
	m.Create("t", 1)
	m.Produce("t", []Input{in("a"), in("a"), in("a")})
	s.Close()
	m2, s2 := newMgr(t, dir, clock.Real())
	defer s2.Close()
	got, ok, err := m2.Get("t", 0, 1)
	if err != nil || !ok || string(got.Payload) != `{"x":1}` {
		t.Fatalf("get: %v %v %s", ok, err, got.Payload)
	}
	next, _ := m2.Produce("t", []Input{in("a")})
	if next[0].Offset != 3 {
		t.Fatalf("next offset %d", next[0].Offset)
	}
}

func TestOnAppendCalledAfterCommit(t *testing.T) {
	m, s := newMgr(t, t.TempDir(), clock.Real())
	defer s.Close()
	m.Create("t", 1)
	var seen []Message
	m.OnAppend(func(ms []Message) {
		for _, x := range ms {
			if _, ok, _ := m.Get(x.Topic, x.Shard, x.Offset); !ok {
				t.Error("listener ran before commit")
			}
		}
		seen = append(seen, ms...)
	})
	m.Produce("t", []Input{in("a"), in("b")})
	if len(seen) != 2 {
		t.Fatalf("listener saw %d", len(seen))
	}
}

func TestIDRoundTrip(t *testing.T) {
	id := FormatID("orders.v1", 3, 42)
	tp, sh, off, err := ParseID(id)
	if err != nil || tp != "orders.v1" || sh != 3 || off != 42 {
		t.Fatalf("%s -> %s %d %d %v", id, tp, sh, off, err)
	}
}
