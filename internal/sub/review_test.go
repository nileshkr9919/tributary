package sub

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/topic"
)

// C1: an aged lower-band message joins the band above; it must not take over that band's slot.
func TestAgedEntriesDoNotStarveTopBand(t *testing.T) {
	s := newScheduler(llmSettings(t, ""))
	for i := 0; i < 100; i++ {
		s.push(ent("standard", "a", t0))
	}
	now := t0.Add(60 * time.Second) // past standard's 30 s max wait, under 10 min total
	for i := 0; i < 10; i++ {
		s.push(ent("interactive", "a", now))
	}
	inter := 0
	for i := 0; i < 50; i++ {
		if take(s, now, all).priority == "interactive" {
			inter++
		}
	}
	if inter < 10 {
		t.Fatalf("interactive got %d of 50 picks while standard messages aged", inter)
	}
}

// gate admitting only one priority band
type bandGate struct{ band string }

func (g bandGate) Reserve(_ string, m topic.Message) (float64, bool, time.Time) {
	return 0, m.Priority == g.band, time.Now().Add(time.Second)
}
func (bandGate) Settle(string, float64, time.Time, map[string]float64) {}

// C2: a denied overdue or lower-band message must not stop Receive from trying the top band.
func TestGateDenialDoesNotBlockOtherBands(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("llm-requests", map[string]string{"pool": "p"}, "")
	s.d.Gate = bandGate{"interactive"}
	e.produce(topic.Input{Priority: "background", Payload: json.RawMessage(`1`)})
	e.c.Advance(11 * time.Minute) // background is now overdue
	e.produce(topic.Input{Priority: "interactive", Payload: json.RawMessage(`2`)})
	for i := 0; i < 3; i++ {
		ds := s.Receive("w", 5)
		if len(ds) == 1 && ds[0].Priority == "interactive" {
			return
		}
	}
	t.Fatal("interactive message never delivered behind a gated overdue message")
}

// I1: per-key ordering holds while the key's head is in back-off.
func TestPerKeyHeadInBackoffBlocksKey(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"ordering":"per-key"}`)
	a := e.produce(msg("k"), msg("k"))[0]
	d := s.Receive("w", 5)
	if len(d) != 1 || d[0].ID != a.ID {
		t.Fatalf("%+v", d)
	}
	s.Nack(d[0].Token, NackRequest{Reason: "retry"})
	if got := s.Receive("w", 5); len(got) != 0 {
		t.Fatalf("delivered %s while the key's head is in back-off", got[0].ID)
	}
	e.pass(s, 2*time.Second)
	got := s.Receive("w", 5)
	if len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("after back-off got %+v, want the head again", got)
	}
}

// I1: a lease expiry also keeps the key.
func TestPerKeyHeadExpiredBlocksKey(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"ordering":"per-key"}`)
	e.produce(msg("k"), msg("k"))
	s.Receive("w", 5)
	e.pass(s, 31*time.Second) // lease expired, head in back-off
	if got := s.Receive("w", 5); len(got) != 0 {
		t.Fatal("second message for the key delivered while the head is in back-off")
	}
}

// I2: arrivals delivered out of offset order are applied in order.
func TestOutOfOrderArrivalsAppliedInOrder(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("agent-session", nil, "")
	now := e.c.Now()
	a := topic.Message{ID: topic.FormatID("t", 0, 0), Topic: "t", Shard: 0, Offset: 0, Key: "u", ProducedAt: now}
	b := topic.Message{ID: topic.FormatID("t", 0, 1), Topic: "t", Shard: 0, Offset: 1, Key: "u", ProducedAt: now}
	s.OnAppend([]topic.Message{b})
	s.OnAppend([]topic.Message{a})
	var got []string
	for i := 0; i < 3; i++ {
		for _, d := range s.Receive("w", 5) {
			got = append(got, d.ID)
			s.Ack(d.Token, nil)
		}
	}
	if len(got) != 1 || got[0] != b.ID {
		t.Fatalf("delivered %v, want only the newer %s", got, b.ID)
	}
}

// I3: a duplicate arrival is ignored, and a subscription tracks appends without outside wiring.
func TestSubscriptionListensToItsTopic(t *testing.T) {
	e := newEnv(t, 1)
	st := e.sub("work-queue", nil, "").Settings()
	s2, err := New("s2", "t", st, e.deps())
	if err != nil {
		t.Fatal(err)
	}
	m := e.produce(msg(""))[0]
	s2.OnAppend([]topic.Message{m}) // duplicate of the automatic arrival
	if got := s2.Receive("w", 5); len(got) != 1 {
		t.Fatalf("got %d deliveries, want exactly 1", len(got))
	}
}

// I5: quota_exhausted that pauses nothing must not redeliver immediately.
func TestQuotaExhaustedWithoutPauseBacksOff(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "quota_exhausted"}) // no pool bound, no retry_after
	if len(s.Receive("w", 1)) != 0 {
		t.Fatal("redelivered immediately: quota_exhausted with no effective pause spins")
	}
}

// I8: removed entries do not accumulate in a queue blocked at its head.
func TestFifoCompactsRemovedEntries(t *testing.T) {
	f := &fifo{}
	head := &entry{queued: true}
	f.push(head)
	var rest []*entry
	for i := 0; i < 1000; i++ {
		e := &entry{queued: true}
		f.push(e)
		rest = append(rest, e)
	}
	for _, e := range rest {
		e.queued = false
	}
	f.first(func(*entry) bool { return false }, nil)
	if len(f.q) > 10 {
		t.Fatalf("queue still holds %d entries after 1000 removals", len(f.q))
	}
}
