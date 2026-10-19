package sub

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/topic"
)

// M1: dead-lettering again after a requeue makes a fresh copy with the latest state.
func TestSecondDeadLetterAfterRequeueIsFresh(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"failure":{"max_attempts":1}}`)
	m := e.produce(msg(""))[0]
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "retry", Error: "first", Checkpoint: json.RawMessage(`{"v":1}`)})
	first := s.DeadLetters()[0].CopyID
	s.Requeue([]string{m.ID})
	d = s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "retry", Error: "second", Checkpoint: json.RawMessage(`{"v":2}`)})
	dl := s.DeadLetters()
	if len(dl) != 1 || dl[0].CopyID == first || dl[0].Reason != "second" {
		t.Fatalf("second dead letter reused the first copy: %+v (first %s)", dl, first)
	}
	cp, _, _ := s.loadRef(dl[0].CopyID)
	if cp.Headers["trib-checkpoint"] != `{"v":2}` {
		t.Fatalf("copy has stale checkpoint %q", cp.Headers["trib-checkpoint"])
	}
}

// M2: a JSON null checkpoint does not erase the stored one.
func TestNullCheckpointKeepsStored(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w1", 1)[0]
	s.Extend(d.Token, time.Minute, json.RawMessage(`{"step":3}`))
	s.Extend(d.Token, time.Minute, json.RawMessage(`null`))
	s.Nack(d.Token, NackRequest{Reason: "retry", Checkpoint: json.RawMessage(`null`)})
	e.pass(s, 2*time.Second)
	got := s.Receive("w2", 1)
	if len(got) != 1 || string(got[0].Checkpoint) != `{"step":3}` {
		t.Fatalf("%+v", got)
	}
}

// M3: a failed disk write stops the node instead of leaving memory and disk different.
func TestCommitFailureIsFatal(t *testing.T) {
	e := newEnv(t, 1)
	var fatal error
	d := e.deps()
	d.OnCommitError = func(err error) { fatal = err }
	s := e.sub("work-queue", nil, "")
	s.d.OnCommitError = d.OnCommitError
	e.produce(msg(""))
	dl := s.Receive("w", 1)[0]
	e.s.FailCommits(errors.New("disk full"))
	s.Ack(dl.Token, nil)
	if fatal == nil {
		t.Fatal("a failed commit was not reported as fatal")
	}
}

// M5: a throttle pause survives a restart.
func TestThrottlePauseSurvivesRestart(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""), msg(""))
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "throttled", RetryAfter: time.Minute, Scope: "subscription"})
	s2 := e.reopen()
	if n := len(s2.Receive("w", 5)); n != 0 {
		t.Fatalf("dispatched %d after restart during a throttle pause", n)
	}
	e.pass(s2, 61*time.Second)
	if n := len(s2.Receive("w", 5)); n != 2 {
		t.Fatalf("got %d after the pause", n)
	}
}

type settleRecorder struct {
	Unlimited
	settled int
}

func (r *settleRecorder) Settle(string, float64, time.Time, map[string]float64) { r.settled++ }

// M6: a superseded lease that ends by expiry still settles its quota charge.
func TestSupersededLeaseExpirySettles(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("agent-session", nil, "")
	rec := &settleRecorder{}
	s.d.Gate = rec
	e.produce(msg("u"))
	s.Receive("w", 1)
	e.produce(msg("u")) // supersedes the leased message
	e.pass(s, 6*time.Minute)
	if rec.settled != 1 {
		t.Fatalf("settled %d times, want 1", rec.settled)
	}
}

// M7: a non-extendable lease is never renewed, but a checkpoint can still be saved.
func TestNonExtendableLeaseNotRenewed(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"lease":{"extendable":false}}`)
	e.produce(msg(""))
	d := s.Receive("w", 1)[0]
	e.c.Advance(20 * time.Second)
	if st, err := s.Extend(d.Token, 0, json.RawMessage(`{"s":1}`)); err != nil || st != StatusOK {
		t.Fatalf("checkpoint save failed: %s %v", st, err)
	}
	if _, err := s.Extend(d.Token, time.Minute, nil); err != ErrNotExtendable {
		t.Fatalf("got %v", err)
	}
	e.pass(s, 11*time.Second) // 31 s after the grant: the original 30 s lease has ended
	e.pass(s, 2*time.Second)
	got := s.Receive("w2", 1)
	if len(got) != 1 || got[0].Attempt != 2 || string(got[0].Checkpoint) != `{"s":1}` {
		t.Fatalf("lease was renewed or checkpoint lost: %+v", got)
	}
}

// M8: after a restart, overdue messages are found in eligibility order across shards.
func TestOverdueOrderAfterRecovery(t *testing.T) {
	e := newEnv(t, 2)
	s := e.sub("llm-requests", map[string]string{"pool": "p"}, "")
	bg := func() topic.Input { return topic.Input{Priority: "background", Payload: json.RawMessage(`1`)} }
	old := e.produce(bg(), bg()) // shard 0 and shard 1, keyless round-robin
	e.c.Advance(5 * time.Minute)
	e.produce(bg(), bg())
	e.c.Advance(6 * time.Minute) // the first two are 11 min old: overdue
	s2 := e.reopen()
	_ = s
	got := map[string]bool{}
	for _, d := range s2.Receive("w", 2) {
		got[d.ID] = true
	}
	if !got[old[0].ID] || !got[old[1].ID] {
		t.Fatalf("first two deliveries %v, want both overdue messages %s %s", got, old[0].ID, old[1].ID)
	}
}

// M12: an operator can release a held message back to pending.
func TestReleaseHeldMessage(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"failure":{"on_exhausted":"hold","max_attempts":1}}`)
	m := e.produce(msg(""))[0]
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "retry"})
	if s.Counts()["held"] != 1 {
		t.Fatal("not held")
	}
	if n := s.Release([]string{m.ID}); n != 1 {
		t.Fatalf("released %d", n)
	}
	got := s.Receive("w", 1)
	if len(got) != 1 || got[0].ID != m.ID || s.Counts()["held"] != 0 {
		t.Fatalf("%+v %v", got, s.Counts())
	}
}
