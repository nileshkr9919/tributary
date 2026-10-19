package sub

import (
	"testing"
	"time"
)

func TestThrottledUsesNoAttempt(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	for i := 0; i < 100; i++ {
		ds := s.Receive("w", 1)
		if len(ds) != 1 {
			t.Fatalf("round %d: no delivery", i)
		}
		if ds[0].Attempt != 1 {
			t.Fatalf("round %d: attempt %d", i, ds[0].Attempt)
		}
		s.Nack(ds[0].Token, NackRequest{Reason: "throttled", RetryAfter: 100 * time.Millisecond})
		e.pass(s, 101*time.Millisecond)
	}
	if len(s.DeadLetters()) != 0 {
		t.Fatal("throttled message dead-lettered")
	}
}

func TestThrottledRetryAfterIsClamped(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "throttled", RetryAfter: time.Millisecond})
	e.pass(s, 50*time.Millisecond)
	if len(s.Receive("w", 1)) != 0 {
		t.Fatal("retry_after below 100ms was not raised to the minimum")
	}
	e.pass(s, 60*time.Millisecond)
	if len(s.Receive("w", 1)) != 1 {
		t.Fatal("not redelivered after 100ms")
	}
}

func TestThrottledSubscriptionScopePausesAll(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""), msg(""))
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "throttled", RetryAfter: 5 * time.Second, Scope: "subscription"})
	e.pass(s, 4*time.Second)
	if n := len(s.Receive("w", 10)); n != 0 {
		t.Fatalf("dispatched %d during subscription throttle", n)
	}
	e.pass(s, 2*time.Second)
	if n := len(s.Receive("w", 10)); n != 2 {
		t.Fatalf("got %d after throttle", n)
	}
}

func TestThrottledPastMaxThrottledCountsAttempts(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"failure":{"max_throttled":"1s","max_attempts":2}}`)
	e.produce(msg(""))
	for i := 0; i < 30; i++ {
		e.pass(s, 20*time.Minute)
		ds := s.Receive("w", 1)
		if len(ds) == 0 {
			break
		}
		s.Nack(ds[0].Token, NackRequest{Reason: "throttled", RetryAfter: 500 * time.Millisecond})
	}
	if len(s.DeadLetters()) != 1 {
		t.Fatal("endless throttling never reached on_exhausted")
	}
}

func TestPermanentDeadLettersImmediately(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "permanent", Error: "bad input"})
	dl := s.DeadLetters()
	if len(dl) != 1 || dl[0].Reason != "bad input" || dl[0].Attempts != 1 {
		t.Fatalf("%+v", dl)
	}
}

func TestMaxCostDeadLetters(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"failure":{"max_cost":100}}`)
	e.produce(msg(""))
	for i := 0; i < 2; i++ {
		e.pass(s, 20*time.Minute)
		d := s.Receive("w", 1)
		if len(d) != 1 {
			t.Fatalf("round %d", i)
		}
		s.Nack(d[0].Token, NackRequest{Reason: "retry", Usage: map[string]float64{"cost": 60}})
	}
	dl := s.DeadLetters()
	if len(dl) != 1 || dl[0].Reason != "max cost exceeded" {
		t.Fatalf("%+v", dl)
	}
}
