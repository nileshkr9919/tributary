package sub

import (
	"testing"
	"time"
)

func TestPerKeyOneInFlight(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"ordering":"per-key"}`)
	e.produce(msg("u1"), msg("u1"), msg("u2"))
	ds := s.Receive("w", 10)
	if len(ds) != 2 {
		t.Fatalf("got %d, want one per key", len(ds))
	}
	var u1 Delivery
	for _, d := range ds {
		if d.Key == "u1" {
			u1 = d
		}
	}
	s.Ack(u1.Token, nil)
	next := s.Receive("w", 10)
	if len(next) != 1 || next[0].Key != "u1" {
		t.Fatalf("got %+v", next)
	}
}

func TestSupersedeCoversDelayed(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("agent-session", nil, "")
	a := e.produce(msg("u1"))[0]
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "retry"}) // A goes into back-off
	b := e.produce(msg("u1"))[0]
	got := []string{}
	for i := 0; i < 5; i++ {
		e.pass(s, 10*time.Minute)
		for _, x := range s.Receive("w", 10) {
			got = append(got, x.ID)
			s.Ack(x.Token, nil)
		}
	}
	if len(got) != 1 || got[0] != b.ID {
		t.Fatalf("delivered %v; want only %s (A=%s superseded)", got, b.ID, a.ID)
	}
}

func TestSupersedeNotifiesHolder(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("agent-session", nil, "")
	e.produce(msg("u1"))
	a := s.Receive("w", 1)[0]
	b := e.produce(msg("u1"))[0]
	if st, _ := s.Extend(a.Token, time.Minute, nil); st != StatusSuperseded {
		t.Fatalf("extend got %s", st)
	}
	if len(s.Receive("w", 1)) != 0 {
		t.Fatal("B delivered while A's lease is still held")
	}
	if st, _ := s.Ack(a.Token, nil); st != StatusSuperseded {
		t.Fatalf("ack got %s", st)
	}
	nb := s.Receive("w", 1)
	if len(nb) != 1 || nb[0].ID != b.ID {
		t.Fatalf("got %+v", nb)
	}
	if s.Counts()["pending"] != 0 {
		t.Fatal("A redelivered")
	}
}

func TestSupersedeCoversHeld(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("agent-session", nil, `{"failure":{"on_exhausted":"hold","max_attempts":1}}`)
	e.produce(msg("u1"))
	d := s.Receive("w", 1)[0]
	s.Nack(d.Token, NackRequest{Reason: "retry"})
	if s.Counts()["held"] != 1 {
		t.Fatalf("counts %v", s.Counts())
	}
	b := e.produce(msg("u1"))[0]
	got := s.Receive("w", 1)
	if len(got) != 1 || got[0].ID != b.ID || s.Counts()["held"] != 0 {
		t.Fatalf("got %+v counts %v", got, s.Counts())
	}
}

func TestCancelByID(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	m := e.produce(msg(""), msg(""))
	if n := s.Cancel(m[0].ID); n != 1 {
		t.Fatalf("cancelled %d", n)
	}
	ds := s.Receive("w", 10)
	if len(ds) != 1 || ds[0].ID != m[1].ID {
		t.Fatalf("got %+v", ds)
	}
}

func TestCancelLeasedNotifiesHolder(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	m := e.produce(msg(""))[0]
	d := s.Receive("w", 1)[0]
	s.Cancel(m.ID)
	if st, _ := s.Extend(d.Token, time.Minute, nil); st != StatusCancelled {
		t.Fatalf("got %s", st)
	}
	e.pass(s, 2*time.Minute)
	e.pass(s, 20*time.Minute)
	if len(s.Receive("w", 1)) != 0 {
		t.Fatal("cancelled message redelivered")
	}
}

func TestCancelByKey(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg("a"), msg("a"), msg("b"))
	if n := s.CancelKey("a"); n != 2 {
		t.Fatalf("cancelled %d", n)
	}
	if ds := s.Receive("w", 10); len(ds) != 1 || ds[0].Key != "b" {
		t.Fatalf("%+v", ds)
	}
}

func TestPauseFairnessValue(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("llm-requests", map[string]string{"pool": "p"}, "")
	e.produce(hdr("", map[string]string{"tenant": "bulk"}), hdr("", map[string]string{"tenant": "users"}))
	s.Pause("bulk")
	ds := s.Receive("w", 10)
	if len(ds) != 1 || ds[0].Headers["tenant"] != "users" {
		t.Fatalf("%+v", ds)
	}
	s.Resume("bulk")
	if ds := s.Receive("w", 10); len(ds) != 1 || ds[0].Headers["tenant"] != "bulk" {
		t.Fatalf("after resume %+v", ds)
	}
}

func TestPauseWholeSubscriptionAndExtendReportsPaused(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""), msg(""))
	d := s.Receive("w", 1)[0]
	s.Pause("")
	if st, _ := s.Extend(d.Token, time.Minute, nil); st != StatusPaused {
		t.Fatalf("got %s", st)
	}
	if len(s.Receive("w", 5)) != 0 {
		t.Fatal("dispatched while paused")
	}
}

func TestPauseSurvivesRestart(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	s.Pause("")
	s2 := e.reopen()
	if len(s2.Receive("w", 1)) != 0 {
		t.Fatal("pause lost on restart")
	}
	s2.Resume("")
	if len(s2.Receive("w", 1)) != 1 {
		t.Fatal("resume failed")
	}
}
