package sub

import (
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/topic"
)

func TestReceiveLeasesAndAckFloorAdvances(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""), msg(""), msg(""))
	ds := s.Receive("w1", 10)
	if len(ds) != 3 {
		t.Fatalf("got %d deliveries", len(ds))
	}
	if ds[0].Attempt != 1 || ds[0].OperationID != ds[0].ID {
		t.Fatalf("delivery %+v", ds[0])
	}
	for _, d := range ds {
		if st, err := s.Ack(d.Token, nil); err != nil || st != StatusOK {
			t.Fatalf("ack %v %v", st, err)
		}
	}
	if c := s.Counts(); c["leased"] != 0 || c["pending"] != 0 {
		t.Fatalf("counts %v", c)
	}
	n := 0
	e.s.Scan(store.Prefix(store.TagState, "s"), func(k, v []byte) error { n++; return nil })
	if n != 0 {
		t.Fatalf("%d state records remain after floor advanced", n)
	}
}

func TestLeaseExpiryRedeliversWithAttempt(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	if len(s.Receive("w1", 1)) != 1 {
		t.Fatal("no delivery")
	}
	e.pass(s, 31*time.Second)
	if got := s.Receive("w1", 1); len(got) != 0 {
		t.Fatal("redelivered before back-off")
	}
	e.pass(s, time.Second)
	ds := s.Receive("w2", 1)
	if len(ds) != 1 || ds[0].Attempt != 2 || ds[0].PreviousHolder != "w1" {
		t.Fatalf("got %+v", ds)
	}
}

func TestStaleTokenRejected(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	old := s.Receive("w1", 1)[0]
	e.pass(s, 31*time.Second)
	e.pass(s, time.Second)
	fresh := s.Receive("w2", 1)[0]
	if st, _ := s.Ack(old.Token, nil); st != StatusLost {
		t.Fatalf("stale ack got %s", st)
	}
	if s.Counts()["leased"] != 1 {
		t.Fatal("stale ack changed the redelivered copy")
	}
	if st, _ := s.Ack(fresh.Token, nil); st != StatusOK {
		t.Fatal(st)
	}
	if st, _ := s.Ack("garbage", nil); st != StatusLost {
		t.Fatal("garbage token not lost")
	}
}

func TestQuarantineAfterThreeExpiries(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	for i := 0; i < 3; i++ {
		e.pass(s, 20*time.Minute) // past any back-off
		if len(s.Receive("w", 1)) != 1 {
			t.Fatalf("round %d: no delivery", i)
		}
		e.pass(s, 31*time.Second)
	}
	dl := s.DeadLetters()
	if len(dl) != 1 || dl[0].Reason != "consumer crash loop" {
		t.Fatalf("dead letters %+v", dl)
	}
}

func TestMaxAttemptsDeadLetters(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"failure":{"max_attempts":3}}`)
	e.produce(msg("k"))
	for i := 0; i < 3; i++ {
		e.pass(s, 20*time.Minute)
		ds := s.Receive("w", 1)
		if len(ds) != 1 {
			t.Fatalf("round %d", i)
		}
		s.Nack(ds[0].Token, NackRequest{Reason: "retry", Error: "boom"})
	}
	dl := s.DeadLetters()
	if len(dl) != 1 || dl[0].Attempts != 3 || dl[0].Reason != "boom" {
		t.Fatalf("%+v", dl)
	}
	var copies []topic.Message
	e.tm.Scan("t.dlq.s", 0, 0, func(m topic.Message) error { copies = append(copies, m); return nil })
	n := 0
	for sh := 0; sh < 1; sh++ {
		e.tm.Scan("t.dlq.s", sh, 0, func(m topic.Message) error {
			n++
			if m.Headers["trib-attempts"] != "3" || m.Headers["trib-failure-reason"] != "boom" || m.Headers["trib-operation-id"] == "" {
				t.Fatalf("headers %v", m.Headers)
			}
			return nil
		})
	}
	if n != 1 {
		t.Fatalf("%d dead-letter copies", n)
	}
}

func TestRequeueOnce(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"failure":{"max_attempts":1}}`)
	m := e.produce(msg(""))[0]
	ds := s.Receive("w", 1)
	s.Nack(ds[0].Token, NackRequest{Reason: "retry"})
	if n := s.Requeue([]string{m.ID}); n != 1 {
		t.Fatalf("requeued %d", n)
	}
	if n := s.Requeue([]string{m.ID}); n != 0 {
		t.Fatal("second requeue had effect")
	}
	again := s.Receive("w", 5)
	if len(again) != 1 || again[0].Attempt != 1 || again[0].OperationID != m.ID {
		t.Fatalf("%+v", again)
	}
}

func TestCheckpointReturnedOnRedelivery(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w1", 1)[0]
	if st, err := s.Extend(d.Token, 30*time.Second, []byte(`{"step":2}`)); err != nil || st != StatusOK {
		t.Fatal(st, err)
	}
	e.pass(s, 61*time.Second)
	e.pass(s, 2*time.Second)
	d2 := s.Receive("w2", 1)
	if len(d2) != 1 || string(d2[0].Checkpoint) != `{"step":2}` || d2[0].PreviousHolder != "w1" {
		t.Fatalf("%+v", d2)
	}
}

func TestCheckpointTooLarge(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w1", 1)[0]
	big := make([]byte, 70*1024)
	for i := range big {
		big[i] = 'a'
	}
	if _, err := s.Extend(d.Token, time.Second, append(append([]byte(`"`), big...), '"')); err != ErrCheckpointTooLarge {
		t.Fatalf("got %v", err)
	}
}

func TestRestartPreservesLeases(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""), msg(""))
	d := s.Receive("w1", 1)[0]
	s2 := e.reopen()
	if st, err := s2.Ack(d.Token, nil); err != nil || st != StatusOK {
		t.Fatalf("ack after restart: %s %v", st, err)
	}
	if got := s2.Receive("w1", 5); len(got) != 1 {
		t.Fatalf("pending after restart: %d", len(got))
	}
}

func TestRestartRedeliversExpiredLeaseWithCheckpoint(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	e.produce(msg(""))
	d := s.Receive("w1", 1)[0]
	s.Extend(d.Token, 30*time.Second, []byte(`{"step":1}`))
	s2 := e.reopen()
	e.pass(s2, 61*time.Second)
	e.pass(s2, 2*time.Second)
	got := s2.Receive("w2", 1)
	if len(got) != 1 || got[0].Attempt != 2 || string(got[0].Checkpoint) != `{"step":1}` {
		t.Fatalf("%+v", got)
	}
}

func TestDelayedMessageNotDispatchedEarly(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, "")
	in := msg("")
	in.DeliverAt = e.c.Now().Add(10 * time.Second)
	e.produce(in)
	if len(s.Receive("w", 1)) != 0 {
		t.Fatal("delivered early")
	}
	e.pass(s, 10*time.Second)
	if len(s.Receive("w", 1)) != 1 {
		t.Fatal("not delivered when due")
	}
}

func TestMaxInFlight(t *testing.T) {
	e := newEnv(t, 1)
	s := e.sub("work-queue", nil, `{"max_in_flight":2}`)
	e.produce(msg(""), msg(""), msg(""))
	if got := s.Receive("w", 10); len(got) != 2 {
		t.Fatalf("got %d", len(got))
	}
}
