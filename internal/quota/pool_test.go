package quota

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/settings"
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/sub"
	"github.com/nileshkr9919/tributary/internal/topic"
)

type env struct {
	c   *clock.Fake
	st  *store.Store
	tm  *topic.Manager
	reg *Registry
	s   *sub.Subscription
}

func setup(t *testing.T, perMinute, burst float64) *env {
	t.Helper()
	e := &env{c: clock.NewFake(time.Unix(1_700_000_000, 0))}
	var err error
	if e.st, err = store.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.st.Close() })
	e.tm, _ = topic.NewManager(e.st, e.c)
	e.tm.Create("llm", 1)
	if e.reg, err = NewRegistry(e.st, e.c); err != nil {
		t.Fatal(err)
	}
	if err := e.reg.Create("openai", "tokens", perMinute, burst); err != nil {
		t.Fatal(err)
	}
	st, _ := settings.Resolve("llm-requests", map[string]string{"pool": "openai"}, json.RawMessage(`{"failure":{"jitter":false}}`))
	e.reg.Bind("jobs", st.Pool, st.EstimateHeader, st.DefaultEstimate, "")
	if e.s, err = sub.New("jobs", "llm", st, sub.Deps{Store: e.st, Topics: e.tm, Clock: e.c, Gate: e.reg.Gate()}); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) produce(t *testing.T, estimates ...int) {
	var in []topic.Input
	for _, est := range estimates {
		in = append(in, topic.Input{Headers: map[string]string{"x-est-tokens": strconv.Itoa(est)}, Payload: json.RawMessage(`1`)})
	}
	if _, err := e.tm.Produce("llm", in); err != nil {
		t.Fatal(err)
	}
}

func TestTokenWeightedDispatch(t *testing.T) {
	e := setup(t, 10000, 1000)
	var ests []int
	for i := 0; i < 400; i++ {
		if i%2 == 0 {
			ests = append(ests, 100)
		} else {
			ests = append(ests, 900)
		}
	}
	e.produce(t, ests...)
	granted := 0.0
	for sec := 0; sec < 300; sec++ {
		for _, d := range e.s.Receive("w", 50) {
			est, _ := strconv.Atoi(d.Headers["x-est-tokens"])
			granted += float64(est)
			e.s.Ack(d.Token, map[string]float64{"tokens": float64(est)})
		}
		e.c.Advance(time.Second)
		e.s.Tick()
	}
	limit := (10000*5 + 1000) * 1.10
	if granted > limit || granted < 45000 {
		t.Fatalf("granted %.0f tokens in 5 minutes, want between 45000 and %.0f", granted, limit)
	}
}

func TestSettleCreatesDebt(t *testing.T) {
	e := setup(t, 600, 100) // 10 tokens per second
	e.produce(t, 100, 100)
	d := e.s.Receive("w", 1)
	if len(d) != 1 {
		t.Fatal("first message not granted")
	}
	e.s.Ack(d[0].Token, map[string]float64{"tokens": 600}) // 500 over estimate
	e.c.Advance(30 * time.Second)                          // refills 300: still in debt
	if len(e.s.Receive("w", 1)) != 0 {
		t.Fatal("dispatched while in debt")
	}
	if st := e.reg.Stats("openai"); st.Overshoot != 500 {
		t.Fatalf("overshoot %.0f", st.Overshoot)
	}
	e.c.Advance(60 * time.Second)
	if len(e.s.Receive("w", 1)) != 1 {
		t.Fatal("not dispatched after debt repaid")
	}
}

func TestLearnedRatio(t *testing.T) {
	e := setup(t, 1e9, 1e9)
	for i := 0; i < 20; i++ {
		e.produce(t, 100)
		d := e.s.Receive("w", 1)[0]
		e.s.Ack(d.Token, map[string]float64{"tokens": 200})
	}
	if c := e.reg.Charge("jobs", 100); c < 180 || c > 220 {
		t.Fatalf("charge for estimate 100 is %.1f", c)
	}
}

func TestQuotaExhaustedPausesPoolUntilResume(t *testing.T) {
	e := setup(t, 1e6, 1e6)
	e.produce(t, 10, 10)
	d := e.s.Receive("w", 1)[0]
	e.s.Nack(d.Token, sub.NackRequest{Reason: "quota_exhausted"})
	e.c.Advance(time.Hour)
	e.s.Tick()
	if len(e.s.Receive("w", 5)) != 0 {
		t.Fatal("dispatched while pool paused")
	}
	e.reg.Resume("openai")
	if n := len(e.s.Receive("w", 5)); n != 2 {
		t.Fatalf("after resume got %d", n)
	}
}

func TestPoolStatePersists(t *testing.T) {
	e := setup(t, 600, 100)
	e.produce(t, 100)
	d := e.s.Receive("w", 1)[0]
	e.s.Ack(d.Token, map[string]float64{"tokens": 600})
	if err := e.reg.Flush(); err != nil {
		t.Fatal(err)
	}
	r2, err := NewRegistry(e.st, e.c)
	if err != nil {
		t.Fatal(err)
	}
	if st := r2.Stats("openai"); st.Tokens > -400 {
		t.Fatalf("debt lost across reload: tokens %.0f", st.Tokens)
	}
}

func TestAckWithoutUsageKeepsCharge(t *testing.T) {
	e := setup(t, 60, 100) // 1 token per second
	e.produce(t, 100, 100)
	d := e.s.Receive("w", 1)[0]
	e.s.Ack(d.Token, nil)
	if len(e.s.Receive("w", 1)) != 0 {
		t.Fatal("ack without usage refunded the charge")
	}
}
