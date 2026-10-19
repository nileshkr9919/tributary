package quota

import (
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/topic"
)

func newReg(t *testing.T) (*Registry, *clock.Fake) {
	t.Helper()
	c := clock.NewFake(time.Unix(1_700_000_000, 0))
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r, err := NewRegistry(st, c)
	if err != nil {
		t.Fatal(err)
	}
	return r, c
}

func m(prio string, est string) topic.Message {
	return topic.Message{Priority: prio, Headers: map[string]string{"x-est-tokens": est}}
}

func TestWindowModeMatchesSlidingLimit(t *testing.T) {
	r, c := newReg(t)
	if err := r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 1000, Mode: "window"}); err != nil {
		t.Fatal(err)
	}
	r.Bind("s", "p", "x-est-tokens", 100, "")
	g := r.Gate()
	if _, ok, _ := g.Reserve("s", m("", "600")); !ok {
		t.Fatal("first 600 denied")
	}
	_, ok, retryAt := g.Reserve("s", m("", "600"))
	if ok {
		t.Fatal("window over-admitted: 1200 > 1000 within a minute")
	}
	if want := c.Now().Add(60 * time.Second); !retryAt.Equal(want) {
		t.Fatalf("retryAt %v, want %v", retryAt, want)
	}
	c.Advance(30 * time.Second)
	if _, ok, _ := g.Reserve("s", m("", "600")); ok {
		t.Fatal("admitted before the first grant left the window")
	}
	c.Advance(31 * time.Second)
	if _, ok, _ := g.Reserve("s", m("", "600")); !ok {
		t.Fatal("denied after the window moved")
	}
}

func TestWindowSettleAdjustsUsage(t *testing.T) {
	r, _ := newReg(t)
	r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 1000, Mode: "window"})
	r.Bind("s", "p", "x-est-tokens", 100, "")
	g := r.Gate()
	charge, _, _ := g.Reserve("s", m("", "100"))
	g.Settle("s", charge, r.c.Now(), map[string]float64{"tokens": 500})
	if _, ok, _ := g.Reserve("s", m("", "600")); ok {
		t.Fatal("settled overuse not counted in the window")
	}
	if st := r.Stats("p"); st.Used != 500 || st.Overshoot != 400 {
		t.Fatalf("used %.0f overshoot %.0f", st.Used, st.Overshoot)
	}
}

func TestHeadroomReservedForTopBand(t *testing.T) {
	r, _ := newReg(t)
	r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 1000, Mode: "window", Headroom: 0.2})
	r.Bind("s", "p", "x-est-tokens", 100, "interactive")
	g := r.Gate()
	if _, ok, _ := g.Reserve("s", m("background", "700")); !ok {
		t.Fatal("background denied within its 80%")
	}
	if _, ok, _ := g.Reserve("s", m("background", "200")); ok {
		t.Fatal("background allowed into the reserved 20%")
	}
	if _, ok, _ := g.Reserve("s", m("interactive", "250")); !ok {
		t.Fatal("interactive denied its reserved headroom")
	}
}

func TestHeadroomInBucketMode(t *testing.T) {
	r, _ := newReg(t)
	r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 60, Burst: 1000, Headroom: 0.5})
	r.Bind("s", "p", "x-est-tokens", 100, "interactive")
	g := r.Gate()
	if _, ok, _ := g.Reserve("s", m("standard", "400")); !ok {
		t.Fatal("non-top band denied above the headroom")
	}
	if _, ok, _ := g.Reserve("s", m("standard", "200")); ok {
		t.Fatal("non-top band drained the bucket below headroom")
	}
	if _, ok, _ := g.Reserve("s", m("interactive", "200")); !ok {
		t.Fatal("top band denied its headroom")
	}
}

func TestOversizedNonTopRequestNotStarved(t *testing.T) {
	r, _ := newReg(t)
	r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 60, Burst: 1000, Headroom: 0.5})
	r.Bind("s", "p", "x-est-tokens", 100, "interactive")
	if _, ok, _ := r.Gate().Reserve("s", m("standard", "600")); !ok {
		t.Fatal("a request larger than the non-reserved capacity must still run when the bucket is full")
	}
}

func TestNoBandsMeansNoHeadroom(t *testing.T) {
	r, _ := newReg(t)
	r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 1000, Mode: "window", Headroom: 0.5})
	r.Bind("s", "p", "x-est-tokens", 100, "")
	if _, ok, _ := r.Gate().Reserve("s", m("", "900")); !ok {
		t.Fatal("subscription without bands should not be limited by headroom")
	}
}

func TestInvalidPoolConfig(t *testing.T) {
	r, _ := newReg(t)
	for _, pc := range []PoolConfig{
		{Name: "a", Unit: "tokens", PerMinute: 10, Mode: "spiky"},
		{Name: "b", Unit: "tokens", PerMinute: 10, Mode: "window", Headroom: 1},
		{Name: "c", Unit: "tokens", PerMinute: 10, Headroom: -0.1},
	} {
		if err := r.CreateWith(pc); err == nil {
			t.Errorf("%+v accepted", pc)
		}
	}
}

func TestPaceSpreadsLowerBands(t *testing.T) {
	r, c := newReg(t)
	// 6000/min, 20% headroom, 10 s pace: lower bands may use 4800*10/60 = 800 per 10 s
	if err := r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 6000, Mode: "window", Headroom: 0.2, Pace: "10s"}); err != nil {
		t.Fatal(err)
	}
	r.Bind("s", "p", "x-est-tokens", 100, "interactive")
	g := r.Gate()
	if _, ok, _ := g.Reserve("s", m("background", "700")); !ok {
		t.Fatal("first slice denied")
	}
	_, ok, retryAt := g.Reserve("s", m("background", "200"))
	if ok {
		t.Fatal("background front-loaded past its pace")
	}
	if want := c.Now().Add(10 * time.Second); !retryAt.Equal(want) {
		t.Fatalf("retryAt %v want %v", retryAt, want)
	}
	if _, ok, _ := g.Reserve("s", m("interactive", "2000")); !ok {
		t.Fatal("pace must not limit the top band")
	}
	c.Advance(11 * time.Second)
	if _, ok, _ := g.Reserve("s", m("background", "200")); !ok {
		t.Fatal("background denied in the next pace interval")
	}
}

func TestPaceRequiresWindowMode(t *testing.T) {
	r, _ := newReg(t)
	for _, pc := range []PoolConfig{
		{Name: "a", Unit: "tokens", PerMinute: 10, Burst: 5, Pace: "10s"},
		{Name: "b", Unit: "tokens", PerMinute: 10, Mode: "window", Pace: "2m"},
		{Name: "c", Unit: "tokens", PerMinute: 10, Mode: "window", Pace: "soon"},
	} {
		if err := r.CreateWith(pc); err == nil {
			t.Errorf("%+v accepted", pc)
		}
	}
}

// I4: a refund changes the original grant, so it leaves the window when the grant does.
func TestWindowRefundAdjustsOriginalGrant(t *testing.T) {
	r, c := newReg(t)
	r.CreateWith(PoolConfig{Name: "p", Unit: "tokens", PerMinute: 1500, Mode: "window"})
	r.Bind("s", "p", "x-est-tokens", 100, "")
	g := r.Gate()
	t0 := c.Now()
	ch, _, _ := g.Reserve("s", m("", "1000"))
	c.Advance(50 * time.Second)
	g.Settle("s", ch, t0, nil) // refund the t=0 grant
	if _, ok, _ := g.Reserve("s", m("", "1000")); !ok {
		t.Fatal("refund not applied")
	}
	c.Advance(11 * time.Second) // t=61: the t=0 grant has left; the t=50 grant of 1000 remains
	if _, ok, _ := g.Reserve("s", m("", "1000")); ok {
		t.Fatal("over-admitted: 2000 in the window against a 1500 limit")
	}
}
