package sub

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/settings"
)

var t0 = time.Unix(1_700_000_000, 0)

func llmSettings(t *testing.T, overrides string) settings.Settings {
	var ov json.RawMessage
	if overrides != "" {
		ov = json.RawMessage(overrides)
	}
	s, err := settings.Resolve("llm-requests", map[string]string{"pool": "p"}, ov)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var seq uint64

func ent(prio, tenant string, eligible time.Time) *entry {
	seq++
	return &entry{p: pos{0, seq}, priority: prio, headers: map[string]string{"tenant": tenant},
		rec: record{State: statePending, Eligible: eligible}}
}

func all(*entry) bool { return true }

func take(s *scheduler, now time.Time, avail func(*entry) bool) *entry {
	e := s.next(now, avail)
	if e != nil {
		s.remove(e)
	}
	return e
}

func TestWeightedBands(t *testing.T) {
	st := llmSettings(t, `{"priority":[{"name":"interactive","weight":8,"max_wait":"1h"},{"name":"background","weight":1,"max_wait":"1h"}],"max_total_wait":"10h"}`)
	s := newScheduler(st)
	for i := 0; i < 900; i++ {
		s.push(ent("interactive", "a", t0))
		s.push(ent("background", "a", t0))
	}
	inter := 0
	for i := 0; i < 90; i++ {
		if take(s, t0, all).priority == "interactive" {
			inter++
		}
	}
	if inter < 79 || inter > 81 {
		t.Fatalf("interactive picks %d of 90, want 80±1", inter)
	}
}

func TestAgeingPromotes(t *testing.T) {
	s := newScheduler(llmSettings(t, ""))
	old := ent("background", "a", t0)
	s.push(old)
	now := t0.Add(6 * time.Minute) // past background's 5 min max wait, under 10 min total
	for i := 0; i < 50; i++ {
		s.push(ent("standard", "a", now))
	}
	if got := take(s, now, all); got != old {
		t.Fatalf("aged background message not served first; got %s", got.priority)
	}
}

func TestOverdueLaneFirst(t *testing.T) {
	s := newScheduler(llmSettings(t, ""))
	old := ent("background", "a", t0)
	s.push(old)
	now := t0.Add(11 * time.Minute)
	for i := 0; i < 50; i++ {
		s.push(ent("interactive", "a", now))
	}
	if got := take(s, now, all); got != old {
		t.Fatal("overdue message not served first")
	}
}

func TestFairnessRoundRobin(t *testing.T) {
	s := newScheduler(llmSettings(t, ""))
	for i := 0; i < 1000; i++ {
		s.push(ent("interactive", "bulk", t0))
	}
	u := ent("interactive", "users", t0)
	s.push(u)
	a, b := take(s, t0, all), take(s, t0, all)
	if a != u && b != u {
		t.Fatal("small tenant not served within 2 picks")
	}
}

func TestPerValueInFlightCap(t *testing.T) {
	s := newScheduler(llmSettings(t, ""))
	for i := 0; i < 5; i++ {
		s.push(ent("interactive", "bulk", t0))
	}
	s.push(ent("interactive", "users", t0))
	capped := func(e *entry) bool { return e.headers["tenant"] != "bulk" }
	got := take(s, t0, capped)
	if got == nil || got.headers["tenant"] != "users" {
		t.Fatal("capped value not skipped")
	}
	if take(s, t0, capped) != nil {
		t.Fatal("capped value dispatched")
	}
}

func TestUnknownPriorityGoesToMiddleBand(t *testing.T) {
	s := newScheduler(llmSettings(t, ""))
	x := ent("", "a", t0)
	s.push(x)
	if x.band != 1 {
		t.Fatalf("band %d", x.band)
	}
}

func TestNoBandsIsFIFO(t *testing.T) {
	st, _ := settings.Resolve("work-queue", nil, nil)
	s := newScheduler(st)
	a, b := ent("", "", t0), ent("", "", t0)
	s.push(a)
	s.push(b)
	if take(s, t0, all) != a || take(s, t0, all) != b {
		t.Fatal("not FIFO")
	}
}
