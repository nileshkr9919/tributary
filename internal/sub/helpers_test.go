package sub

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/settings"
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/topic"
)

type env struct {
	t   *testing.T
	dir string
	s   *store.Store
	tm  *topic.Manager
	c   *clock.Fake
}

func newEnv(t *testing.T, shards int) *env {
	t.Helper()
	e := &env{t: t, dir: t.TempDir(), c: clock.NewFake(time.Unix(1_700_000_000, 0))}
	e.open()
	if err := e.tm.Create("t", shards); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) open() {
	s, err := store.Open(e.dir)
	if err != nil {
		e.t.Fatal(err)
	}
	tm, err := topic.NewManager(s, e.c)
	if err != nil {
		e.t.Fatal(err)
	}
	e.s, e.tm = s, tm
	e.t.Cleanup(func() { e.s.Close() })
}

func (e *env) deps() Deps { return Deps{Store: e.s, Topics: e.tm, Clock: e.c, Gate: Unlimited{}} }

func (e *env) sub(profile string, params map[string]string, overrides string) *Subscription {
	e.t.Helper()
	var ov json.RawMessage
	if overrides != "" {
		ov = json.RawMessage(overrides)
	}
	st, err := settings.Resolve(profile, params, ov)
	if err != nil {
		e.t.Fatal(err)
	}
	s, err := New("s", "t", st, e.deps())
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

// reopen closes the store and loads the subscription fresh from disk.
func (e *env) reopen() *Subscription {
	e.t.Helper()
	e.s.Close()
	e.open()
	s, err := Load("s", e.deps())
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func (e *env) produce(inputs ...topic.Input) []topic.Message {
	e.t.Helper()
	ms, err := e.tm.Produce("t", inputs)
	if err != nil {
		e.t.Fatal(err)
	}
	return ms
}

func msg(key string) topic.Input {
	return topic.Input{Key: key, Payload: json.RawMessage(`{"n":1}`)}
}

func hdr(key string, headers map[string]string) topic.Input {
	return topic.Input{Key: key, Headers: headers, Payload: json.RawMessage(`{}`)}
}

// pass advances the clock and runs the ticker.
func (e *env) pass(s *Subscription, d time.Duration) {
	e.c.Advance(d)
	s.Tick()
}
