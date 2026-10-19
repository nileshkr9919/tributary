package settings

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestLLMRequestsRequiresPool(t *testing.T) {
	if _, err := Resolve("llm-requests", nil, nil); !errors.Is(err, ErrMissingParam) {
		t.Fatalf("got %v", err)
	}
	s, err := Resolve("llm-requests", map[string]string{"pool": "openai"}, nil)
	if err != nil || s.Pool != "openai" {
		t.Fatalf("%v %+v", err, s)
	}
}

func TestOverridesApply(t *testing.T) {
	s, err := Resolve("work-queue", nil, json.RawMessage(`{"failure":{"max_attempts":2},"lease":{"initial":"10s"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Failure.MaxAttempts != 2 || s.Lease.Initial != 10*time.Second {
		t.Fatalf("%+v", s)
	}
	if s.Lease.MaxTotal != 12*time.Hour {
		t.Fatal("untouched field changed")
	}
}

func TestSupersedeNeedsPerKey(t *testing.T) {
	_, err := Resolve("work-queue", nil, json.RawMessage(`{"supersede":true}`))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestUnversionedNameMeansV1(t *testing.T) {
	s, err := Resolve("work-queue@1", nil, nil)
	if err != nil || s.Profile != "work-queue@1" {
		t.Fatalf("%v %s", err, s.Profile)
	}
	s2, _ := Resolve("work-queue", nil, nil)
	if s2.Profile != "work-queue@1" {
		t.Fatal(s2.Profile)
	}
	if _, err := Resolve("work-queue@2", nil, nil); !errors.Is(err, ErrUnknownProfile) {
		t.Fatal("unknown version accepted")
	}
}

func TestAgentSessionValues(t *testing.T) {
	s, err := Resolve("agent-session", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Ordering != "per-key" || !s.Supersede || s.Lease.Initial != 5*time.Minute ||
		s.Lease.MaxTotal != 6*time.Hour || s.Failure.MaxAttempts != 3 || s.Failure.OnExhausted != "dead-letter" {
		t.Fatalf("%+v", s)
	}
}

func TestLLMRequestsBands(t *testing.T) {
	s, _ := Resolve("llm-requests", map[string]string{"pool": "p"}, nil)
	want := []Band{{"interactive", 8, 2 * time.Second}, {"standard", 2, 30 * time.Second}, {"background", 1, 5 * time.Minute}}
	if len(s.Priority) != 3 {
		t.Fatalf("%+v", s.Priority)
	}
	for i := range want {
		if s.Priority[i] != want[i] {
			t.Fatalf("band %d: %+v", i, s.Priority[i])
		}
	}
	if s.MaxTotalWait != 10*time.Minute || s.FairnessHeader != "tenant" || s.EstimateHeader != "x-est-tokens" ||
		s.DefaultEstimate != 1000 || s.Failure.MaxAttempts != 5 {
		t.Fatalf("%+v", s)
	}
}

func TestInvalidValues(t *testing.T) {
	cases := []string{
		`{"lease":{"initial":"13h"}}`,
		`{"lease":{"checkpoint_limit":70000}}`,
		`{"failure":{"on_exhausted":"explode"}}`,
		`{"priority":[{"name":"a","weight":0,"max_wait":"1s"}]}`,
		`{"lease":{"initial":"banana"}}`,
	}
	for _, c := range cases {
		if _, err := Resolve("work-queue", nil, json.RawMessage(c)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v", c, err)
		}
	}
}
