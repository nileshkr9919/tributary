// Package settings resolves a subscription's effective settings from a built-in profile,
// profile parameters and inline overrides.
package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrUnknownProfile = errors.New("unknown profile")
	ErrMissingParam   = errors.New("missing required profile parameter")
	ErrInvalid        = errors.New("invalid settings")
)

// Band is one priority band.
type Band struct {
	Name    string        `json:"name"`
	Weight  int           `json:"weight"`
	MaxWait time.Duration `json:"max_wait"`
}

// Lease settings.
type Lease struct {
	Initial         time.Duration `json:"initial"`
	MaxTotal        time.Duration `json:"max_total"`
	Extendable      bool          `json:"extendable"`
	CheckpointLimit int           `json:"checkpoint_limit"`
}

// Failure settings.
type Failure struct {
	MaxAttempts     int           `json:"max_attempts"`
	BackoffMin      time.Duration `json:"backoff_min"`
	BackoffMax      time.Duration `json:"backoff_max"`
	Jitter          bool          `json:"jitter"`
	QuarantineAfter int           `json:"quarantine_after"`
	OnExhausted     string        `json:"on_exhausted"`
	MaxCost         float64       `json:"max_cost"`
	MaxThrottled    time.Duration `json:"max_throttled"`
}

// Settings are a subscription's effective settings.
type Settings struct {
	Profile             string        `json:"profile"`
	Ordering            string        `json:"ordering"`
	Supersede           bool          `json:"supersede"`
	Lease               Lease         `json:"lease"`
	Failure             Failure       `json:"failure"`
	Priority            []Band        `json:"priority"`
	MaxTotalWait        time.Duration `json:"max_total_wait"`
	FairnessHeader      string        `json:"fairness_header"`
	MaxInFlightPerValue int           `json:"max_in_flight_per_value"`
	Pool                string        `json:"pool"`
	EstimateHeader      string        `json:"estimate_header"`
	DefaultEstimate     float64       `json:"default_estimate"`
	MaxInFlight         int           `json:"max_in_flight"`
}

func workQueue() Settings {
	return Settings{
		Profile:  "work-queue@1",
		Ordering: "none",
		Lease:    Lease{Initial: 30 * time.Second, MaxTotal: 12 * time.Hour, Extendable: true, CheckpointLimit: 64 * 1024},
		Failure: Failure{MaxAttempts: 10, BackoffMin: time.Second, BackoffMax: 15 * time.Minute, Jitter: true,
			QuarantineAfter: 3, OnExhausted: "dead-letter", MaxThrottled: 24 * time.Hour},
		MaxInFlight: 10000,
	}
}

type builtin struct {
	settings Settings
	required []string
}

func builtins() map[string]builtin {
	llm := workQueue()
	llm.Profile = "llm-requests@1"
	llm.Failure.MaxAttempts = 5
	llm.Priority = []Band{{"interactive", 8, 2 * time.Second}, {"standard", 2, 30 * time.Second}, {"background", 1, 5 * time.Minute}}
	llm.MaxTotalWait = 10 * time.Minute
	llm.FairnessHeader = "tenant"
	llm.EstimateHeader = "x-est-tokens"
	llm.DefaultEstimate = 1000

	agent := workQueue()
	agent.Profile = "agent-session@1"
	agent.Ordering = "per-key"
	agent.Supersede = true
	agent.Lease.Initial = 5 * time.Minute
	agent.Lease.MaxTotal = 6 * time.Hour
	agent.Failure.MaxAttempts = 3

	return map[string]builtin{
		"work-queue@1":    {settings: workQueue()},
		"llm-requests@1":  {settings: llm, required: []string{"pool"}},
		"agent-session@1": {settings: agent},
	}
}

// Names lists the built-in profiles.
func Names() []string { return []string{"work-queue@1", "llm-requests@1", "agent-session@1"} }

// Resolve builds effective settings.
func Resolve(profile string, params map[string]string, overrides json.RawMessage) (Settings, error) {
	if profile == "" {
		profile = "work-queue"
	}
	if !strings.Contains(profile, "@") {
		profile += "@1"
	}
	b, ok := builtins()[profile]
	if !ok {
		return Settings{}, fmt.Errorf("%w: %s", ErrUnknownProfile, profile)
	}
	s := b.settings
	for _, p := range b.required {
		if params[p] == "" {
			return Settings{}, fmt.Errorf("%w: %s requires %q", ErrMissingParam, profile, p)
		}
	}
	if v := params["pool"]; v != "" {
		s.Pool = v
	}
	if len(bytes.TrimSpace(overrides)) > 0 && string(bytes.TrimSpace(overrides)) != "null" {
		if err := applyOverrides(&s, overrides); err != nil {
			return Settings{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	if err := validate(s); err != nil {
		return Settings{}, err
	}
	return s, nil
}

// dur unmarshals "30s"-style strings.
type dur struct{ d *time.Duration }

func (x *dur) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\"")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*x.d = d
	return nil
}

func applyOverrides(s *Settings, raw json.RawMessage) error {
	var o struct {
		Ordering            *string          `json:"ordering"`
		Supersede           *bool            `json:"supersede"`
		Lease               *json.RawMessage `json:"lease"`
		Failure             *json.RawMessage `json:"failure"`
		Priority            *json.RawMessage `json:"priority"`
		MaxTotalWait        *string          `json:"max_total_wait"`
		FairnessHeader      *string          `json:"fairness_header"`
		MaxInFlightPerValue *int             `json:"max_in_flight_per_value"`
		Pool                *string          `json:"pool"`
		EstimateHeader      *string          `json:"estimate_header"`
		DefaultEstimate     *float64         `json:"default_estimate"`
		MaxInFlight         *int             `json:"max_in_flight"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil {
		return err
	}
	set := func(dst *string, v *string) {
		if v != nil {
			*dst = *v
		}
	}
	set(&s.Ordering, o.Ordering)
	set(&s.FairnessHeader, o.FairnessHeader)
	set(&s.Pool, o.Pool)
	set(&s.EstimateHeader, o.EstimateHeader)
	if o.Supersede != nil {
		s.Supersede = *o.Supersede
	}
	if o.MaxInFlightPerValue != nil {
		s.MaxInFlightPerValue = *o.MaxInFlightPerValue
	}
	if o.DefaultEstimate != nil {
		s.DefaultEstimate = *o.DefaultEstimate
	}
	if o.MaxInFlight != nil {
		s.MaxInFlight = *o.MaxInFlight
	}
	if o.MaxTotalWait != nil {
		d, err := time.ParseDuration(*o.MaxTotalWait)
		if err != nil {
			return err
		}
		s.MaxTotalWait = d
	}
	if o.Lease != nil {
		var l struct {
			Initial         dur   `json:"initial"`
			MaxTotal        dur   `json:"max_total"`
			Extendable      *bool `json:"extendable"`
			CheckpointLimit *int  `json:"checkpoint_limit"`
		}
		l.Initial.d, l.MaxTotal.d = &s.Lease.Initial, &s.Lease.MaxTotal
		if err := strict(*o.Lease, &l); err != nil {
			return err
		}
		if l.Extendable != nil {
			s.Lease.Extendable = *l.Extendable
		}
		if l.CheckpointLimit != nil {
			s.Lease.CheckpointLimit = *l.CheckpointLimit
		}
	}
	if o.Failure != nil {
		var f struct {
			MaxAttempts     *int     `json:"max_attempts"`
			BackoffMin      dur      `json:"backoff_min"`
			BackoffMax      dur      `json:"backoff_max"`
			Jitter          *bool    `json:"jitter"`
			QuarantineAfter *int     `json:"quarantine_after"`
			OnExhausted     *string  `json:"on_exhausted"`
			MaxCost         *float64 `json:"max_cost"`
			MaxThrottled    dur      `json:"max_throttled"`
		}
		f.BackoffMin.d, f.BackoffMax.d, f.MaxThrottled.d = &s.Failure.BackoffMin, &s.Failure.BackoffMax, &s.Failure.MaxThrottled
		if err := strict(*o.Failure, &f); err != nil {
			return err
		}
		if f.MaxAttempts != nil {
			s.Failure.MaxAttempts = *f.MaxAttempts
		}
		if f.Jitter != nil {
			s.Failure.Jitter = *f.Jitter
		}
		if f.QuarantineAfter != nil {
			s.Failure.QuarantineAfter = *f.QuarantineAfter
		}
		if f.OnExhausted != nil {
			s.Failure.OnExhausted = *f.OnExhausted
		}
		if f.MaxCost != nil {
			s.Failure.MaxCost = *f.MaxCost
		}
	}
	if o.Priority != nil {
		var bands []struct {
			Name    string `json:"name"`
			Weight  int    `json:"weight"`
			MaxWait string `json:"max_wait"`
		}
		if err := strict(*o.Priority, &bands); err != nil {
			return err
		}
		s.Priority = nil
		for _, b := range bands {
			d, err := time.ParseDuration(b.MaxWait)
			if err != nil {
				return fmt.Errorf("band %q max_wait: %v", b.Name, err)
			}
			s.Priority = append(s.Priority, Band{Name: b.Name, Weight: b.Weight, MaxWait: d})
		}
	}
	return nil
}

func strict(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func validate(s Settings) error {
	bad := func(f string, a ...any) error { return fmt.Errorf("%w: "+f, append([]any{ErrInvalid}, a...)...) }
	if s.Ordering != "none" && s.Ordering != "per-key" {
		return bad("ordering must be none or per-key")
	}
	if s.Supersede && s.Ordering != "per-key" {
		return bad("supersede requires ordering per-key")
	}
	for _, b := range s.Priority {
		if b.Name == "" || b.Weight < 1 {
			return bad("priority band %q needs a name and weight >= 1", b.Name)
		}
	}
	if s.Lease.Initial <= 0 || s.Lease.Initial > s.Lease.MaxTotal {
		return bad("lease.initial must be positive and at most lease.max_total")
	}
	if s.Lease.CheckpointLimit < 0 || s.Lease.CheckpointLimit > 65536 {
		return bad("lease.checkpoint_limit must be at most 65536")
	}
	switch s.Failure.OnExhausted {
	case "dead-letter", "drop", "hold":
	default:
		return bad("failure.on_exhausted must be dead-letter, drop or hold")
	}
	if s.Failure.MaxAttempts < 1 || s.Failure.QuarantineAfter < 1 || s.Failure.BackoffMin <= 0 || s.Failure.BackoffMax < s.Failure.BackoffMin {
		return bad("failure attempts and back-off must be positive")
	}
	if s.MaxInFlight < 1 {
		return bad("max_in_flight must be at least 1")
	}
	return nil
}
