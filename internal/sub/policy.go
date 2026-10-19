package sub

import (
	"time"

	"github.com/nileshkr9919/tributary/internal/store"
)

// afterRelease runs when an entry stops being in flight.
func (s *Subscription) afterRelease(e *entry) {}

const (
	minRetryAfter = 100 * time.Millisecond
	maxRetryAfter = 15 * time.Minute
)

// handleNack applies a nack's reason: retry (default), throttled, quota_exhausted, permanent.
func (s *Subscription) handleNack(b *store.Batch, e *entry, n NackRequest, now time.Time) {
	reason := n.Error
	if reason == "" {
		reason = n.Reason
	}
	switch n.Reason {
	case "throttled":
		ra := clampRetryAfter(n.RetryAfter)
		e.rec.Throttled += ra
		if e.rec.Throttled > s.cfg.Settings.Failure.MaxThrottled {
			s.failAttempt(b, e, "throttled past max_throttled", now)
			return
		}
		e.rec.Attempt-- // a throttle is not an attempt
		s.pauseScope(e, n.Scope, "message", now.Add(ra))
		s.delay(b, e, now.Add(ra))
	case "quota_exhausted":
		e.rec.Attempt--
		var until time.Time
		if n.RetryAfter > 0 {
			until = now.Add(clampRetryAfter(n.RetryAfter))
		}
		scope := n.Scope
		if scope == "" {
			scope = "pool"
		}
		paused := false
		if until.IsZero() && scope == "subscription" {
			s.cfg.Paused[""] = true
			saveConfig(s.d.Store, s.cfg)
			paused = true
		} else {
			paused = s.pauseScope(e, scope, "pool", until)
		}
		if until.IsZero() && !paused {
			// Nothing was paused (no pool bound, or a message or key scope with no time):
			// back off like a throttle so the message cannot spin.
			ra := s.backoff(1)
			e.rec.Throttled += ra
			if e.rec.Throttled > s.cfg.Settings.Failure.MaxThrottled {
				e.rec.Attempt++
				s.failAttempt(b, e, "quota exhausted past max_throttled", now)
				return
			}
			s.delay(b, e, now.Add(ra))
			return
		}
		if until.IsZero() {
			e.rec.State = statePending
			e.rec.Eligible = now
			s.putRecord(b, e)
			s.enqueue(e)
		} else {
			s.delay(b, e, until)
		}
	case "permanent":
		s.exhaust(b, e, reason, now)
	default:
		s.failAttempt(b, e, reason, now)
	}
}

func clampRetryAfter(d time.Duration) time.Duration {
	if d < minRetryAfter {
		return minRetryAfter
	}
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}

// pauseScope pauses dispatch for the nack's scope and reports whether a pause took effect.
// until zero means until resumed (pool only).
func (s *Subscription) pauseScope(e *entry, scope, def string, until time.Time) bool {
	if scope == "" {
		scope = def
	}
	switch scope {
	case "subscription":
		if until.IsZero() {
			return false
		}
		s.pauseUntil[""] = until
		return true
	case "key":
		if e.key == "" || until.IsZero() {
			return false
		}
		s.pauseUntil["k:"+e.key] = until
		return true
	case "pool":
		if p, ok := s.d.Gate.(Pauser); ok {
			return p.PausePool(s.cfg.Name, until)
		}
	}
	return false
}

// unpaused reports whether no timed pause covers e.
func (s *Subscription) unpaused(now time.Time, e *entry) bool {
	if t, ok := s.pauseUntil[""]; ok && now.Before(t) {
		return false
	}
	if v := e.fairness(s.cfg.Settings.FairnessHeader); v != "" {
		if t, ok := s.pauseUntil["v:"+v]; ok && now.Before(t) {
			return false
		}
	}
	if e.key != "" {
		if t, ok := s.pauseUntil["k:"+e.key]; ok && now.Before(t) {
			return false
		}
	}
	return true
}

// pausedFor reports whether an operator pause covers e.
func (s *Subscription) pausedFor(e *entry) bool {
	if s.cfg.Paused[""] {
		return true
	}
	v := e.fairness(s.cfg.Settings.FairnessHeader)
	return v != "" && s.cfg.Paused["v:"+v]
}

// pausesExpired drops expired timed pauses and reports whether any expired.
func (s *Subscription) pausesExpired(now time.Time) bool {
	any := false
	for k, t := range s.pauseUntil {
		if !now.Before(t) {
			delete(s.pauseUntil, k)
			any = true
		}
	}
	return any
}
