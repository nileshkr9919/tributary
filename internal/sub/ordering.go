package sub

import (
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/topic"
)

// onArrival supersedes earlier messages for the same key when the subscription asks for it.
// It runs after the new record is committed, so a record that never commits never supersedes.
func (s *Subscription) onArrival(b *store.Batch, e *entry) {
	if !s.cfg.Settings.Supersede || e.key == "" {
		return
	}
	for _, old := range s.entries {
		if old.key != e.key || old == e {
			continue
		}
		if old.p.shard == e.p.shard && old.p.offset > e.p.offset {
			continue // only earlier records are superseded
		}
		s.endOrNotify(b, old, stateSuperseded)
	}
}

// endOrNotify ends a non-leased entry now, or marks a leased one so its holder is told.
func (s *Subscription) endOrNotify(b *store.Batch, e *entry, state string) bool {
	switch e.rec.State {
	case stateLeased:
		if e.rec.Notified != "" {
			return false
		}
		e.rec.Notified = state
		s.putRecord(b, e)
		return true
	case stateHeld:
		s.finish(b, e, state, "")
		return true
	case statePending, stateDelayed:
		s.finish(b, e, state, "")
		return true
	}
	return false
}

// Cancel cancels one message by ID. It returns the number of messages cancelled.
func (s *Subscription) Cancel(id string) int {
	t, sh, off, err := topic.ParseID(id)
	if err != nil || t != s.cfg.Topic {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[pos{sh, off}]
	if !ok {
		return 0
	}
	return s.cancelEntries([]*entry{e})
}

// CancelKey cancels every pending, delayed, leased or held message with key.
func (s *Subscription) CancelKey(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var es []*entry
	for _, e := range s.entries {
		if e.key == key {
			es = append(es, e)
		}
	}
	return s.cancelEntries(es)
}

func (s *Subscription) cancelEntries(es []*entry) int {
	b := s.d.Store.NewBatch()
	n := 0
	for _, e := range es {
		if s.endOrNotify(b, e, stateCancelled) {
			n++
		}
	}
	if err := b.Commit(); err != nil {
		return 0
	}
	if n > 0 {
		s.notify()
	}
	return n
}

// Pause stops dispatch for the whole subscription (value "") or for one fairness value.
func (s *Subscription) Pause(value string) error { return s.setPaused(value, true) }

// Resume undoes Pause.
func (s *Subscription) Resume(value string) error { return s.setPaused(value, false) }

func (s *Subscription) setPaused(value string, on bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := ""
	if value != "" {
		k = "v:" + value
	}
	if on {
		s.cfg.Paused[k] = true
	} else {
		delete(s.cfg.Paused, k)
	}
	if err := saveConfig(s.d.Store, s.cfg); err != nil {
		return err
	}
	s.notify()
	return nil
}
