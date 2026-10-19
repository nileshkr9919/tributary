package sub

import (
	"time"

	"github.com/nileshkr9919/tributary/internal/settings"
)

// fifo is a queue with lazy removal: removed entries have queued == false.
type fifo struct{ q []*entry }

func (f *fifo) push(e *entry) { f.q = append(f.q, e) }

func (f *fifo) compact() {
	i := 0
	for i < len(f.q) && !f.q[i].queued {
		i++
	}
	if i > 0 {
		f.q = f.q[i:]
	}
}

// first returns the first queued entry satisfying ok, scanning at most while cont holds.
// Removed entries behind a blocked head are dropped once they are most of the queue.
func (f *fifo) first(ok func(*entry) bool, cont func(*entry) bool) *entry {
	f.compact()
	dead := 0
	defer func() {
		if dead > 32 && dead*2 > len(f.q) {
			live := f.q[:0]
			for _, e := range f.q {
				if e.queued {
					live = append(live, e)
				}
			}
			for i := len(live); i < len(f.q); i++ {
				f.q[i] = nil
			}
			f.q = live
		}
	}()
	for _, e := range f.q {
		if !e.queued {
			dead++
			continue
		}
		if cont != nil && !cont(e) {
			return nil
		}
		if ok(e) {
			return e
		}
	}
	return nil
}

type band struct {
	settings.Band
	all     fifo             // every entry in this band, in push order (for ageing)
	values  map[string]*fifo // per fairness value
	ring    []string         // fairness values in round-robin order
	next    int
	curr    int  // smooth weighted round-robin state
	ownTurn bool // when both an aged entry and the band's own entry wait: aged first, then alternate
}

// scheduler orders pending entries: the overdue lane first, then priority bands by smooth
// weighted round-robin (an entry past its band's max wait competes one band higher), then
// round-robin across fairness values within a band, then FIFO within a value.
type scheduler struct {
	bands    []*band
	maxTotal time.Duration
	header   string
	all      fifo
	middle   int
}

func newScheduler(st settings.Settings) *scheduler {
	s := &scheduler{maxTotal: st.MaxTotalWait, header: st.FairnessHeader}
	bs := st.Priority
	if len(bs) == 0 {
		bs = []settings.Band{{Name: "", Weight: 1}}
	}
	for _, b := range bs {
		s.bands = append(s.bands, &band{Band: b, values: map[string]*fifo{}})
	}
	s.middle = len(s.bands) / 2
	return s
}

func (s *scheduler) bandOf(prio string) int {
	for i, b := range s.bands {
		if b.Name == prio && prio != "" {
			return i
		}
	}
	return s.middle
}

func (s *scheduler) push(e *entry) {
	e.queued = true
	e.band = s.bandOf(e.priority)
	e.value = e.fairness(s.header)
	b := s.bands[e.band]
	b.all.push(e)
	q, ok := b.values[e.value]
	if !ok {
		q = &fifo{}
		b.values[e.value] = q
		b.ring = append(b.ring, e.value)
	}
	q.push(e)
	s.all.push(e)
}

func (s *scheduler) remove(e *entry) { e.queued = false }

func age(now time.Time, e *entry) time.Duration { return now.Sub(e.rec.Eligible) }

// fairPick returns the next available entry in band b by round-robin across values.
func (s *scheduler) fairPick(b *band, available func(*entry) bool) (*entry, int) {
	n := len(b.ring)
	for i := 0; i < n; i++ {
		idx := (b.next + i) % n
		v := b.ring[idx]
		q := b.values[v]
		if e := q.first(available, nil); e != nil {
			return e, idx
		}
	}
	// drop values whose queues are empty
	kept := b.ring[:0]
	for _, v := range b.ring {
		if q := b.values[v]; q != nil {
			q.compact()
			if len(q.q) > 0 {
				kept = append(kept, v)
				continue
			}
			delete(b.values, v)
		}
	}
	b.ring = kept
	if b.next >= len(b.ring) {
		b.next = 0
	}
	return nil, -1
}

// next returns the entry to dispatch, without removing it.
func (s *scheduler) next(now time.Time, available func(*entry) bool) *entry {
	// 1. overdue lane, oldest first
	if s.maxTotal > 0 {
		overdue := func(e *entry) bool { return age(now, e) > s.maxTotal }
		if e := s.all.first(available, overdue); e != nil {
			return e
		}
	} else {
		s.all.compact()
	}
	// 2. a candidate per band: an aged entry from the band below, else a fair pick
	// An aged entry from the band below joins this band: it takes turns with the band's own
	// entries rather than replacing them, so ageing never starves a higher band.
	type cand struct {
		e    *entry
		idx  int  // ring index to advance past, -1 for aged entries
		both bool // an aged entry and an own entry were both waiting
	}
	cands := make([]cand, len(s.bands))
	any := false
	for i, b := range s.bands {
		var agedE *entry
		if i+1 < len(s.bands) {
			lower := s.bands[i+1]
			if lower.MaxWait > 0 {
				isAged := func(e *entry) bool { return age(now, e) > lower.MaxWait }
				agedE = lower.all.first(available, isAged)
			}
		}
		own, idx := s.fairPick(b, available)
		switch {
		case agedE != nil && own != nil:
			if b.ownTurn {
				cands[i] = cand{own, idx, true}
			} else {
				cands[i] = cand{agedE, -1, true}
			}
		case agedE != nil:
			cands[i] = cand{agedE, -1, false}
		case own != nil:
			cands[i] = cand{own, idx, false}
		default:
			continue
		}
		any = true
	}
	if !any {
		return nil
	}
	// 3. smooth weighted round-robin across bands with a candidate
	total, best := 0, -1
	for i, b := range s.bands {
		if cands[i].e == nil {
			continue
		}
		b.curr += b.Weight
		total += b.Weight
		if best < 0 || b.curr > s.bands[best].curr {
			best = i
		}
	}
	s.bands[best].curr -= total
	c := cands[best]
	if c.both {
		s.bands[best].ownTurn = !s.bands[best].ownTurn
	}
	if c.idx >= 0 {
		b := s.bands[best]
		b.next = (c.idx + 1) % len(b.ring)
	}
	return c.e
}
