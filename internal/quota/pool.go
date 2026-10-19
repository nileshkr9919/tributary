// Package quota implements token-bucket quota pools that gate dispatch by estimated cost
// and settle against the usage consumers report on ack.
package quota

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/nileshkr9919/tributary/internal/clock"
	"github.com/nileshkr9919/tributary/internal/store"
	"github.com/nileshkr9919/tributary/internal/sub"
	"github.com/nileshkr9919/tributary/internal/topic"
)

var (
	ErrExists   = errors.New("pool already exists")
	ErrNotFound = errors.New("pool not found")
	ErrInvalid  = errors.New("pool needs a unit, per_minute > 0, burst > 0 in bucket mode, mode bucket or window, and 0 <= headroom < 1")
)

// Window is the period a window-mode pool counts usage over, matching provider per-minute limits.
const Window = 60 * time.Second

// PoolConfig describes a pool.
//
// Mode "bucket" (default) is a token bucket refilled at per_minute with capacity burst.
// Mode "window" admits a dispatch only if the units granted in the last 60 seconds plus the
// charge stay within per_minute, the same rule providers use for per-minute limits.
// Headroom is the fraction of capacity that only the subscription's top priority band may use.
// Pace (window mode only, for example "10s") spreads lower bands evenly: in any pace interval
// they may use at most their share of the minute's non-reserved capacity, so batch work cannot
// spend the whole minute's budget in its first seconds.
type PoolConfig struct {
	Name      string  `json:"name"`
	Unit      string  `json:"unit"`
	PerMinute float64 `json:"per_minute"`
	Burst     float64 `json:"burst"`
	Mode      string  `json:"mode"`
	Headroom  float64 `json:"headroom"`
	Pace      string  `json:"pace,omitempty"`
}

// PoolStats reports a pool's state.
type PoolStats struct {
	Name      string  `json:"name"`
	Unit      string  `json:"unit"`
	PerMinute float64 `json:"per_minute"`
	Burst     float64 `json:"burst"`
	Tokens    float64 `json:"tokens"`
	Debt      float64 `json:"debt"`
	Granted   float64 `json:"granted"`
	Settled   float64 `json:"settled"`
	Overshoot float64 `json:"overshoot"`
	Paused    bool    `json:"paused"`
	Mode      string  `json:"mode"`
	Headroom  float64 `json:"headroom"`
	Used      float64 `json:"used"` // window mode: units counted in the last 60 seconds
}

type grant struct {
	T time.Time `json:"t"`
	N float64   `json:"n"`
}

type pool struct {
	PoolStats
	Last        time.Time     `json:"last"`
	PausedUntil time.Time     `json:"paused_until"`
	Manual      bool          `json:"manual"`
	Grants      []grant       `json:"grants,omitempty"`
	PaceD       time.Duration `json:"pace_d,omitempty"`
}

type binding struct {
	pool, header, top string
	def, ratio        float64
}

// Registry holds every pool and which subscription draws from which pool.
type Registry struct {
	mu    sync.Mutex
	s     *store.Store
	c     clock.Clock
	pools map[string]*pool
	subs  map[string]*binding
	dirty bool
}

// NewRegistry loads pools from the store.
func NewRegistry(s *store.Store, c clock.Clock) (*Registry, error) {
	r := &Registry{s: s, c: c, pools: map[string]*pool{}, subs: map[string]*binding{}}
	err := s.Scan([]byte{store.TagPool}, func(k, v []byte) error {
		var p pool
		if err := json.Unmarshal(v, &p); err != nil {
			return err
		}
		r.pools[p.Name] = &p
		return nil
	})
	return r, err
}

// Create makes a token-bucket pool, initially full.
func (r *Registry) Create(name, unit string, perMinute, burst float64) error {
	return r.CreateWith(PoolConfig{Name: name, Unit: unit, PerMinute: perMinute, Burst: burst})
}

// CreateWith makes a pool from a full configuration.
func (r *Registry) CreateWith(pc PoolConfig) error {
	if pc.Mode == "" {
		pc.Mode = "bucket"
	}
	if pc.Mode == "window" && pc.Burst == 0 {
		pc.Burst = pc.PerMinute
	}
	if pc.Name == "" || pc.Unit == "" || pc.PerMinute <= 0 || pc.Burst <= 0 ||
		(pc.Mode != "bucket" && pc.Mode != "window") || pc.Headroom < 0 || pc.Headroom >= 1 {
		return ErrInvalid
	}
	var pace time.Duration
	if pc.Pace != "" {
		d, err := time.ParseDuration(pc.Pace)
		if err != nil || pc.Mode != "window" || d <= 0 || d > Window {
			return ErrInvalid
		}
		pace = d
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pools[pc.Name]; ok {
		return ErrExists
	}
	p := &pool{PoolStats: PoolStats{Name: pc.Name, Unit: pc.Unit, PerMinute: pc.PerMinute, Burst: pc.Burst,
		Tokens: pc.Burst, Mode: pc.Mode, Headroom: pc.Headroom}, Last: r.c.Now(), PaceD: pace}
	r.pools[pc.Name] = p
	return r.save(p)
}

// Exists reports whether a pool exists.
func (r *Registry) Exists(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pools[name]
	return ok
}

// Bind records that sub draws from pool, estimating cost from header or def. top is the
// subscription's top priority band; only it may use the pool's headroom ("" means no bands).
func (r *Registry) Bind(subName, poolName, header string, def float64, top string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if poolName == "" {
		delete(r.subs, subName)
		return
	}
	if def <= 0 {
		def = 1
	}
	r.subs[subName] = &binding{pool: poolName, header: header, def: def, ratio: 1, top: top}
}

func (r *Registry) save(p *pool) error {
	b := r.s.NewBatch()
	v, _ := json.Marshal(p)
	b.Set(store.Key(store.TagPool, p.Name), v)
	return b.Commit()
}

// Flush persists every pool's state.
func (r *Registry) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.dirty {
		return nil
	}
	b := r.s.NewBatch()
	for _, p := range r.pools {
		v, _ := json.Marshal(p)
		b.Set(store.Key(store.TagPool, p.Name), v)
	}
	r.dirty = false
	return b.Commit()
}

func (p *pool) refill(now time.Time) {
	if p.Mode == "window" {
		cut := now.Add(-Window)
		i := 0
		for i < len(p.Grants) && !p.Grants[i].T.After(cut) {
			i++
		}
		p.Grants = p.Grants[i:]
		used := 0.0
		for _, g := range p.Grants {
			used += g.N
		}
		p.Used = math.Max(0, used)
		p.Tokens = p.PerMinute - p.Used
		p.Debt = math.Max(0, -p.Tokens)
		return
	}
	if now.After(p.Last) {
		p.Tokens = math.Min(p.Burst, p.Tokens+now.Sub(p.Last).Seconds()*p.PerMinute/60)
		p.Last = now
	}
	p.Debt = math.Max(0, -p.Tokens)
}

// add records units used now (negative for refunds).
func (p *pool) add(now time.Time, n float64) {
	if p.Mode == "window" {
		p.Grants = append(p.Grants, grant{T: now, N: n})
		p.Used = math.Max(0, p.Used+n)
		p.Tokens = p.PerMinute - p.Used
	} else {
		p.Tokens = math.Min(p.Burst, p.Tokens-n)
	}
	p.Debt = math.Max(0, -p.Tokens)
}

// adjust applies a settlement. In window mode the original grant (made at grantedAt) is
// changed in place, so a refund leaves the window when the grant does; a grant that has
// already left the window needs no change. In bucket mode the difference applies now.
func (p *pool) adjust(now, grantedAt time.Time, delta float64) {
	if p.Mode != "window" {
		p.add(now, delta)
		return
	}
	for i := range p.Grants {
		g := &p.Grants[i]
		if g.T.Equal(grantedAt) && (delta >= 0 || g.N >= -delta) {
			g.N += delta
			p.Used = math.Max(0, p.Used+delta)
			p.Tokens = p.PerMinute - p.Used
			p.Debt = math.Max(0, -p.Tokens)
			return
		}
	}
}

// paced checks a lower band's share of the current pace interval.
func (p *pool) paced(now time.Time, charge, limit float64) (bool, time.Time) {
	sliceLimit := limit * p.PaceD.Seconds() / Window.Seconds()
	cut := now.Add(-p.PaceD)
	var slice []grant
	used := 0.0
	for _, g := range p.Grants {
		if g.T.After(cut) {
			slice = append(slice, g)
			used += g.N
		}
	}
	used = math.Max(0, used)
	if used+charge <= sliceLimit || (used <= 0 && charge > sliceLimit) {
		return true, time.Time{}
	}
	freed := 0.0
	for _, g := range slice {
		freed += g.N
		if used-freed+charge <= sliceLimit {
			return false, g.T.Add(p.PaceD)
		}
	}
	return false, now.Add(p.PaceD)
}

// admit decides whether charge fits, keeping the headroom for the top band.
func (p *pool) admit(now time.Time, charge float64, top bool) (bool, time.Time) {
	reserve := 0.0
	if !top {
		reserve = p.Headroom * p.Burst
	}
	if p.Mode == "window" {
		limit := p.PerMinute - reserve
		if !top && p.PaceD > 0 {
			if ok, at := p.paced(now, charge, limit); !ok {
				return false, at
			}
		}
		if p.Used+charge <= limit || (p.Used <= 0 && charge > limit) {
			return true, time.Time{}
		}
		freed := 0.0
		for _, g := range p.Grants {
			freed += g.N
			if p.Used-freed+charge <= limit {
				return false, g.T.Add(Window)
			}
		}
		return false, now.Add(Window)
	}
	if p.Tokens-charge >= reserve || (p.Tokens >= p.Burst && charge >= p.Burst-reserve) {
		return true, time.Time{}
	}
	need := charge + reserve - p.Tokens
	if charge > p.Burst-reserve {
		need = p.Burst - p.Tokens
	}
	return false, now.Add(time.Duration(need / (p.PerMinute / 60) * float64(time.Second)))
}

func (p *pool) paused(now time.Time) bool {
	return p.Manual || now.Before(p.PausedUntil)
}

// Charge returns what sub would be charged for a message with the given raw estimate.
func (r *Registry) Charge(subName string, estimate float64) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.subs[subName]; ok {
		return estimate * b.ratio
	}
	return estimate
}

// Pause pauses a pool until a time; a zero time pauses it until Resume.
func (r *Registry) Pause(name string, until time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pools[name]; ok {
		if until.IsZero() {
			p.Manual = true
		} else if until.After(p.PausedUntil) {
			p.PausedUntil = until
		}
		if err := r.save(p); err != nil {
			r.dirty = true
		}
	}
}

// Resume clears every pause on a pool.
func (r *Registry) Resume(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pools[name]; ok {
		p.Manual = false
		p.PausedUntil = time.Time{}
		if err := r.save(p); err != nil {
			r.dirty = true
		}
	}
}

// Stats returns a pool's state.
func (r *Registry) Stats(name string) PoolStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.pools[name]
	if !ok {
		return PoolStats{}
	}
	now := r.c.Now()
	p.refill(now)
	st := p.PoolStats
	st.Paused = p.paused(now)
	return st
}

// List returns every pool's state.
func (r *Registry) List() []PoolStats {
	r.mu.Lock()
	names := make([]string, 0, len(r.pools))
	for n := range r.pools {
		names = append(names, n)
	}
	r.mu.Unlock()
	out := make([]PoolStats, 0, len(names))
	for _, n := range names {
		out = append(out, r.Stats(n))
	}
	return out
}

// Gate returns the sub.Gate backed by this registry.
func (r *Registry) Gate() sub.Gate { return gate{r} }

type gate struct{ r *Registry }

func (g gate) Reserve(subName string, m topic.Message) (float64, bool, time.Time) {
	r := g.r
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.subs[subName]
	if !ok {
		return 0, true, time.Time{}
	}
	p, ok := r.pools[b.pool]
	if !ok {
		return 0, true, time.Time{}
	}
	now := r.c.Now()
	p.refill(now)
	if p.paused(now) {
		if p.Manual {
			return 0, false, now.Add(time.Second)
		}
		return 0, false, p.PausedUntil
	}
	est := b.def
	if v, err := strconv.ParseFloat(m.Headers[b.header], 64); err == nil && v > 0 {
		est = v
	}
	charge := est * b.ratio
	band := m.Priority
	if band == "" {
		band = m.Headers["trib-priority"]
	}
	top := b.top == "" || band == b.top
	ok, retryAt := p.admit(now, charge, top)
	if !ok {
		return 0, false, retryAt
	}
	p.add(now, charge)
	p.Granted += charge
	r.dirty = true
	return charge, true, time.Time{}
}

// Settle corrects the pool by the reported usage. Without usage the charge is refunded,
// because a nacked or expired call's cost is unknown and a throttled call cost nothing.
func (g gate) Settle(subName string, charged float64, grantedAt time.Time, usage map[string]float64) {
	r := g.r
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.subs[subName]
	if !ok || charged == 0 {
		return
	}
	p, ok := r.pools[b.pool]
	if !ok {
		return
	}
	now := r.c.Now()
	p.refill(now)
	actual, has := usage[p.Unit]
	if !has {
		actual, has = usage["tokens"]
	}
	if !has {
		if usage == nil {
			p.adjust(now, grantedAt, -charged)
			p.Granted -= charged
			r.dirty = true
		}
		return
	}
	diff := actual - charged
	p.adjust(now, grantedAt, diff)
	if diff > 0 {
		p.Overshoot += diff
	}
	p.Settled += actual
	if charged > 0 {
		b.ratio = math.Max(0.25, math.Min(4, b.ratio*(0.8+0.2*actual/charged)))
	}
	r.dirty = true
}

func (g gate) PausePool(subName string, until time.Time) bool {
	g.r.mu.Lock()
	b, ok := g.r.subs[subName]
	g.r.mu.Unlock()
	if !ok || !g.r.Exists(b.pool) {
		return false
	}
	g.r.Pause(b.pool, until)
	return true
}
