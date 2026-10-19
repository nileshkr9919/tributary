package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/nileshkr9919/tributary/internal/broker"
)

func esc(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func (a *api) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	lines = append(lines, "# TYPE trib_messages gauge")
	for _, s := range a.b.Subs() {
		counts := s.Counts()
		states := make([]string, 0, len(counts))
		for st := range counts {
			states = append(states, st)
		}
		sort.Strings(states)
		for _, st := range states {
			add(`trib_messages{subscription="%s",state="%s"} %d`, esc(s.Name()), st, counts[st])
		}
	}
	m := a.b.Metrics.Snapshot()
	lines = append(lines, "# TYPE trib_deliveries_total counter")
	for name, v := range m.Deliveries {
		add(`trib_deliveries_total{subscription="%s"} %g`, esc(name), v)
	}
	lines = append(lines, "# TYPE trib_nacks_total counter")
	for k, v := range m.Nacks {
		add(`trib_nacks_total{subscription="%s",reason="%s"} %g`, esc(k[0]), esc(k[1]), v)
	}
	lines = append(lines, "# TYPE trib_wait_seconds histogram")
	for k, count := range m.WaitCount {
		for _, le := range broker.WaitBuckets {
			lek := broker.BucketKey(le)
			add(`trib_wait_seconds_bucket{subscription="%s",band="%s",le="%s"} %g`, esc(k[0]), esc(k[1]), lek,
				m.WaitBucket[[3]string{k[0], k[1], lek}])
		}
		add(`trib_wait_seconds_bucket{subscription="%s",band="%s",le="+Inf"} %g`, esc(k[0]), esc(k[1]), count)
		add(`trib_wait_seconds_sum{subscription="%s",band="%s"} %g`, esc(k[0]), esc(k[1]), m.WaitSum[k])
		add(`trib_wait_seconds_count{subscription="%s",band="%s"} %g`, esc(k[0]), esc(k[1]), count)
	}
	lines = append(lines, "# TYPE trib_pool_tokens gauge", "# TYPE trib_pool_debt gauge", "# TYPE trib_pool_overshoot_total counter")
	for _, p := range a.b.Pools.List() {
		add(`trib_pool_tokens{pool="%s"} %g`, esc(p.Name), p.Tokens)
		add(`trib_pool_debt{pool="%s"} %g`, esc(p.Name), p.Debt)
		add(`trib_pool_overshoot_total{pool="%s"} %g`, esc(p.Name), p.Overshoot)
	}
	lines = append(lines, "# TYPE trib_usage_total counter")
	for _, s := range a.b.Subs() {
		for k, v := range s.UsageTotals() {
			add(`trib_usage_total{subscription="%s",field="%s",label="%s"} %g`, esc(s.Name()), esc(k[0]), esc(k[1]), v)
		}
	}
	fmt.Fprintln(w, strings.Join(lines, "\n"))
}
