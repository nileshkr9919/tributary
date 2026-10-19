package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nileshkr9919/tributary/internal/broker"
	"github.com/nileshkr9919/tributary/internal/clock"
)

type client struct {
	t   *testing.T
	url string
}

func newServer(t *testing.T) *client {
	t.Helper()
	b, err := broker.Open(t.TempDir(), clock.Real())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(b))
	t.Cleanup(func() { srv.Close(); b.Close() })
	return &client{t: t, url: srv.URL}
}

func (c *client) do(method, path, body string) (int, map[string]any) {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.url+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func (c *client) ok(method, path, body string) map[string]any {
	c.t.Helper()
	code, out := c.do(method, path, body)
	if code/100 != 2 {
		c.t.Fatalf("%s %s: %d %v", method, path, code, out)
	}
	return out
}

func deliveries(out map[string]any) []map[string]any {
	var ds []map[string]any
	for _, d := range out["deliveries"].([]any) {
		ds = append(ds, d.(map[string]any))
	}
	return ds
}

func TestEndToEndWorkQueue(t *testing.T) {
	c := newServer(t)
	c.ok("POST", "/v1/topics", `{"name":"jobs"}`)
	c.ok("POST", "/v1/subscriptions", `{"name":"w","topic":"jobs","profile":"work-queue"}`)
	ids := c.ok("POST", "/v1/topics/jobs/messages", `{"messages":[{"payload":{"n":1}},{"payload":{"n":2}}]}`)["ids"].([]any)
	if len(ids) != 2 {
		t.Fatalf("ids %v", ids)
	}
	ds := deliveries(c.ok("POST", "/v1/subscriptions/w/receive", `{"member":"m","max":10}`))
	if len(ds) != 2 {
		t.Fatalf("got %d", len(ds))
	}
	var acks []string
	for _, d := range ds {
		acks = append(acks, `{"token":"`+d["token"].(string)+`"}`)
	}
	res := c.ok("POST", "/v1/ack", `{"acks":[`+strings.Join(acks, ",")+`]}`)
	for _, r := range res["results"].([]any) {
		if r.(map[string]any)["status"] != "ok" {
			t.Fatalf("ack %v", r)
		}
	}
	st := c.ok("GET", "/v1/subscriptions/w", "")
	counts := st["counts"].(map[string]any)
	if counts["pending"].(float64) != 0 || counts["leased"].(float64) != 0 {
		t.Fatalf("counts %v", counts)
	}
}

func TestLongPollWakesOnProduce(t *testing.T) {
	c := newServer(t)
	c.ok("POST", "/v1/topics", `{"name":"jobs"}`)
	c.ok("POST", "/v1/subscriptions", `{"name":"w","topic":"jobs"}`)
	done := make(chan time.Time, 1)
	go func() {
		c.ok("POST", "/v1/subscriptions/w/receive", `{"member":"m","max":1,"wait_ms":2000}`)
		done <- time.Now()
	}()
	time.Sleep(300 * time.Millisecond)
	produced := time.Now()
	c.ok("POST", "/v1/topics/jobs/messages", `{"messages":[{"payload":1}]}`)
	select {
	case at := <-done:
		if at.Sub(produced) > 200*time.Millisecond {
			t.Fatalf("woke after %v", at.Sub(produced))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll never returned")
	}
}

func TestLLMRequestsFlow(t *testing.T) {
	c := newServer(t)
	c.ok("POST", "/v1/topics", `{"name":"llm"}`)
	c.ok("POST", "/v1/pools", `{"name":"openai","unit":"tokens","per_minute":6000,"burst":100}`)
	if code, _ := c.do("POST", "/v1/subscriptions", `{"name":"jobs","topic":"llm","profile":"llm-requests"}`); code != 400 {
		t.Fatalf("missing pool param: %d", code)
	}
	c.ok("POST", "/v1/subscriptions", `{"name":"jobs","topic":"llm","profile":"llm-requests","params":{"pool":"openai"}}`)
	c.ok("POST", "/v1/topics/llm/messages", `{"messages":[{"payload":"hi","priority":"interactive","headers":{"tenant":"a","x-est-tokens":"100"}}]}`)
	d := deliveries(c.ok("POST", "/v1/subscriptions/jobs/receive", `{"member":"m","max":1}`))[0]
	c.ok("POST", "/v1/ack", `{"acks":[{"token":"`+d["token"].(string)+`","usage":{"tokens":500}}]}`)
	req, _ := http.Get(c.url + "/metrics")
	body, _ := io.ReadAll(req.Body)
	req.Body.Close()
	// debt is 400 at the ack and shrinks as the pool refills (100 tokens/s), so allow a small margin
	if !regexp.MustCompile(`trib_pool_debt\{pool="openai"\} (39\d|400)`).Match(body) {
		t.Errorf("pool debt not ~400:\n%s", body)
	}
	for _, want := range []string{`trib_usage_total{subscription="jobs",field="tokens",label="a"} 500`, `trib_deliveries_total{subscription="jobs"} 1`} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestAPIErrors(t *testing.T) {
	c := newServer(t)
	c.ok("POST", "/v1/topics", `{"name":"jobs"}`)
	c.ok("POST", "/v1/subscriptions", `{"name":"w","topic":"jobs"}`)
	cases := []struct {
		method, path, body string
		code               int
	}{
		{"POST", "/v1/topics/nope/messages", `{"messages":[{"payload":1}]}`, 404},
		{"POST", "/v1/topics", `{not json`, 400},
		{"POST", "/v1/topics", `{"name":"jobs"}`, 409},
		{"POST", "/v1/subscriptions", `{"name":"x","topic":"nope"}`, 404},
		{"POST", "/v1/subscriptions", `{"name":"w","topic":"jobs"}`, 409},
		{"POST", "/v1/subscriptions", `{"name":"y","topic":"jobs","profile":"nope"}`, 400},
		{"POST", "/v1/subscriptions/nope/receive", `{"member":"m"}`, 404},
		{"GET", "/v1/subscriptions/nope", ``, 404},
	}
	for _, tc := range cases {
		code, out := c.do(tc.method, tc.path, tc.body)
		if code != tc.code || out["error"] == nil {
			t.Errorf("%s %s %s: got %d %v, want %d with error", tc.method, tc.path, tc.body, code, out, tc.code)
		}
	}
	// garbage token: 200 with status lost
	if _, out := c.do("POST", "/v1/extend", `{"token":"garbage"}`); out["status"] != "lost" {
		t.Errorf("garbage token: %v", out)
	}
	// oversized checkpoint
	c.ok("POST", "/v1/topics/jobs/messages", `{"messages":[{"payload":1}]}`)
	d := deliveries(c.ok("POST", "/v1/subscriptions/w/receive", `{"member":"m","max":1}`))[0]
	big := `"` + strings.Repeat("a", 70*1024) + `"`
	if code, _ := c.do("POST", "/v1/extend", `{"token":"`+d["token"].(string)+`","checkpoint":`+big+`}`); code != 413 {
		t.Errorf("big checkpoint: %d", code)
	}
}

func TestHealthz(t *testing.T) {
	c := newServer(t)
	resp, err := http.Get(c.url + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatal(err, resp.StatusCode)
	}
}

func TestWindowPoolWithHeadroom(t *testing.T) {
	c := newServer(t)
	out := c.ok("POST", "/v1/pools", `{"name":"prov","per_minute":120000,"mode":"window","headroom":0.2}`)
	if out["mode"] != "window" || out["headroom"].(float64) != 0.2 {
		t.Fatalf("%v", out)
	}
	if code, _ := c.do("POST", "/v1/pools", `{"name":"bad","per_minute":10,"mode":"spiky"}`); code != 400 {
		t.Fatalf("bad mode: %d", code)
	}
}

func TestInvalidNamesAre400(t *testing.T) {
	c := newServer(t)
	c.ok("POST", "/v1/topics", `{"name":"jobs"}`)
	for _, tc := range []struct{ path, body string }{
		{"/v1/topics", `{"name":"a~b"}`},
		{"/v1/subscriptions", `{"name":"w~x","topic":"jobs"}`},
		{"/v1/subscriptions", `{"name":"","topic":"jobs"}`},
	} {
		if code, out := c.do("POST", tc.path, tc.body); code != 400 || out["error"] == nil {
			t.Errorf("%s %s: got %d %v", tc.path, tc.body, code, out)
		}
	}
}

func TestReleaseRoute(t *testing.T) {
	c := newServer(t)
	c.ok("POST", "/v1/topics", `{"name":"jobs"}`)
	c.ok("POST", "/v1/subscriptions", `{"name":"w","topic":"jobs","overrides":{"failure":{"on_exhausted":"hold","max_attempts":1}}}`)
	id := c.ok("POST", "/v1/topics/jobs/messages", `{"messages":[{"payload":1}]}`)["ids"].([]any)[0].(string)
	d := deliveries(c.ok("POST", "/v1/subscriptions/w/receive", `{"member":"m"}`))[0]
	c.ok("POST", "/v1/nack", `{"token":"`+d["token"].(string)+`","reason":"retry"}`)
	out := c.ok("POST", "/v1/subscriptions/w/release", `{"ids":["`+id+`"]}`)
	if out["released"].(float64) != 1 {
		t.Fatalf("%v", out)
	}
	if ds := deliveries(c.ok("POST", "/v1/subscriptions/w/receive", `{"member":"m"}`)); len(ds) != 1 {
		t.Fatal("released message not delivered")
	}
}
