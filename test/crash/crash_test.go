// Package crash kills a real trib server with SIGKILL while it serves traffic and checks
// that no acknowledged produce is lost and no acknowledged message is delivered again.
package crash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

type server struct {
	cmd *exec.Cmd
	url string
}

func freePort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func start(t *testing.T, bin, dir string) *server {
	addr := freePort(t)
	cmd := exec.Command(bin, "server", "--data", dir, "--listen", addr)
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &server{cmd: cmd, url: "http://" + addr}
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(s.url + "/healthz"); err == nil {
			resp.Body.Close()
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return nil
}

func (s *server) kill() {
	s.cmd.Process.Signal(syscall.SIGKILL)
	s.cmd.Wait()
}

var hc = &http.Client{Timeout: 3 * time.Second}

func post(url string, body any, out any) error {
	b, _ := json.Marshal(body)
	resp, err := hc.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type delivery struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

type ledger struct {
	mu        sync.Mutex
	produced  map[string]bool
	delivered map[string]bool
	acked     map[string]bool
	violation []string
}

func (l *ledger) deliver(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acked[id] {
		l.violation = append(l.violation, "redelivered after ack: "+id)
	}
	l.delivered[id] = true
}

func consume(url string, l *ledger, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		var r struct {
			Deliveries []delivery `json:"deliveries"`
		}
		if err := post(url+"/v1/subscriptions/w/receive", map[string]any{"member": "c", "max": 20, "wait_ms": 100}, &r); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		for _, d := range r.Deliveries {
			l.deliver(d.ID)
			var res struct {
				Results []struct {
					Status string `json:"status"`
				} `json:"results"`
			}
			if err := post(url+"/v1/ack", map[string]any{"acks": []map[string]any{{"token": d.Token}}}, &res); err == nil &&
				len(res.Results) == 1 && res.Results[0].Status == "ok" {
				l.mu.Lock()
				l.acked[d.ID] = true
				l.mu.Unlock()
			}
		}
	}
}

func produce(url string, l *ledger, stop <-chan struct{}) {
	n := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		n++
		var r struct {
			IDs []string `json:"ids"`
		}
		msgs := []map[string]any{{"payload": n, "key": fmt.Sprint(n % 7)}, {"payload": n}}
		if err := post(url+"/v1/topics/t/messages", map[string]any{"messages": msgs}, &r); err == nil {
			l.mu.Lock()
			for _, id := range r.IDs {
				l.produced[id] = true
			}
			l.mu.Unlock()
		}
	}
}

func TestKill9NoAckedLoss(t *testing.T) {
	if os.Getenv("TRIB_CRASH") != "1" {
		t.Skip("set TRIB_CRASH=1 to run (needs bin/trib)")
	}
	bin, _ := filepath.Abs("../../bin/trib")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("build first: %v", err)
	}
	dir := t.TempDir()
	s := start(t, bin, dir)
	if err := post(s.url+"/v1/topics", map[string]any{"name": "t", "shards": 4}, nil); err != nil {
		t.Fatal(err)
	}
	ov := json.RawMessage(`{"lease":{"initial":"1s"},"failure":{"backoff_min":"100ms","backoff_max":"200ms","max_attempts":1000,"quarantine_after":1000}}`)
	if err := post(s.url+"/v1/subscriptions", map[string]any{"name": "w", "topic": "t", "overrides": ov}, nil); err != nil {
		t.Fatal(err)
	}
	l := &ledger{produced: map[string]bool{}, delivered: map[string]bool{}, acked: map[string]bool{}}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		if i > 0 {
			s = start(t, bin, dir)
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); produce(s.url, l, stop) }()
		go func() { defer wg.Done(); consume(s.url, l, stop) }()
		time.Sleep(time.Duration(200+rng.Intn(600)) * time.Millisecond)
		s.kill()
		close(stop)
		wg.Wait()
	}
	// drain
	s = start(t, bin, dir)
	defer s.kill()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { consume(s.url, l, stop); close(done) }()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var st struct {
			Counts map[string]int `json:"counts"`
		}
		resp, err := hc.Get(s.url + "/v1/subscriptions/w")
		if err == nil {
			json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if st.Counts["pending"]+st.Counts["delayed"]+st.Counts["leased"] == 0 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	<-done
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.violation) > 0 {
		t.Fatalf("%d violations, first: %s", len(l.violation), l.violation[0])
	}
	lost := 0
	for id := range l.produced {
		if !l.delivered[id] {
			lost++
			if lost < 5 {
				t.Errorf("acknowledged produce never delivered: %s", id)
			}
		}
	}
	if lost > 0 {
		t.Fatalf("%d of %d acknowledged produces lost", lost, len(l.produced))
	}
	t.Logf("produced %d, delivered %d, acked %d across 50 SIGKILLs", len(l.produced), len(l.delivered), len(l.acked))
	if len(l.produced) == 0 || len(l.acked) == 0 {
		t.Fatal("no traffic")
	}
}
