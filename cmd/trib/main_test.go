package main

import (
	"bufio"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// M4: shutdown with a long-poll in progress exits cleanly and trib dev removes its data.
func TestDevShutdownWithLongPoll(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "trib")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	cmd := exec.Command(bin, "dev", "--listen", addr)
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var dataDir string
	sc := bufio.NewScanner(stderr)
	re := regexp.MustCompile(`data (\S+)`)
	for sc.Scan() {
		if m := re.FindStringSubmatch(sc.Text()); m != nil {
			dataDir = m[1]
			break
		}
	}
	go func() {
		for sc.Scan() {
		}
	}()
	url := "http://" + addr
	http.Post(url+"/v1/topics", "application/json", strings.NewReader(`{"name":"t"}`))
	http.Post(url+"/v1/subscriptions", "application/json", strings.NewReader(`{"name":"s","topic":"t"}`))
	go http.Post(url+"/v1/subscriptions/s/receive", "application/json", strings.NewReader(`{"member":"m","wait_ms":30000}`))
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	cmd.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		t.Fatal("did not exit within 10 s of SIGINT with a long-poll open")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("shutdown took %v; long-polls should be cancelled", time.Since(start))
	}
	if dataDir == "" {
		t.Fatal("data dir not logged")
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("dev data dir %s not removed", dataDir)
	}
}
