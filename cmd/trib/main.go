// Command trib is the Tributary server and command-line tool.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nileshkr9919/tributary/internal/api"
	"github.com/nileshkr9919/tributary/internal/broker"
	"github.com/nileshkr9919/tributary/internal/clock"
)

const version = "trib v0.1.0 (v0: single node, on-disk format not stable)"

func usage() {
	fmt.Fprintf(os.Stderr, `%s

Usage:
  trib server --data DIR [--listen ADDR]   run a server (default listen 127.0.0.1:7070)
  trib dev [--listen ADDR]                 run a throwaway server on a temporary directory
  trib version                             print the version
`, version)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "--version":
		fmt.Println(version)
	case "server":
		fs := flag.NewFlagSet("server", flag.ExitOnError)
		data := fs.String("data", "", "data directory (required)")
		listen := fs.String("listen", "127.0.0.1:7070", "listen address")
		fs.Parse(os.Args[2:])
		if *data == "" {
			fmt.Fprintln(os.Stderr, "trib server: --data is required")
			os.Exit(2)
		}
		if err := serve(*data, *listen, false); err != nil {
			log.Fatal(err)
		}
	case "dev":
		fs := flag.NewFlagSet("dev", flag.ExitOnError)
		listen := fs.String("listen", "127.0.0.1:7070", "listen address")
		fs.Parse(os.Args[2:])
		dir, err := os.MkdirTemp("", "trib-dev-*")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(dir)
		if err := serve(dir, *listen, true); err != nil {
			log.Fatal(err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func serve(dir, listen string, dev bool) error {
	b, err := broker.Open(dir, clock.Real())
	if err != nil {
		return err
	}
	defer b.Close()
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: api.Handler(b), ReadHeaderTimeout: 10 * time.Second}
	mode := "server"
	if dev {
		mode = "dev (temporary data, deleted on exit)"
	}
	log.Printf("%s", version)
	log.Printf("%s listening on http://%s, data %s", mode, ln.Addr(), dir)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Printf("received %s, shutting down", s)
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}
