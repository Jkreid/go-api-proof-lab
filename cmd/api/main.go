package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/db"
	"example.com/go-api-reliability-lab/internal/httpapi"
	"example.com/go-api-reliability-lab/internal/provider"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	port := flag.Int("port", 8080, "loopback HTTP port")
	flag.Parse()
	if *port < 1 || *port > 65535 || flag.NArg() != 0 {
		return errors.New("port must be between 1 and 65535; no positional arguments are accepted")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := db.OpenSample(ctx)
	if err != nil {
		return errors.New("cannot open isolated sample database; start with make serve")
	}
	defer pool.Close()
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(*port))
	server := &http.Server{
		Addr:              address,
		Handler:           httpapi.NewHandler(catalog.New(pool, provider.NewSimulator(), time.Now)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       httpapi.OperationTimeout,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("cannot bind loopback HTTP port")
	}
	fmt.Printf("Synthetic catalog listening on http://%s (doc-101 and doc-202)\n", address)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return errors.New("HTTP shutdown did not complete")
		}
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP server stopped unexpectedly")
	}
}
