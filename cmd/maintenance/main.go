package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/db"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "maintenance:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("expected evict -external-id ID, purge -limit N, or purge-receipts -limit N")
	}
	flags := flag.NewFlagSet("maintenance", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var externalID string
	var limit int
	switch args[0] {
	case "evict":
		flags.StringVar(&externalID, "external-id", "", "external identity to evict")
	case "purge", "purge-receipts":
		flags.IntVar(&limit, "limit", 100, "maximum rows to remove (1–1000)")
	default:
		return errors.New("expected evict, purge, or purge-receipts")
	}
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return errors.New("invalid maintenance arguments")
	}
	if args[0] != "evict" && (limit < 1 || limit > int(catalog.MaxPurgeBatch)) {
		return errors.New("limit must be between 1 and 1000")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pool, err := db.OpenSample(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	service := catalog.New(pool, nil, time.Now)
	var count int64
	switch args[0] {
	case "evict":
		count, err = service.Evict(ctx, externalID)
	case "purge":
		count, err = service.Purge(ctx, int32(limit))
	case "purge-receipts":
		count, err = service.PurgeReceipts(ctx, int32(limit))
	}
	if err != nil {
		return errors.New("maintenance failed")
	}
	if args[0] == "purge-receipts" {
		fmt.Printf("purged_receipt_rows=%d\n", count)
		return nil
	}
	fmt.Printf("evicted_metadata_rows=%d\n", count)
	return nil
}
