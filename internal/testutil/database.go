//go:build integration

// Package testutil provisions independent schemas inside an explicitly disposable
// sample cluster. An absent cluster is a test failure, never an implicit skip.
package testutil

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// OpenDB applies the actual migrations to a unique schema. Tests may change their
// schema's fixtures without truncating tables used by other tests.
func OpenDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := db.OpenSample(ctx)
	if err != nil {
		t.Fatalf("open isolated database (run make verify): %v", err)
	}
	t.Cleanup(admin.Close)
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	config := admin.Config()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migrationDB := stdlib.OpenDB(*config.ConnConfig)
	defer migrationDB.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, migrationDB, os.DirFS(migrationsPath(t)),
		goose.WithDisableGlobalRegistry(true), goose.WithLogger(goose.NopLogger()))
	if err != nil {
		t.Fatalf("configure migrations: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations to test schema: %v", err)
	}
	return pool
}

func migrationsPath(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		candidate := filepath.Join(dir, "db", "migrations")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot locate repository migrations")
		}
		dir = parent
	}
}
