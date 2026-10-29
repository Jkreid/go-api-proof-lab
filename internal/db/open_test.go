package db

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func configureFakeCluster(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "socket"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".sample-cluster"), []byte(ClusterMarker), 0600); err != nil {
		t.Fatal(err)
	}
	values := url.Values{"host": {filepath.Join(dir, "socket")}, "port": {"5432"}, "sslmode": {"disable"}}
	dsn := "postgresql://sample_user@/sample_catalog?" + values.Encode()
	t.Setenv("SAMPLE_CLUSTER_DIR", dir)
	t.Setenv("SAMPLE_DATABASE_URL", dsn)
	return dsn
}

func TestSampleConfigCannotFallBackToAmbientDatabase(t *testing.T) {
	t.Setenv("SAMPLE_CLUSTER_DIR", "")
	t.Setenv("SAMPLE_DATABASE_URL", "")
	t.Setenv("DATABASE_URL", "postgresql://unrelated@localhost/unrelated")
	if _, err := sampleConfig(); !errors.Is(err, ErrSampleConfig) {
		t.Fatalf("missing sample configuration error = %v", err)
	}
}

func TestSampleConfigRejectsDestinationOverrides(t *testing.T) {
	for _, mutation := range []string{"host", "database", "user", "port", "service", "duplicate", "ambient", "marker"} {
		t.Run(mutation, func(t *testing.T) {
			dsn := configureFakeCluster(t)
			parsed, err := url.Parse(dsn)
			if err != nil {
				t.Fatal(err)
			}
			values := parsed.Query()
			switch mutation {
			case "host":
				values.Set("host", "localhost")
			case "database":
				parsed.Path = "/unrelated"
			case "user":
				parsed.User = url.User("unrelated")
			case "port":
				values.Set("port", "5433")
			case "service":
				values.Set("service", "unrelated")
			case "duplicate":
				values.Add("host", "localhost")
			case "ambient":
				t.Setenv("PGSERVICE", "unrelated")
			case "marker":
				if err := os.Remove(filepath.Join(os.Getenv("SAMPLE_CLUSTER_DIR"), ".sample-cluster")); err != nil {
					t.Fatal(err)
				}
			}
			parsed.RawQuery = values.Encode()
			t.Setenv("SAMPLE_DATABASE_URL", parsed.String())
			if _, err := sampleConfig(); !errors.Is(err, ErrSampleConfig) {
				t.Fatalf("unsafe config error = %v", err)
			}
		})
	}
}

func TestSampleConfigUsesOnlyPrivateSocket(t *testing.T) {
	configureFakeCluster(t)
	cfg, err := sampleConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != filepath.Join(os.Getenv("SAMPLE_CLUSTER_DIR"), "socket") || cfg.ConnConfig.Database != "sample_catalog" ||
		cfg.ConnConfig.User != "sample_user" || cfg.ConnConfig.TLSConfig != nil || len(cfg.ConnConfig.Fallbacks) != 0 {
		t.Fatal("configuration escaped the expected disposable cluster")
	}
}
