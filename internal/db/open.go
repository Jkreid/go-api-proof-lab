package db

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const ClusterMarker = "go-api-reliability-lab disposable PostgreSQL cluster v1\n"

var ErrSampleConfig = errors.New("use the sample's disposable PostgreSQL wrapper")

// OpenSample deliberately has no generic DATABASE_URL fallback. The executable
// and tests only connect to the private Unix socket created by our wrapper.
// This is an accidental-destination guard, not a security boundary.
func OpenSample(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := sampleConfig()
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("open sample database")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("sample database unavailable")
	}
	return pool, nil
}

func sampleConfig() (*pgxpool.Config, error) {
	// pgx accepts libpq environment settings and reads password/service files.
	// Refuse ambient PG settings and explicitly disable the default passfile.
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PG") {
			return nil, ErrSampleConfig
		}
	}
	dir := os.Getenv("SAMPLE_CLUSTER_DIR")
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, ErrSampleConfig
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, ErrSampleConfig
	}
	marker, err := os.ReadFile(filepath.Join(dir, ".sample-cluster"))
	if err != nil || string(marker) != ClusterMarker {
		return nil, ErrSampleConfig
	}
	socketDir := filepath.Join(dir, "socket")
	info, err = os.Lstat(socketDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrSampleConfig
	}
	parsed, err := url.Parse(os.Getenv("SAMPLE_DATABASE_URL"))
	if err != nil || parsed.Scheme != "postgresql" || parsed.Host != "" ||
		parsed.Path != "/sample_catalog" || parsed.Fragment != "" || parsed.User == nil ||
		parsed.User.Username() != "sample_user" {
		return nil, ErrSampleConfig
	}
	if _, present := parsed.User.Password(); present {
		return nil, ErrSampleConfig
	}
	params, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(params) != 3 || len(params["host"]) != 1 || len(params["port"]) != 1 ||
		len(params["sslmode"]) != 1 || params.Get("host") != socketDir ||
		params.Get("port") != "5432" || params.Get("sslmode") != "disable" {
		return nil, ErrSampleConfig
	}
	params.Set("passfile", os.DevNull)
	parsed.RawQuery = params.Encode()
	cfg, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		return nil, ErrSampleConfig
	}
	cfg.ConnConfig.Fallbacks = nil
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	cfg.ConnConfig.RuntimeParams = map[string]string{
		"timezone":                      "UTC",
		"application_name":              "identity-metadata-sample",
		"default_transaction_isolation": "read committed",
	}
	cfg.MaxConns = 16
	return cfg, nil
}
