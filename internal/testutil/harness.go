// Package testutil provides shared setup for tests that need Postgres.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/httpapi"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// IDs returns event, call, and account identifiers unique to this test, and
// removes any rows owned by them before the test runs and again afterwards.
//
// `go test ./...` runs package test binaries in parallel against one shared
// database, so tests must never truncate shared tables. Every row carries an
// account_id, so deleting by account removes exactly this test's data and
// nothing else.
func IDs(t *testing.T, s *store.Store) (eventID, callID, accountID string) {
	t.Helper()
	base := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	eventID, callID, accountID = "evt_"+base, "call_"+base, "acc_"+base

	clean := func() {
		ctx := context.Background()
		for _, table := range []string{"events", "calls", "account_stats"} {
			if _, err := s.Pool().Exec(ctx,
				"DELETE FROM "+table+" WHERE account_id = $1", accountID); err != nil {
				t.Fatalf("clean %s: %v", table, err)
			}
		}
	}
	clean()
	t.Cleanup(clean)

	return eventID, callID, accountID
}

// NewStore opens a store against the configured database and closes it
// when the test finishes.
func NewStore(t *testing.T) *store.Store {
	t.Helper()
	cfg := config.Load()
	s, err := store.New(context.Background(), cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		t.Fatalf("connect to postgres (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// NewServer starts an in-process HTTP server backed by the configured
// Postgres and Redis, and returns it alongside the store for assertions.
func NewServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	srv, s, _ := NewServerWithService(t)
	return srv, s
}

// NewServerWithService also exposes the service so tests can control worker
// lifecycle explicitly.
func NewServerWithService(t *testing.T) (*httptest.Server, *store.Store, *ingest.Service) {
	t.Helper()
	s := NewStore(t)
	return newServerWithService(t, s)
}

// NewIsolatedServerWithService creates a migration-complete schema owned by
// this test. It is useful for workers that intentionally consume every ready
// job and therefore must not share a queue with other test packages.
func NewIsolatedServerWithService(t *testing.T, options ...ingest.Option) (*httptest.Server, *store.Store, *ingest.Service) {
	t.Helper()
	cfg := config.Load()
	admin := NewStore(t)

	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate test schema name: %v", err)
	}
	schema := "test_" + hex.EncodeToString(random)
	if _, err := admin.Pool().Exec(context.Background(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Pool().Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	databaseURL, err := url.Parse(cfg.PostgresDSN)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	query := databaseURL.Query()
	query.Set("search_path", schema)
	databaseURL.RawQuery = query.Encode()

	s, err := store.New(context.Background(), databaseURL.String(), cfg.DBMaxConns)
	if err != nil {
		t.Fatalf("connect to isolated postgres schema: %v", err)
	}
	t.Cleanup(s.Close)

	_, harnessFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migrations directory")
	}
	migrationDir := filepath.Join(filepath.Dir(harnessFile), "..", "..", "migrations")
	for _, name := range []string{"001_init.sql", "002_event_idempotency.sql", "003_recording_jobs.sql"} {
		sql, err := os.ReadFile(filepath.Join(migrationDir, name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := s.Pool().Exec(context.Background(), string(sql)); err != nil {
			t.Fatalf("execute migration %s: %v", name, err)
		}
	}

	return newServerWithService(t, s, options...)
}

func newServerWithService(t *testing.T, s *store.Store, options ...ingest.Option) (*httptest.Server, *store.Store, *ingest.Service) {
	t.Helper()
	cfg := config.Load()

	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(s, stats.NewCache(), rdb, log, options...)

	srv := httptest.NewServer(httpapi.NewRouter(svc, log))
	t.Cleanup(srv.Close)
	return srv, s, svc
}
