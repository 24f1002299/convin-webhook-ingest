package store_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/convin/webhook-ingest/internal/config"
)

func TestEventIdempotencyMigration(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, config.Load().PostgresDSN)
	if err != nil {
		t.Fatalf("connect to postgres (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Exec(ctx, `SET search_path TO pg_temp`); err != nil {
		t.Fatalf("use temporary schema: %v", err)
	}
	execMigrationFile(t, ctx, conn, "../../migrations/001_init.sql")

	if _, err := conn.Exec(ctx, `
		INSERT INTO calls (call_id, account_id, status, duration_sec) VALUES
			('call_1', 'acc_1', 'completed', 10),
			('call_2', 'acc_1', 'completed', 20),
			('call_3', 'acc_2', 'completed', 7);

		INSERT INTO events (event_id, call_id, account_id, payload) VALUES
			('evt_duplicate', 'call_1', 'acc_1', '{}'),
			('evt_duplicate', 'call_2', 'acc_1', '{}'),
			('evt_unique', 'call_3', 'acc_2', '{}');

		INSERT INTO account_stats (account_id, call_count, total_duration_sec) VALUES
			('acc_1', 99, 999),
			('acc_stale', 1, 100);
	`); err != nil {
		t.Fatalf("seed pre-migration data: %v", err)
	}

	execMigrationFile(t, ctx, conn, "../../migrations/002_event_idempotency.sql")

	var copies int
	var retainedCallID string
	if err := conn.QueryRow(ctx, `
		SELECT count(*), min(call_id)
		FROM events
		WHERE event_id = 'evt_duplicate'
	`).Scan(&copies, &retainedCallID); err != nil {
		t.Fatalf("read migrated events: %v", err)
	}
	if copies != 1 || retainedCallID != "call_1" {
		t.Fatalf("duplicate event: got copies=%d retained call=%q, want copies=1 retained call=%q", copies, retainedCallID, "call_1")
	}

	assertDurableStats(t, ctx, conn, "acc_1", 2, 30)
	assertDurableStats(t, ctx, conn, "acc_2", 1, 7)

	var staleRows int
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM account_stats WHERE account_id = 'acc_stale'`).Scan(&staleRows); err != nil {
		t.Fatalf("read stale aggregate: %v", err)
	}
	if staleRows != 0 {
		t.Fatalf("stale aggregate rows: got %d, want 0", staleRows)
	}

	_, err = conn.Exec(ctx, `
		INSERT INTO events (event_id, call_id, account_id, payload)
		VALUES ('evt_duplicate', 'call_3', 'acc_2', '{}')
	`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("duplicate insert: got error %v, want unique_violation (23505)", err)
	}
}

func TestRecordingJobsMigrationBackfillsPendingRecordings(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, config.Load().PostgresDSN)
	if err != nil {
		t.Fatalf("connect to postgres (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Exec(ctx, `SET search_path TO pg_temp`); err != nil {
		t.Fatalf("use temporary schema: %v", err)
	}
	execMigrationFile(t, ctx, conn, "../../migrations/001_init.sql")
	execMigrationFile(t, ctx, conn, "../../migrations/002_event_idempotency.sql")

	if _, err := conn.Exec(ctx, `
		INSERT INTO calls (
			call_id, account_id, status, duration_sec, recording_url, recording_processed
		) VALUES
			('call_pending', 'acc_1', 'completed', 10, 'https://example.com/pending.wav', FALSE),
			('call_processed', 'acc_1', 'completed', 20, 'https://example.com/done.wav', TRUE),
			('call_without_url', 'acc_1', 'completed', 30, NULL, FALSE),
			('call_with_empty_url', 'acc_1', 'completed', 40, '', FALSE)
	`); err != nil {
		t.Fatalf("seed calls: %v", err)
	}

	execMigrationFile(t, ctx, conn, "../../migrations/003_recording_jobs.sql")

	var (
		callID          string
		attempts        int
		ready           bool
		ownerIsNull     bool
		leaseIsNull     bool
		lastErrorIsNull bool
	)
	if err := conn.QueryRow(ctx, `
		SELECT
			call_id,
			attempts,
			next_attempt_at <= now(),
			lease_owner IS NULL,
			lease_expires_at IS NULL,
			last_error IS NULL
		FROM recording_jobs
	`).Scan(&callID, &attempts, &ready, &ownerIsNull, &leaseIsNull, &lastErrorIsNull); err != nil {
		t.Fatalf("read backfilled job: %v", err)
	}
	if callID != "call_pending" || attempts != 0 || !ready || !ownerIsNull || !leaseIsNull || !lastErrorIsNull {
		t.Fatalf("unexpected backfilled job state: call_id=%q attempts=%d ready=%t owner_null=%t lease_null=%t last_error_null=%t",
			callID, attempts, ready, ownerIsNull, leaseIsNull, lastErrorIsNull)
	}

	var jobs int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM recording_jobs`).Scan(&jobs); err != nil {
		t.Fatalf("count backfilled jobs: %v", err)
	}
	if jobs != 1 {
		t.Fatalf("backfilled jobs: got %d, want 1", jobs)
	}
}

func execMigrationFile(t *testing.T, ctx context.Context, conn *pgx.Conn, path string) {
	t.Helper()
	sql, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	if _, err := conn.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("execute migration %s: %v", path, err)
	}
}

func assertDurableStats(t *testing.T, ctx context.Context, conn *pgx.Conn, accountID string, wantCalls, wantDuration int64) {
	t.Helper()
	var gotCalls, gotDuration int64
	if err := conn.QueryRow(ctx, `
		SELECT call_count, total_duration_sec
		FROM account_stats
		WHERE account_id = $1
	`, accountID).Scan(&gotCalls, &gotDuration); err != nil {
		t.Fatalf("read stats for %s: %v", accountID, err)
	}
	if gotCalls != wantCalls || gotDuration != wantDuration {
		t.Fatalf("stats for %s: got calls=%d duration=%d, want calls=%d duration=%d",
			accountID, gotCalls, gotDuration, wantCalls, wantDuration)
	}
}
