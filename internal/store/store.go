// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// IngestEvent atomically stores an event and applies its call and aggregate
// effects. accepted is true only after the transaction commits.
func (s *Store) IngestEvent(ctx context.Context, e Event) (accepted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := tx.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	if err != nil {
		return false, err
	}
	if inserted.RowsAffected() == 0 {
		return false, nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL); err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO recording_jobs (call_id)
		 SELECT $1
		 WHERE $2 <> ''
		 ON CONFLICT (call_id) DO NOTHING`,
		e.CallID, e.RecordingURL); err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		e.AccountID, e.DurationSec); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}
