package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrRecordingJobLeaseLost means a job is no longer owned by the attempt
// trying to retry or complete it.
var ErrRecordingJobLeaseLost = errors.New("recording job lease lost")

// RecordingJob is one leased recording-processing attempt.
type RecordingJob struct {
	CallID         string
	RecordingURL   string
	Attempts       int
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

// ClaimRecordingJob leases one ready job without waiting for rows already
// being claimed by another worker.
func (s *Store) ClaimRecordingJob(ctx context.Context, leaseOwner string, leaseDuration time.Duration) (RecordingJob, bool, error) {
	if leaseOwner == "" {
		return RecordingJob{}, false, errors.New("recording job lease owner is required")
	}
	if leaseDuration <= 0 {
		return RecordingJob{}, false, errors.New("recording job lease duration must be positive")
	}

	var job RecordingJob
	err := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT call_id
			FROM recording_jobs
			WHERE next_attempt_at <= now()
			  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
			ORDER BY next_attempt_at, created_at, call_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE recording_jobs AS job
		SET lease_owner      = $1,
		    lease_expires_at = now() + ($2::double precision * interval '1 second'),
		    attempts         = job.attempts + 1,
		    updated_at       = now()
		FROM candidate
		WHERE job.call_id = candidate.call_id
		RETURNING
			job.call_id,
			COALESCE((SELECT recording_url FROM calls WHERE call_id = job.call_id), ''),
			job.attempts,
			job.lease_owner,
			job.lease_expires_at
	`, leaseOwner, leaseDuration.Seconds()).Scan(
		&job.CallID,
		&job.RecordingURL,
		&job.Attempts,
		&job.LeaseOwner,
		&job.LeaseExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RecordingJob{}, false, nil
	}
	if err != nil {
		return RecordingJob{}, false, err
	}
	return job, true, nil
}

// RetryRecordingJob releases a currently owned lease and schedules its next
// attempt. A stale attempt cannot modify a reclaimed job.
func (s *Store) RetryRecordingJob(ctx context.Context, job RecordingJob, lastError string, nextAttemptAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE recording_jobs
		SET lease_owner      = NULL,
		    lease_expires_at = NULL,
		    next_attempt_at  = $4,
		    last_error       = $5,
		    updated_at       = now()
		WHERE call_id = $1
		  AND lease_owner = $2
		  AND attempts = $3
		  AND lease_expires_at > now()
	`, job.CallID, job.LeaseOwner, job.Attempts, nextAttemptAt, lastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrRecordingJobLeaseLost
	}
	return nil
}

// CompleteRecordingJob atomically marks the call processed and removes its
// currently owned job. A stale attempt cannot complete a reclaimed job.
func (s *Store) CompleteRecordingJob(ctx context.Context, job RecordingJob) error {
	tag, err := s.pool.Exec(ctx, `
		WITH completed_job AS (
			DELETE FROM recording_jobs
			WHERE call_id = $1
			  AND lease_owner = $2
			  AND attempts = $3
			  AND lease_expires_at > now()
			RETURNING call_id
		)
		UPDATE calls AS c
		SET recording_processed = TRUE,
		    updated_at = now()
		FROM completed_job
		WHERE c.call_id = completed_job.call_id
	`, job.CallID, job.LeaseOwner, job.Attempts)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrRecordingJobLeaseLost
	}
	return nil
}
