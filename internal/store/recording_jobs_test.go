package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestConcurrentRecordingJobClaimersGetDifferentJobs(t *testing.T) {
	const jobCount = 10

	firstStore := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, firstStore)
	secondStore := testutil.NewStore(t)
	ctx := context.Background()

	for i := 0; i < jobCount; i++ {
		seedRecordingJob(t, ctx, firstStore,
			fmt.Sprintf("%s_%d", eventID, i),
			fmt.Sprintf("%s_%d", callID, i),
			accountID,
		)
	}

	type claimResult struct {
		job   store.RecordingJob
		found bool
		err   error
	}
	results := make(chan claimResult, jobCount)
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(jobCount)
	for i := 0; i < jobCount; i++ {
		claimer := firstStore
		if i%2 == 1 {
			claimer = secondStore
		}
		owner := fmt.Sprintf("worker_%d", i)
		go func(claimer *store.Store, owner string) {
			defer wg.Done()
			<-start
			job, found, err := claimer.ClaimRecordingJob(ctx, owner, time.Minute)
			results <- claimResult{job: job, found: found, err: err}
		}(claimer, owner)
	}

	close(start)
	wg.Wait()
	close(results)

	claimedCalls := make(map[string]string, jobCount)
	for result := range results {
		if result.err != nil {
			t.Errorf("claim job: %v", result.err)
			continue
		}
		if !result.found {
			t.Error("claim job: no job returned")
			continue
		}
		if previousOwner, exists := claimedCalls[result.job.CallID]; exists {
			t.Errorf("job %s claimed by both %s and %s", result.job.CallID, previousOwner, result.job.LeaseOwner)
		}
		claimedCalls[result.job.CallID] = result.job.LeaseOwner
	}
	if len(claimedCalls) != jobCount {
		t.Fatalf("unique claimed jobs: got %d, want %d", len(claimedCalls), jobCount)
	}
}

func TestExpiredRecordingJobLeaseCanBeReclaimed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()
	seedRecordingJob(t, ctx, s, eventID, callID, accountID)

	first, found, err := s.ClaimRecordingJob(ctx, "worker_a", time.Minute)
	if err != nil || !found {
		t.Fatalf("first claim: found=%t err=%v", found, err)
	}
	if _, err := s.Pool().Exec(ctx, `
		UPDATE recording_jobs
		SET lease_expires_at = now() - interval '1 second'
		WHERE call_id = $1
	`, callID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	reclaimed, found, err := s.ClaimRecordingJob(ctx, "worker_b", time.Minute)
	if err != nil || !found {
		t.Fatalf("reclaim: found=%t err=%v", found, err)
	}
	if reclaimed.CallID != callID || reclaimed.LeaseOwner != "worker_b" || reclaimed.Attempts != 2 {
		t.Fatalf("reclaimed job: got %+v, want call=%s owner=worker_b attempts=2", reclaimed, callID)
	}
	if err := s.CompleteRecordingJob(ctx, first); !errors.Is(err, store.ErrRecordingJobLeaseLost) {
		t.Fatalf("stale completion: got %v, want ErrRecordingJobLeaseLost", err)
	}
}

func TestRetryAndCompleteRecordingJob(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()
	seedRecordingJob(t, ctx, s, eventID, callID, accountID)

	first, found, err := s.ClaimRecordingJob(ctx, "worker_a", time.Minute)
	if err != nil || !found {
		t.Fatalf("first claim: found=%t err=%v", found, err)
	}
	retryAt := time.Now().Add(time.Hour)
	if err := s.RetryRecordingJob(ctx, first, "temporary failure", retryAt); err != nil {
		t.Fatalf("retry job: %v", err)
	}

	var leaseReleased, scheduledLater bool
	var lastError string
	if err := s.Pool().QueryRow(ctx, `
		SELECT
			lease_owner IS NULL AND lease_expires_at IS NULL,
			next_attempt_at > now(),
			last_error
		FROM recording_jobs
		WHERE call_id = $1
	`, callID).Scan(&leaseReleased, &scheduledLater, &lastError); err != nil {
		t.Fatalf("read retried job: %v", err)
	}
	if !leaseReleased || !scheduledLater || lastError != "temporary failure" {
		t.Fatalf("retried job: released=%t scheduled_later=%t last_error=%q", leaseReleased, scheduledLater, lastError)
	}
	if _, found, err := s.ClaimRecordingJob(ctx, "worker_b", time.Minute); err != nil || found {
		t.Fatalf("early retry claim: found=%t err=%v", found, err)
	}

	if _, err := s.Pool().Exec(ctx, `
		UPDATE recording_jobs
		SET next_attempt_at = now() - interval '1 second'
		WHERE call_id = $1
	`, callID); err != nil {
		t.Fatalf("make retry ready: %v", err)
	}
	retry, found, err := s.ClaimRecordingJob(ctx, "worker_b", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry claim: found=%t err=%v", found, err)
	}
	if retry.Attempts != 2 {
		t.Fatalf("retry attempts: got %d, want 2", retry.Attempts)
	}
	if err := s.CompleteRecordingJob(ctx, retry); err != nil {
		t.Fatalf("complete job: %v", err)
	}

	var jobs int
	var processed bool
	if err := s.Pool().QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM recording_jobs WHERE call_id = $1),
			(SELECT recording_processed FROM calls WHERE call_id = $1)
	`, callID).Scan(&jobs, &processed); err != nil {
		t.Fatalf("read completed job: %v", err)
	}
	if jobs != 0 || !processed {
		t.Fatalf("completed job: jobs=%d recording_processed=%t, want jobs=0 recording_processed=true", jobs, processed)
	}
}

func seedRecordingJob(t *testing.T, ctx context.Context, s *store.Store, eventID, callID, accountID string) {
	t.Helper()
	accepted, err := s.IngestEvent(ctx, store.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/" + callID + ".wav",
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("seed recording job %s: %v", callID, err)
	}
	if !accepted {
		t.Fatalf("seed recording job %s: event was not accepted", callID)
	}
}
