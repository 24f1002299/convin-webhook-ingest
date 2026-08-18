package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestRecordingWorkerProcessesJobAfterWebhookReturns(t *testing.T) {
	srv, st, svc := testutil.NewIsolatedServerWithService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	var processed bool
	var jobs int
	if err := st.Pool().QueryRow(ctx, `
		SELECT
			(SELECT recording_processed FROM calls WHERE call_id = $1),
			(SELECT count(*) FROM recording_jobs WHERE call_id = $1)
	`, callID).Scan(&processed, &jobs); err != nil {
		t.Fatalf("read queued recording: %v", err)
	}
	if processed || jobs != 1 {
		t.Fatalf("before worker: processed=%t jobs=%d, want processed=false jobs=1", processed, jobs)
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		svc.RunRecordingWorker(workerCtx, "test-worker")
	}()
	t.Cleanup(func() {
		cancelWorker()
		<-workerDone
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := st.Pool().QueryRow(ctx, `
			SELECT
				(SELECT recording_processed FROM calls WHERE call_id = $1),
				(SELECT count(*) FROM recording_jobs WHERE call_id = $1)
		`, callID).Scan(&processed, &jobs); err != nil {
			t.Fatalf("read recording progress: %v", err)
		}
		if processed && jobs == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recording was not processed: processed=%t jobs=%d", processed, jobs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRecordingWorkerRetriesLoggedFailureThenSucceeds(t *testing.T) {
	var logOutput bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logOutput, nil))
	injectedFailure := errors.New("injected first-attempt failure")
	allowSuccess := make(chan struct{})
	var processorAttempts atomic.Int32

	srv, st, svc := testutil.NewIsolatedServerWithService(t,
		ingest.WithLogger(log),
		ingest.WithRecordingProcessor(func(ctx context.Context, _ string) error {
			attempt := processorAttempts.Add(1)
			if attempt == 1 {
				return injectedFailure
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-allowSuccess:
				return nil
			}
		}),
	)
	eventID, callID, accountID := testutil.IDs(t, st)
	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		svc.RunRecordingWorker(workerCtx, "retry-test-worker")
	}()
	t.Cleanup(func() {
		cancelWorker()
		<-workerDone
	})

	ctx := context.Background()
	retryDeadline := time.Now().Add(2 * time.Second)
	for {
		var attempts int
		var lastError string
		var leaseReleased, scheduledLater bool
		err := st.Pool().QueryRow(ctx, `
			SELECT
				attempts,
				COALESCE(last_error, ''),
				lease_owner IS NULL AND lease_expires_at IS NULL,
				next_attempt_at > now()
			FROM recording_jobs
			WHERE call_id = $1
		`, callID).Scan(&attempts, &lastError, &leaseReleased, &scheduledLater)
		if err != nil {
			t.Fatalf("read scheduled retry: %v", err)
		}
		if attempts == 1 && lastError == injectedFailure.Error() && leaseReleased && scheduledLater {
			break
		}
		if time.Now().After(retryDeadline) {
			t.Fatalf("retry was not persisted: attempts=%d last_error=%q released=%t scheduled=%t",
				attempts, lastError, leaseReleased, scheduledLater)
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(allowSuccess)
	completionDeadline := time.Now().Add(2 * time.Second)
	for {
		var processed bool
		var jobs int
		if err := st.Pool().QueryRow(ctx, `
			SELECT
				(SELECT recording_processed FROM calls WHERE call_id = $1),
				(SELECT count(*) FROM recording_jobs WHERE call_id = $1)
		`, callID).Scan(&processed, &jobs); err != nil {
			t.Fatalf("read retry progress: %v", err)
		}
		if processed && jobs == 0 {
			break
		}
		if time.Now().After(completionDeadline) {
			t.Fatalf("retry did not complete: processed=%t jobs=%d", processed, jobs)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelWorker()
	<-workerDone
	if got := processorAttempts.Load(); got != 2 {
		t.Fatalf("processor attempts: got %d, want 2", got)
	}
	logs := logOutput.String()
	for _, want := range []string{
		"recording processing failed",
		"call_id=" + callID,
		"attempt=1",
		injectedFailure.Error(),
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	row = st.Pool().QueryRow(ctx, `SELECT count(*) FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of call %s, want 1", n, callID)
	}

	row = st.Pool().QueryRow(ctx, `SELECT count(*) FROM recording_jobs WHERE call_id = $1`, callID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count recording jobs: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d recording jobs for %s, want 1", n, callID)
	}

	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != 1 || durable.TotalDurationSec != 143 {
		t.Fatalf("durable stats: got %+v, want CallCount=1 TotalDurationSec=143", durable)
	}

	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get cached stats: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var cached struct {
		CallCount        int64 `json:"call_count"`
		TotalDurationSec int64 `json:"total_duration_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cached); err != nil {
		t.Fatalf("decode cached stats: %v", err)
	}
	if cached.CallCount != 1 || cached.TotalDurationSec != 143 {
		t.Fatalf("cached stats: got %+v, want CallCount=1 TotalDurationSec=143", cached)
	}
}

func TestConcurrentDuplicateDeliveriesAreIdempotent(t *testing.T) {
	const deliveries = 50

	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	body := eventJSON(eventID, callID, accountID)

	type result struct {
		status int
		err    error
	}
	results := make(chan result, deliveries)
	start := make(chan struct{})
	client := &http.Client{Timeout: 10 * time.Second}

	var wg sync.WaitGroup
	wg.Add(deliveries)
	for range deliveries {
		go func() {
			defer wg.Done()
			<-start

			resp, err := client.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				results <- result{err: err}
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			results <- result{status: resp.StatusCode}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	delivery := 0
	for got := range results {
		delivery++
		if got.err != nil {
			t.Errorf("delivery %d: %v", delivery, got.err)
			continue
		}
		if got.status != http.StatusOK {
			t.Errorf("delivery %d: got %d, want 200", delivery, got.status)
		}
	}

	ctx := context.Background()
	var events, calls, jobs int
	if err := st.Pool().QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM events WHERE event_id = $1),
			(SELECT count(*) FROM calls WHERE call_id = $2),
			(SELECT count(*) FROM recording_jobs WHERE call_id = $2)
	`, eventID, callID).Scan(&events, &calls, &jobs); err != nil {
		t.Fatalf("count stored records: %v", err)
	}
	if events != 1 || calls != 1 || jobs != 1 {
		t.Fatalf("stored records: got events=%d calls=%d jobs=%d, want events=1 calls=1 jobs=1", events, calls, jobs)
	}

	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != 1 || durable.TotalDurationSec != 143 {
		t.Fatalf("durable stats: got %+v, want CallCount=1 TotalDurationSec=143", durable)
	}

	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get cached stats: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var cached struct {
		CallCount        int64 `json:"call_count"`
		TotalDurationSec int64 `json:"total_duration_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cached); err != nil {
		t.Fatalf("decode cached stats: %v", err)
	}
	if cached.CallCount != 1 || cached.TotalDurationSec != 143 {
		t.Fatalf("cached stats: got %+v, want CallCount=1 TotalDurationSec=143", cached)
	}
}
