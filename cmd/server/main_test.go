package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestRunShutsDownPromptlyAndPreservesPendingJobs(t *testing.T) {
	seedStore, databaseURL := testutil.NewIsolatedStore(t)
	eventID, callID, accountID := testutil.IDs(t, seedStore)
	databaseCtx := context.Background()

	accepted, err := seedStore.IngestEvent(databaseCtx, store.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/pending.wav",
		Payload:      []byte(`{}`),
	})
	if err != nil || !accepted {
		t.Fatalf("seed recording job: accepted=%t err=%v", accepted, err)
	}
	if _, err := seedStore.Pool().Exec(databaseCtx, `
		UPDATE recording_jobs
		SET next_attempt_at = now() + interval '1 hour'
		WHERE call_id = $1
	`, callID); err != nil {
		t.Fatalf("schedule pending job: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve HTTP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release HTTP address: %v", err)
	}

	cfg := config.Load()
	cfg.HTTPAddr = address
	cfg.PostgresDSN = databaseURL
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	runStopped := false
	go func() { runDone <- run(runCtx, cfg, log) }()
	t.Cleanup(func() {
		stopRun()
		if runStopped {
			return
		}
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("server did not stop during cleanup")
		}
	})

	client := &http.Client{Timeout: 100 * time.Millisecond}
	healthURL := "http://" + address + "/healthz"
	readyDeadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := client.Get(healthURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(readyDeadline) {
			t.Fatal("server did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	shutdownStarted := time.Now()
	stopRun()
	select {
	case err := <-runDone:
		runStopped = true
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within 2 seconds")
	}
	if elapsed := time.Since(shutdownStarted); elapsed >= shutdownTimeout {
		t.Fatalf("shutdown took %s, timeout is %s", elapsed, shutdownTimeout)
	}

	var jobs int
	var unleased, processed bool
	if err := seedStore.Pool().QueryRow(databaseCtx, `
		SELECT
			count(*),
			bool_and(lease_owner IS NULL AND lease_expires_at IS NULL),
			(SELECT recording_processed FROM calls WHERE call_id = $1)
		FROM recording_jobs
		WHERE call_id = $1
	`, callID).Scan(&jobs, &unleased, &processed); err != nil {
		t.Fatalf("read pending job after shutdown: %v", err)
	}
	if jobs != 1 || !unleased || processed {
		t.Fatalf("pending work changed: jobs=%d unleased=%t processed=%t", jobs, unleased, processed)
	}

	if resp, err := client.Get(healthURL); err == nil {
		_ = resp.Body.Close()
		t.Fatal("HTTP server still accepts traffic after shutdown")
	}
}
