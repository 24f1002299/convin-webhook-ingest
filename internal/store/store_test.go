package store_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestIngestEventPersistsAllEffects(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	accepted, err := s.IngestEvent(ctx, evt)
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	if !accepted {
		t.Fatal("expected event to be accepted")
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}

	var storedAccount string
	if err := s.Pool().QueryRow(ctx,
		`SELECT account_id FROM calls WHERE call_id = $1`, callID).Scan(&storedAccount); err != nil {
		t.Fatalf("read call: %v", err)
	}
	if storedAccount != accountID {
		t.Fatalf("call account: got %q, want %q", storedAccount, accountID)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 10 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=10", got)
	}
}

func TestIngestEventAccumulatesAccountStats(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	for _, evt := range []store.Event{
		{EventID: eventID, CallID: callID, AccountID: accountID, Status: "completed", DurationSec: 30, Payload: []byte(`{}`)},
		{EventID: eventID + "_2", CallID: callID + "_2", AccountID: accountID, Status: "completed", DurationSec: 12, Payload: []byte(`{}`)},
	} {
		if _, err := s.IngestEvent(ctx, evt); err != nil {
			t.Fatalf("IngestEvent(%s): %v", evt.EventID, err)
		}
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestIngestEventRollsBackOnFailure(t *testing.T) {
	const maxInt64 = int64(1<<63 - 1)

	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		VALUES ($1, 7, $2)
	`, accountID, maxInt64); err != nil {
		t.Fatalf("seed account stats: %v", err)
	}

	accepted, err := s.IngestEvent(ctx, store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 1, Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("IngestEvent: got nil error, want aggregate overflow")
	}
	if accepted {
		t.Fatal("failed event must not be accepted")
	}

	exists, existsErr := s.EventExists(ctx, eventID)
	if existsErr != nil {
		t.Fatalf("EventExists: %v", existsErr)
	}
	if exists {
		t.Fatal("event insert was not rolled back")
	}

	var calls int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM calls WHERE call_id = $1`, callID).Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if calls != 0 {
		t.Fatalf("call rows: got %d, want 0", calls)
	}

	got, statsErr := s.AccountStats(ctx, accountID)
	if statsErr != nil {
		t.Fatalf("AccountStats: %v", statsErr)
	}
	if got.CallCount != 7 || got.TotalDurationSec != maxInt64 {
		t.Fatalf("stats changed after rollback: got %+v, want CallCount=7 TotalDurationSec=%d", got, maxInt64)
	}
}

func TestIngestEventThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if _, err := s.IngestEvent(ctx, evt); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}
