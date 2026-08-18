// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const (
	recordingWork         = 50 * time.Millisecond
	recordingPollInterval = 25 * time.Millisecond
	recordingLease        = 30 * time.Second
)

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest durably stores a delivery and its recording job. Recording processing
// is independent of the request and is performed by RunRecordingWorker.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	accepted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !accepted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)
	return nil
}

// RunRecordingWorker polls durable jobs until ctx is canceled. Only one job is
// processed at a time; multiple service instances safely share the queue via
// leased claims in Store.
func (s *Service) RunRecordingWorker(ctx context.Context, leaseOwner string) {
	for {
		job, found, err := s.store.ClaimRecordingJob(ctx, leaseOwner, recordingLease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("claim recording job", "err", err)
		} else if found {
			if err := s.processRecording(ctx, job); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("process recording job", "call_id", job.CallID, "err", err)
			}
		}

		timer := time.NewTimer(recordingPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// processRecording stands in for downloading and transcoding one recording.
func (s *Service) processRecording(ctx context.Context, job store.RecordingJob) error {
	timer := time.NewTimer(recordingWork)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return s.store.CompleteRecordingJob(ctx, job)
}
