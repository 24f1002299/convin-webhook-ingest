// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const (
	recordingWork         = 50 * time.Millisecond
	recordingPollInterval = 25 * time.Millisecond
	recordingLease        = 30 * time.Second
	recordingRetryBase    = 500 * time.Millisecond
	recordingRetryMax     = 30 * time.Second
)

// RecordingProcessor performs the external work for one recording URL.
type RecordingProcessor func(context.Context, string) error

// Option customizes a Service dependency.
type Option func(*Service)

// WithRecordingProcessor replaces the recording processor, primarily for
// supplying the real downloader/transcoder or deterministic test failures.
func WithRecordingProcessor(processor RecordingProcessor) Option {
	return func(s *Service) {
		if processor != nil {
			s.processor = processor
		}
	}
}

// WithLogger replaces the service logger.
func WithLogger(log *slog.Logger) Option {
	return func(s *Service) {
		if log != nil {
			s.log = log
		}
	}
}

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	log   *slog.Logger

	processor RecordingProcessor
}

// New builds a Service after restoring its cache from durable account totals.
func New(ctx context.Context, s *store.Store, c *stats.Cache, log *slog.Logger, options ...Option) (*Service, error) {
	durableStats, err := s.AllAccountStats(ctx)
	if err != nil {
		return nil, err
	}
	cacheStats := make(map[string]stats.AccountStats, len(durableStats))
	for accountID, durable := range durableStats {
		cacheStats[accountID] = stats.AccountStats{
			CallCount:        durable.CallCount,
			TotalDurationSec: durable.TotalDurationSec,
		}
	}
	c.Replace(cacheStats)

	svc := &Service{
		store:     s,
		cache:     c,
		log:       log,
		processor: defaultRecordingProcessor,
	}
	for _, option := range options {
		option(svc)
	}
	return svc, nil
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
			if err := s.processor(ctx, job.RecordingURL); err != nil {
				if ctx.Err() != nil {
					return
				}

				retryAt := time.Now().Add(recordingRetryDelay(job.Attempts))
				retryErr := s.store.RetryRecordingJob(ctx, job, err.Error(), retryAt)
				s.log.Error("recording processing failed",
					"call_id", job.CallID,
					"attempt", job.Attempts,
					"error", err,
				)
				if retryErr != nil {
					s.log.Error("schedule recording retry",
						"call_id", job.CallID,
						"attempt", job.Attempts,
						"error", retryErr,
					)
				}
			} else if err := s.store.CompleteRecordingJob(ctx, job); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("complete recording job",
					"call_id", job.CallID,
					"attempt", job.Attempts,
					"error", err,
				)
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

// defaultRecordingProcessor stands in for downloading and transcoding.
func defaultRecordingProcessor(ctx context.Context, _ string) error {
	timer := time.NewTimer(recordingWork)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return nil
}

func recordingRetryDelay(attempt int) time.Duration {
	delay := recordingRetryBase
	for current := 1; current < attempt; current++ {
		if delay >= recordingRetryMax/2 {
			return recordingRetryMax
		}
		delay *= 2
	}
	if delay > recordingRetryMax {
		return recordingRetryMax
	}
	return delay
}
