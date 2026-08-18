package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordConcurrent(t *testing.T) {
	const (
		workers          = 32
		recordsPerWorker = 1_000
	)

	c := stats.NewCache()
	var wg sync.WaitGroup
	wg.Add(workers)

	for worker := 0; worker < workers; worker++ {
		durationSec := worker + 1
		go func() {
			defer wg.Done()
			for range recordsPerWorker {
				c.Record("acc_concurrent", durationSec)
			}
		}()
	}

	wg.Wait()

	got := c.Get("acc_concurrent")
	wantCallCount := int64(workers * recordsPerWorker)
	wantTotalDuration := int64(recordsPerWorker * workers * (workers + 1) / 2)
	if got.CallCount != wantCallCount || got.TotalDurationSec != wantTotalDuration {
		t.Fatalf("got %+v, want CallCount=%d TotalDurationSec=%d", got, wantCallCount, wantTotalDuration)
	}
}

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}
