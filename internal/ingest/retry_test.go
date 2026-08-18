package ingest

import (
	"testing"
	"time"
)

func TestRecordingRetryDelayIsExponentialAndBounded(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 500 * time.Millisecond},
		{attempt: 2, want: time.Second},
		{attempt: 3, want: 2 * time.Second},
		{attempt: 6, want: 16 * time.Second},
		{attempt: 7, want: 30 * time.Second},
		{attempt: 100, want: 30 * time.Second},
	}

	for _, test := range tests {
		if got := recordingRetryDelay(test.attempt); got != test.want {
			t.Errorf("attempt %d: got %s, want %s", test.attempt, got, test.want)
		}
	}
}
