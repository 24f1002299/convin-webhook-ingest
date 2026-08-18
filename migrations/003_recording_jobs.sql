BEGIN;

CREATE TABLE recording_jobs (
    call_id          TEXT PRIMARY KEY REFERENCES calls (call_id) ON DELETE CASCADE,
    attempts         INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recording_jobs_lease_pair CHECK (
        (lease_owner IS NULL) = (lease_expires_at IS NULL)
    )
);

CREATE INDEX idx_recording_jobs_ready
    ON recording_jobs (next_attempt_at, lease_expires_at);

INSERT INTO recording_jobs (call_id)
SELECT call_id
FROM calls
WHERE recording_processed = FALSE
  AND recording_url IS NOT NULL
  AND recording_url <> ''
ON CONFLICT (call_id) DO NOTHING;

COMMIT;
