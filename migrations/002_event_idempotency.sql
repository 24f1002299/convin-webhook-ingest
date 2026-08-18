BEGIN;

LOCK TABLE events, calls, account_stats IN ACCESS EXCLUSIVE MODE;

-- Keep the first stored copy of each provider event.
WITH duplicate_events AS (
    SELECT id
    FROM (
        SELECT id, row_number() OVER (PARTITION BY event_id ORDER BY id) AS copy_number
        FROM events
    ) AS ranked_events
    WHERE copy_number > 1
)
DELETE FROM events
USING duplicate_events
WHERE events.id = duplicate_events.id;

-- Calls are unique by call_id, so rebuild the durable totals from that source.
DELETE FROM account_stats;

INSERT INTO account_stats (account_id, call_count, total_duration_sec)
SELECT account_id, count(*), sum(duration_sec)
FROM calls
GROUP BY account_id;

DROP INDEX IF EXISTS idx_events_event_id;

ALTER TABLE events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);

COMMIT;
