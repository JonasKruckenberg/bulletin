-- Recurrence schedule: replace the interval_days "period" with an explicit daily/weekly recurrence
-- at a local time-of-day. "weekly Tuesdays 17:00" now pins a stable weekday (not "every 7 days from
-- whenever the last one happened to land"), and the boundary is computed on the subscriber's wall
-- clock so it stays put across DST. Supersedes next_digest_boundary (dropped below).

ALTER TABLE subscriber
    ADD COLUMN freq       text NOT NULL DEFAULT 'daily' CHECK (freq IN ('daily', 'weekly')),
    ADD COLUMN on_weekday int  CHECK (on_weekday BETWEEN 0 AND 6);   -- 0=Sun..6=Sat (Postgres DOW)

-- weekly carries a weekday; daily must not — keeps the two states from drifting.
ALTER TABLE subscriber
    ADD CONSTRAINT subscriber_weekday_iff_weekly CHECK ((freq = 'weekly') = (on_weekday IS NOT NULL));

-- The one schedule function: the first occurrence of local `at_time` in `tz` strictly after `ref`,
-- on the right weekday for weekly. All day arithmetic is on the *local* wall clock (timestamp
-- without zone); only the final AT TIME ZONE re-anchors to UTC → DST-safe by construction. Callers
-- vary only the reference instant: signup/advance pass now(), a preference change passes
-- max(now, last_run) so it snaps to the next earliest slot without losing the pending window.
CREATE FUNCTION next_run(ref timestamptz, tz text, at_time time, freq text, on_weekday int)
RETURNS timestamptz
LANGUAGE plpgsql STABLE AS $$
DECLARE
    local_ref timestamp := ref AT TIME ZONE tz;   -- wall clock in tz
    cand      timestamp;
    shift     int;
BEGIN
    IF freq = 'daily' THEN
        cand := local_ref::date + at_time;
        IF cand <= local_ref THEN
            cand := cand + interval '1 day';
        END IF;
    ELSIF freq = 'weekly' THEN
        shift := ((on_weekday - extract(dow from local_ref)::int) % 7 + 7) % 7;  -- 0..6 days ahead
        cand := (local_ref::date + shift) + at_time;
        IF cand <= local_ref THEN
            cand := cand + interval '7 days';
        END IF;
    ELSE
        RAISE EXCEPTION 'next_run: unknown freq %', freq;
    END IF;
    RETURN cand AT TIME ZONE tz;
END;
$$;

-- Migrate existing rows onto the recurrence: a weekly cadence (>= 7 days) keeps its current local
-- weekday, derived from the *original* next_run_at (0007 left it untouched for exactly this). Then
-- snap next_run_at onto the new grid (weekday-aware via next_run).
UPDATE subscriber
SET freq       = 'weekly',
    on_weekday = extract(dow from (next_run_at AT TIME ZONE timezone))::int
WHERE interval_days >= 7;

UPDATE subscriber
SET next_run_at = next_run(now(), timezone, digest_time, freq, on_weekday);

ALTER TABLE subscriber DROP COLUMN interval_days;
DROP FUNCTION next_digest_boundary(timestamptz, text, time, int);
