-- Subscribers schedule on a *wall-clock target* — a local time-of-day in their own timezone —
-- instead of a fixed offset from signup. So a digest stays at e.g. 09:00 local across DST and
-- across travel: changing timezone or digest_time only reshapes where the next boundary falls,
-- it never moves the selection window's lower bound (last_run_at), so no digest is lost.

ALTER TABLE subscriber
    ADD COLUMN timezone    text NOT NULL DEFAULT 'UTC',     -- IANA name, e.g. 'America/New_York'
    ADD COLUMN digest_time time NOT NULL DEFAULT '09:00';   -- desired local delivery time-of-day

-- The one place the schedule math lives: the first occurrence of local time-of-day `t` in zone
-- `tz` strictly after `ref`. If today's slot is still ahead of `ref` it's today's; otherwise it
-- steps `step_days` forward. Callers vary only the reference and the step:
--   * signup / preference change → ref = now()-ish, step = 1   (the next earliest daily slot)
--   * advance after delivery     → ref = delivered boundary, step = interval_days  (the cadence)
-- DST-safe by construction: the day arithmetic happens on the *local* wall clock (timestamp
-- without zone), and only the final `AT TIME ZONE tz` re-anchors it to a real UTC instant — so a
-- 09:00-local digest stays 09:00 local even across a spring-forward/fall-back.
CREATE FUNCTION next_digest_boundary(ref timestamptz, tz text, t time, step_days int)
RETURNS timestamptz
LANGUAGE sql STABLE AS $$
    -- `(ref AT TIME ZONE tz)::date + t` is the local date's slot (date + time → timestamp); the
    -- whole CASE stays in the local wall-clock frame, and only the trailing AT TIME ZONE re-anchors.
    SELECT (
        CASE
            WHEN ((ref AT TIME ZONE tz)::date + t) > (ref AT TIME ZONE tz)
                THEN (ref AT TIME ZONE tz)::date + t
            ELSE (ref AT TIME ZONE tz)::date + t
                 + (GREATEST(step_days, 1) || ' days')::interval
        END
    ) AT TIME ZONE tz
$$;

-- Snap every existing subscriber onto the new grid immediately (they default to 09:00 UTC): the
-- next daily occurrence of their digest_time, in their zone, strictly after now().
UPDATE subscriber
SET next_run_at = next_digest_boundary(now(), timezone, digest_time, 1);
