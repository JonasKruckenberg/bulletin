//! Domain ↔ proto conversion, kept in one place so the wire shape and the engine vocabulary drift in
//! exactly one file. These are pure projections (design §3.0): no logic, just re-encoding rows the
//! engine already materialized.

use bulletin_core::common::event::Event;
use bulletin_core::digest::store::DigestRow;
use bulletin_core::digest::subscriber::{Recurrence, SubscriberRow};
use bulletin_core::ingest::store::ConnectionRow;
use bulletin_core::status::{
    BuildStatus, ClusterStats, ConnectionStats, DigestStats, EventStats, QueueStats, StatusReport,
    SubscriberStats,
};
use chrono::{DateTime, Utc};

use super::proto;

/// A UTC instant as a protobuf `Timestamp`. Sub-second nanos are always < 1e9, so the cast is exact.
pub fn ts(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn opt_ts(dt: Option<DateTime<Utc>>) -> Option<prost_types::Timestamp> {
    dt.map(ts)
}

pub fn connection(row: ConnectionRow) -> proto::Connection {
    proto::Connection {
        id: row.id.to_string(),
        source: row.source.as_str().to_string(),
        status: row.status,
        // The config is already valid JSON in the DB; re-serializing a `Value` cannot fail.
        config_json: serde_json::to_string(&row.config).unwrap_or_default(),
        poll_interval_secs: row.poll_interval_secs,
        next_poll_at: Some(ts(row.next_poll_at)),
        last_polled_at: opt_ts(row.last_polled_at),
        consecutive_failures: row.consecutive_failures as i32,
        subscriber_id: row.subscriber_id.map(|id| id.to_string()),
    }
}

pub fn subscriber(row: SubscriberRow) -> proto::Subscriber {
    let (freq, weekday) = match row.recurrence {
        Recurrence::Daily => ("daily".to_string(), None),
        Recurrence::Weekly { weekday } => ("weekly".to_string(), Some(weekday)),
    };
    proto::Subscriber {
        id: row.id.to_string(),
        email: row.email,
        name: row.name,
        freq,
        weekday,
        max_items: row.max_items,
        timezone: row.timezone,
        digest_time: row.digest_time.format("%H:%M").to_string(),
        next_run_at: Some(ts(row.next_run_at)),
        last_run_at: opt_ts(row.last_run_at),
    }
}

pub fn event_summary(ev: Event) -> proto::EventSummary {
    proto::EventSummary {
        ingest_time: Some(ts(ev.ingest_time)),
        source: ev.source.as_str().to_string(),
        title: ev.title,
        links: ev.links,
    }
}

pub fn digest_summary(row: DigestRow, subscriber_email: String, item_count: i64) -> proto::DigestSummary {
    proto::DigestSummary {
        id: row.id.to_string(),
        subscriber_email,
        window_end: Some(ts(row.window_end)),
        delivered_at: opt_ts(row.delivered_at),
        item_count,
    }
}

pub fn status_report(r: StatusReport) -> proto::StatusReport {
    let (queue_initialized, queue) = match r.queue {
        Some(rows) => (true, rows.into_iter().map(queue_stats).collect()),
        None => (false, Vec::new()),
    };
    proto::StatusReport {
        connections: Some(connection_stats(r.connections)),
        events: Some(event_stats(r.events)),
        build: Some(build_status(r.build)),
        clusters: Some(cluster_stats(r.clusters)),
        subscribers: Some(subscriber_stats(r.subscribers)),
        digests: Some(digest_stats(r.digests)),
        queue_initialized,
        queue,
    }
}

fn connection_stats(s: ConnectionStats) -> proto::ConnectionStats {
    proto::ConnectionStats {
        total: s.total,
        active: s.active,
        paused: s.paused,
        errored: s.errored,
        due_now: s.due_now,
    }
}

fn event_stats(s: EventStats) -> proto::EventStats {
    proto::EventStats {
        total: s.total,
        unbuilt: s.unbuilt,
        latest_ingest: opt_ts(s.latest_ingest),
        by_source: s
            .by_source
            .into_iter()
            .map(|(source, count)| proto::SourceCount { source, count })
            .collect(),
    }
}

fn build_status(s: BuildStatus) -> proto::BuildStatus {
    proto::BuildStatus {
        built_through: Some(ts(s.built_through)),
        lag_secs: s.lag_secs,
    }
}

fn cluster_stats(s: ClusterStats) -> proto::ClusterStats {
    proto::ClusterStats {
        total: s.total,
        latest_updated: opt_ts(s.latest_updated),
    }
}

fn subscriber_stats(s: SubscriberStats) -> proto::SubscriberStats {
    proto::SubscriberStats {
        total: s.total,
        daily: s.daily,
        weekly: s.weekly,
        due_now: s.due_now,
        next_run: opt_ts(s.next_run),
    }
}

fn digest_stats(s: DigestStats) -> proto::DigestStats {
    proto::DigestStats {
        total: s.total,
        pending: s.pending,
        delivered: s.delivered,
        last_delivered: opt_ts(s.last_delivered),
    }
}

fn queue_stats(s: QueueStats) -> proto::QueueStats {
    proto::QueueStats {
        job_type: s.job_type,
        pending: s.pending,
        running: s.running,
        done: s.done,
        failed: s.failed,
        killed: s.killed,
        oldest_pending_secs: s.oldest_pending_secs,
    }
}
