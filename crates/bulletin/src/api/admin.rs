//! The admin-plane gRPC service: control-plane management + read access mirroring the `debug`
//! operator commands. Authentication is enforced once by the interceptor in [`super`] (not per
//! handler), so each handler is pure "convert → call core → convert"; the `core` store fns open their
//! own `ScopeCtx::Admin` transaction, so there's no scope ceremony here either.

use bulletin_core::digest::select::ScoringConfig;
use bulletin_core::{cluster, digest, ingest, status, subscription};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::transport::EmailConfig;

use super::proto::admin_service_server::AdminService;
use super::proto::unstable_debug_service_server::UnstableDebugService;
use super::{convert, error, proto};

/// The default row cap for the list RPCs when the caller leaves `limit` unset — matches the CLI's.
const DEFAULT_LIST_LIMIT: i64 = 20;
/// The default eval window when the caller leaves `limit` unset — matches the CLI's `digest-eval`.
const DEFAULT_EVAL_LIMIT: i64 = 50;
/// The default ad-hoc dispatch lookback when the caller leaves it unset — matches the CLI.
const DEFAULT_LOOKBACK_DAYS: i32 = 7;

/// Clone is cheap (the pool is an `Arc` handle, the email config is small) and lets the one instance
/// back both gRPC services — the stable `AdminService` and the `UnstableDebugService` — each registered
/// behind its own copy of the auth interceptor.
#[derive(Clone)]
pub struct AdminApi {
    pool: PgPool,
    /// Email delivery config the engine owns. The send RPCs build the mailer here, server-side, so
    /// the caller (the `debug` CLI) never needs the SMTP credential to fire a digest.
    email: EmailConfig,
}

impl AdminApi {
    pub fn new(pool: PgPool, email: EmailConfig) -> Self {
        Self { pool, email }
    }
}

fn parse_uuid(s: &str, field: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s).map_err(|_| Status::invalid_argument(format!("{field} must be a UUID")))
}

/// A non-positive limit means "unset" → the default; otherwise honour the caller's value.
fn normalize_limit(limit: i64) -> i64 {
    if limit > 0 {
        limit
    } else {
        DEFAULT_LIST_LIMIT
    }
}

#[tonic::async_trait]
impl AdminService for AdminApi {
    async fn list_connections(
        &self,
        _req: Request<proto::ListConnectionsRequest>,
    ) -> Result<Response<proto::ListConnectionsResponse>, Status> {
        let rows = ingest::store::list_connections(&self.pool)
            .await
            .map_err(error::db("list connections"))?;
        Ok(Response::new(proto::ListConnectionsResponse {
            connections: rows.into_iter().map(convert::connection).collect(),
        }))
    }

    async fn create_connection(
        &self,
        req: Request<proto::CreateConnectionRequest>,
    ) -> Result<Response<proto::Connection>, Status> {
        let r = req.into_inner();
        let owner = match r.owner.as_deref() {
            Some(s) => Some(parse_uuid(s, "owner")?),
            None => None,
        };
        // Shared validation (source, owner guard, github routing key) — same path as `debug`.
        let conn =
            ingest::prepare_connection(&r.source, &r.config_json, r.poll_interval_secs, owner)
                .map_err(Status::invalid_argument)?;
        let id = ingest::store::insert_connection(
            &self.pool,
            conn.source,
            conn.config,
            conn.poll_interval_secs,
            conn.owner,
            conn.provider_account_id.as_deref(),
        )
        .await
        .map_err(error::db("insert connection"))?;

        let row = ingest::store::load_connection(&self.pool, id)
            .await
            .map_err(error::db("load connection"))?
            .ok_or_else(|| Status::internal("created connection not found"))?;
        Ok(Response::new(convert::connection(row)))
    }

    async fn delete_connection(
        &self,
        req: Request<proto::DeleteConnectionRequest>,
    ) -> Result<Response<proto::DeleteConnectionResponse>, Status> {
        let id = parse_uuid(&req.into_inner().id, "id")?;
        let deleted = ingest::store::delete_connection(&self.pool, id)
            .await
            .map_err(error::db("delete connection"))?;
        Ok(Response::new(proto::DeleteConnectionResponse { deleted }))
    }

    async fn list_subscribers(
        &self,
        _req: Request<proto::ListSubscribersRequest>,
    ) -> Result<Response<proto::ListSubscribersResponse>, Status> {
        let rows = digest::subscriber::list_subscribers(&self.pool)
            .await
            .map_err(error::db("list subscribers"))?;
        Ok(Response::new(proto::ListSubscribersResponse {
            subscribers: rows.into_iter().map(convert::subscriber).collect(),
        }))
    }

    async fn create_subscriber(
        &self,
        req: Request<proto::CreateSubscriberRequest>,
    ) -> Result<Response<proto::Subscriber>, Status> {
        let r = req.into_inner();
        // Shared cadence/timezone/time validation — including up-front IANA tz checking, so a bogus
        // zone is `InvalidArgument`, not an opaque `Internal` from the DB.
        let schedule =
            digest::subscriber::validate_schedule(&r.freq, r.weekday, &r.timezone, &r.digest_time)
                .map_err(Status::invalid_argument)?;
        let id = digest::subscriber::insert_subscriber(
            &self.pool,
            &r.email,
            r.name.as_deref(),
            schedule.recurrence,
            &schedule.timezone,
            schedule.digest_time,
        )
        .await
        .map_err(error::db("insert subscriber"))?;

        let row = digest::subscriber::load_subscriber(&self.pool, id)
            .await
            .map_err(error::db("load subscriber"))?
            .ok_or_else(|| Status::internal("created subscriber not found"))?;
        Ok(Response::new(convert::subscriber(row)))
    }

    async fn delete_subscriber(
        &self,
        req: Request<proto::DeleteSubscriberRequest>,
    ) -> Result<Response<proto::DeleteSubscriberResponse>, Status> {
        let id = parse_uuid(&req.into_inner().id, "id")?;
        let deleted = digest::subscriber::delete_subscriber(&self.pool, id)
            .await
            .map_err(error::db("delete subscriber"))?;
        Ok(Response::new(proto::DeleteSubscriberResponse { deleted }))
    }

    async fn subscribe(
        &self,
        req: Request<proto::SubscribeRequest>,
    ) -> Result<Response<proto::SubscribeResponse>, Status> {
        let r = req.into_inner();
        let subscriber = parse_uuid(&r.subscriber, "subscriber")?;
        let connection = parse_uuid(&r.connection, "connection")?;
        let created = subscription::subscribe(&self.pool, subscriber, connection)
            .await
            .map_err(error::db("subscribe"))?;
        Ok(Response::new(proto::SubscribeResponse { created }))
    }

    async fn unsubscribe(
        &self,
        req: Request<proto::UnsubscribeRequest>,
    ) -> Result<Response<proto::UnsubscribeResponse>, Status> {
        let r = req.into_inner();
        let subscriber = parse_uuid(&r.subscriber, "subscriber")?;
        let connection = parse_uuid(&r.connection, "connection")?;
        let removed = subscription::unsubscribe(&self.pool, subscriber, connection)
            .await
            .map_err(error::db("unsubscribe"))?;
        Ok(Response::new(proto::UnsubscribeResponse { removed }))
    }

    async fn list_subscriptions(
        &self,
        req: Request<proto::ListSubscriptionsRequest>,
    ) -> Result<Response<proto::ListSubscriptionsResponse>, Status> {
        let subscriber = parse_uuid(&req.into_inner().subscriber, "subscriber")?;
        let rows = subscription::list_subscriptions(&self.pool, subscriber)
            .await
            .map_err(error::db("list subscriptions"))?;
        Ok(Response::new(proto::ListSubscriptionsResponse {
            connections: rows.into_iter().map(convert::connection).collect(),
        }))
    }

    async fn get_status(
        &self,
        _req: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::StatusReport>, Status> {
        let report = status::gather(&self.pool)
            .await
            .map_err(error::db("gather status"))?;
        Ok(Response::new(convert::status_report(report)))
    }

    async fn list_events(
        &self,
        req: Request<proto::ListEventsRequest>,
    ) -> Result<Response<proto::ListEventsResponse>, Status> {
        let limit = normalize_limit(req.into_inner().limit);
        let events = ingest::store::list_events(&self.pool, limit)
            .await
            .map_err(error::db("list events"))?;
        Ok(Response::new(proto::ListEventsResponse {
            events: events.into_iter().map(convert::event_summary).collect(),
        }))
    }

    async fn list_digests(
        &self,
        req: Request<proto::ListDigestsRequest>,
    ) -> Result<Response<proto::ListDigestsResponse>, Status> {
        let limit = normalize_limit(req.into_inner().limit);
        let rows = digest::store::list_digests(&self.pool, limit)
            .await
            .map_err(error::db("list digests"))?;
        Ok(Response::new(proto::ListDigestsResponse {
            digests: rows
                .into_iter()
                .map(|(d, email, count)| convert::digest_summary(d, email, count))
                .collect(),
        }))
    }
}

/// The UNSTABLE debug/operator plane (mirrors the `bulletin debug …` commands). A distinct service so
/// its instability is in the name — it can't be mistaken for the wire-stable `AdminService`. Same
/// `AdminApi` backs both: identical auth (admin bearer) and scope (`ScopeCtx::Admin`).
#[tonic::async_trait]
impl UnstableDebugService for AdminApi {
    async fn run_build(
        &self,
        _req: Request<proto::RunBuildRequest>,
    ) -> Result<Response<proto::RunBuildResponse>, Status> {
        let resp = match cluster::build(&self.pool)
            .await
            .map_err(error::internal("run build"))?
        {
            Some(stats) => proto::RunBuildResponse {
                skipped: false,
                dirty_groups: stats.dirty_groups as u64,
                built_through: Some(convert::ts(stats.built_through)),
            },
            None => proto::RunBuildResponse {
                skipped: true,
                dirty_groups: 0,
                built_through: None,
            },
        };
        Ok(Response::new(resp))
    }

    async fn run_digest(
        &self,
        req: Request<proto::RunDigestRequest>,
    ) -> Result<Response<proto::DigestOutcome>, Status> {
        let subscriber = parse_uuid(&req.into_inner().subscriber, "subscriber")?;
        let sender = self.build_sender()?;
        // A manual operator run is a single shot (attempt 0): if the lead defers, the outcome says so and
        // the operator can re-run; there's no apalis retry behind this RPC.
        let outcome = digest::generate(&self.pool, &sender, subscriber, &self.email.content(), 0)
            .await
            .map_err(error::internal("generate digest"))?;
        Ok(Response::new(convert::digest_outcome(outcome)))
    }

    async fn dispatch_digest(
        &self,
        req: Request<proto::DispatchDigestRequest>,
    ) -> Result<Response<proto::DigestOutcome>, Status> {
        let r = req.into_inner();
        let subscriber = parse_uuid(&r.subscriber, "subscriber")?;
        // 0 ⇒ default; a negative window is rejected (the CLI guarded `>= 1` inline).
        let lookback_days = if r.lookback_days == 0 {
            DEFAULT_LOOKBACK_DAYS
        } else if r.lookback_days < 1 {
            return Err(Status::invalid_argument("lookback_days must be >= 1"));
        } else {
            r.lookback_days
        };
        let sender = self.build_sender()?;
        let outcome = digest::dispatch_now(
            &self.pool,
            &sender,
            subscriber,
            lookback_days,
            &self.email.content(),
        )
        .await
        .map_err(error::internal("dispatch digest"))?;
        Ok(Response::new(convert::digest_outcome(outcome)))
    }

    async fn explain_digest(
        &self,
        req: Request<proto::ExplainDigestRequest>,
    ) -> Result<Response<proto::ExplainDigestResponse>, Status> {
        let subscriber = parse_uuid(&req.into_inner().subscriber, "subscriber")?;
        let rows = digest::explain(&self.pool, subscriber)
            .await
            .map_err(error::internal("explain digest"))?;
        Ok(Response::new(proto::ExplainDigestResponse {
            rows: rows.into_iter().map(convert::explain_row).collect(),
        }))
    }

    async fn eval_digest(
        &self,
        req: Request<proto::EvalDigestRequest>,
    ) -> Result<Response<proto::EvalDigestResponse>, Status> {
        let r = req.into_inner();
        let subscriber = parse_uuid(&r.subscriber, "subscriber")?;
        let limit = if r.limit > 0 {
            r.limit
        } else {
            DEFAULT_EVAL_LIMIT
        };
        // The metrics report is rendered by core's `Metrics` Display so layout stays in one place.
        let resp = match r.trial_config_json {
            None => proto::EvalDigestResponse {
                baseline: digest::eval_report(&self.pool, subscriber, limit)
                    .await
                    .map_err(error::internal("eval report"))?
                    .to_string(),
                trial: None,
            },
            Some(json) => {
                let trial: ScoringConfig = serde_json::from_str(&json)
                    .map_err(|e| Status::invalid_argument(format!("parse trial config: {e}")))?;
                let (base, trial_m) = digest::eval_sweep(&self.pool, subscriber, limit, trial)
                    .await
                    .map_err(error::internal("eval sweep"))?;
                proto::EvalDigestResponse {
                    baseline: base.to_string(),
                    trial: Some(trial_m.to_string()),
                }
            }
        };
        Ok(Response::new(resp))
    }

    async fn get_digest_provenance(
        &self,
        req: Request<proto::GetDigestProvenanceRequest>,
    ) -> Result<Response<proto::GetDigestProvenanceResponse>, Status> {
        let story_id = parse_uuid(&req.into_inner().story_id, "story_id")?;
        let entries = digest::provenance(&self.pool, story_id)
            .await
            .map_err(error::internal("digest provenance"))?;
        Ok(Response::new(proto::GetDigestProvenanceResponse {
            entries: entries.into_iter().map(convert::timeline_entry).collect(),
        }))
    }

    async fn get_config(
        &self,
        _req: Request<proto::GetConfigRequest>,
    ) -> Result<Response<proto::ScoringConfig>, Status> {
        let cfg = digest::get_config(&self.pool)
            .await
            .map_err(error::internal("get config"))?;
        Ok(Response::new(convert::scoring_config(cfg)))
    }

    async fn set_config(
        &self,
        req: Request<proto::SetConfigRequest>,
    ) -> Result<Response<proto::ScoringConfig>, Status> {
        let r = req.into_inner();
        // Sparse merge: load the live config, overwrite only the fields the caller sent — same
        // semantics as `debug config-set`, just executed engine-side.
        let mut cfg = digest::get_config(&self.pool)
            .await
            .map_err(error::internal("get config"))?;
        if let Some(v) = r.relevance_floor {
            cfg.relevance_floor = v;
        }
        if let Some(v) = r.scope_bonus {
            cfg.scope_bonus = v;
        }
        if let Some(v) = r.severity_weight {
            cfg.severity_weight = v;
        }
        if let Some(v) = r.recency_half_life_days {
            cfg.recency_half_life_days = v;
        }
        if let Some(v) = r.thread_half_life_days {
            cfg.thread_half_life_days = v;
        }
        if let Some(v) = r.story_cap {
            cfg.story_cap = v as usize;
        }
        if let Some(v) = r.note_cap {
            cfg.note_cap = v as usize;
        }
        if let Some(v) = r.resurface_penalty {
            cfg.resurface_penalty = v;
        }
        if let Some(v) = r.resurface_cap {
            cfg.resurface_cap = v as usize;
        }
        digest::set_config(&self.pool, cfg)
            .await
            .map_err(error::internal("set config"))?;
        Ok(Response::new(convert::scoring_config(cfg)))
    }
}

impl AdminApi {
    /// Build the engine's mailer for a send RPC. A misconfigured transport is a *server* config fault
    /// (the caller can't fix it), so it surfaces as `Internal` rather than `InvalidArgument`.
    fn build_sender(&self) -> Result<crate::transport::Sender, Status> {
        self.email
            .build_sender()
            .map_err(|e| Status::internal(format!("build mailer: {e}")))
    }
}
