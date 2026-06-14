//! The admin-plane gRPC service: control-plane management + read access mirroring the `debug`
//! operator commands. Each handler authenticates the admin bearer, then calls the matching `core`
//! store fn — which opens its own `ScopeCtx::Admin` transaction, so there is no scope ceremony here.

use std::sync::Arc;

use bulletin_core::digest::subscriber::Recurrence;
use bulletin_core::kind::SourceKind;
use bulletin_core::{digest, ingest, status};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::auth::AuthState;
use super::proto::admin_service_server::AdminService;
use super::{convert, error, proto};

pub struct AdminApi {
    pool: PgPool,
    auth: Arc<AuthState>,
}

impl AdminApi {
    pub fn new(pool: PgPool, auth: Arc<AuthState>) -> Self {
        Self { pool, auth }
    }
}

fn parse_uuid(s: &str, field: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s).map_err(|_| Status::invalid_argument(format!("{field} must be a UUID")))
}

#[tonic::async_trait]
impl AdminService for AdminApi {
    async fn list_connections(
        &self,
        req: Request<proto::ListConnectionsRequest>,
    ) -> Result<Response<proto::ListConnectionsResponse>, Status> {
        self.auth.require_admin(req.metadata())?;
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
        self.auth.require_admin(req.metadata())?;
        let r = req.into_inner();

        let source = SourceKind::try_from(r.source.as_str())
            .map_err(|_| Status::invalid_argument(format!("unknown source '{}'", r.source)))?;
        // A private-capable source must be owned, or its private events would have no scope to bind to
        // (the DB CHECK enforces this too; this is the friendly up-front error — parity with `debug`).
        if source.can_emit_private() && r.owner.is_none() {
            return Err(Status::invalid_argument(format!(
                "a {} connection can see private content and must be owned — set `owner`",
                source.as_str()
            )));
        }
        let owner = match r.owner.as_deref() {
            Some(s) => Some(parse_uuid(s, "owner")?),
            None => None,
        };
        let config: serde_json::Value = serde_json::from_str(&r.config_json)
            .map_err(|e| Status::invalid_argument(format!("config_json is not valid JSON: {e}")))?;
        // Webhook routing key: for GitHub the installation_id (in config, not a secret) doubles as the
        // provider_account_id so content/lifecycle webhooks resolve to THIS row (the IDOR boundary).
        let provider_account_id = match source {
            SourceKind::Github => Some(
                config
                    .get("installation_id")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "a github config_json needs an integer \"installation_id\"",
                        )
                    })?
                    .to_string(),
            ),
            _ => None,
        };
        let poll_interval = if r.poll_interval_secs > 0 {
            r.poll_interval_secs
        } else {
            900
        };

        let id = ingest::store::insert_connection(
            &self.pool,
            source,
            config,
            poll_interval,
            owner,
            provider_account_id.as_deref(),
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
        self.auth.require_admin(req.metadata())?;
        let id = parse_uuid(&req.into_inner().id, "id")?;
        let deleted = ingest::store::delete_connection(&self.pool, id)
            .await
            .map_err(error::db("delete connection"))?;
        Ok(Response::new(proto::DeleteConnectionResponse { deleted }))
    }

    async fn list_subscribers(
        &self,
        req: Request<proto::ListSubscribersRequest>,
    ) -> Result<Response<proto::ListSubscribersResponse>, Status> {
        self.auth.require_admin(req.metadata())?;
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
        self.auth.require_admin(req.metadata())?;
        let r = req.into_inner();

        let freq = if r.freq.is_empty() { "daily" } else { &r.freq };
        let recurrence =
            Recurrence::new(freq, r.weekday).map_err(Status::invalid_argument)?;
        let digest_time_str = if r.digest_time.is_empty() {
            "09:00"
        } else {
            &r.digest_time
        };
        let digest_time = chrono::NaiveTime::parse_from_str(digest_time_str, "%H:%M")
            .map_err(|_| Status::invalid_argument("digest_time must be HH:MM (24-hour)"))?;
        let timezone = if r.timezone.is_empty() {
            "UTC"
        } else {
            &r.timezone
        };

        let id = digest::subscriber::insert_subscriber(
            &self.pool,
            &r.email,
            r.name.as_deref(),
            recurrence,
            timezone,
            digest_time,
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
        self.auth.require_admin(req.metadata())?;
        let id = parse_uuid(&req.into_inner().id, "id")?;
        let deleted = digest::subscriber::delete_subscriber(&self.pool, id)
            .await
            .map_err(error::db("delete subscriber"))?;
        Ok(Response::new(proto::DeleteSubscriberResponse { deleted }))
    }

    async fn get_status(
        &self,
        req: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::StatusReport>, Status> {
        self.auth.require_admin(req.metadata())?;
        let report = status::gather(&self.pool)
            .await
            .map_err(error::db("gather status"))?;
        Ok(Response::new(convert::status_report(report)))
    }

    async fn list_events(
        &self,
        req: Request<proto::ListEventsRequest>,
    ) -> Result<Response<proto::ListEventsResponse>, Status> {
        self.auth.require_admin(req.metadata())?;
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
        self.auth.require_admin(req.metadata())?;
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

/// A non-positive limit means "unset" → the CLI default of 20; otherwise honour the caller's value.
fn normalize_limit(limit: i64) -> i64 {
    if limit > 0 {
        limit
    } else {
        20
    }
}
