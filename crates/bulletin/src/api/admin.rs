//! The admin-plane gRPC service: control-plane management + read access mirroring the `debug`
//! operator commands. Authentication is enforced once by the interceptor in [`super`] (not per
//! handler), so each handler is pure "convert → call core → convert"; the `core` store fns open their
//! own `ScopeCtx::Admin` transaction, so there's no scope ceremony here either.

use bulletin_core::{digest, ingest, status};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::proto::admin_service_server::AdminService;
use super::{convert, error, proto};

/// The default row cap for the list RPCs when the caller leaves `limit` unset — matches the CLI's.
const DEFAULT_LIST_LIMIT: i64 = 20;

pub struct AdminApi {
    pool: PgPool,
}

impl AdminApi {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
