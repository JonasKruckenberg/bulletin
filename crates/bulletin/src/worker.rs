use apalis::prelude::*;
use apalis_cron::{CronStream, Tick};
use chrono::Utc;
use cron::Schedule;
use sqlx::PgPool;
use std::str::FromStr;

pub async fn setup_storage(pool: &PgPool) -> Result<(), sqlx::Error> {
    apalis_postgres::PostgresStorage::setup(pool).await
}

async fn handle_tick(_: Tick<Utc>) -> Result<(), BoxDynError> {
    tracing::info!("tick");
    Ok(())
}

pub async fn start(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    setup_storage(&pool).await?;

    let schedule = Schedule::from_str("0 * * * * *")?;

    Monitor::new()
        .register(move |_| {
            WorkerBuilder::new("bulletin-tick")
                .backend(CronStream::new(schedule.clone()))
                .build(handle_tick)
        })
        .run()
        .await?;

    Ok(())
}
