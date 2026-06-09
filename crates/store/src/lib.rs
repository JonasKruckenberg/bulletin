pub mod cluster;
pub mod connection;
pub mod digest;
pub mod event;
pub mod status;
pub mod subscriber;

use sqlx::PgPool;

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPool::connect(database_url).await
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    let mut m = sqlx::migrate!("./migrations");
    m.ignore_missing = true;
    m.run(pool).await
}
