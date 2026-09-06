use std::fmt::Debug;

use alternate_migration::Migration;
pub use consumer::*;
pub use producer::*;
use rust_embed::RustEmbed;

use crate::QueueError;

mod consumer;
mod producer;

#[derive(RustEmbed)]
#[folder = "migrations/postgres"]
struct Migrations;

/// # Panics
///
/// Panics if embedded migration cannot be found
pub fn migrations() -> Result<Vec<Migration>, alternate_migration::MigrationError> {
    Migrations::iter()
        .map(|filename| {
            let migration = Migrations::get(&filename).expect("migration file not found");
            let sql = String::from_utf8_lossy(migration.data.as_ref()).to_string();

            Migration::try_new(&filename, sql)
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum PostgresQueueError {
    #[error(transparent)]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),

    #[error(transparent)]
    Queue(#[from] QueueError),

    #[error("message attributes could not be decoded: {0}")]
    Attributes(#[from] serde_json::Error),

    #[error("attempt limit must not overflow")]
    InvalidAttemptLimit,

    #[error("duration must not overflow")]
    InvalidDuration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embeds_migrations() {
        assert_eq!(
            migrations().expect("embedded migrations are valid").len(),
            1
        );
    }
}
