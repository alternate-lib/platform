use std::time::Duration;

#[cfg(feature = "typed")]
pub use typed::{KvExpiryTyped, KvTyped, KvTypedError};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "typed")]
mod typed;

pub trait KvClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get(&self, key: &str) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;

    fn exists(&self, key: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn set(&self, key: &str, value: &[u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(&self, key: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait KvExpiry: KvClient {
    fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
