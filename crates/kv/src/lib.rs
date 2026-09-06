use std::time::Duration;

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub trait KvClient {
    type Error: std::error::Error;

    fn get(&self, key: &str) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>>;

    fn exists(&self, key: &str) -> impl Future<Output = Result<bool, Self::Error>>;

    fn set(&self, key: &str, value: &[u8]) -> impl Future<Output = Result<(), Self::Error>>;

    fn delete(&self, key: &str) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait KvExpiry: KvClient {
    fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}
