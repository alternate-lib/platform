use std::time::Duration;

#[cfg(feature = "typed")]
pub use typed::{KvClientExpiryTyped, KvClientTyped, KvClientTypedError};

#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "typed")]
mod typed;

pub trait KvClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get(&self, key: &str) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;

    fn exists(&self, key: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn set(&self, key: &str, value: &[u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(&self, key: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait KvClientExpiry: KvClient {
    fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[async_trait::async_trait]
pub trait DynKvClient: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error>;

    async fn exists(&self, key: &str) -> Result<bool, anyhow::Error>;

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), anyhow::Error>;

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl<KC: KvClient> DynKvClient for KC {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
        KC::get(self, key).await.map_err(anyhow::Error::from)
    }

    async fn exists(&self, key: &str) -> Result<bool, anyhow::Error> {
        KC::exists(self, key).await.map_err(anyhow::Error::from)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), anyhow::Error> {
        KC::set(self, key, value).await.map_err(anyhow::Error::from)
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        KC::delete(self, key).await.map_err(anyhow::Error::from)
    }
}

#[async_trait::async_trait]
pub trait DynKvClientExpiry: DynKvClient {
    async fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl<KC: KvClientExpiry> DynKvClientExpiry for KC {
    async fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), anyhow::Error> {
        KC::set_with_ttl(self, key, value, ttl)
            .await
            .map_err(anyhow::Error::from)
    }
}
