#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "s3")]
pub mod s3;

use std::time::Duration;

pub trait StorageClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get(&self, path: &str) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;

    fn exists(&self, path: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn put(&self, path: &str, data: &[u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(&self, path: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait StorageClientPresign: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get_presigned(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> impl Future<Output = Result<String, Self::Error>> + Send;

    fn put_presigned(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> impl Future<Output = Result<String, Self::Error>> + Send;
}

#[async_trait::async_trait]
pub trait DynStorageClient: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error>;

    async fn exists(&self, key: &str) -> Result<bool, anyhow::Error>;

    async fn put(&self, key: &str, data: &[u8]) -> Result<(), anyhow::Error>;

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl<SC: StorageClient> DynStorageClient for SC {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
        SC::get(self, key).await.map_err(anyhow::Error::from)
    }

    async fn exists(&self, key: &str) -> Result<bool, anyhow::Error> {
        SC::exists(self, key).await.map_err(anyhow::Error::from)
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<(), anyhow::Error> {
        SC::put(self, key, data).await.map_err(anyhow::Error::from)
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        SC::delete(self, key).await.map_err(anyhow::Error::from)
    }
}

#[async_trait::async_trait]
pub trait DynStorageClientPresign: Send + Sync {
    async fn get_presigned(&self, key: &str, expires_in: Duration)
    -> Result<String, anyhow::Error>;

    async fn put_presigned(&self, key: &str, expires_in: Duration)
    -> Result<String, anyhow::Error>;
}

#[async_trait::async_trait]
impl<SC: StorageClientPresign> DynStorageClientPresign for SC {
    async fn get_presigned(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> Result<String, anyhow::Error> {
        SC::get_presigned(self, key, expires_in)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn put_presigned(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> Result<String, anyhow::Error> {
        SC::put_presigned(self, key, expires_in)
            .await
            .map_err(anyhow::Error::from)
    }
}
