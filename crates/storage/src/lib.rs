#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "s3")]
pub mod s3;

pub trait StorageClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get(&self, path: &str) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;

    fn exists(&self, path: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn put(&self, path: &str, data: &[u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(&self, path: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
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
