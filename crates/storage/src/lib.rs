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

pub trait StoragePresign: StorageClient {
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
