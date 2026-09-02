use std::io::ErrorKind;

use tokio::{fs, io::Error as IoError};

use crate::StorageClient;

#[derive(Clone, Default)]
pub struct FsClient;

impl StorageClient for FsClient {
    type Error = FsClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        match fs::read(path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FsClientError::Io(e)),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn exists(&self, path: &str) -> Result<bool, Self::Error> {
        let exists = fs::try_exists(path).await?;

        Ok(exists)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, data), err(Debug))
    )]
    async fn put(&self, path: &str, data: &[u8]) -> Result<(), Self::Error> {
        fs::write(path, data).await?;

        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, path: &str) -> Result<(), Self::Error> {
        fs::remove_file(path).await?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsClientError {
    #[error(transparent)]
    Io(#[from] IoError),
}
