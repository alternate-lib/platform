use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use tokio::{fs, io::Error as IoError};

use crate::StorageClient;

#[derive(Debug, Clone, Default)]
pub struct FsClient {
    root: Option<PathBuf>,
}

impl FsClient {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(config), fields(root = ?config.root), err(Debug))
    )]
    pub async fn create(config: FsConfig) -> Result<Self, FsClientError> {
        if let Some(root) = &config.root {
            let metadata = match fs::metadata(root).await {
                Ok(metadata) => metadata,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    return Err(FsClientError::RootNotFound(root.clone()));
                }
                Err(e) => return Err(FsClientError::Io(e)),
            };
            if !metadata.is_dir() {
                return Err(FsClientError::RootNotADirectory(root.clone()));
            }
        }

        Ok(Self { root: config.root })
    }

    fn resolve(&self, path: &str) -> Result<PathBuf, FsClientError> {
        match &self.root {
            Some(root) => {
                let path = Path::new(path);
                if path.is_absolute() {
                    return Err(FsClientError::AbsolutePath(path.display().to_string()));
                }

                Ok(root.join(path))
            }
            None => Ok(PathBuf::from(path)),
        }
    }
}

impl StorageClient for FsClient {
    type Error = FsClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = self.resolve(path)?;

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
        let path = self.resolve(path)?;

        let exists = fs::try_exists(path).await?;

        Ok(exists)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, data), err(Debug))
    )]
    async fn put(&self, path: &str, data: &[u8]) -> Result<(), Self::Error> {
        let path = self.resolve(path)?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::write(path, data).await?;

        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, path: &str) -> Result<(), Self::Error> {
        let path = self.resolve(path)?;

        fs::remove_file(path).await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsConfig {
    root: Option<PathBuf>,
}

impl FsConfig {
    pub fn builder() -> FsConfigBuilder {
        FsConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct FsConfigBuilder {
    root: Option<PathBuf>,
}

impl FsConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn root(mut self, root: impl Into<PathBuf>) -> Self {
        self.root = Some(root.into());
        self
    }

    pub fn build(self) -> FsConfig {
        FsConfig { root: self.root }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsClientError {
    #[error("root path does not exist: {0}")]
    RootNotFound(PathBuf),

    #[error("root path is not a directory: {0}")]
    RootNotADirectory(PathBuf),

    #[error("path must be relative when a root is configured: {0}")]
    AbsolutePath(String),

    #[error(transparent)]
    Io(#[from] IoError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_to_no_root() {
        let config = FsConfig::builder().build();

        assert_eq!(config, FsConfig { root: None });
    }

    #[test]
    fn builder_accepts_root() {
        let config = FsConfig::builder().root("/var/lib/alternate").build();

        assert_eq!(
            config,
            FsConfig {
                root: Some("/var/lib/alternate".into())
            }
        );
    }
}
