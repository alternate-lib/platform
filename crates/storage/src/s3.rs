use s3::{
    Bucket, Region,
    creds::{Credentials, error::CredentialsError},
    error::S3Error,
};

use crate::StorageClient;

#[derive(Clone)]
pub struct S3Client {
    bucket: Box<Bucket>,
}

impl S3Client {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(config), fields(access_key_id = config.access_key_id, region = config.region, endpoint = config.endpoint, bucket = config.bucket), err(Debug))
    )]
    pub async fn init(config: S3Config) -> Result<Self, S3ClientError> {
        let region = Region::Custom {
            region: config.region,
            endpoint: config.endpoint,
        };
        let credentials = Credentials::new(
            Some(&config.access_key_id),
            Some(&config.secret_access_key),
            None,
            None,
            None,
        )?;

        let mut bucket = Bucket::new(&config.bucket, region, credentials)?;

        if config.path_style_enabled {
            bucket.set_path_style();
        }

        let exists = bucket.exists().await?;
        if !exists {
            return Err(S3ClientError::BucketNotFound(config.bucket));
        }

        Ok(Self { bucket })
    }
}

impl StorageClient for S3Client {
    type Error = S3ClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let data = self.bucket.get_object(path).await?;
        if data.status_code() == 404 {
            return Ok(None);
        }

        Ok(Some(data.to_vec()))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn exists(&self, path: &str) -> Result<bool, Self::Error> {
        let (_, status) = self.bucket.head_object(path).await?;

        Ok(status != 404)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, data), err(Debug))
    )]
    async fn put(&self, path: &str, data: &[u8]) -> Result<(), Self::Error> {
        self.bucket.put_object(path, data).await?;

        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, path: &str) -> Result<(), Self::Error> {
        self.bucket.delete_object(path).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub path_style_enabled: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum S3ClientError {
    #[error("bucket not found with name {0}")]
    BucketNotFound(String),

    #[error(transparent)]
    Credentials(#[from] CredentialsError),

    #[error(transparent)]
    Client(#[from] S3Error),
}
