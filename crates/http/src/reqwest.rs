use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt as _;
use reqwest::{Body, Client, Error as ClientError, Proxy};

use crate::HttpClient;

#[derive(Debug, Clone)]
pub struct ReqwestClient {
    client: Client,
}

impl ReqwestClient {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(config), fields(timeout_sec = config.timeout_sec, proxy_urls = ?config.proxy_urls), err(Debug)))]
    pub fn create(config: ReqwestClientConfig) -> Result<Self, ReqwestClientError> {
        let mut builder = Client::builder();
        if let Some(timeout_sec) = config.timeout_sec {
            builder = builder.timeout(Duration::from_secs(timeout_sec.into()));
        }
        for proxy_url in config.proxy_urls {
            builder = builder.proxy(Proxy::all(proxy_url)?);
        }

        let client = builder.build()?;

        Ok(Self { client })
    }
}

impl HttpClient for ReqwestClient {
    type Error = ReqwestClientError;

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip(self, request), fields(method = %request.method(), uri = %request.uri()), err(Debug)))]
    async fn send(&self, request: Request<Bytes>) -> Result<Response<Bytes>, Self::Error> {
        let req = request.try_into().map_err(ReqwestClientError::Client)?;

        let resp = self
            .client
            .execute(req)
            .await
            .map_err(ReqwestClientError::Client)?;

        let resp: Response<Body> = resp.into();
        let (parts, body) = resp.into_parts();
        let raw = body.collect().await?.to_bytes();
        let resp = Response::from_parts(parts, raw);

        Ok(resp)
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestClientConfig {
    timeout_sec: Option<u8>,
    proxy_urls: Vec<String>,
}

impl ReqwestClientConfig {
    pub fn builder() -> ReqwestClientConfigBuilder {
        ReqwestClientConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReqwestClientConfigBuilder {
    timeout_sec: Option<u8>,
    proxy_urls: Vec<String>,
}

impl ReqwestClientConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn timeout_sec(mut self, timeout_sec: u8) -> Self {
        self.timeout_sec = Some(timeout_sec);
        self
    }

    #[must_use]
    pub fn proxy_url(mut self, proxy_url: impl Into<String>) -> Self {
        self.proxy_urls.push(proxy_url.into());
        self
    }

    pub fn build(self) -> ReqwestClientConfig {
        ReqwestClientConfig {
            timeout_sec: self.timeout_sec,
            proxy_urls: self.proxy_urls,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReqwestClientError {
    #[error(transparent)]
    Client(#[from] ClientError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_uses_sensible_defaults() {
        let config = ReqwestClientConfig::builder().build();

        assert!(config.timeout_sec.is_none());
        assert_eq!(config.proxy_urls, [] as [String; 0]);
    }

    #[test]
    fn builder_accepts_overrides() {
        let config = ReqwestClientConfig::builder()
            .timeout_sec(1)
            .proxy_url("http://proxy1.example.test")
            .proxy_url("https://proxy2.example.test")
            .build();

        assert_eq!(config.timeout_sec, Some(1));
        assert_eq!(
            config.proxy_urls,
            vec!["http://proxy1.example.test", "https://proxy2.example.test"]
        );
    }

    #[test]
    fn rejects_invalid_proxy_url() {
        let result = ReqwestClient::create(
            ReqwestClientConfig::builder()
                .proxy_url("not a valid url")
                .build(),
        );

        assert!(matches!(result, Err(ReqwestClientError::Client(_))));
    }
}
