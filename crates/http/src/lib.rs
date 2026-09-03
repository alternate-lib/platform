use bytes::Bytes;
use http::{Error as HttpError, Method, Request, Response};
#[cfg(feature = "typed")]
pub use typed::{HttpClientExtTyped, HttpClientTyped, HttpClientTypedError};

#[cfg(feature = "reqwest")]
pub mod reqwest;
#[cfg(feature = "typed")]
mod typed;

pub trait HttpClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn send(
        &self,
        request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>, Self::Error>> + Send;
}

pub trait HttpClientExt: HttpClient {
    fn get(
        &self,
        url: &str,
    ) -> impl Future<Output = Result<Bytes, HttpClientExtError<Self::Error>>> + Send;
}

impl<HC: HttpClient> HttpClientExt for HC {
    async fn get(&self, url: &str) -> Result<Bytes, HttpClientExtError<Self::Error>> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .body(Bytes::new())
            .map_err(HttpClientExtError::Request)?;

        let resp = self.send(request).await?;

        Ok(resp.into_body())
    }
}

#[async_trait::async_trait]
pub trait DynHttpClient: Send + Sync {
    async fn send(&self, request: Request<Bytes>) -> Result<Response<Bytes>, anyhow::Error>;
}

#[async_trait::async_trait]
impl<HC: HttpClient> DynHttpClient for HC {
    async fn send(&self, request: Request<Bytes>) -> Result<Response<Bytes>, anyhow::Error> {
        HC::send(self, request).await.map_err(anyhow::Error::from)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientExtError<ClientErr: std::error::Error> {
    #[error("request: {0}")]
    Request(HttpError),

    #[error(transparent)]
    Client(#[from] ClientErr),
}
