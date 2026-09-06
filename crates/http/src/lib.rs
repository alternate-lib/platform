use bytes::Bytes;
use http::{Error as HttpError, Method, Request, Response};
#[cfg(feature = "typed")]
pub use typed::{HttpExtTyped, HttpTyped, HttpTypedError};

#[cfg(feature = "cache")]
pub mod cache;
#[cfg(feature = "reqwest")]
pub mod reqwest;
#[cfg(feature = "tower")]
pub mod tower;
#[cfg(feature = "typed")]
mod typed;

pub trait HttpClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn send(
        &self,
        request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>, Self::Error>> + Send;
}

pub trait HttpExt: HttpClient {
    fn get(
        &self,
        url: &str,
    ) -> impl Future<Output = Result<Bytes, HttpExtError<Self::Error>>> + Send;
}

impl<HC: HttpClient> HttpExt for HC {
    async fn get(&self, url: &str) -> Result<Bytes, HttpExtError<Self::Error>> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .body(Bytes::new())
            .map_err(HttpExtError::Request)?;

        let resp = self.send(request).await?;

        Ok(resp.into_body())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpExtError<ClientErr: std::error::Error> {
    #[error("request: {0}")]
    Request(HttpError),

    #[error(transparent)]
    Client(#[from] ClientErr),
}
