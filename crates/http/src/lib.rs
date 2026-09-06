use bytes::Bytes;
use http::{Request, Response};

// #[cfg(feature = "cache")]
// pub mod cache;
#[cfg(feature = "reqwest")]
pub mod reqwest;
#[cfg(feature = "tower")]
pub mod tower;

pub trait HttpClient {
    type Error: std::error::Error;

    fn send(
        &self,
        request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>, Self::Error>>;
}
