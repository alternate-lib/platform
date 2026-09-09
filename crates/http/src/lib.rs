use bytes::Bytes;
use http::{Request, Response};

#[cfg(feature = "reqwest")]
pub mod reqwest;

pub trait HttpClient {
    type Error: std::error::Error;

    fn send(
        &self,
        request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>, Self::Error>>;
}
