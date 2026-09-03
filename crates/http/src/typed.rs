use alternate_codec::Codec;
use http::{Request, Response};
use serde::{Serialize, de::DeserializeOwned};

use crate::{HttpClient, HttpClientExt, HttpClientExtError};

pub trait HttpClientTyped<C: Codec>: HttpClient {
    fn send_typed<U: Serialize + Send, V: DeserializeOwned>(
        &self,
        request: Request<U>,
    ) -> impl Future<Output = Result<Response<V>, HttpClientTypedError<C::Error, Self::Error>>> + Send;
}

impl<HC: HttpClient, C: Codec> HttpClientTyped<C> for HC {
    async fn send_typed<U: Serialize, V: DeserializeOwned>(
        &self,
        request: Request<U>,
    ) -> Result<Response<V>, HttpClientTypedError<C::Error, Self::Error>> {
        let (parts, data) = request.into_parts();
        let raw = C::encode(&data).map_err(HttpClientTypedError::Codec)?;
        let request = Request::from_parts(parts, raw.into());

        let resp = self.send(request).await?;

        let (parts, raw) = resp.into_parts();
        let data = C::decode(&raw).map_err(HttpClientTypedError::Codec)?;
        let resp = Response::from_parts(parts, data);

        Ok(resp)
    }
}

pub trait HttpClientExtTyped<C: Codec>: HttpClientExt {
    fn get_typed<V: DeserializeOwned>(
        &self,
        url: &str,
    ) -> impl Future<
        Output = Result<V, HttpClientTypedError<C::Error, HttpClientExtError<Self::Error>>>,
    > + Send;
}

impl<HC: HttpClientExt, C: Codec> HttpClientExtTyped<C> for HC {
    async fn get_typed<V: DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<V, HttpClientTypedError<C::Error, HttpClientExtError<Self::Error>>> {
        let raw = self.get(url).await?;

        let data = C::decode(&raw).map_err(HttpClientTypedError::Codec)?;

        Ok(data)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientTypedError<CodecErr: std::error::Error, HttpErr: std::error::Error> {
    #[error("codec: {0}")]
    Codec(CodecErr),

    #[error(transparent)]
    Http(#[from] HttpErr),
}
