use std::marker::PhantomData;

use alternate_codec::Codec;
use alternate_kv::KvTyped;
use http_cache::{CacheManager, HttpResponse};
use http_cache_semantics::CachePolicy;

pub struct KvCacheManager<C: Codec, KC: KvTyped<C>> {
    kv_client: KC,
    _codec: PhantomData<C>,
}

impl<C: Codec, KC: KvTyped<C>> KvCacheManager<C, KC> {
    pub fn new(kv_client: KC) -> Self {
        Self {
            kv_client,
            _codec: PhantomData,
        }
    }
}

impl<C: Codec + Send + Sync + 'static, KC: KvTyped<C> + 'static> CacheManager
    for KvCacheManager<C, KC>
where
    C::Error: Send + Sync + 'static,
{
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, key: &str) -> http_cache::Result<Option<(HttpResponse, CachePolicy)>> {
        let value = self.kv_client.get_typed(key).await?;

        Ok(value)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, res, policy), err(Debug))
    )]
    async fn put(
        &self,
        cache_key: String,
        res: HttpResponse,
        policy: CachePolicy,
    ) -> http_cache::Result<HttpResponse> {
        self.kv_client
            .set_typed(&cache_key, &(&res, &policy))
            .await?;

        Ok(res)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, cache_key: &str) -> http_cache::Result<()> {
        self.kv_client.delete(cache_key).await?;

        Ok(())
    }
}
