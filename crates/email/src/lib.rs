use anyhow::Context as _;

#[cfg(feature = "smtp")]
pub mod smtp;

pub trait EmailClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn send(&self, email: Email) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Clone)]
pub struct Email {
    pub to_address: String,
    pub subject: String,
    pub body: String,
}

#[async_trait::async_trait]
pub trait DynEmailClient: Send + Sync {
    async fn send(&self, email: Email) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl<EC: EmailClient> DynEmailClient for EC {
    async fn send(&self, email: Email) -> Result<(), anyhow::Error> {
        EC::send(self, email).await.context("send email")
    }
}
