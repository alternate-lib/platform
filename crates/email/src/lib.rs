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
