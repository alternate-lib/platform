#[cfg(feature = "smtp")]
pub mod smtp;

pub trait EmailClient {
    type Error: std::error::Error;

    fn send(&self, email: Email) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Clone)]
pub struct Email {
    pub to_address: String,
    pub subject: String,
    pub body: String,
}
