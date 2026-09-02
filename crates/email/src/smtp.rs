use lettre::{
    AsyncSmtpTransport, AsyncTransport as _, Message, Tokio1Executor,
    address::AddressError,
    error::Error as EmailError,
    message::{
        Mailbox, Mailboxes,
        header::{self, ContentType},
    },
    transport::smtp::{Error as TransportError, authentication::Credentials},
};

use crate::{Email, EmailClient};

#[derive(Debug, Clone)]
pub struct SmtpClient {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
}

impl SmtpClient {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(config), fields(host = config.host, username = config.username, from_address = config.from_address), err(Debug))
    )]
    pub fn create(config: SmtpConfig) -> Result<Self, SmtpClientError> {
        let mut builder = if config.use_tls {
            AsyncSmtpTransport::<Tokio1Executor>::relay(&config.host)?
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&config.host)
        };
        if !config.username.is_empty() {
            builder = builder.credentials(Credentials::new(config.username, config.password));
        }

        let transport = builder.port(config.port).build();

        let from = config.from_address.parse::<Mailbox>()?;

        Ok(Self { transport, from })
    }
}

impl EmailClient for SmtpClient {
    type Error = SmtpClientError;
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn send(&self, email: Email) -> Result<(), Self::Error> {
        let message = Message::builder()
            .mailbox(header::From::from(Mailboxes::from(self.from.clone())))
            .to(email.to_address.parse()?)
            .subject(email.subject)
            .header(ContentType::TEXT_PLAIN)
            .body(email.body)?;

        self.transport.send(message).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub username: String,
    pub password: String,
    pub from_address: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SmtpClientError {
    #[error(transparent)]
    Address(#[from] AddressError),

    #[error(transparent)]
    Email(#[from] EmailError),

    #[error(transparent)]
    Client(#[from] TransportError),
}
