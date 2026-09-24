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
    pub fn create(config: SmtpClientConfig) -> Result<Self, SmtpClientError> {
        let mut builder = match config.security {
            SmtpSecurity::ImplicitTls => AsyncSmtpTransport::<Tokio1Executor>::relay(&config.host)?,
            SmtpSecurity::StartTls => {
                AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.host)?
            }
            SmtpSecurity::Plaintext => {
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&config.host)
            }
        };
        if let Some(port) = config.port {
            builder = builder.port(port);
        }
        if !config.username.is_empty() {
            builder = builder.credentials(Credentials::new(config.username, config.password));
        }

        let transport = builder.build();

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SmtpSecurity {
    #[default]
    ImplicitTls,
    StartTls,
    Plaintext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmtpClientConfig {
    host: String,
    port: Option<u16>,
    security: SmtpSecurity,
    username: String,
    password: String,
    from_address: String,
}

impl SmtpClientConfig {
    pub fn builder() -> SmtpClientConfigBuilder {
        SmtpClientConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SmtpClientConfigBuilder {
    host: Option<String>,
    port: Option<u16>,
    security: SmtpSecurity,
    username: Option<String>,
    password: Option<String>,
    from_address: Option<String>,
}

impl SmtpClientConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    #[must_use]
    pub fn security(mut self, security: SmtpSecurity) -> Self {
        self.security = security;
        self
    }

    #[must_use]
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    #[must_use]
    pub fn from_address(mut self, from_address: impl Into<String>) -> Self {
        self.from_address = Some(from_address.into());
        self
    }

    pub fn build(self) -> Result<SmtpClientConfig, SmtpConfigError> {
        Ok(SmtpClientConfig {
            host: self.host.ok_or(SmtpConfigError::MissingField("host"))?,
            port: self.port,
            security: self.security,
            username: self.username.unwrap_or_default(),
            password: self.password.unwrap_or_default(),
            from_address: self
                .from_address
                .ok_or(SmtpConfigError::MissingField("from_address"))?,
        })
    }
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SmtpConfigError {
    #[error("missing SMTP config value: {0}")]
    MissingField(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_uses_sensible_defaults() {
        let config = SmtpClientConfig::builder()
            .host("smtp.example.test")
            .from_address("sender@example.test")
            .build()
            .unwrap();

        assert_eq!(config.host, "smtp.example.test");
        assert_eq!(config.port, None);
        assert_eq!(config.security, SmtpSecurity::ImplicitTls);
        assert_eq!(config.username, "");
        assert_eq!(config.password, "");
        assert_eq!(config.from_address, "sender@example.test");
    }

    #[test]
    fn builder_accepts_overrides() {
        let config = SmtpClientConfig::builder()
            .host("smtp.example.test")
            .port(2525)
            .security(SmtpSecurity::Plaintext)
            .username("user")
            .password("secret")
            .from_address("sender@example.test")
            .build()
            .unwrap();

        assert_eq!(config.host, "smtp.example.test");
        assert_eq!(config.port, Some(2525));
        assert_eq!(config.security, SmtpSecurity::Plaintext);
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "secret");
        assert_eq!(config.from_address, "sender@example.test");
    }

    #[test]
    fn builder_accepts_starttls_without_a_port_override() {
        let config = SmtpClientConfig::builder()
            .host("smtp.example.test")
            .security(SmtpSecurity::StartTls)
            .from_address("sender@example.test")
            .build()
            .unwrap();

        assert_eq!(config.security, SmtpSecurity::StartTls);
        assert_eq!(config.port, None);
    }

    #[test]
    fn builder_requires_host_and_from_address() {
        assert_eq!(
            SmtpClientConfig::builder()
                .from_address("sender@example.test")
                .build(),
            Err(SmtpConfigError::MissingField("host"))
        );
        assert_eq!(
            SmtpClientConfig::builder()
                .host("smtp.example.test")
                .build(),
            Err(SmtpConfigError::MissingField("from_address"))
        );
    }
}
