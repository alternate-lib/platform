#![cfg(feature = "smtp")]

use alternate_email::{
    Email, EmailClient,
    smtp::{SmtpClient, SmtpClientConfig},
};
use testcontainers::{ContainerAsync, GenericImage, core::IntoContainerPort, runners::AsyncRunner};
use tokio::time::{self, Duration};

const SMTP_PORT: u16 = 1025;
const HTTP_PORT: u16 = 8025;

struct MailpitServer {
    _container: ContainerAsync<GenericImage>,
    host: String,
    smtp_port: u16,
    http_port: u16,
}

impl MailpitServer {
    async fn init() -> Result<Self, Box<dyn std::error::Error>> {
        let container = GenericImage::new("axllent/mailpit", "v1.31.0")
            .with_exposed_port(SMTP_PORT.tcp())
            .with_exposed_port(HTTP_PORT.tcp())
            .start()
            .await?;

        let smtp_port = container.get_host_port_ipv4(SMTP_PORT).await?;
        let http_port = container.get_host_port_ipv4(HTTP_PORT).await?;

        let host = container
            .get_host()
            .await?
            .to_string()
            .replace("localhost", "127.0.0.1");

        Ok(MailpitServer {
            _container: container,
            host,
            smtp_port,
            http_port,
        })
    }
}

struct MailpitClient {
    client: reqwest::Client,
    base_url: String,
}

impl MailpitClient {
    fn new(host: &str, port: u16) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: format!("http://{host}:{port}/api/v1"),
        }
    }

    async fn wait(&self) {
        let mut ready = false;
        let mut last_error = String::new();

        let info_endpoint = format!("{}/info", self.base_url);

        for _ in 0..30 {
            match self.client.get(&info_endpoint).send().await {
                Ok(response) if response.status().is_success() => {
                    ready = true;
                    break;
                }
                Ok(response) => last_error = response.status().to_string(),
                Err(error) => last_error = error.to_string(),
            }

            time::sleep(Duration::from_millis(1000)).await;
        }

        assert!(
            ready,
            "Mailpit HTTP API did not become ready ({last_error}); URL: {info_endpoint}"
        );
    }

    async fn get_latest_message(&self) -> Result<String, reqwest::Error> {
        self.client
            .get(format!("{}/message/latest", self.base_url))
            .send()
            .await?
            .text()
            .await
    }
}

#[tokio::test]
async fn sends_email_to_smtp_server() -> Result<(), Box<dyn std::error::Error>> {
    let mailpit_server = MailpitServer::init().await?;

    let mailpit_client = MailpitClient::new(&mailpit_server.host, mailpit_server.http_port);
    mailpit_client.wait().await;

    let smtp_client = SmtpClient::create(
        SmtpClientConfig::builder()
            .host(mailpit_server.host)
            .port(mailpit_server.smtp_port)
            .use_tls(false)
            .from_address("sender@example.test")
            .build()?,
    )?;

    smtp_client
        .send(Email {
            to_address: "recipient@example.test".into(),
            subject: "SMTP integration test".into(),
            body: "Delivered by alternate-email".into(),
        })
        .await?;

    let message = mailpit_client.get_latest_message().await?;

    assert!(message.contains("SMTP integration test"));
    assert!(message.contains("Delivered by alternate-email"));

    Ok(())
}
