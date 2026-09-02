#![cfg(feature = "smtp")]

use alternate_email::{
    Email, EmailClient,
    smtp::{SmtpClient, SmtpConfig},
};
use testcontainers::{ContainerAsync, GenericImage, core::IntoContainerPort, runners::AsyncRunner};
use tokio::time::Duration;

const SMTP_PORT: u16 = 1025;
const HTTP_PORT: u16 = 8025;

#[tokio::test]
async fn sends_email_to_mailpit() -> Result<(), Box<dyn std::error::Error>> {
    let smtp_server = start_container().await?;
    let http_client = create_http_client(&smtp_server).await;

    let client = SmtpClient::create(SmtpConfig {
        host: smtp_server.host,
        port: smtp_server.smtp_port,
        use_tls: false,
        username: String::new(),
        password: String::new(),
        from_address: "sender@example.test".into(),
    })?;

    client
        .send(Email {
            to_address: "recipient@example.test".into(),
            subject: "Mailpit integration test".into(),
            body: "Delivered by alternate-email".into(),
        })
        .await?;

    let message = http_client.get_message().await?;
    assert!(message.contains("Mailpit integration test"));
    assert!(message.contains("Delivered by alternate-email"));

    Ok(())
}

async fn start_container() -> Result<MailpitSmtpServer, Box<dyn std::error::Error>> {
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

    Ok(MailpitSmtpServer {
        _container: container,
        host,
        smtp_port,
        http_port,
    })
}

async fn create_http_client(server: &MailpitSmtpServer) -> MailpitHttpClient {
    let api_url = format!(
        "http://{}:{}/api/v1/message/latest",
        server.host, server.http_port
    );

    let client = reqwest::Client::new();

    let mut ready = false;
    let mut last_error = String::new();

    for _ in 0..30 {
        match client.get(&api_url).send().await {
            Ok(response) if response.status().is_success() || response.status().as_u16() == 404 => {
                ready = true;
                break;
            }
            Ok(response) => last_error = response.status().to_string(),
            Err(error) => last_error = error.to_string(),
        }

        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    assert!(
        ready,
        "Mailpit HTTP API did not become ready ({last_error}); URL: {api_url}"
    );

    MailpitHttpClient { client, api_url }
}

struct MailpitSmtpServer {
    _container: ContainerAsync<GenericImage>,
    host: String,
    smtp_port: u16,
    http_port: u16,
}

struct MailpitHttpClient {
    client: reqwest::Client,
    api_url: String,
}

impl MailpitHttpClient {
    async fn get_message(&self) -> Result<String, reqwest::Error> {
        self.client.get(&self.api_url).send().await?.text().await
    }
}
