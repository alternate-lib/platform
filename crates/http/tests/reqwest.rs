#![cfg(feature = "reqwest")]

use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use alternate_http::{
    HttpClient as _,
    reqwest::{ReqwestClient, ReqwestClientConfig, ReqwestClientError},
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::{io::AsyncReadExt as _, net::TcpListener, time};

type TestResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<Full<Bytes>>, Infallible>> + Send>>;

#[derive(Debug, Clone)]
struct CapturedRequest {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

async fn spawn_server(
    handler: Arc<dyn Fn(Request<Incoming>) -> TestResponseFuture + Send + Sync>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("test server address");

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let handler = Arc::clone(&handler);

            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service_fn(move |req| handler(req)))
                    .await;
            });
        }
    });

    addr
}

fn init_client() -> ReqwestClient {
    ReqwestClient::create(ReqwestClientConfig::builder().build()).expect("create client")
}

fn test_request(method: Method, uri: String, body: Bytes) -> Request<Bytes> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .expect("build request")
}

fn test_response(
    status: StatusCode,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }

    builder
        .body(Full::new(Bytes::copy_from_slice(body)))
        .map(Ok)
        .expect("build response")
}

#[tokio::test]
async fn preserves_request_details() {
    let captured = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&captured);

    let addr = spawn_server(Arc::new(move |req| {
        let slot = Arc::clone(&slot);

        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let body = body
                .collect()
                .await
                .expect("collect request body")
                .to_bytes();

            *slot.lock().expect("lock captured request") = Some(CapturedRequest {
                method: parts.method.clone(),
                uri: parts.uri.clone(),
                headers: parts.headers.clone(),
                body: body.clone(),
            });

            test_response(StatusCode::OK, &[], b"ok")
        })
    }))
    .await;

    let client = init_client();
    let payload = Bytes::from_static(&[0x00, 0x01, 0x02, 0xFA, 0xFB]);

    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{addr}/upload?version=2"))
        .header("x-test", "value")
        .body(payload.clone())
        .expect("build request");

    let resp = client.send(req).await.expect("send request");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body(), &Bytes::from_static(b"ok"));

    let captured = captured
        .lock()
        .expect("lock captured request")
        .clone()
        .expect("captured request");

    assert_eq!(captured.method, Method::POST);
    assert_eq!(captured.uri.path(), "/upload");
    assert_eq!(captured.uri.query(), Some("version=2"));
    assert_eq!(
        captured.headers.get("x-test"),
        Some(&HeaderValue::from_static("value"))
    );
    assert_eq!(captured.body, payload);
}

#[tokio::test]
async fn preserves_response_details() {
    let addr = spawn_server(Arc::new(|_| {
        Box::pin(async {
            test_response(
                StatusCode::CREATED,
                &[("x-custom", "hello")],
                &[0x00, 0xFF, 0xFE, 0x80],
            )
        })
    }))
    .await;

    let client = init_client();
    let req = test_request(Method::GET, format!("http://{addr}/"), Bytes::new());

    let resp = client.send(req).await.expect("send request");

    assert_eq!(resp.status(), StatusCode::CREATED);
    assert_eq!(
        resp.headers().get("x-custom"),
        Some(&HeaderValue::from_static("hello"))
    );
    assert_eq!(resp.body(), &Bytes::from_static(&[0x00, 0xFF, 0xFE, 0x80]));
}

#[tokio::test]
async fn returns_non_success_status_without_client_error() {
    let addr = spawn_server(Arc::new(|_| {
        Box::pin(async { test_response(StatusCode::NOT_FOUND, &[], b"missing") })
    }))
    .await;

    let client = init_client();
    let req = test_request(Method::GET, format!("http://{addr}/"), Bytes::new());

    let resp = client
        .send(req)
        .await
        .expect("not produce client error for non-2xx status");

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(resp.body(), &Bytes::from_static(b"missing"));
}

#[tokio::test]
async fn maps_connection_failure_to_client_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
    let addr = listener.local_addr().expect("probe address");
    drop(listener);

    let client = init_client();
    let req = test_request(Method::GET, format!("http://{addr}/"), Bytes::new());

    let result = client.send(req).await;

    assert!(matches!(result, Err(ReqwestClientError::Client(_))));
}

#[tokio::test]
async fn enforces_request_timeout() {
    let addr = spawn_server(Arc::new(|_| {
        Box::pin(async {
            time::sleep(Duration::from_secs(2)).await;
            test_response(StatusCode::OK, &[], b"late")
        })
    }))
    .await;

    let client = ReqwestClient::create(ReqwestClientConfig::builder().timeout_sec(1).build())
        .expect("create client");

    let req = test_request(Method::GET, format!("http://{addr}/"), Bytes::new());

    let Err(ReqwestClientError::Client(error)) = client.send(req).await else {
        panic!("request should time out");
    };

    assert!(error.is_timeout(), "expected timeout error, got: {error}");
}

#[tokio::test]
async fn routes_request_through_configured_proxy() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy address");

    let client = ReqwestClient::create(
        ReqwestClientConfig::builder()
            .proxy_url(format!("http://{proxy_addr}"))
            .build(),
    )
    .expect("create client");

    let req = test_request(
        Method::GET,
        "https://proxy-target.example/".to_owned(),
        Bytes::new(),
    );

    let send_task = tokio::spawn(async move { client.send(req).await });

    let accepted = time::timeout(Duration::from_secs(5), proxy_listener.accept())
        .await
        .expect("client never connected to proxy")
        .expect("accept proxy connection");

    let (mut stream, _) = accepted;

    let mut connect_buffer = [0u8; 64];
    let read = time::timeout(Duration::from_secs(5), stream.read(&mut connect_buffer))
        .await
        .expect("proxy handshake never arrived")
        .expect("read proxy handshake");

    let connect_line = String::from_utf8_lossy(&connect_buffer[..read]);
    assert!(
        connect_line.starts_with("CONNECT "),
        "expected CONNECT request, got: {connect_line}"
    );

    drop(stream);

    let send_result = time::timeout(Duration::from_secs(5), send_task)
        .await
        .expect("send task never finished")
        .expect("send task panicked");

    assert!(matches!(send_result, Err(ReqwestClientError::Client(_))));
}
