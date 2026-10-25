#![cfg(all(
    unix,
    feature = "transport-streamable-http-client-unix-socket",
    not(feature = "local")
))]

use std::{collections::HashMap, sync::Arc};

use axum::{
    Router, body::Bytes, extract::State, http::StatusCode, response::IntoResponse, routing::post,
};
use http::{HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use rmcp::{
    ServiceExt,
    model::{ClientJsonRpcMessage, ClientRequest, CustomRequest, PingRequest, RequestId},
    transport::{
        StreamableHttpClientTransport, UnixSocketHttpClient,
        streamable_http_client::{
            StreamableHttpClient, StreamableHttpClientTransportConfig, StreamableHttpError,
        },
    },
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

#[derive(Clone)]
struct ServerState {
    received_headers: Arc<Mutex<HashMap<String, String>>>,
    initialize_called: Arc<tokio::sync::Notify>,
}

async fn mcp_handler(
    State(state): State<ServerState>,
    headers: http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut headers_map = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if (name_str.starts_with("x-") || name_str == "host")
            && let Ok(v) = value.to_str()
        {
            headers_map.insert(name_str.to_string(), v.to_string());
        }
    }

    let mut stored = state.received_headers.lock().await;
    stored.extend(headers_map);
    drop(stored);

    if let Ok(json_body) = serde_json::from_slice::<serde_json::Value>(&body)
        && let Some(method) = json_body.get("method").and_then(|m| m.as_str())
    {
        if method == "initialize" {
            state.initialize_called.notify_one();
            let response = json!({
                "jsonrpc": "2.0",
                "id": json_body.get("id"),
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "serverInfo": {
                        "name": "test-unix-server",
                        "version": "1.0.0"
                    }
                }
            });
            return (
                StatusCode::OK,
                [
                    (http::header::CONTENT_TYPE, "application/json"),
                    (
                        http::HeaderName::from_static("mcp-session-id"),
                        "unix-test-session",
                    ),
                ],
                response.to_string(),
            );
        } else if method == "notifications/initialized" {
            return (
                StatusCode::ACCEPTED,
                [
                    (http::header::CONTENT_TYPE, "application/json"),
                    (
                        http::HeaderName::from_static("mcp-session-id"),
                        "unix-test-session",
                    ),
                ],
                String::new(),
            );
        } else if method == "skills/list" {
            let response = json!({
                "jsonrpc": "2.0",
                "id": json_body.get("id"),
                "result": {
                    "resultType": "complete",
                    "skills": ["unix-example"],
                    "_meta": {"vendorExtension": {"retained": true}}
                }
            });
            return (
                StatusCode::OK,
                [
                    (http::header::CONTENT_TYPE, "application/json"),
                    (
                        http::HeaderName::from_static("mcp-session-id"),
                        "unix-test-session",
                    ),
                ],
                response.to_string(),
            );
        }
    }

    let request_id = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|j| j.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    let response = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {}
    });
    (
        StatusCode::OK,
        [
            (http::header::CONTENT_TYPE, "application/json"),
            (
                http::HeaderName::from_static("mcp-session-id"),
                "unix-test-session",
            ),
        ],
        response.to_string(),
    )
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct SkillsListResult {
    result_type: String,
    skills: Vec<String>,
    #[serde(rename = "_meta")]
    meta: serde_json::Value,
}

struct TemporarySocketDirectory(std::path::PathBuf);

impl TemporarySocketDirectory {
    fn new() -> std::io::Result<Self> {
        // Keep the pathname below the small sockaddr_un limit on macOS; its
        // resolved system temporary directory can itself be very long.
        let path = std::path::Path::new("/tmp").join(format!("rmcp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TemporarySocketDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct AbortServerOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortServerOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Typed extension results must retain fields that the core response union does
/// not know about when transported over a Unix-domain HTTP connection.
#[tokio::test]
async fn test_unix_socket_typed_custom_response_preserves_extension_fields() -> anyhow::Result<()> {
    let dir = TemporarySocketDirectory::new()?;
    let socket_path = dir.0.join("mcp.sock");

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };
    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state);
    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let _server_guard = AbortServerOnDrop(spawn_unix_server(listener, app));

    let socket_str = socket_path.to_str().expect("UTF-8 temporary path");
    let uri = "http://mcp-server.internal/mcp";
    let transport = StreamableHttpClientTransport::with_client(
        UnixSocketHttpClient::new(socket_str, uri),
        StreamableHttpClientTransportConfig::with_uri(uri),
    );
    let client = ().serve(transport).await?;

    let response: SkillsListResult = client
        .send_request_as(ClientRequest::CustomRequest(CustomRequest::new(
            "skills/list",
            None,
        )))
        .await?;

    assert_eq!(response.result_type, "complete");
    assert_eq!(response.skills, ["unix-example"]);
    assert_eq!(response.meta["vendorExtension"]["retained"], true);

    client.cancel().await?;
    Ok(())
}

/// Spawns an HTTP/1.1 server on a Unix socket using hyper directly.
/// Avoids `axum::serve(UnixListener, ...)` which uses `spawn_local` on Linux.
fn spawn_unix_server(
    listener: tokio::net::UnixListener,
    app: Router,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tower_service = app.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let hyper_service = hyper::service::service_fn(
                    move |req: hyper::Request<hyper::body::Incoming>| {
                        let mut tower_service = tower_service.clone();
                        async move {
                            use tower_service::Service;
                            tower_service.call(req).await
                        }
                    },
                );
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .await
                    .ok();
            });
        }
    })
}

/// A configured body cap must reject oversized buffered JSON responses before deserialization.
#[tokio::test]
async fn test_unix_socket_response_body_cap() -> anyhow::Result<()> {
    let dir = TemporarySocketDirectory::new()?;
    let socket_path = dir.0.join("mcp.sock");
    let app = Router::new().route(
        "/mcp",
        post(|| async {
            (
                StatusCode::OK,
                [(http::header::CONTENT_TYPE, "application/json")],
                "x".repeat(1024),
            )
        }),
    );
    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let _server_guard = AbortServerOnDrop(spawn_unix_server(listener, app));

    let socket_str = socket_path.to_str().expect("UTF-8 temporary path");
    let uri = "http://mcp-server.internal/mcp";
    let client = UnixSocketHttpClient::new(socket_str, uri).with_max_response_bytes(128);
    let transport = StreamableHttpClientTransport::with_client(
        client,
        StreamableHttpClientTransportConfig::with_uri("/mcp"),
    );

    let error = ().serve(transport).await.expect_err("oversized response must fail");
    assert!(
        error.to_string().contains("response_too_large"),
        "unexpected error: {error}"
    );
    Ok(())
}

/// Integration test: MCP client connects and completes handshake over a Unix domain socket.
#[tokio::test]
async fn test_unix_socket_mcp_handshake() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");

    let _ = std::fs::remove_file(&socket_path);

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };

    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());

    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let socket_str = socket_path.to_str().unwrap();
    let uri = "http://mcp-server.internal/mcp";
    let client = UnixSocketHttpClient::new(socket_str, uri);
    let config = StreamableHttpClientTransportConfig::with_uri(uri);
    let transport = StreamableHttpClientTransport::with_client(client, config);

    let mcp_client = ().serve(transport).await.expect("MCP handshake should succeed");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.initialize_called.notified(),
    )
    .await
    .expect("Initialize request should be received");

    let headers = state.received_headers.lock().await;
    assert_eq!(
        headers.get("host"),
        Some(&"mcp-server.internal".to_string()),
        "Host header should be derived from URI"
    );

    drop(mcp_client);
    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    Ok(())
}

/// Integration test: Custom headers are sent through the Unix socket transport.
#[tokio::test]
async fn test_unix_socket_custom_headers() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-headers-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");
    let _ = std::fs::remove_file(&socket_path);

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };

    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());

    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let mut custom_headers = HashMap::new();
    custom_headers.insert(
        HeaderName::from_static("x-test-header"),
        HeaderValue::from_static("test-value-123"),
    );
    custom_headers.insert(
        HeaderName::from_static("x-client-id"),
        HeaderValue::from_static("unix-test-client"),
    );

    let socket_str = socket_path.to_str().unwrap();
    let uri = "http://mcp-server.internal/mcp";
    let client = UnixSocketHttpClient::new(socket_str, uri);
    let config = StreamableHttpClientTransportConfig::with_uri(uri).custom_headers(custom_headers);
    let transport = StreamableHttpClientTransport::with_client(client, config);

    let mcp_client = ().serve(transport).await.expect("MCP handshake should succeed");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.initialize_called.notified(),
    )
    .await
    .expect("Initialize request should be received");

    let headers = state.received_headers.lock().await;
    assert_eq!(
        headers.get("x-test-header"),
        Some(&"test-value-123".to_string()),
        "Custom header x-test-header should be received"
    );
    assert_eq!(
        headers.get("x-client-id"),
        Some(&"unix-test-client".to_string()),
        "Custom header x-client-id should be received"
    );

    drop(mcp_client);
    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    Ok(())
}

/// Integration test: Convenience constructor `from_unix_socket` works end-to-end.
#[tokio::test]
async fn test_unix_socket_convenience_constructor() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-conv-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");
    let _ = std::fs::remove_file(&socket_path);

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };

    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());

    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let socket_str = socket_path.to_str().unwrap();
    let transport =
        StreamableHttpClientTransport::from_unix_socket(socket_str, "http://localhost/mcp");

    let mcp_client = ().serve(transport).await.expect("MCP handshake should succeed");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.initialize_called.notified(),
    )
    .await
    .expect("Initialize request should be received");

    drop(mcp_client);
    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    Ok(())
}

/// A request POST that gets 200 + `application/json` + `{}` must error.
/// Mapping that body to Accepted would hang the caller waiting on SSE.
#[tokio::test]
async fn test_unix_socket_request_malformed_json_is_unexpected() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-bad-json-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");
    let _ = std::fs::remove_file(&socket_path);

    async fn bad_json_handler() -> impl IntoResponse {
        (
            StatusCode::OK,
            [(http::header::CONTENT_TYPE, "application/json")],
            "{}",
        )
    }

    let app = Router::new().route("/mcp", post(bad_json_handler));
    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let socket_str = socket_path.to_str().unwrap();
    let uri = "http://localhost/mcp";
    let client = UnixSocketHttpClient::new(socket_str, uri);
    let result = client
        .post_message(
            Arc::from(uri),
            ClientJsonRpcMessage::request(
                ClientRequest::PingRequest(PingRequest::default()),
                RequestId::Number(1),
            ),
            None,
            None,
            HashMap::new(),
        )
        .await;

    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    match result {
        Err(StreamableHttpError::UnexpectedServerResponse(ref msg)) => {
            assert!(
                msg.contains("{}"),
                "expected body preview in error, got: {msg}"
            );
        }
        other => panic!("expected UnexpectedServerResponse, got: {other:?}"),
    }
    Ok(())
}

/// Repeated challenge fields are one ordered challenge list; later Bearer
/// challenges must remain available to consent policy after a Basic challenge.
#[tokio::test]
async fn unix_auth_errors_retain_all_challenges_for_get_and_post() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        for get in [false, true] {
            let dir = TemporarySocketDirectory::new()?;
            let socket = dir.0.join("challenges.sock");
            let hits = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&hits);
            let app = Router::new().route(
                "/mcp",
                axum::routing::any(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let mut response = status.into_response();
                        response.headers_mut().append(
                            http::header::WWW_AUTHENTICATE,
                            HeaderValue::from_static("Basic realm=\"local, proxy\""),
                        );
                        response.headers_mut().append(
                            http::header::WWW_AUTHENTICATE,
                            HeaderValue::from_static(
                                "Bearer error=\"insufficient_scope\", scope=\"files:write\"",
                            ),
                        );
                        response
                    }
                }),
            );
            let listener = tokio::net::UnixListener::bind(&socket)?;
            let _server = AbortServerOnDrop(spawn_unix_server(listener, app));
            let client =
                UnixSocketHttpClient::new(socket.to_str().unwrap(), "http://local.test/mcp");
            let challenge = if get {
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.get_stream("/mcp".into(), None, None, None, HashMap::new()),
                )
                .await?;
                match result {
                    Err(StreamableHttpError::AuthRequired(error)) => error.www_authenticate_header,
                    Err(StreamableHttpError::InsufficientScope(error)) => {
                        error.www_authenticate_header
                    }
                    _ => panic!("expected typed authentication challenge from GET"),
                }
            } else {
                let message =
                    serde_json::from_value(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))?;
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.post_message("/mcp".into(), message, None, None, HashMap::new()),
                )
                .await?;
                match result {
                    Err(StreamableHttpError::AuthRequired(error)) => error.www_authenticate_header,
                    Err(StreamableHttpError::InsufficientScope(error)) => {
                        error.www_authenticate_header
                    }
                    _ => panic!("expected typed authentication challenge from POST"),
                }
            };
            assert_eq!(
                challenge,
                "Basic realm=\"local, proxy\", Bearer error=\"insufficient_scope\", scope=\"files:write\"",
                "all authentication challenges must survive transport classification"
            );
            assert_eq!(
                hits.load(Ordering::SeqCst),
                1,
                "classification must not replay the request"
            );
        }
    }
    Ok(())
}

/// The same challenge list must survive the reqwest-backed adapter as well.
#[tokio::test]
async fn http_auth_errors_retain_all_challenges_for_get_and_post() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        for get in [false, true] {
            let hits = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&hits);
            let app = Router::new().route(
                "/mcp",
                axum::routing::any(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let mut response = status.into_response();
                        response.headers_mut().append(
                            http::header::WWW_AUTHENTICATE,
                            HeaderValue::from_static("Basic realm=\"local, proxy\""),
                        );
                        response.headers_mut().append(
                            http::header::WWW_AUTHENTICATE,
                            HeaderValue::from_static(
                                "Bearer error=\"insufficient_scope\", scope=\"files:write\"",
                            ),
                        );
                        response
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let uri: Arc<str> = format!("http://{}/mcp", listener.local_addr()?).into();
            let _server = AbortServerOnDrop(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
            let client = reqwest::Client::new();
            let challenge = if get {
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.get_stream(uri, None, None, None, HashMap::new()),
                )
                .await?;
                match result {
                    Err(StreamableHttpError::AuthRequired(error)) => error.www_authenticate_header,
                    Err(StreamableHttpError::InsufficientScope(error)) => {
                        error.www_authenticate_header
                    }
                    _ => panic!("expected typed authentication challenge from GET"),
                }
            } else {
                let message =
                    serde_json::from_value(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))?;
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.post_message(uri, message, None, None, HashMap::new()),
                )
                .await?;
                match result {
                    Err(StreamableHttpError::AuthRequired(error)) => error.www_authenticate_header,
                    Err(StreamableHttpError::InsufficientScope(error)) => {
                        error.www_authenticate_header
                    }
                    _ => panic!("expected typed authentication challenge from POST"),
                }
            };
            assert_eq!(
                challenge,
                "Basic realm=\"local, proxy\", Bearer error=\"insufficient_scope\", scope=\"files:write\"",
                "all authentication challenges must survive transport classification"
            );
            assert_eq!(
                hits.load(Ordering::SeqCst),
                1,
                "classification must not replay the request"
            );
        }
    }
    Ok(())
}
