#![cfg(not(feature = "local"))]
use rmcp::{
    ErrorData, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, CustomRequest,
        CustomResult, ProgressNotificationParam, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    transport::{
        StreamableHttpClientTransport,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use serde::Deserialize;
use serde_json::json;
use tokio_util::sync::CancellationToken;

mod common;
use common::calculator::Calculator;

const INIT_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#;
const CALL_WITH_PROGRESS_BODY: &str = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"progress","arguments":{},"_meta":{"progressToken":"progress-test-1"}}}"#;
const NEGOTIATED_CALL_BODY: &str = r#"{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
        "name": "terminal",
        "arguments": {},
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {
                "name": "test",
                "version": "1.0"
            },
            "io.modelcontextprotocol/clientCapabilities": {}
        }
    }
}"#;
const NEGOTIATED_CALL_WITH_PROGRESS_BODY: &str = r#"{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
        "name": "progress",
        "arguments": {},
        "_meta": {
            "progressToken": "progress-test-1",
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {
                "name": "test",
                "version": "1.0"
            },
            "io.modelcontextprotocol/clientCapabilities": {}
        }
    }
}"#;

#[derive(Clone)]
struct ProgressServer;

#[derive(Clone)]
struct TypedExtensionServer;

impl ServerHandler for TypedExtensionServer {
    async fn on_custom_request(
        &self,
        _request: CustomRequest,
        _context: RequestContext<rmcp::RoleServer>,
    ) -> Result<CustomResult, ErrorData> {
        Ok(CustomResult::new(json!({
            "resultType": "complete",
            "skills": ["http-skill"],
            "_meta": {"source": "streamable-http"}
        })))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypedExtensionResult {
    result_type: String,
    skills: Vec<String>,
    #[serde(rename = "_meta")]
    meta: serde_json::Value,
}

impl ServerHandler for ProgressServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name == "progress" {
            let progress_token = context
                .meta
                .get_progress_token()
                .expect("request includes progressToken");
            context
                .peer
                .notify_progress(
                    ProgressNotificationParam::new(progress_token, 50.0)
                        .with_total(100.0)
                        .with_message("working"),
                )
                .await
                .expect("progress notification is delivered");
        }
        Ok(CallToolResult::success(vec![ContentBlock::text("done")]).into())
    }
}

async fn spawn_server(
    config: StreamableHttpServerConfig,
) -> (reqwest::Client, String, CancellationToken) {
    let ct = config.cancellation_token.clone();
    let service: StreamableHttpService<Calculator, LocalSessionManager> =
        StreamableHttpService::new(|| Ok(Calculator::new()), Default::default(), config);

    let router = axum::Router::new().nest_service("/mcp", service);
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(tcp_listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}/mcp");
    (client, base_url, ct)
}

async fn spawn_progress_server(
    config: StreamableHttpServerConfig,
) -> (reqwest::Client, String, CancellationToken) {
    let ct = config.cancellation_token.clone();
    let service: StreamableHttpService<ProgressServer, LocalSessionManager> =
        StreamableHttpService::new(|| Ok(ProgressServer), Default::default(), config);

    let router = axum::Router::new().nest_service("/mcp", service);
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(tcp_listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}/mcp");
    (client, base_url, ct)
}

#[tokio::test]
async fn streamable_http_preserves_typed_extension_results() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let service: StreamableHttpService<TypedExtensionServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(TypedExtensionServer),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_sse_keep_alive(None)
                .with_cancellation_token(ct.child_token()),
        );
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/mcp", listener.local_addr()?);
    let server_ct = ct.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_ct.cancelled().await })
            .await;
    });

    let client = ().serve(StreamableHttpClientTransport::from_uri(url)).await?;
    let result: TypedExtensionResult = client
        .send_request_as(rmcp::model::ClientRequest::CustomRequest(
            CustomRequest::new("skills/list", None),
        ))
        .await?;
    assert_eq!(result.result_type, "complete");
    assert_eq!(result.skills, ["http-skill"]);
    assert_eq!(result.meta["source"], "streamable-http");
    client.cancel().await?;
    ct.cancel();
    Ok(())
}

#[tokio::test]
async fn stateless_json_response_returns_application_json() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let (client, url, ct) = spawn_server(
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .with_cancellation_token(ct.child_token()),
    )
    .await;

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(INIT_BODY)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/json"),
        "Expected application/json, got: {content_type}"
    );

    let body = response.text().await?;
    let parsed: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(parsed["jsonrpc"], "2.0");
    assert_eq!(parsed["id"], 1);
    assert!(parsed["result"].is_object(), "Expected result object");

    ct.cancel();
    Ok(())
}

#[tokio::test]
async fn stateless_negotiated_terminal_response_returns_application_json() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let (client, url, ct) = spawn_progress_server(
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .with_cancellation_token(ct.child_token()),
    )
    .await;

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "terminal")
        .body(NEGOTIATED_CALL_BODY)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/json"),
        "Expected application/json, got: {content_type}"
    );

    let body: serde_json::Value = response.json().await?;
    assert_eq!(body["id"], 2);
    assert!(body["result"].is_object(), "Expected result object");

    ct.cancel();
    Ok(())
}

#[tokio::test]
async fn stateless_json_response_falls_back_to_sse_for_progress() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let (client, url, ct) = spawn_progress_server(
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .with_cancellation_token(ct.child_token()),
    )
    .await;

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(CALL_WITH_PROGRESS_BODY)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "Expected SSE fallback, got: {content_type}"
    );

    let body = response.text().await?;
    let messages: Vec<serde_json::Value> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(messages.len(), 2, "Expected progress and result: {body}");
    assert_eq!(messages[0]["method"], "notifications/progress");
    assert_eq!(messages[1]["id"], 2);
    assert!(
        messages[1]["result"].is_object(),
        "Expected result object: {body}"
    );

    ct.cancel();
    Ok(())
}

#[tokio::test]
async fn stateless_negotiated_json_response_falls_back_to_sse_for_progress() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let (client, url, ct) = spawn_progress_server(
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .with_cancellation_token(ct.child_token()),
    )
    .await;

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "progress")
        .body(NEGOTIATED_CALL_WITH_PROGRESS_BODY)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "Expected SSE fallback, got: {content_type}"
    );

    let body = response.text().await?;
    let messages: Vec<serde_json::Value> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(messages.len(), 2, "Expected progress and result: {body}");
    assert_eq!(messages[0]["method"], "notifications/progress");
    assert_eq!(messages[1]["id"], 2);
    assert!(
        messages[1]["result"].is_object(),
        "Expected result object: {body}"
    );

    ct.cancel();
    Ok(())
}

#[tokio::test]
async fn stateless_sse_mode_default_unchanged() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let (client, url, ct) = spawn_server(
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_sse_keep_alive(None)
            .with_cancellation_token(ct.child_token()),
    )
    .await;

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(INIT_BODY)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "Expected text/event-stream, got: {content_type}"
    );

    let body = response.text().await?;
    assert!(
        body.contains("data:"),
        "Expected SSE framing (data: prefix), got: {body}"
    );

    ct.cancel();
    Ok(())
}

#[tokio::test]
async fn json_response_ignored_in_legacy_session_mode() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    // json_response: true has no effect when legacy_session_mode: true — server still uses SSE
    let (client, url, ct) = spawn_server(
        StreamableHttpServerConfig::default()
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .with_cancellation_token(ct.child_token()),
    )
    .await;

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(INIT_BODY)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "Stateful mode should always use SSE regardless of json_response, got: {content_type}"
    );

    ct.cancel();
    Ok(())
}
