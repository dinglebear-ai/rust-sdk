#![cfg(not(feature = "local"))]
use std::{future::Future, sync::Arc, time::Duration};

use rmcp::{
    ClientHandler, RoleClient, ServerHandler, ServiceExt,
    model::{
        ClientRequest, ClientResult, CustomRequest, CustomResult, ErrorCode, ErrorData,
        PingRequest, ServerRequest, ServerResult,
    },
    service::{PeerRequestOptions, ServiceError},
    transport::Transport,
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Mutex, Notify};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

type CustomRequestPayload = (String, Option<serde_json::Value>);

struct LegacyTypedTransport;

impl Transport<RoleClient> for LegacyTypedTransport {
    type Error = std::convert::Infallible;

    fn send(
        &mut self,
        _item: rmcp::service::TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    async fn receive(&mut self) -> Option<rmcp::service::RxJsonRpcMessage<RoleClient>> {
        None
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn existing_transport_implementations_get_the_raw_receive_compatibility_default() {
    let mut transport = LegacyTypedTransport;
    assert!(transport.receive_raw().await.is_none());
}

struct CustomRequestServer {
    receive_signal: Arc<Notify>,
    payload: Arc<Mutex<Option<CustomRequestPayload>>>,
}

impl ServerHandler for CustomRequestServer {
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CustomResult, rmcp::ErrorData> {
        let CustomRequest { method, params, .. } = request;
        *self.payload.lock().await = Some((method, params));
        self.receive_signal.notify_one();
        Ok(CustomResult::new(json!({ "status": "ok" })))
    }
}

#[tokio::test]
async fn test_custom_client_request_reaches_server() -> anyhow::Result<()> {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let receive_signal = Arc::new(Notify::new());
    let payload = Arc::new(Mutex::new(None));

    {
        let receive_signal = receive_signal.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            let server = CustomRequestServer {
                receive_signal,
                payload,
            }
            .serve(server_transport)
            .await?;
            server.waiting().await?;
            anyhow::Ok(())
        });
    }

    let client = ().serve(client_transport).await?;

    let response = client
        .send_request(ClientRequest::CustomRequest(CustomRequest::new(
            "requests/custom-test",
            Some(json!({ "foo": "bar" })),
        )))
        .await?;

    tokio::time::timeout(std::time::Duration::from_secs(5), receive_signal.notified()).await?;

    let (method, params) = payload.lock().await.take().expect("payload set");
    assert_eq!("requests/custom-test", method);
    assert_eq!(Some(json!({ "foo": "bar" })), params);

    match response {
        ServerResult::CustomResult(result) => {
            assert_eq!(result.0, json!({ "status": "ok" }));
        }
        other => panic!("Expected custom result, got: {other:?}"),
    }

    client.cancel().await?;
    Ok(())
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct SkillsListResult {
    result_type: String,
    skills: Vec<String>,
    #[serde(rename = "_meta")]
    meta: serde_json::Value,
}

struct TypedCustomRequestServer;

impl ServerHandler for TypedCustomRequestServer {
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CustomResult, rmcp::ErrorData> {
        if request.method == "skills/malformed" {
            return Ok(CustomResult::new(json!({
                "resultType": "complete",
                "skills": "not-an-array",
                "_meta": {}
            })));
        }
        if request.method == "skills/error" {
            return Err(ErrorData::new(
                ErrorCode::INVALID_PARAMS,
                "invalid skills request",
                None,
            ));
        }
        if request.method == "skills/slow" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(CustomResult::new(json!({
            "resultType": "complete",
            "skills": ["example"],
            "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "skills"}}
        })))
    }
}

async fn typed_test_client() -> anyhow::Result<rmcp::service::RunningService<rmcp::RoleClient, ()>>
{
    let (server_transport, client_transport) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let server = TypedCustomRequestServer.serve(server_transport).await?;
        server.waiting().await?;
        anyhow::Ok(())
    });
    Ok(().serve(client_transport).await?)
}

#[cfg(all(
    feature = "auth",
    feature = "transport-streamable-http-server",
    feature = "transport-streamable-http-client-reqwest"
))]
#[tokio::test]
async fn typed_skills_request_survives_oauth_http_wrapper() -> anyhow::Result<()> {
    use rmcp::transport::{
        auth::{AuthClient, AuthorizationManager},
        streamable_http_client::{StreamableHttpClientTransportConfig, StreamableHttpClientWorker},
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    };
    let stop = tokio_util::sync::CancellationToken::new();
    let server: StreamableHttpService<TypedCustomRequestServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(TypedCustomRequestServer),
            Default::default(),
            StreamableHttpServerConfig::default().with_cancellation_token(stop.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/mcp", listener.local_addr()?);
    let router = axum::Router::new().nest_service("/mcp", server);
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let manager = AuthorizationManager::new(&endpoint).await?;
        let auth = AuthClient::new(reqwest::Client::new(), manager);
        let transport = StreamableHttpClientWorker::new(
            auth,
            StreamableHttpClientTransportConfig::with_uri(endpoint),
        );
        let client = ().serve(transport).await?;
        let response = client
            .send_request_as::<SkillsListResult>(ClientRequest::CustomRequest(CustomRequest::new(
                "skills/list",
                Some(json!({})),
            )))
            .await;
        client.cancel().await?;
        let response = response?;
        assert_eq!(response.skills, ["example"]);
        assert_eq!(
            response.meta["io.modelcontextprotocol/serverInfo"]["name"],
            "skills"
        );
        anyhow::Ok(())
    })
    .await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    result??;
    Ok(())
}

#[cfg(all(
    feature = "auth",
    feature = "transport-streamable-http-server",
    feature = "transport-streamable-http-client-reqwest"
))]
#[tokio::test]
async fn json_tool_listing_populates_parameter_headers_through_auth_client() -> anyhow::Result<()> {
    use rmcp::{
        ClientServiceExt,
        model::{
            CallToolRequestParams, CallToolResponse, CallToolResult, ListToolsResult,
            PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
        },
        service::RequestContext,
        transport::{
            auth::{AuthClient, AuthorizationManager},
            streamable_http_client::{
                StreamableHttpClientTransportConfig, StreamableHttpClientWorker,
            },
            streamable_http_server::{
                StreamableHttpServerConfig, StreamableHttpService,
                session::local::LocalSessionManager,
            },
        },
    };

    struct AnnotatedToolServer;
    impl ServerHandler for AnnotatedToolServer {
        fn supported_protocol_versions(
            &self,
        ) -> std::borrow::Cow<'static, [rmcp::model::ProtocolVersion]> {
            std::borrow::Cow::Borrowed(&[rmcp::model::ProtocolVersion::V_2026_07_28])
        }

        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
        }

        fn get_tool(&self, name: &str) -> Option<Tool> {
            (name == "deploy").then(|| {
                Tool::new(
                    "deploy",
                    "Deploy in a region",
                    Arc::new(
                        json!({"type": "object", "properties": {
                            "region": {"type": "string", "x-mcp-header": "Region"}
                        }})
                        .as_object()
                        .unwrap()
                        .clone(),
                    ),
                )
            })
        }

        async fn list_tools(
            &self,
            _: Option<PaginatedRequestParams>,
            _: RequestContext<rmcp::RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            Ok(ListToolsResult::with_all_items(vec![
                self.get_tool("deploy").unwrap(),
            ]))
        }

        async fn call_tool(
            &self,
            _: CallToolRequestParams,
            _: RequestContext<rmcp::RoleServer>,
        ) -> Result<CallToolResponse, ErrorData> {
            Ok(CallToolResult::success(vec![]).into())
        }
    }

    let observed_headers = Arc::new(Mutex::new(Vec::new()));
    let capture = observed_headers.clone();
    let stop = tokio_util::sync::CancellationToken::new();
    let server: StreamableHttpService<AnnotatedToolServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(AnnotatedToolServer),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_cancellation_token(stop.child_token()),
        );
    let router = axum::Router::new()
        .nest_service("/mcp", server)
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let capture = capture.clone();
                async move {
                    if request
                        .headers()
                        .get("Mcp-Method")
                        .is_some_and(|method| method == "tools/call")
                    {
                        capture
                            .lock()
                            .await
                            .push(request.headers().get("Mcp-Param-Region").cloned());
                    }
                    next.run(request).await
                }
            },
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/mcp", listener.local_addr()?);
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let manager = AuthorizationManager::new(&endpoint).await?;
        let transport = StreamableHttpClientWorker::new(
            AuthClient::new(reqwest::Client::new(), manager),
            StreamableHttpClientTransportConfig::with_uri(endpoint),
        );
        let client = ()
            .serve_with_lifecycle(
                transport,
                rmcp::service::ClientLifecycleMode::Discover {
                    preferred_versions: vec![rmcp::model::ProtocolVersion::V_2026_07_28],
                },
            )
            .await?;
        let listed = client.list_tools(None).await?;
        assert_eq!(listed.tools.len(), 1);
        let response = client
            .call_tool(
                CallToolRequestParams::new("deploy")
                    .with_arguments(json!({"region": "us-east-1"}).as_object().unwrap().clone()),
            )
            .await;
        client.cancel().await?;
        response?;
        assert_eq!(
            *observed_headers.lock().await,
            vec![Some(http::HeaderValue::from_static("us-east-1"))]
        );
        anyhow::Ok(())
    })
    .await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    result??;
    Ok(())
}

#[tokio::test]
async fn typed_custom_request_bypasses_the_core_response_union() -> anyhow::Result<()> {
    let client = typed_test_client().await?;
    let response: SkillsListResult = client
        .send_request_as(ClientRequest::CustomRequest(CustomRequest::new(
            "skills/list",
            Some(json!({})),
        )))
        .await?;

    assert_eq!(response.result_type, "complete");
    assert_eq!(response.skills, ["example"]);
    assert_eq!(
        response.meta["io.modelcontextprotocol/serverInfo"]["name"],
        "skills"
    );

    client.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn typed_custom_requests_preserve_errors_correlation_and_recovery() -> anyhow::Result<()> {
    let client = typed_test_client().await?;

    let malformed = client
        .send_request_as::<SkillsListResult>(ClientRequest::CustomRequest(CustomRequest::new(
            "skills/malformed",
            None,
        )))
        .await;
    assert!(matches!(
        malformed,
        Err(ServiceError::ResponseDeserialization(_))
    ));

    let protocol_error = client
        .send_request_as::<SkillsListResult>(ClientRequest::CustomRequest(CustomRequest::new(
            "skills/error",
            None,
        )))
        .await;
    assert!(matches!(protocol_error, Err(ServiceError::McpError(_))));

    let typed = client.send_request_as::<SkillsListResult>(ClientRequest::CustomRequest(
        CustomRequest::new("skills/list", None),
    ));
    let standard = client.send_request(ClientRequest::PingRequest(PingRequest::default()));
    let (typed, standard) = tokio::join!(typed, standard);
    assert_eq!(typed?.skills, ["example"]);
    assert!(matches!(standard?, ServerResult::EmptyResult(_)));

    client.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn typed_custom_requests_use_standard_timeout_and_cancellation() -> anyhow::Result<()> {
    let client = typed_test_client().await?;
    let handle = client
        .send_request_as_with_option::<SkillsListResult>(
            ClientRequest::CustomRequest(CustomRequest::new("skills/slow", None)),
            PeerRequestOptions::with_timeout(Duration::from_millis(10)),
        )
        .await?;
    assert!(matches!(
        handle.await_response().await,
        Err(ServiceError::Timeout { .. })
    ));

    let recovered: SkillsListResult = client
        .send_request_as(ClientRequest::CustomRequest(CustomRequest::new(
            "skills/list",
            None,
        )))
        .await?;
    assert_eq!(recovered.skills, ["example"]);

    client.cancel().await?;
    Ok(())
}

struct CustomRequestClient {
    receive_signal: Arc<Notify>,
    payload: Arc<Mutex<Option<CustomRequestPayload>>>,
}

impl ClientHandler for CustomRequestClient {
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        _context: rmcp::service::RequestContext<rmcp::RoleClient>,
    ) -> Result<CustomResult, rmcp::ErrorData> {
        let CustomRequest { method, params, .. } = request;
        *self.payload.lock().await = Some((method, params));
        self.receive_signal.notify_one();
        Ok(CustomResult::new(json!({ "status": "ok" })))
    }
}

struct CustomRequestServerNotifier {
    receive_signal: Arc<Notify>,
    response: Arc<Mutex<Option<Result<serde_json::Value, String>>>>,
}

impl ServerHandler for CustomRequestServerNotifier {
    async fn on_initialized(&self, context: rmcp::service::NotificationContext<rmcp::RoleServer>) {
        let peer = context.peer.clone();
        let receive_signal = self.receive_signal.clone();
        let response = self.response.clone();
        tokio::spawn(async move {
            let result = peer
                .send_request(ServerRequest::CustomRequest(CustomRequest::new(
                    "requests/custom-server",
                    Some(json!({ "ping": "pong" })),
                )))
                .await;
            let payload = match result {
                Ok(ClientResult::CustomResult(result)) => Ok(result.0),
                Ok(other) => Err(format!("Unexpected response: {other:?}")),
                Err(err) => Err(format!("Failed to send request: {err:?}")),
            };
            *response.lock().await = Some(payload);
            receive_signal.notify_one();
        });
    }
}

#[tokio::test]
async fn test_custom_server_request_reaches_client() -> anyhow::Result<()> {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let response_signal = Arc::new(Notify::new());
    let response = Arc::new(Mutex::new(None));
    tokio::spawn({
        let response_signal = response_signal.clone();
        let response = response.clone();
        async move {
            let server = CustomRequestServerNotifier {
                receive_signal: response_signal,
                response,
            }
            .serve(server_transport)
            .await?;
            server.waiting().await?;
            anyhow::Ok(())
        }
    });

    let receive_signal = Arc::new(Notify::new());
    let payload = Arc::new(Mutex::new(None));

    let client = CustomRequestClient {
        receive_signal: receive_signal.clone(),
        payload: payload.clone(),
    }
    .serve(client_transport)
    .await?;

    tokio::time::timeout(std::time::Duration::from_secs(5), receive_signal.notified()).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        response_signal.notified(),
    )
    .await?;

    let (method, params) = payload.lock().await.take().expect("payload set");
    assert_eq!("requests/custom-server", method);
    assert_eq!(Some(json!({ "ping": "pong" })), params);

    let response = response.lock().await.take().expect("response set");
    let response = response.expect("custom request response ok");
    assert_eq!(response, json!({ "status": "ok" }));

    client.cancel().await?;
    Ok(())
}
