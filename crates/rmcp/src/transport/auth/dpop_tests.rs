use std::collections::VecDeque;

use oauth2::HttpResponse;

use super::*;
fn claims(proof: &str) -> serde_json::Value {
    serde_json::from_slice(
        &BASE64_URL_SAFE_NO_PAD
            .decode(proof.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap()
}
#[test]
fn proof_is_signed_fresh_and_has_only_public_jwk() {
    let key = DpopKey::generate().unwrap();
    let a = key
        .proof(
            "POST",
            "https://mcp.example/a?secret=query#fragment",
            Some("server-nonce"),
            Some("access-secret"),
        )
        .unwrap();
    let b = key
        .proof("POST", "https://mcp.example/a", None, None)
        .unwrap();
    let parts: Vec<_> = a.split('.').collect();
    let header: serde_json::Value =
        serde_json::from_slice(&BASE64_URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
    assert_eq!(header["typ"], "dpop+jwt");
    assert_eq!(header["alg"], "ES256");
    assert!(header["jwk"].get("d").is_none());
    assert_eq!(claims(&a)["htu"], "https://mcp.example/a");
    assert_eq!(claims(&a)["htm"], "POST");
    assert_eq!(claims(&a)["nonce"], "server-nonce");
    assert_eq!(
        claims(&a)["ath"],
        BASE64_URL_SAFE_NO_PAD.encode(Sha256::digest(b"access-secret"))
    );
    assert_ne!(claims(&a)["jti"], claims(&b)["jti"]);
    assert!(claims(&b).get("ath").is_none());
    aws_lc_rs::signature::UnparsedPublicKey::new(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED,
        key.key.public_key().as_ref(),
    )
    .verify(
        format!("{}.{}", parts[0], parts[1]).as_bytes(),
        &BASE64_URL_SAFE_NO_PAD.decode(parts[2]).unwrap(),
    )
    .unwrap();
    assert!(
        key.proof("post", "https://mcp.example", None, None)
            .is_err()
    );
    assert!(
        key.proof("POST", "http://remote.example", None, None)
            .is_err()
    );
    assert!(
        key.proof("GET", "https://user:pass@mcp.example", None, None)
            .is_err()
    );
    assert!(!format!("{key:?}").contains("server-nonce"));
    assert!(bound_endpoint("https://mcp.example#fragment").is_err());
}
struct Scripted {
    responses: Mutex<VecDeque<HttpResponse>>,
    proofs: Mutex<Vec<String>>,
}
impl OAuthHttpClient for Scripted {
    fn execute(&self, request: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            assert_eq!(request.redirect_policy, OAuthHttpRedirectPolicy::Stop);
            self.proofs.lock().unwrap().push(
                request.request.headers()["dpop"]
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
            Ok(self.responses.lock().unwrap().pop_front().unwrap())
        })
    }
}
fn response(status: u16, body: &str, nonce: Option<&str>) -> HttpResponse {
    let mut r = http::Response::builder().status(status);
    if let Some(n) = nonce {
        r = r.header("dpop-nonce", n);
    }
    r.body(body.as_bytes().to_vec()).unwrap()
}
fn token_request() -> OAuthHttpRequest {
    OAuthHttpRequest::new(
        http::Request::builder()
            .method("POST")
            .uri("https://as.example/token")
            .body(b"grant_type=authorization_code&resource=https%3A%2F%2Fmcp.example%2F".to_vec())
            .unwrap(),
        OAuthHttpRedirectPolicy::Follow,
    )
}
#[tokio::test]
async fn token_nonce_retry_is_bounded_fresh_and_rejects_bearer_downgrade() {
    let inner = Arc::new(Scripted {
        responses: Mutex::new(VecDeque::from([
            response(400, r#"{"error":"use_dpop_nonce"}"#, Some("as-only")),
            response(
                200,
                r#"{"access_token":"secret","token_type":"DPoP"}"#,
                None,
            ),
        ])),
        proofs: Mutex::new(vec![]),
    });
    let wrapper = DpopOAuthHttpClient::new(
        inner.clone(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    assert!(wrapper.resource_client("https://mcp.example").is_err());
    wrapper.execute(token_request()).await.unwrap();
    {
        let proofs = inner.proofs.lock().unwrap();
        assert_eq!(proofs.len(), 2);
        assert!(claims(&proofs[0]).get("nonce").is_none());
        assert_eq!(claims(&proofs[1])["nonce"], "as-only");
        assert_ne!(claims(&proofs[0])["jti"], claims(&proofs[1])["jti"]);
    }
    assert!(wrapper.resource_client("https://other.example").is_err());
    let resource = wrapper.resource_client("https://mcp.example").unwrap();
    assert!(resource.nonce.lock().unwrap().is_none());
    assert!(!format!("{resource:?}").contains("secret"));
    let inner = Arc::new(Scripted {
        responses: Mutex::new(VecDeque::from([response(
            200,
            r#"{"access_token":"secret","token_type":"Bearer"}"#,
            None,
        )])),
        proofs: Mutex::new(vec![]),
    });
    let wrapper = DpopOAuthHttpClient::new(
        inner,
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    assert!(wrapper.execute(token_request()).await.is_err());
    assert!(wrapper.resource_client("https://mcp.example").is_err());
}
#[tokio::test]
async fn repeated_nonce_challenge_stops_after_two_attempts() {
    let inner = Arc::new(Scripted {
        responses: Mutex::new(VecDeque::from([
            response(400, r#"{"error":"use_dpop_nonce"}"#, Some("a")),
            response(400, r#"{"error":"use_dpop_nonce"}"#, Some("b")),
        ])),
        proofs: Mutex::new(vec![]),
    });
    let wrapper = DpopOAuthHttpClient::new(
        inner.clone(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    assert!(wrapper.execute(token_request()).await.is_err());
    assert_eq!(inner.proofs.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn resource_binding_blocks_foreign_url_and_ambient_auth_before_network() {
    let c = DpopClient::bound(
        reqwest::Client::builder(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://mcp.example/mcp",
        "secret".into(),
    )
    .unwrap();
    assert!(
        c.send_resource_request(c.http.get("https://elsewhere.example"))
            .await
            .is_err()
    );
    assert!(
        c.send_resource_request(c.http.post("https://mcp.example/mcp").bearer_auth("other"))
            .await
            .is_err()
    );
    assert!(
        c.send_resource_request(
            c.http
                .post("https://mcp.example/mcp")
                .header("dpop", "forged")
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn resource_all_methods_get_fresh_proofs_and_only_one_nonce_retry() {
    use axum::{Router, body::Body, extract::State, routing::any};
    type Seen = Arc<Mutex<Vec<(String, serde_json::Value)>>>;
    async fn handler(
        State(seen): State<Seen>,
        request: http::Request<Body>,
    ) -> http::Response<Body> {
        assert_eq!(request.headers()["authorization"], "DPoP test-token");
        let proof = request.headers()["dpop"].to_str().unwrap();
        let payload = claims(proof);
        let mut seen = seen.lock().unwrap();
        seen.push((request.method().to_string(), payload.clone()));
        if payload.get("nonce").is_none() {
            return http::Response::builder()
                .status(401)
                .header(
                    "www-authenticate",
                    "Bearer realm=\"other\", DPoP error=\"use_dpop_nonce\"",
                )
                .header("dpop-nonce", "rs-only")
                .body(Body::empty())
                .unwrap();
        }
        http::Response::builder()
            .status(204)
            .body(Body::empty())
            .unwrap()
    }
    let seen: Seen = Default::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/mcp", any(handler))
        .with_state(seen.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let uri = format!("http://{address}/mcp?query=excluded");
    let c = DpopClient::bound(
        reqwest::Client::builder(),
        Arc::new(DpopKey::generate().unwrap()),
        &uri,
        "test-token".into(),
    )
    .unwrap();
    for method in [http::Method::POST, http::Method::GET, http::Method::DELETE] {
        let response = c
            .send_resource_request(c.http.request(method, &uri))
            .await
            .unwrap();
        assert_eq!(response.status(), 204);
    }
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 4);
        let mut ids = std::collections::HashSet::new();
        for (method, payload) in requests.iter() {
            assert_eq!(payload["htm"], method.as_str());
            assert_eq!(payload["htu"], format!("http://{address}/mcp"));
            assert!(ids.insert(payload["jti"].as_str().unwrap()));
            assert_eq!(
                payload["ath"],
                BASE64_URL_SAFE_NO_PAD.encode(Sha256::digest(b"test-token"))
            );
        }
    }
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn repeated_resource_nonce_challenges_are_not_replayed() {
    use axum::{Router, body::Body, extract::State, routing::any};
    type Calls = Arc<std::sync::atomic::AtomicUsize>;
    async fn handler(State(calls): State<Calls>) -> http::Response<Body> {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        http::Response::builder()
            .status(401)
            .header("www-authenticate", "Bearer realm=\"other\"")
            .header("www-authenticate", "DPoP error=\"use_dpop_nonce\"")
            .header("dpop-nonce", "repeat")
            .body(Body::empty())
            .unwrap()
    }
    let calls: Calls = Default::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/mcp", any(handler))
        .with_state(calls.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = DpopClient::bound(
        reqwest::Client::builder(),
        Arc::new(DpopKey::generate().unwrap()),
        &uri,
        "secret".into(),
    )
    .unwrap();
    assert!(
        client
            .send_resource_request(client.http.post(&uri))
            .await
            .is_err()
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
}
#[test]
fn malformed_nonce_is_rejected() {
    for n in [
        "",
        "contains space",
        "contains\nnewline",
        "quoted\"",
        "back\\slash",
    ] {
        assert!(validate_nonce(n).is_err());
    }
    assert!(validate_nonce(&"a".repeat(4097)).is_err());
}

#[test]
fn only_a_dpop_nonce_auth_challenge_permits_resource_replay() {
    assert!(resource_nonce_challenge("DPoP error=\"use_dpop_nonce\""));
    assert!(resource_nonce_challenge("dpop error=use_dpop_nonce"));
    assert!(resource_nonce_challenge(
        "Bearer realm=\"other\", DPoP realm=\"mcp\", error=\"use_dpop_nonce\""
    ));
    assert!(!resource_nonce_challenge(
        "DPoP error_description=\"foo, error=use_dpop_nonce\""
    ));
    assert!(!resource_nonce_challenge(
        "Bearer realm=\"DPoP error=use_dpop_nonce\""
    ));
    assert!(!resource_nonce_challenge("Bearer error=\"use_dpop_nonce\""));
    assert!(!resource_nonce_challenge("DPoP error=\"invalid_token\""));
    assert!(!resource_nonce_challenge("DPoP error=\"xuse_dpop_nonce\""));
}

#[tokio::test]
async fn expired_operation_budget_never_dispatches_nonce_retry() {
    struct SlowNonce {
        calls: std::sync::atomic::AtomicUsize,
    }
    impl OAuthHttpClient for SlowNonce {
        fn execute(&self, _: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
            Box::pin(async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(75)).await;
                Ok(response(
                    400,
                    r#"{"error":"use_dpop_nonce"}"#,
                    Some("challenge"),
                ))
            })
        }
    }
    let inner = Arc::new(SlowNonce {
        calls: Default::default(),
    });
    let wrapper = DpopOAuthHttpClient::new(
        inner.clone(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    let mut request = token_request();
    request.timeout = Some(std::time::Duration::from_millis(50));
    assert!(wrapper.execute(request).await.is_err());
    assert_eq!(inner.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let wrapper = DpopOAuthHttpClient::new(
        inner.clone(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    let mut request = token_request();
    request.timeout = Some(std::time::Duration::ZERO);
    assert!(wrapper.execute(request).await.is_err());
    assert_eq!(inner.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_wrapper_cannot_replace_one_subjects_token_with_another() {
    let inner = Arc::new(Scripted {
        responses: Mutex::new(VecDeque::from([
            response(
                200,
                r#"{"access_token":"subject-a","token_type":"DPoP"}"#,
                None,
            ),
            response(
                200,
                r#"{"access_token":"subject-b","token_type":"DPoP"}"#,
                None,
            ),
        ])),
        proofs: Mutex::new(vec![]),
    });
    let wrapper = DpopOAuthHttpClient::new(
        inner.clone(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    let (first, second) = tokio::join!(
        wrapper.execute(token_request()),
        wrapper.execute(token_request())
    );
    assert!(first.is_ok());
    assert!(second.is_err());
    assert_eq!(inner.proofs.lock().unwrap().len(), 1);
    let client = wrapper.resource_client("https://mcp.example").unwrap();
    assert_eq!(&*client.token, "subject-a");
}

#[tokio::test]
async fn foreign_resource_token_grant_is_rejected_before_dispatch() {
    let inner = Arc::new(Scripted {
        responses: Mutex::new(VecDeque::new()),
        proofs: Mutex::new(vec![]),
    });
    let wrapper = DpopOAuthHttpClient::new(
        inner.clone(),
        Arc::new(DpopKey::generate().unwrap()),
        "https://as.example/token",
        "https://mcp.example",
    )
    .unwrap();
    let mut request = token_request();
    *request.request.body_mut() =
        b"grant_type=authorization_code&resource=https%3A%2F%2Fother.example".to_vec();
    assert!(wrapper.execute(request).await.is_err());
    assert!(inner.proofs.lock().unwrap().is_empty());
}
