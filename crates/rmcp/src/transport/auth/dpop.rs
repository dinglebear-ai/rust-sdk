//! Opt-in RFC 9449 sender-constrained OAuth tokens.
//!
//! Keep one key for the entire token lifetime. This API deliberately does not
//! persist tokens: persisting a token without its private key breaks binding.
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use aws_lc_rs::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
};
use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

use super::{OAuthHttpClient, OAuthHttpClientFuture, OAuthHttpRedirectPolicy, OAuthHttpRequest};

/// A sanitized DPoP error; never contains proofs, tokens, nonces or HTTP bodies.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DpopError {
    /// Key generation or signing failed.
    #[error("DPoP cryptographic operation failed")]
    Crypto,
    /// An endpoint, method, token type or header violates the binding policy.
    #[error("DPoP binding policy rejected the request")]
    Binding,
    /// The request failed or the server repeated its nonce challenge.
    #[error("DPoP HTTP operation failed")]
    Http,
}

/// Ephemeral ES256 proof key, shared by token and resource HTTP clients.
pub struct DpopKey {
    key: EcdsaKeyPair,
}
impl fmt::Debug for DpopKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DpopKey([REDACTED])")
    }
}
impl DpopKey {
    /// Generate a fresh P-256 key using the operating system RNG.
    pub fn generate() -> Result<Self, DpopError> {
        Ok(Self {
            key: EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING)
                .map_err(|_| DpopError::Crypto)?,
        })
    }
    /// Sign a fresh proof. Query and fragment never appear in `htu`.
    pub fn proof(
        &self,
        method: &str,
        uri: &str,
        nonce: Option<&str>,
        token: Option<&str>,
    ) -> Result<String, DpopError> {
        let mut url = endpoint(uri)?;
        url.set_query(None);
        url.set_fragment(None);
        if method.is_empty() || !method.bytes().all(|b| b.is_ascii_uppercase()) {
            return Err(DpopError::Binding);
        }
        let public = self.key.public_key();
        let public = public.as_ref();
        let header = serde_json::json!({"typ":"dpop+jwt", "alg":"ES256", "jwk":{"kty":"EC", "crv":"P-256", "x": BASE64_URL_SAFE_NO_PAD.encode(&public[1..33]), "y": BASE64_URL_SAFE_NO_PAD.encode(&public[33..65])}});
        let mut random = [0; 32];
        aws_lc_rs::rand::SecureRandom::fill(&SystemRandom::new(), &mut random)
            .map_err(|_| DpopError::Crypto)?;
        let mut claims = serde_json::json!({"jti":BASE64_URL_SAFE_NO_PAD.encode(random), "htm":method,"htu":url.as_str(),"iat":SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| DpopError::Crypto)?.as_secs()});
        if let Some(nonce) = nonce {
            validate_nonce(nonce)?;
            claims["nonce"] = nonce.into();
        }
        if let Some(token) = token {
            claims["ath"] = BASE64_URL_SAFE_NO_PAD
                .encode(Sha256::digest(token.as_bytes()))
                .into();
        }
        let input = format!(
            "{}.{}",
            BASE64_URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&header).map_err(|_| DpopError::Crypto)?),
            BASE64_URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&claims).map_err(|_| DpopError::Crypto)?)
        );
        let signature = self
            .key
            .sign(&SystemRandom::new(), input.as_bytes())
            .map_err(|_| DpopError::Crypto)?;
        Ok(format!(
            "{input}.{}",
            BASE64_URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
    }
}
fn endpoint(uri: &str) -> Result<url::Url, DpopError> {
    let url = url::Url::parse(uri).map_err(|_| DpopError::Binding)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || !(url.scheme() == "https"
            || (url.scheme() == "http"
                && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))))
    {
        return Err(DpopError::Binding);
    }
    Ok(url)
}
fn bound_endpoint(uri: &str) -> Result<url::Url, DpopError> {
    let url = endpoint(uri)?;
    if url.fragment().is_some() {
        return Err(DpopError::Binding);
    }
    Ok(url)
}
fn validate_nonce(nonce: &str) -> Result<(), DpopError> {
    if nonce.is_empty()
        || nonce.len() > 4096
        || !nonce
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'"' && b != b'\\')
    {
        return Err(DpopError::Binding);
    }
    Ok(())
}
fn sensitive_header(value: &str) -> Result<http::HeaderValue, DpopError> {
    let mut header = http::HeaderValue::from_str(value).map_err(|_| DpopError::Binding)?;
    header.set_sensitive(true);
    Ok(header)
}
fn nonce(headers: &http::HeaderMap) -> Result<Option<String>, DpopError> {
    headers
        .get("dpop-nonce")
        .map(|v| {
            let s = v.to_str().map_err(|_| DpopError::Binding)?;
            validate_nonce(s)?;
            Ok(s.to_owned())
        })
        .transpose()
}

fn resource_nonce_challenge(value: &str) -> bool {
    // RFC 9110 challenge lists contain commas both between challenges and
    // inside quoted auth-param values. Never interpret quoted text as params.
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' {
            quoted = !quoted;
        }
        if byte == b',' && !quoted {
            parts.push(&value[start..index]);
            start = index + 1;
        }
    }
    if quoted || escaped {
        return false;
    }
    parts.push(&value[start..]);
    let mut dpop = false;
    for part in parts {
        let part = part.trim();
        let end = part
            .find(|c: char| c.is_ascii_whitespace() || c == '=')
            .unwrap_or(part.len());
        let name = &part[..end];
        let rest = part[end..].trim_start();
        let param = if rest.starts_with('=') {
            part
        } else {
            dpop = name.eq_ignore_ascii_case("DPoP");
            rest
        };
        if !dpop {
            continue;
        }
        if let Some((key, raw)) = param.split_once('=')
            && key.trim().eq_ignore_ascii_case("error")
            && let Some((value, used)) =
                super::AuthorizationManager::parse_next_header_value(raw.trim_start())
            && value == "use_dpop_nonce"
            && raw.trim_start()[used..].trim().is_empty()
        {
            return true;
        }
    }
    false
}

/// Token endpoint proof wrapper around the caller's existing network policy.
/// Nonces are isolated from resource nonces. Token POSTs never follow redirects.
/// Each wrapper permits exactly one grant attempt (including its nonce retry).
/// Create a new wrapper for a new grant; retain the same proof key for refresh.
/// A wrapper cannot be shared between authorization flows or subjects.
pub struct DpopOAuthHttpClient {
    inner: Arc<dyn OAuthHttpClient>,
    key: Arc<DpopKey>,
    token_endpoint: url::Url,
    resource: url::Url,
    nonce: Mutex<Option<String>>,
    issued_token: Mutex<Option<String>>,
    grant_started: std::sync::atomic::AtomicBool,
}
impl fmt::Debug for DpopOAuthHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DpopOAuthHttpClient([REDACTED])")
    }
}
impl DpopOAuthHttpClient {
    /// Bind a proof key and an explicitly approved authorization-server endpoint.
    pub fn new(
        inner: Arc<dyn OAuthHttpClient>,
        key: Arc<DpopKey>,
        token_endpoint: &str,
        resource: &str,
    ) -> Result<Self, DpopError> {
        Ok(Self {
            inner,
            key,
            token_endpoint: bound_endpoint(token_endpoint)?,
            resource: bound_endpoint(resource)?,
            nonce: Mutex::new(None),
            issued_token: Mutex::new(None),
            grant_started: std::sync::atomic::AtomicBool::new(false),
        })
    }
    /// Build a resource client from this wrapper's acquired DPoP token.
    /// A bearer or foreign token cannot enter through this constructor.
    pub fn resource_client(&self, resource: &str) -> Result<DpopClient, DpopError> {
        if endpoint(resource)? != self.resource {
            return Err(DpopError::Binding);
        }
        let token = self
            .issued_token
            .lock()
            .map_err(|_| DpopError::Binding)?
            .clone()
            .ok_or(DpopError::Binding)?;
        DpopClient::bound(
            reqwest::Client::builder(),
            self.key.clone(),
            resource,
            token,
        )
    }
    /// Preserve caller DNS/proxy/TLS policy while enforcing no redirects.
    /// Uses an ephemeral token acquired by this wrapper under the same key.
    pub fn resource_client_with_builder(
        &self,
        resource: &str,
        builder: reqwest::ClientBuilder,
    ) -> Result<DpopClient, DpopError> {
        if endpoint(resource)? != self.resource {
            return Err(DpopError::Binding);
        }
        let token = self
            .issued_token
            .lock()
            .map_err(|_| DpopError::Binding)?
            .clone()
            .ok_or(DpopError::Binding)?;
        DpopClient::bound(builder, self.key.clone(), resource, token)
    }
}
impl OAuthHttpClient for DpopOAuthHttpClient {
    fn execute(&self, request: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            if request.request.uri() != self.token_endpoint.as_str() {
                if request.request.method() == http::Method::POST
                    && url::form_urlencoded::parse(request.request.body())
                        .any(|(name, _)| name == "grant_type")
                {
                    return Err(Box::new(DpopError::Binding) as _);
                }
                return self.inner.execute(request).await;
            }
            if request.request.method() != http::Method::POST {
                return Err(Box::new(DpopError::Binding) as _);
            }
            let resources: Vec<_> = url::form_urlencoded::parse(request.request.body())
                .filter(|(name, _)| name == "resource")
                .map(|(_, value)| value)
                .collect();
            if resources.len() != 1 || resources[0].as_ref() != self.resource.as_str() {
                return Err(Box::new(DpopError::Binding) as _);
            }
            // One wrapper belongs to one grant attempt, including its internal
            // nonce retry. Sharing it between subjects must never replace a token.
            if self
                .grant_started
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_err()
            {
                return Err(Box::new(DpopError::Binding) as _);
            }

            let mut template = request.request;
            let started = std::time::Instant::now();
            for attempt in 0..2 {
                let current_nonce = self.nonce.lock().map_err(|_| DpopError::Binding)?.clone();
                let proof = self.key.proof(
                    "POST",
                    self.token_endpoint.as_str(),
                    current_nonce.as_deref(),
                    None,
                )?;
                template
                    .headers_mut()
                    .insert("dpop", sensitive_header(&proof)?);
                let remaining = request
                    .timeout
                    .map(|limit| limit.saturating_sub(started.elapsed()));
                if remaining.is_some_and(|duration| duration.is_zero()) {
                    return Err(Box::new(DpopError::Http) as _);
                }
                let response = self
                    .inner
                    .execute(OAuthHttpRequest {
                        request: template.clone(),
                        redirect_policy: OAuthHttpRedirectPolicy::Stop,
                        timeout: remaining,
                    })
                    .await?;
                let next = nonce(response.headers())?;
                if let Some(ref n) = next {
                    *self.nonce.lock().map_err(|_| DpopError::Binding)? = Some(n.clone());
                }
                let body: serde_json::Value =
                    serde_json::from_slice(response.body()).unwrap_or_default();
                if response.status() == http::StatusCode::BAD_REQUEST
                    && body.get("error").and_then(|v| v.as_str()) == Some("use_dpop_nonce")
                    && next.is_some()
                {
                    if attempt == 0 {
                        continue;
                    }
                    return Err(Box::new(DpopError::Http) as _);
                }
                if response.status().is_success() {
                    if !body
                        .get("token_type")
                        .and_then(|v| v.as_str())
                        .is_some_and(|v| v.eq_ignore_ascii_case("DPoP"))
                    {
                        return Err(Box::new(DpopError::Binding) as _);
                    }
                    let token = body
                        .get("access_token")
                        .and_then(|v| v.as_str())
                        .filter(|v| !v.is_empty())
                        .ok_or(DpopError::Binding)?;
                    *self.issued_token.lock().map_err(|_| DpopError::Binding)? =
                        Some(token.to_owned());
                }
                return Ok(response);
            }
            unreachable!()
        })
    }
}

/// A typed DPoP resource client using the ordinary SDK JSON/SSE decoder and limits.
/// It permits only the bound resource URL and uses no redirects or ambient auth.
#[derive(Clone)]
pub struct DpopClient {
    http: reqwest::Client,
    key: Arc<DpopKey>,
    resource: url::Url,
    token: Arc<str>,
    nonce: Arc<Mutex<Option<String>>>,
}
impl fmt::Debug for DpopClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DpopClient([REDACTED])")
    }
}
impl DpopClient {
    fn bound(
        builder: reqwest::ClientBuilder,
        key: Arc<DpopKey>,
        resource: &str,
        token: String,
    ) -> Result<Self, DpopError> {
        let http = builder
            .connection_verbose(false)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| DpopError::Http)?;
        Ok(Self {
            http,
            key,
            resource: bound_endpoint(resource)?,
            token: token.into(),
            nonce: Arc::new(Mutex::new(None)),
        })
    }
    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http
    }
    fn authorize_resource_request(
        &self,
        template: &mut reqwest::Request,
        nonce: Option<&str>,
    ) -> Result<(), DpopError> {
        let proof = self.key.proof(
            template.method().as_str(),
            template.url().as_str(),
            nonce,
            Some(&self.token),
        )?;
        template
            .headers_mut()
            .insert("dpop", sensitive_header(&proof)?);
        template.headers_mut().insert(
            http::header::AUTHORIZATION,
            sensitive_header(&format!("DPoP {}", self.token))?,
        );
        Ok(())
    }
    pub(crate) async fn send_resource_request(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, DpopError> {
        let mut template = builder.build().map_err(|_| DpopError::Http)?;
        let deadline = std::time::Instant::now()
            + template
                .timeout()
                .copied()
                .unwrap_or(std::time::Duration::from_secs(30));
        if template.url() != &self.resource
            || template.headers().contains_key(http::header::AUTHORIZATION)
            || template.headers().contains_key("dpop")
        {
            return Err(DpopError::Binding);
        }
        for attempt in 0..2 {
            let n = self.nonce.lock().map_err(|_| DpopError::Binding)?.clone();
            self.authorize_resource_request(&mut template, n.as_deref())?;
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(DpopError::Http);
            }
            *template.timeout_mut() = Some(remaining);
            let response = self
                .http
                .execute(template.try_clone().ok_or(DpopError::Binding)?)
                .await
                .map_err(|_| DpopError::Http)?;
            let next = nonce(response.headers())?;
            if let Some(ref n) = next {
                *self.nonce.lock().map_err(|_| DpopError::Binding)? = Some(n.clone());
            }
            let challenged = response.status() == http::StatusCode::UNAUTHORIZED
                && next.is_some()
                && response
                    .headers()
                    .get_all(http::header::WWW_AUTHENTICATE)
                    .iter()
                    .any(|value| value.to_str().ok().is_some_and(resource_nonce_challenge));
            if challenged {
                if attempt == 0 {
                    continue;
                }
                return Err(DpopError::Http);
            }
            return Ok(response);
        }
        unreachable!()
    }
}

#[cfg(test)]
#[path = "dpop_tests.rs"]
mod tests;
