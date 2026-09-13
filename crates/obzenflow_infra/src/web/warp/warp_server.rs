// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private Warp route adapter for the managed application host.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use async_trait::async_trait;
use chrono::Utc;
use matchit::Router as MatchItRouter;
use warp::sse::Event as SseEvent;
use warp::{filters::BoxedFilter, Filter, Rejection, Reply};

use crate::web::endpoint_tags::SURFACE_NAME_TAG_PREFIX;
use crate::web::host_config::{HostConfig, HostCorsMode};
use crate::web::host_error::ManagedWebHostError;
use crate::web::routing::{matchit_template_to_public, public_template_to_matchit};
use crate::web::surface_metrics::{HttpSurfaceMetricsCollector, HttpSurfaceObservation};
#[cfg(test)]
use obzenflow_core::web::EndpointError;
use obzenflow_core::web::{
    AuthPolicy, HttpEndpoint, HttpMethod, ManagedResponse, ManagedRouteInfo, Request, Response,
    RouteKind, SseBody, SseFrame,
};

/// Warp-based web server implementation
pub(crate) struct WarpWebHost {
    tasks: crate::web::managed_host::HostTasks,
    endpoints: Vec<Arc<dyn HttpEndpoint>>,
    surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
}

const DEFAULT_MAX_BODY_SIZE_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone)]
struct HostPolicy {
    max_body_size_bytes: u64,
    request_timeout: Option<Duration>,
    control_plane_auth: Option<AuthPolicy>,
}

impl WarpWebHost {
    /// Create a new Warp server
    pub fn new() -> Self {
        Self {
            tasks: Default::default(),
            endpoints: Vec::new(),
            surface_metrics: None,
        }
    }

    /// Attach an in-memory metrics collector for hosted web surfaces (FLOWIP-093a).
    pub fn with_surface_metrics(&mut self, collector: Arc<HttpSurfaceMetricsCollector>) {
        self.surface_metrics = Some(collector);
    }

    /// Build Warp filter from endpoints.
    fn build_filter(
        &self,
        host_policy: HostPolicy,
    ) -> Result<BoxedFilter<(Box<dyn Reply>,)>, ManagedWebHostError> {
        let router = Arc::new(self.build_route_router()?);

        let query = warp::filters::query::query::<HashMap<String, String>>()
            .or(warp::any().map(HashMap::new))
            .unify();
        let headers = warp::header::headers_cloned();

        let get_host_policy = host_policy.clone();
        let get_route = warp::get()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap| {
                    dispatch_request(
                        router.clone(),
                        get_host_policy.clone(),
                        HttpMethod::Get,
                        path,
                        headers,
                        query_params,
                        None,
                    )
                }
            });

        let head_host_policy = host_policy.clone();
        let head_route = warp::head()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap| {
                    dispatch_request(
                        router.clone(),
                        head_host_policy.clone(),
                        HttpMethod::Head,
                        path,
                        headers,
                        query_params,
                        None,
                    )
                }
            });

        let delete_host_policy = host_policy.clone();
        let delete_route = warp::delete()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap| {
                    dispatch_request(
                        router.clone(),
                        delete_host_policy.clone(),
                        HttpMethod::Delete,
                        path,
                        headers,
                        query_params,
                        None,
                    )
                }
            });

        let options_host_policy = host_policy.clone();
        let options_route = warp::options()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap| {
                    dispatch_request(
                        router.clone(),
                        options_host_policy.clone(),
                        HttpMethod::Options,
                        path,
                        headers,
                        query_params,
                        None,
                    )
                }
            });

        let post_host_policy = host_policy.clone();
        let post_route = warp::post()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and(warp::body::content_length_limit(
                host_policy.max_body_size_bytes,
            ))
            .and(warp::body::bytes())
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap,
                      body: bytes::Bytes| {
                    dispatch_request(
                        router.clone(),
                        post_host_policy.clone(),
                        HttpMethod::Post,
                        path,
                        headers,
                        query_params,
                        Some(body),
                    )
                }
            });

        let put_host_policy = host_policy.clone();
        let put_route = warp::put()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and(warp::body::content_length_limit(
                host_policy.max_body_size_bytes,
            ))
            .and(warp::body::bytes())
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap,
                      body: bytes::Bytes| {
                    dispatch_request(
                        router.clone(),
                        put_host_policy.clone(),
                        HttpMethod::Put,
                        path,
                        headers,
                        query_params,
                        Some(body),
                    )
                }
            });

        let patch_host_policy = host_policy.clone();
        let patch_route = warp::patch()
            .and(warp::path::full())
            .and(query)
            .and(headers)
            .and(warp::body::content_length_limit(
                host_policy.max_body_size_bytes,
            ))
            .and(warp::body::bytes())
            .and_then({
                let router = router.clone();
                move |path: warp::path::FullPath,
                      query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap,
                      body: bytes::Bytes| {
                    dispatch_request(
                        router.clone(),
                        patch_host_policy.clone(),
                        HttpMethod::Patch,
                        path,
                        headers,
                        query_params,
                        Some(body),
                    )
                }
            });

        let combined_route = get_route
            .or(post_route)
            .unify()
            .or(put_route)
            .unify()
            .or(patch_route)
            .unify()
            .or(delete_route)
            .unify()
            .or(head_route)
            .unify()
            .or(options_route)
            .unify()
            .boxed();

        Ok(combined_route)
    }

    fn build_route_router(&self) -> Result<MatchItRouter<RouteDispatch>, ManagedWebHostError> {
        const ALL_METHODS: [HttpMethod; 7] = [
            HttpMethod::Get,
            HttpMethod::Post,
            HttpMethod::Put,
            HttpMethod::Delete,
            HttpMethod::Patch,
            HttpMethod::Head,
            HttpMethod::Options,
        ];

        let mut by_template: HashMap<String, RouteDispatch> = HashMap::new();

        for endpoint in &self.endpoints {
            let endpoint = endpoint.clone();
            let template = endpoint.path().to_string();

            let managed = endpoint.managed_route();
            let auth = managed
                .as_ref()
                .map(|managed| resolve_managed_auth(managed, &template))
                .transpose()?
                .flatten();

            let surface_metrics = match (&self.surface_metrics, surface_name_tag_value(&endpoint)) {
                (Some(collector), Some(surface_name)) => Some(SurfaceMetricsRouteContext {
                    collector: collector.clone(),
                    surface_name,
                    path: Arc::from(template.as_str()),
                }),
                _ => None,
            };

            let claimed_methods: Vec<HttpMethod> = if endpoint.methods().is_empty() {
                ALL_METHODS.to_vec()
            } else {
                endpoint.methods().to_vec()
            };

            let dispatch = by_template
                .entry(template.clone())
                .or_insert_with(|| RouteDispatch::new(Arc::from(template.as_str())));

            for method in claimed_methods {
                dispatch.insert_method(
                    method,
                    RoutedEndpoint {
                        endpoint: endpoint.clone(),
                        managed: managed.clone(),
                        auth: auth.clone(),
                        surface_metrics: surface_metrics.clone(),
                    },
                    &template,
                )?;
            }
        }

        // Resolve all declarations before reading material. A route cannot hide
        // an invalid surface requirement behind a different credential.
        for dispatch in by_template.values() {
            for routed in dispatch.by_method.values() {
                if let Some(auth) = &routed.auth {
                    validate_auth_policy_startup(auth)?;
                }
            }
        }

        let mut entries: Vec<(String, RouteDispatch)> = by_template.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut router = MatchItRouter::new();
        for (template, dispatch) in entries {
            let matchit_path = public_template_to_matchit(&template).map_err(|message| {
                ManagedWebHostError::EndpointRegistrationFailed {
                    path: template.clone(),
                    message,
                }
            })?;

            if let Err(err) = router.insert(matchit_path, dispatch) {
                let message = match err {
                    matchit::InsertError::Conflict { with } => format!(
                        "Route template conflicts with previously registered route: {}",
                        matchit_template_to_public(&with)
                    ),
                    other => other.to_string(),
                };
                return Err(ManagedWebHostError::EndpointRegistrationFailed {
                    path: template,
                    message,
                });
            }
        }

        Ok(router)
    }
}

impl Default for WarpWebHost {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct SurfaceMetricsRouteContext {
    collector: Arc<HttpSurfaceMetricsCollector>,
    surface_name: Arc<str>,
    path: Arc<str>,
}

impl SurfaceMetricsRouteContext {
    fn observe(
        &self,
        method: HttpMethod,
        status: u16,
        duration_ms: u64,
        request_bytes: u64,
        response_bytes: u64,
    ) {
        self.collector.observe(HttpSurfaceObservation {
            surface_name: self.surface_name.clone(),
            method,
            path: self.path.clone(),
            status,
            duration_ms,
            request_bytes,
            response_bytes,
        });
    }
}

fn surface_name_tag_value(endpoint: &Arc<dyn HttpEndpoint>) -> Option<Arc<str>> {
    let meta = endpoint.metadata()?;
    meta.tags
        .iter()
        .find_map(|tag| tag.strip_prefix(SURFACE_NAME_TAG_PREFIX))
        .map(Arc::from)
}

#[derive(Clone)]
struct RoutedEndpoint {
    endpoint: Arc<dyn HttpEndpoint>,
    // Freeze declarations at admission; only secret material is request-resolved.
    managed: Option<ManagedRouteInfo>,
    auth: Option<AuthPolicy>,
    surface_metrics: Option<SurfaceMetricsRouteContext>,
}

#[derive(Clone)]
struct RouteDispatch {
    template: Arc<str>,
    by_method: HashMap<HttpMethod, RoutedEndpoint>,
}

impl RouteDispatch {
    fn new(template: Arc<str>) -> Self {
        Self {
            template,
            by_method: HashMap::new(),
        }
    }

    fn insert_method(
        &mut self,
        method: HttpMethod,
        routed: RoutedEndpoint,
        template_for_error: &str,
    ) -> Result<(), ManagedWebHostError> {
        if self.by_method.contains_key(&method) {
            return Err(ManagedWebHostError::EndpointRegistrationFailed {
                path: template_for_error.to_string(),
                message: format!(
                    "Duplicate endpoint registered for {} {}",
                    method.as_str(),
                    template_for_error
                ),
            });
        }

        self.by_method.insert(method, routed);
        Ok(())
    }

    fn for_method(&self, method: HttpMethod) -> Option<RoutedEndpoint> {
        self.by_method.get(&method).cloned()
    }
}

async fn dispatch_request(
    router: Arc<MatchItRouter<RouteDispatch>>,
    host_policy: HostPolicy,
    method: HttpMethod,
    path: warp::path::FullPath,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    body: Option<bytes::Bytes>,
) -> Result<Box<dyn Reply>, Rejection> {
    let raw_path = path.as_str().to_string();

    let matched = match router.at(raw_path.as_str()) {
        Ok(matched) => matched,
        Err(_) => return Err(warp::reject::not_found()),
    };

    let dispatch = matched.value;
    let routed = match dispatch.for_method(method) {
        Some(routed) => routed,
        None => return Err(warp::reject::not_found()),
    };

    let mut path_params: HashMap<String, String> = HashMap::new();
    for (k, v) in matched.params.iter() {
        path_params.insert(k.to_string(), v.to_string());
    }

    let matched_route = dispatch.template.to_string();
    match body {
        Some(body) => {
            handle_request_with_body(
                routed,
                host_policy,
                method,
                headers,
                query_params,
                body,
                raw_path,
                matched_route,
                path_params,
            )
            .await
        }
        None => {
            handle_request_no_body(
                routed,
                host_policy,
                method,
                headers,
                query_params,
                raw_path,
                matched_route,
                path_params,
            )
            .await
        }
    }
}

fn headers_to_owned_map(headers: &warp::http::HeaderMap) -> HashMap<String, String> {
    let mut req_headers = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            req_headers.insert(name.to_string(), value_str.to_string());
        }
    }
    req_headers
}

fn header_value<'a>(headers: &'a HashMap<String, String>, header_name: &str) -> Option<&'a str> {
    headers.iter().find_map(|(k, v)| {
        if k.eq_ignore_ascii_case(header_name) {
            Some(v.as_str())
        } else {
            None
        }
    })
}

fn normalize_content_type(value: &str) -> &str {
    value.split(';').next().unwrap_or(value).trim()
}

fn content_type_matches(provided: Option<&str>, expected: &str) -> bool {
    let Some(provided) = provided else {
        return false;
    };
    let provided = normalize_content_type(provided);
    let expected = normalize_content_type(expected);
    provided.eq_ignore_ascii_case(expected)
}

fn resolve_managed_auth(
    managed: &ManagedRouteInfo,
    path: &str,
) -> Result<Option<AuthPolicy>, ManagedWebHostError> {
    let surface_auth = managed
        .surface_policy
        .as_ref()
        .and_then(|policy| policy.auth.as_ref());
    if let Some(required) = surface_auth.filter(|auth| !matches!(auth, AuthPolicy::None)) {
        if managed
            .route_policy
            .auth
            .as_ref()
            .is_some_and(|route_auth| route_auth != required)
        {
            return Err(ManagedWebHostError::EndpointRegistrationFailed {
                path: path.to_string(),
                message: "Route auth conflicts with required surface auth".to_string(),
            });
        }
        return Ok(Some(required.clone()));
    }
    Ok(managed.route_policy.auth.as_ref().or(surface_auth).cloned())
}

pub(crate) fn is_control_plane_path(path: &str) -> bool {
    matches!(path, "/api/topology" | "/metrics")
        || path == "/api/flow"
        || path.starts_with("/api/flow/")
        || path == "/api/config"
        || path.starts_with("/api/config/")
}

fn is_control_plane_exempt_path(path: &str) -> bool {
    matches!(path, "/health" | "/ready")
}

fn maybe_enforce_control_plane_auth(
    host_policy: &HostPolicy,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Result<(), Response> {
    if is_control_plane_exempt_path(path) || !is_control_plane_path(path) {
        return Ok(());
    }

    match host_policy.control_plane_auth.as_ref() {
        Some(auth) => enforce_auth_policy(auth, headers, body),
        None => Ok(()),
    }
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

// Keep only a fixed reason on failure. VarError::NotUnicode contains the secret
// itself and must never reach Display, Debug, tracing, or an error source chain.
fn validate_secret_material(
    value: Result<String, std::env::VarError>,
) -> Result<String, &'static str> {
    match value {
        Ok(value) if value.is_empty() => Err("empty"),
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Err("missing"),
        Err(std::env::VarError::NotUnicode(_)) => Err("non-Unicode"),
    }
}

fn resolve_auth_secret(env_name: &str, policy: &str) -> Result<String, ManagedWebHostError> {
    validate_secret_material(std::env::var(env_name)).map_err(|reason| {
        ManagedWebHostError::StartupFailed {
            message: format!(
            "AuthPolicy::{policy} expects non-empty environment variable `{env_name}` ({reason})"
        ),
            source: None,
        }
    })
}

fn request_auth_secret(env_name: &str, policy: &str) -> Result<String, Response> {
    resolve_auth_secret(env_name, policy).map_err(|error| {
        tracing::error!(%error, "Authentication policy is misconfigured");
        Response::internal_error().with_text("Internal Server Error")
    })
}

fn validate_auth_policy_startup(auth: &AuthPolicy) -> Result<(), ManagedWebHostError> {
    match auth {
        AuthPolicy::None => Ok(()),
        AuthPolicy::ApiKey { value_env, .. } => {
            resolve_auth_secret(value_env, "ApiKey").map(|_| ())
        }
        AuthPolicy::HmacSha256 {
            secret_env,
            timestamp_header,
            replay_window_secs,
            ..
        } => {
            if replay_window_secs.is_some() && timestamp_header.is_none() {
                return Err(ManagedWebHostError::StartupFailed {
                    message: "AuthPolicy::HmacSha256 requires timestamp_header when replay_window_secs is configured".to_string(),
                    source: None,
                });
            }

            resolve_auth_secret(secret_env, "HmacSha256").map(|_| ())
        }
    }
}

fn build_host_policy(
    config: &HostConfig,
    endpoints: &[Arc<dyn HttpEndpoint>],
) -> Result<HostPolicy, ManagedWebHostError> {
    let max_body_size_bytes = config.max_body_size.unwrap_or(DEFAULT_MAX_BODY_SIZE_BYTES) as u64;
    let request_timeout_secs = config
        .request_timeout_secs
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECS);
    let request_timeout = if request_timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(request_timeout_secs))
    };

    let control_plane_auth = config.control_plane_auth.clone();
    if let Some(auth) = control_plane_auth.as_ref() {
        validate_auth_policy_startup(auth)?;
    }

    let has_control_plane_routes = endpoints.iter().any(|endpoint| {
        let path = endpoint.path();
        is_control_plane_path(path) && !is_control_plane_exempt_path(path)
    });

    if has_control_plane_routes
        && !is_loopback_host(&config.host)
        && !matches!(
            control_plane_auth.as_ref(),
            Some(AuthPolicy::ApiKey { .. } | AuthPolicy::HmacSha256 { .. })
        )
    {
        return Err(ManagedWebHostError::StartupFailed {
            message: format!(
                "Non-loopback host `{}` requires control-plane auth for built-in routes",
                config.host
            ),
            source: None,
        });
    }

    if has_control_plane_routes
        && endpoints
            .iter()
            .any(|endpoint| endpoint.path() == "/metrics")
        && matches!(
            control_plane_auth.as_ref(),
            Some(AuthPolicy::HmacSha256 { .. })
        )
    {
        tracing::warn!(
            "Control-plane HMAC is not the natural fit for pull-based /metrics scraping; prefer ApiKey on Authorization when metrics scraping is enabled"
        );
    }

    Ok(HostPolicy {
        max_body_size_bytes,
        request_timeout,
        control_plane_auth,
    })
}

fn resolve_effective_timeout(
    host: Option<Duration>,
    managed: Option<&ManagedRouteInfo>,
) -> Option<Duration> {
    let surface_timeout = managed
        .and_then(|m| m.surface_policy.as_ref())
        .and_then(|policy| policy.request_timeout_secs)
        .map(Duration::from_secs);

    match (host, surface_timeout) {
        (Some(host), Some(surface)) => Some(std::cmp::min(host, surface)),
        (Some(host), None) => Some(host),
        (None, Some(surface)) => Some(surface),
        (None, None) => None,
    }
}

fn resolve_effective_max_body_size_bytes(host: u64, managed: &ManagedRouteInfo) -> u64 {
    managed
        .surface_policy
        .as_ref()
        .and_then(|policy| policy.max_body_size)
        .map(|surface| std::cmp::min(surface as u64, host))
        .unwrap_or(host)
}

fn enforce_auth_policy(
    auth: &AuthPolicy,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Result<(), Response> {
    match auth {
        AuthPolicy::None => Ok(()),
        AuthPolicy::ApiKey { header, value_env } => {
            use subtle::ConstantTimeEq;

            let expected = request_auth_secret(value_env, "ApiKey")?;
            let Some(provided) = header_value(headers, header) else {
                return Err(Response::new(401).with_text("Unauthorized"));
            };

            let ok: bool = provided.as_bytes().ct_eq(expected.as_bytes()).into();
            if ok {
                Ok(())
            } else {
                Err(Response::new(401).with_text("Unauthorized"))
            }
        }
        AuthPolicy::HmacSha256 {
            secret_env,
            signature_header,
            body_hash: _,
            timestamp_header,
            replay_window_secs,
        } => {
            use ring::hmac;
            use subtle::ConstantTimeEq;

            if replay_window_secs.is_some() && timestamp_header.is_none() {
                tracing::error!(
                    "AuthPolicy::HmacSha256 is misconfigured: replay_window_secs requires timestamp_header"
                );
                return Err(Response::internal_error().with_text("Internal Server Error"));
            }

            let secret = request_auth_secret(secret_env, "HmacSha256")?;

            let provided_sig = match header_value(headers, signature_header) {
                Some(v) => v,
                None => return Err(Response::new(401).with_text("Unauthorized")),
            };

            let timestamp = match timestamp_header {
                Some(header) => match header_value(headers, header) {
                    Some(v) => Some(v),
                    None => return Err(Response::new(401).with_text("Unauthorized")),
                },
                None => None,
            };

            if let (Some(window_secs), Some(ts_str)) = (replay_window_secs, timestamp) {
                let ts = match ts_str.trim().parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => return Err(Response::new(401).with_text("Unauthorized")),
                };
                let now = Utc::now().timestamp();
                let window = (*window_secs).min(i64::MAX as u64);
                if now.abs_diff(ts) > window {
                    return Err(Response::new(401).with_text("Unauthorized"));
                }
            }

            let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
            let expected = if let Some(ts) = timestamp {
                let mut signed =
                    Vec::with_capacity(ts.len().saturating_add(1).saturating_add(body.len()));
                signed.extend_from_slice(ts.as_bytes());
                signed.push(b'.');
                signed.extend_from_slice(body);
                hmac::sign(&key, &signed)
            } else {
                hmac::sign(&key, body)
            };

            let provided_bytes = match decode_hex_signature(provided_sig) {
                Some(bytes) => bytes,
                None => return Err(Response::new(401).with_text("Unauthorized")),
            };

            let ok: bool = expected.as_ref().ct_eq(provided_bytes.as_slice()).into();
            if ok {
                Ok(())
            } else {
                Err(Response::new(401).with_text("Unauthorized"))
            }
        }
    }
}

fn decode_hex_signature(value: &str) -> Option<Vec<u8>> {
    let value = value.trim();

    let signature_hex = if let Some(hex) = value.strip_prefix("sha256=") {
        hex.trim()
    } else if value.contains("v1=") {
        value
            .split(',')
            .find_map(|part| part.trim().strip_prefix("v1="))
            .map(|v| v.trim())
            .unwrap_or(value)
    } else {
        value
    };

    decode_hex(signature_hex)
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    let value = value.trim();
    if !value.len().is_multiple_of(2) {
        return None;
    }

    let bytes: Option<Vec<u8>> = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect();
    bytes
}

fn reply_from_response(response: Response) -> Result<Box<dyn Reply>, Rejection> {
    let mut builder = warp::http::Response::builder().status(response.status);

    for (key, value) in response.headers {
        builder = builder.header(key, value);
    }

    let reply = builder
        .body(response.body)
        .map_err(|_| warp::reject::reject())?;

    Ok(Box::new(reply) as Box<dyn Reply>)
}

/// Response metadata is untrusted endpoint output. Select the fallback before
/// observing the request, so journalled snapshots describe the reply on the wire.
fn finalise_unary(
    response: Response,
    method: HttpMethod,
    route: &str,
    metrics: Option<&SurfaceMetricsRouteContext>,
    start: std::time::Instant,
    request_bytes: u64,
) -> Box<dyn Reply> {
    let mut builder = warp::http::Response::builder().status(response.status);
    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }
    let reply = builder.body(response.body).unwrap_or_else(|_| {
        tracing::error!(
            method = method.as_str(),
            matched_route = route,
            "Invalid endpoint response metadata"
        );
        warp::http::Response::builder()
            .status(500)
            .header("Content-Type", "text/plain")
            .body(b"Internal Server Error".to_vec())
            .expect("static fallback response is valid")
    });
    if let Some(metrics) = metrics {
        metrics.observe(
            method,
            reply.status().as_u16(),
            start.elapsed().as_millis() as u64,
            request_bytes,
            reply.body().len() as u64,
        );
    }
    Box::new(reply)
}

/// Helper function to handle requests without body (GET, HEAD, DELETE, OPTIONS)
#[allow(clippy::too_many_arguments)]
async fn handle_request_no_body(
    routed: RoutedEndpoint,
    host_policy: HostPolicy,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    path: String,
    matched_route: String,
    path_params: HashMap<String, String>,
) -> Result<Box<dyn Reply>, Rejection> {
    let RoutedEndpoint {
        endpoint,
        managed,
        auth,
        surface_metrics,
    } = routed;
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }

    let start = std::time::Instant::now();

    // Convert headers
    let req_headers = headers_to_owned_map(&headers);

    let effective_timeout =
        resolve_effective_timeout(host_policy.request_timeout, managed.as_ref());
    let log_ctx = if managed.is_some() {
        Some((path.clone(), matched_route.clone()))
    } else {
        None
    };

    if managed.is_some() {
        if let Some(auth) = auth.as_ref() {
            if let Err(response) = enforce_auth_policy(auth, &req_headers, &[]) {
                if let Some(metrics) = &surface_metrics {
                    metrics.observe(
                        method,
                        response.status,
                        start.elapsed().as_millis() as u64,
                        0,
                        response.body.len() as u64,
                    );
                }
                return reply_from_response(response);
            }
        }
    } else if let Err(response) =
        maybe_enforce_control_plane_auth(&host_policy, matched_route.as_str(), &req_headers, &[])
    {
        return reply_from_response(response);
    }

    let request = Request {
        method,
        path,
        matched_route,
        path_params,
        headers: req_headers,
        query_params,
        body: Vec::new(),
    };

    // Handle request
    let handler_result = if let Some(timeout) = effective_timeout {
        match tokio::time::timeout(timeout, endpoint.handle(request)).await {
            Ok(res) => res,
            Err(_) => Ok(Response::new(504).with_text("Gateway Timeout").into()),
        }
    } else {
        endpoint.handle(request).await
    };

    let response = handler_result.unwrap_or_else(|error| {
        tracing::error!(
            method = method.as_str(),
            matched_route = endpoint.path(),
            context = error.context(),
            "Managed endpoint failed",
        );
        Response::internal_error()
            .with_text("Internal Server Error")
            .into()
    });
    match response {
        ManagedResponse::Unary(mut response) => {
            if let Some(managed) = managed.as_ref() {
                if matches!(managed.kind, RouteKind::Sse) && response.status < 400 {
                    if let Some((raw_path, template)) = log_ctx.as_ref() {
                        tracing::debug!(
                            method = %method.as_str(),
                            path = %raw_path,
                            matched_route = %template,
                            status = response.status,
                            declared_kind = ?managed.kind,
                            "Managed route returned a successful unary response despite being declared as SSE"
                        );
                    }
                }

                if let Some(ct) = managed.route_policy.response_content_type.as_deref() {
                    if header_value(&response.headers, "content-type").is_none() {
                        response
                            .headers
                            .insert("Content-Type".to_string(), ct.to_string());
                    }
                }
            }

            Ok(finalise_unary(
                response,
                method,
                endpoint.path(),
                surface_metrics.as_ref(),
                start,
                0,
            ))
        }
        ManagedResponse::Sse(body) => {
            if let Some(managed) = managed.as_ref() {
                if matches!(managed.kind, RouteKind::Unary) {
                    if let Some((raw_path, template)) = log_ctx.as_ref() {
                        tracing::debug!(
                            method = %method.as_str(),
                            path = %raw_path,
                            matched_route = %template,
                            declared_kind = ?managed.kind,
                            "Managed route returned SSE response despite being declared as unary"
                        );
                    }
                }
            }

            // FLOWIP-093a: streaming responses do not fit the unary request/response metrics model,
            // so we intentionally skip surface-metrics observation for SSE replies.
            Ok(Box::new(sse_body_reply(body)) as Box<dyn Reply>)
        }
    }
}

/// Helper function to handle requests with body (POST, PUT, PATCH)
#[allow(clippy::too_many_arguments)]
async fn handle_request_with_body(
    routed: RoutedEndpoint,
    host_policy: HostPolicy,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    body: bytes::Bytes,
    path: String,
    matched_route: String,
    path_params: HashMap<String, String>,
) -> Result<Box<dyn Reply>, Rejection> {
    let RoutedEndpoint {
        endpoint,
        managed,
        auth,
        surface_metrics,
    } = routed;
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }

    let start = std::time::Instant::now();
    let request_bytes = body.len() as u64;

    // Convert headers
    let req_headers = headers_to_owned_map(&headers);

    let effective_timeout =
        resolve_effective_timeout(host_policy.request_timeout, managed.as_ref());
    let log_ctx = if managed.is_some() {
        Some((path.clone(), matched_route.clone()))
    } else {
        None
    };

    if let Some(managed) = managed.as_ref() {
        let max_body_size_bytes =
            resolve_effective_max_body_size_bytes(host_policy.max_body_size_bytes, managed);
        if request_bytes > max_body_size_bytes {
            let response = Response::new(413).with_text("Request Entity Too Large");
            if let Some(metrics) = &surface_metrics {
                metrics.observe(
                    method,
                    response.status,
                    start.elapsed().as_millis() as u64,
                    request_bytes,
                    response.body.len() as u64,
                );
            }
            return reply_from_response(response);
        }

        if let Some(expected) = managed.route_policy.request_content_type.as_deref() {
            if !content_type_matches(header_value(&req_headers, "content-type"), expected) {
                let response = Response::new(415).with_text("Unsupported Media Type");
                if let Some(metrics) = &surface_metrics {
                    metrics.observe(
                        method,
                        response.status,
                        start.elapsed().as_millis() as u64,
                        request_bytes,
                        response.body.len() as u64,
                    );
                }
                return reply_from_response(response);
            }
        }

        if let Some(auth) = auth.as_ref() {
            if let Err(response) = enforce_auth_policy(auth, &req_headers, body.as_ref()) {
                if let Some(metrics) = &surface_metrics {
                    metrics.observe(
                        method,
                        response.status,
                        start.elapsed().as_millis() as u64,
                        request_bytes,
                        response.body.len() as u64,
                    );
                }
                return reply_from_response(response);
            }
        }
    } else if let Err(response) = maybe_enforce_control_plane_auth(
        &host_policy,
        matched_route.as_str(),
        &req_headers,
        body.as_ref(),
    ) {
        return reply_from_response(response);
    }

    let request = Request {
        method,
        path,
        matched_route,
        path_params,
        headers: req_headers,
        query_params,
        body: body.to_vec(),
    };

    // Handle request
    let handler_result = if let Some(timeout) = effective_timeout {
        match tokio::time::timeout(timeout, endpoint.handle(request)).await {
            Ok(res) => res,
            Err(_) => Ok(Response::new(504).with_text("Gateway Timeout").into()),
        }
    } else {
        endpoint.handle(request).await
    };

    let response = handler_result.unwrap_or_else(|error| {
        tracing::error!(
            method = method.as_str(),
            matched_route = endpoint.path(),
            context = error.context(),
            "Managed endpoint failed",
        );
        Response::internal_error()
            .with_text("Internal Server Error")
            .into()
    });
    match response {
        ManagedResponse::Unary(mut response) => {
            if let Some(managed) = managed.as_ref() {
                if matches!(managed.kind, RouteKind::Sse) && response.status < 400 {
                    if let Some((raw_path, template)) = log_ctx.as_ref() {
                        tracing::debug!(
                            method = %method.as_str(),
                            path = %raw_path,
                            matched_route = %template,
                            status = response.status,
                            declared_kind = ?managed.kind,
                            "Managed route returned a successful unary response despite being declared as SSE"
                        );
                    }
                }

                if let Some(ct) = managed.route_policy.response_content_type.as_deref() {
                    if header_value(&response.headers, "content-type").is_none() {
                        response
                            .headers
                            .insert("Content-Type".to_string(), ct.to_string());
                    }
                }
            }

            Ok(finalise_unary(
                response,
                method,
                endpoint.path(),
                surface_metrics.as_ref(),
                start,
                request_bytes,
            ))
        }
        ManagedResponse::Sse(body) => {
            if let Some(managed) = managed.as_ref() {
                if matches!(managed.kind, RouteKind::Unary) {
                    if let Some((raw_path, template)) = log_ctx.as_ref() {
                        tracing::debug!(
                            method = %method.as_str(),
                            path = %raw_path,
                            matched_route = %template,
                            declared_kind = ?managed.kind,
                            "Managed route returned SSE response despite being declared as unary"
                        );
                    }
                }
            }

            // FLOWIP-093a: streaming responses do not fit the unary request/response metrics model,
            // so we intentionally skip surface-metrics observation for SSE replies.
            Ok(Box::new(sse_body_reply(body)) as Box<dyn Reply>)
        }
    }
}

fn sse_body_reply(body: SseBody) -> impl Reply {
    use tokio_stream::StreamExt;

    // Warp requires Sync, while Core permits Send-only producers. Polling still
    // needs exclusive access: get_mut satisfies that without acquiring a lock.
    let mut body = std::sync::Mutex::new(body);
    let stream = futures::stream::poll_fn(move |cx| {
        futures::Stream::poll_next(
            std::pin::Pin::new(body.get_mut().expect("exclusive SSE body access")),
            cx,
        )
    });
    let stream = stream.map(|frame: SseFrame| Ok::<SseEvent, Infallible>(sse_frame_to_warp(frame)));
    warp::sse::reply(warp::sse::keep_alive().stream(stream))
}

fn sse_frame_to_warp(frame: SseFrame) -> SseEvent {
    let comment_only = frame.comment.is_some()
        && frame.data.is_empty()
        && frame.event.is_none()
        && frame.id.is_none()
        && frame.retry_ms.is_none();
    let mut ev = SseEvent::default();
    if !comment_only {
        ev = ev.data(frame.data);
    }
    if let Some(event) = frame.event {
        ev = ev.event(event);
    }
    if let Some(id) = frame.id {
        ev = ev.id(id);
    }
    if let Some(retry_ms) = frame.retry_ms {
        ev = ev.retry(std::time::Duration::from_millis(retry_ms));
    }
    if let Some(comment) = frame.comment {
        ev = ev.comment(comment);
    }
    ev
}

// Match both conversions in Warp's panicking IntoOrigin implementation. Keep
// this deployment admission at the private host boundary, before socket binding.
fn validate_cors_origin(raw: &str, index: usize) -> Result<(), ManagedWebHostError> {
    let invalid = || ManagedWebHostError::StartupFailed {
        message: format!("server.cors.allow_origins[{index}]: invalid origin"),
        source: None,
    };
    let (scheme, authority) = raw.split_once("://").ok_or_else(&invalid)?;
    let origin = headers::Origin::try_from_parts(scheme, authority, None).map_err(|_| invalid())?;
    origin
        .to_string()
        .parse::<warp::http::HeaderValue>()
        .map_err(|_| invalid())?;
    Ok(())
}

impl WarpWebHost {
    /// Validate all route policy and bind the real socket before returning success.
    pub(crate) async fn bind(
        self,
        config: HostConfig,
        shutdown: tokio::sync::watch::Sender<bool>,
    ) -> Result<crate::web::managed_host::ManagedWebHost, ManagedWebHostError> {
        let addr: SocketAddr =
            config
                .address()
                .parse()
                .map_err(|e| ManagedWebHostError::BindFailed {
                    address: config.address(),
                    source: Some(Box::new(e)),
                })?;

        let host_policy = build_host_policy(&config, &self.endpoints)?;

        let routes = self.build_filter(host_policy)?;

        let cors_config = config.cors.unwrap_or_default();
        let cors_mode = cors_config.mode;
        if matches!(&cors_mode, HostCorsMode::AllowAnyOrigin) && !cfg!(debug_assertions) {
            tracing::warn!(
                "CORS is configured as AllowAnyOrigin in a release build; prefer an explicit allow-list for production"
            );
        }

        // Add CORS support (configurable).
        //
        // Note: `HostCorsMode::SameOrigin` means "do not add CORS headers"; browsers will enforce
        // the same-origin policy by default.
        let routes_with_cors = if matches!(&cors_mode, HostCorsMode::SameOrigin) {
            routes
        } else {
            let mut cors = warp::cors();
            cors = match cors_mode {
                HostCorsMode::AllowAnyOrigin => cors.allow_any_origin(),
                HostCorsMode::AllowList(origins) => {
                    for (index, origin) in origins.iter().enumerate() {
                        validate_cors_origin(origin, index)?;
                    }
                    let origins: Vec<&str> = origins.iter().map(String::as_str).collect();
                    cors.allow_origins(origins)
                }
                HostCorsMode::SameOrigin => cors,
            };

            cors = cors
                .allow_methods(vec![
                    "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS",
                ])
                .allow_headers(vec!["Content-Type", "Accept", "Authorization", "X-Api-Key"]);

            routes
                .with(cors)
                .map(|reply| -> Box<dyn Reply> { Box::new(reply) })
                .boxed()
        };

        crate::web::managed_host::ManagedWebHost::bind(addr, routes_with_cors, self.tasks, shutdown)
            .await
    }

    pub(crate) fn register_endpoint(
        &mut self,
        endpoint: Box<dyn HttpEndpoint>,
    ) -> Result<(), ManagedWebHostError> {
        self.endpoints.push(Arc::from(endpoint));
        Ok(())
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod auth_tests;

#[cfg(test)]
#[path = "response_tests.rs"]
mod response_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::web::{
        AuthPolicy, HttpMethod, ManagedResponse, ManagedRouteInfo, Request, Response, RouteKind,
        RoutePolicy, SurfacePolicy,
    };

    #[test]
    fn portable_comments_do_not_become_empty_data_events() {
        assert_eq!(
            sse_frame_to_warp(SseFrame::comment("ignored")).to_string(),
            ":ignored\n\n"
        );
        assert_eq!(
            sse_frame_to_warp(SseFrame::data("")).to_string(),
            "data:\n\n"
        );
    }

    #[test]
    fn cors_origin_admission_matches_warps_normalised_representation() {
        for origin in [
            "http://localhost:7010",
            "https://example.com",
            "https://example.com/",
            "http://[::1]:7010",
            "HTTPS://EXAMPLE.COM:443",
        ] {
            validate_cors_origin(origin, 0).unwrap();
            // Preserve Warp's existing accepted origins and normalisation.
            let _ = warp::cors().allow_origins([origin]).build();
        }
        for origin in [
            "not-an-origin",
            "null",
            "",
            "https://",
            "https://example.com/path",
            "http://example.com\n",
        ] {
            let error = validate_cors_origin(origin, 3).unwrap_err();
            assert!(
                matches!(error, ManagedWebHostError::StartupFailed { ref message, .. }
                if message == "server.cors.allow_origins[3]: invalid origin")
            );
        }
    }

    fn test_host_policy() -> HostPolicy {
        HostPolicy {
            max_body_size_bytes: 10,
            request_timeout: None,
            control_plane_auth: None,
        }
    }

    #[cfg(feature = "prometheus")]
    #[tokio::test]
    async fn reporting_enablement_preserves_the_metrics_wire_contract() {
        use obzenflow_adapters::monitoring::MetricsReadModel;
        use obzenflow_core::event::context::StageType;
        use obzenflow_core::metrics::MetricsSnapshotExporter;
        use obzenflow_core::metrics::{AppMetricsSnapshot, StageMetadata};

        // The host's method-first filters return 405 for an unregistered path.
        // Preserve that existing behavior when metrics reporting is disabled.
        for (enabled, expected_status) in [(true, 200), (false, 405)] {
            let model = enabled.then(|| Arc::new(MetricsReadModel::default()));
            let stage = obzenflow_core::StageId::new();
            if let Some(exporter) = &model {
                let mut app = AppMetricsSnapshot::default();
                app.pipeline_state = "Running".into();
                app.stage_metadata.insert(
                    stage,
                    StageMetadata {
                        name: "quoted\"stage\\name\nline".into(),
                        flow_name: "wire".into(),
                        stage_type: StageType::Transform,
                        reference_mode: None,
                        flow_id: None,
                    },
                );
                app.event_counts.insert(stage, 17);
                exporter.publish_app_snapshot(app);
            }
            let mut server = WarpWebHost::new();
            if let Some(model) = model {
                let endpoint = crate::web::endpoints::PrometheusMetricsEndpoint::new(model);
                server.register_endpoint(Box::new(endpoint)).unwrap();
            }
            let filter = server.build_filter(test_host_policy()).unwrap();
            let response = warp::test::request()
                .method("GET")
                .path("/metrics")
                .reply(&filter)
                .await;
            assert_eq!(response.status(), expected_status);
            if expected_status == 200 {
                assert_eq!(
                    response.headers()["content-type"],
                    "text/plain; version=0.0.4; charset=utf-8"
                );
                let text = std::str::from_utf8(response.body()).unwrap();
                assert!(text.contains("# TYPE obzenflow_events_total counter\n"));
                assert!(text.contains(&format!("obzenflow_events_total{{flow=\"wire\",stage=\"quoted\\\"stage\\\\name\\nline\",stage_id=\"{stage}\"}} 17\n")));
                assert!(text.contains("obzenflow_pipeline_state{state=\"Running\"} 1\n"));
                assert_eq!(text.matches("# TYPE obzenflow_build_info gauge").count(), 1);
                let post = warp::test::request()
                    .method("POST")
                    .path("/metrics")
                    .body("")
                    .reply(&filter)
                    .await;
                assert_eq!(post.status(), 405);
            }
        }
    }

    struct EchoEndpoint;

    #[async_trait]
    impl HttpEndpoint for EchoEndpoint {
        fn path(&self) -> &str {
            "/echo"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Post]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("OK").into())
        }
    }

    #[tokio::test]
    async fn build_filter_enforces_content_length_limit() {
        let mut server = WarpWebHost::new();
        server.register_endpoint(Box::new(EchoEndpoint)).unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let response = warp::test::request()
            .method("POST")
            .path("/echo")
            .header("content-length", "11")
            .body(vec![0u8; 11])
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 413);

        let response = warp::test::request()
            .method("POST")
            .path("/echo")
            .header("content-length", "10")
            .body(vec![0u8; 10])
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "OK");
    }

    struct PathParamsEndpoint;

    #[async_trait]
    impl HttpEndpoint for PathParamsEndpoint {
        fn path(&self) -> &str {
            "/items/:id"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, request: Request) -> Result<ManagedResponse, EndpointError> {
            assert_eq!(request.path, "/items/123");
            assert_eq!(request.matched_route, "/items/:id");
            assert_eq!(
                request.path_params.get("id").map(String::as_str),
                Some("123")
            );
            Ok(Response::ok().with_text("OK").into())
        }
    }

    #[tokio::test]
    async fn build_filter_populates_path_params_and_matched_route() {
        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(PathParamsEndpoint))
            .unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/items/123")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "OK");
    }

    struct StaticEndpoint;

    #[async_trait]
    impl HttpEndpoint for StaticEndpoint {
        fn path(&self) -> &str {
            "/items/count"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("STATIC").into())
        }
    }

    struct ParamEndpoint;

    #[async_trait]
    impl HttpEndpoint for ParamEndpoint {
        fn path(&self) -> &str {
            "/items/:id"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("PARAM").into())
        }
    }

    #[tokio::test]
    async fn build_filter_prefers_static_routes_over_parameterised_routes() {
        let mut server = WarpWebHost::new();
        server.register_endpoint(Box::new(ParamEndpoint)).unwrap();
        server.register_endpoint(Box::new(StaticEndpoint)).unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/items/count")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "STATIC");

        let response = warp::test::request()
            .method("GET")
            .path("/items/999")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "PARAM");
    }

    struct ConflictEndpointA;

    #[async_trait]
    impl HttpEndpoint for ConflictEndpointA {
        fn path(&self) -> &str {
            "/conflict/:id"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("A").into())
        }
    }

    struct ConflictEndpointB;

    #[async_trait]
    impl HttpEndpoint for ConflictEndpointB {
        fn path(&self) -> &str {
            "/conflict/:name"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("B").into())
        }
    }

    #[tokio::test]
    async fn build_filter_rejects_conflicting_parameterised_routes() {
        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(ConflictEndpointA))
            .unwrap();
        server
            .register_endpoint(Box::new(ConflictEndpointB))
            .unwrap();

        let err = server.build_filter(test_host_policy()).unwrap_err();
        match err {
            ManagedWebHostError::EndpointRegistrationFailed { path, message } => {
                assert_eq!(path, "/conflict/:name");
                assert!(
                    message.contains("conflicts"),
                    "unexpected message: {message}"
                );
                assert!(
                    message.contains("/conflict/:id"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    struct WeakeningAuthEndpoint;

    #[async_trait]
    impl HttpEndpoint for WeakeningAuthEndpoint {
        fn path(&self) -> &str {
            "/weak"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("OK").into())
        }

        fn managed_route(&self) -> Option<ManagedRouteInfo> {
            Some(ManagedRouteInfo {
                kind: RouteKind::Unary,
                surface_policy: Some(SurfacePolicy {
                    auth: Some(AuthPolicy::ApiKey {
                        header: "X-Api-Key".to_string(),
                        value_env: "OBZENFLOW_TEST_API_KEY_UNUSED".to_string(),
                    }),
                    ..SurfacePolicy::default()
                }),
                route_policy: RoutePolicy {
                    auth: Some(AuthPolicy::None),
                    ..RoutePolicy::default()
                },
            })
        }
    }

    #[tokio::test]
    async fn build_filter_rejects_route_local_auth_that_weakens_surface_auth() {
        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(WeakeningAuthEndpoint))
            .unwrap();

        let err = server.build_filter(test_host_policy()).unwrap_err();

        match err {
            ManagedWebHostError::EndpointRegistrationFailed { path, message } => {
                assert_eq!(path, "/weak");
                assert!(
                    message.contains("conflicts with required surface auth"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    struct ManagedBodySizeEndpoint;

    #[async_trait]
    impl HttpEndpoint for ManagedBodySizeEndpoint {
        fn path(&self) -> &str {
            "/managed-body"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Post]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("OK").into())
        }

        fn managed_route(&self) -> Option<ManagedRouteInfo> {
            Some(ManagedRouteInfo {
                kind: RouteKind::Unary,
                surface_policy: Some(SurfacePolicy {
                    max_body_size: Some(5),
                    ..SurfacePolicy::default()
                }),
                route_policy: RoutePolicy::default(),
            })
        }
    }

    #[tokio::test]
    async fn managed_surface_enforces_surface_max_body_size() {
        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(ManagedBodySizeEndpoint))
            .unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let response = warp::test::request()
            .method("POST")
            .path("/managed-body")
            .header("content-length", "6")
            .body(vec![0u8; 6])
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 413);

        let response = warp::test::request()
            .method("POST")
            .path("/managed-body")
            .header("content-length", "5")
            .body(vec![0u8; 5])
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "OK");
    }

    struct ManagedContentTypeEndpoint;

    #[async_trait]
    impl HttpEndpoint for ManagedContentTypeEndpoint {
        fn path(&self) -> &str {
            "/managed-ct"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Post]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("OK").into())
        }

        fn managed_route(&self) -> Option<ManagedRouteInfo> {
            Some(ManagedRouteInfo {
                kind: RouteKind::Unary,
                surface_policy: Some(SurfacePolicy::default()),
                route_policy: RoutePolicy {
                    request_content_type: Some("application/json".to_string()),
                    ..RoutePolicy::default()
                },
            })
        }
    }

    #[tokio::test]
    async fn managed_surface_enforces_request_content_type_when_declared() {
        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(ManagedContentTypeEndpoint))
            .unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let response = warp::test::request()
            .method("POST")
            .path("/managed-ct")
            .header("content-length", "2")
            .header("content-type", "text/plain")
            .body(b"{}")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 415);

        let response = warp::test::request()
            .method("POST")
            .path("/managed-ct")
            .header("content-length", "2")
            .header("content-type", "application/json; charset=utf-8")
            .body(b"{}")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "OK");
    }

    struct ManagedApiKeyEndpoint;

    #[async_trait]
    impl HttpEndpoint for ManagedApiKeyEndpoint {
        fn path(&self) -> &str {
            "/managed-auth"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("OK").into())
        }

        fn managed_route(&self) -> Option<ManagedRouteInfo> {
            Some(ManagedRouteInfo {
                kind: RouteKind::Unary,
                surface_policy: Some(SurfacePolicy {
                    auth: Some(AuthPolicy::ApiKey {
                        header: "X-Api-Key".to_string(),
                        value_env: "OBZENFLOW_TEST_API_KEY_V1".to_string(),
                    }),
                    ..SurfacePolicy::default()
                }),
                route_policy: RoutePolicy::default(),
            })
        }
    }

    #[tokio::test]
    async fn managed_surface_enforces_api_key_auth() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::managed_surface_enforces_api_key_auth",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(ManagedApiKeyEndpoint))
            .unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/managed-auth")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);

        let response = warp::test::request()
            .method("GET")
            .path("/managed-auth")
            .header("x-api-key", "wrong")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);

        let response = warp::test::request()
            .method("GET")
            .path("/managed-auth")
            .header("x-api-key", "sekret")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "OK");
    }

    fn to_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }

    struct ManagedHmacEndpoint;

    #[async_trait]
    impl HttpEndpoint for ManagedHmacEndpoint {
        fn path(&self) -> &str {
            "/managed-hmac"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("OK").into())
        }

        fn managed_route(&self) -> Option<ManagedRouteInfo> {
            Some(ManagedRouteInfo {
                kind: RouteKind::Unary,
                surface_policy: Some(SurfacePolicy {
                    auth: Some(AuthPolicy::HmacSha256 {
                        secret_env: "OBZENFLOW_TEST_HMAC_SECRET_V1".to_string(),
                        signature_header: "X-Signature".to_string(),
                        body_hash: "raw_body".to_string(),
                        timestamp_header: Some("X-Timestamp".to_string()),
                        replay_window_secs: Some(10),
                    }),
                    ..SurfacePolicy::default()
                }),
                route_policy: RoutePolicy::default(),
            })
        }
    }

    #[tokio::test]
    async fn managed_surface_enforces_hmac_replay_window_when_configured() {
        use ring::hmac;

        if !super::auth_tests::with_auth_env("web::warp::warp_server::tests::managed_surface_enforces_hmac_replay_window_when_configured") {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(ManagedHmacEndpoint))
            .unwrap();
        let filter = server.build_filter(test_host_policy()).unwrap();

        let now_ts = Utc::now().timestamp().to_string();
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"sekret");
        let sig_now = {
            let mut signed = Vec::with_capacity(now_ts.len() + 1);
            signed.extend_from_slice(now_ts.as_bytes());
            signed.push(b'.');
            let expected = hmac::sign(&key, &signed);
            to_hex(expected.as_ref())
        };

        let response = warp::test::request()
            .method("GET")
            .path("/managed-hmac")
            .header("x-timestamp", &now_ts)
            .header("x-signature", &sig_now)
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);

        let old_ts = (Utc::now().timestamp() - 1000).to_string();
        let sig_old = {
            let mut signed = Vec::with_capacity(old_ts.len() + 1);
            signed.extend_from_slice(old_ts.as_bytes());
            signed.push(b'.');
            let expected = hmac::sign(&key, &signed);
            to_hex(expected.as_ref())
        };

        let response = warp::test::request()
            .method("GET")
            .path("/managed-hmac")
            .header("x-timestamp", &old_ts)
            .header("x-signature", &sig_old)
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);
    }

    struct SlowEndpoint;

    #[async_trait]
    impl HttpEndpoint for SlowEndpoint {
        fn path(&self) -> &str {
            "/slow"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Response::ok().with_text("OK").into())
        }
    }

    #[tokio::test]
    async fn host_policy_enforces_request_timeout() {
        let mut server = WarpWebHost::new();
        server.register_endpoint(Box::new(SlowEndpoint)).unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: Some(Duration::from_millis(1)),
                control_plane_auth: None,
            })
            .unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/slow")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 504);
    }

    struct RawMetricsEndpoint;

    #[async_trait]
    impl HttpEndpoint for RawMetricsEndpoint {
        fn path(&self) -> &str {
            "/metrics"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("metrics").into())
        }
    }

    struct RawHealthEndpoint;

    #[async_trait]
    impl HttpEndpoint for RawHealthEndpoint {
        fn path(&self) -> &str {
            "/health"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("healthy").into())
        }
    }

    struct RawReadyEndpoint;

    #[async_trait]
    impl HttpEndpoint for RawReadyEndpoint {
        fn path(&self) -> &str {
            "/ready"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("ready").into())
        }
    }

    struct RawTopologyEndpoint;

    #[async_trait]
    impl HttpEndpoint for RawTopologyEndpoint {
        fn path(&self) -> &str {
            "/api/topology"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("topology").into())
        }
    }

    struct RawFlowControlEndpoint;

    #[async_trait]
    impl HttpEndpoint for RawFlowControlEndpoint {
        fn path(&self) -> &str {
            "/api/flow/control"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Post]
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().with_text("control").into())
        }
    }

    #[tokio::test]
    async fn control_plane_auth_enforces_raw_metrics_endpoint() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_enforces_raw_metrics_endpoint",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(RawMetricsEndpoint))
            .unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/metrics")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);

        let response = warp::test::request()
            .method("GET")
            .path("/metrics")
            .header("authorization", "Bearer sekret")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "metrics");
    }

    #[tokio::test]
    async fn control_plane_auth_does_not_gate_health_endpoint() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_does_not_gate_health_endpoint",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(RawHealthEndpoint))
            .unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/health")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "healthy");
    }

    #[tokio::test]
    async fn control_plane_auth_does_not_gate_ready_endpoint() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_does_not_gate_ready_endpoint",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(RawReadyEndpoint))
            .unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/ready")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "ready");
    }

    #[tokio::test]
    async fn control_plane_auth_enforces_topology_endpoint() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_enforces_topology_endpoint",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(RawTopologyEndpoint))
            .unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/api/topology")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);

        let response = warp::test::request()
            .method("GET")
            .path("/api/topology")
            .header("authorization", "Bearer sekret")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "topology");
    }

    #[tokio::test]
    async fn control_plane_auth_enforces_api_flow_prefix_endpoint() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_enforces_api_flow_prefix_endpoint",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        server
            .register_endpoint(Box::new(RawFlowControlEndpoint))
            .unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let response = warp::test::request()
            .method("POST")
            .path("/api/flow/control")
            .body(Vec::<u8>::new())
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);

        let response = warp::test::request()
            .method("POST")
            .path("/api/flow/control")
            .header("authorization", "Bearer sekret")
            .body(Vec::<u8>::new())
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.body(), "control");
    }

    /// FLOWIP-010 gap 10: every `/api/config/*` route (static and
    /// parameterised) rejects without control-plane credentials and admits
    /// with them, exactly like `/api/topology`.
    #[tokio::test]
    async fn control_plane_auth_enforces_config_endpoints() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_enforces_config_endpoints",
        ) {
            return;
        }

        struct RawConfigEndpoint(&'static str);

        #[async_trait]
        impl HttpEndpoint for RawConfigEndpoint {
            fn path(&self) -> &str {
                self.0
            }

            fn methods(&self) -> &[HttpMethod] {
                &[HttpMethod::Get]
            }

            async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
                Ok(Response::ok().with_text("config").into())
            }
        }

        let templates = [
            "/api/config",
            "/api/config/overlay",
            "/api/config/effective",
            "/api/config/schema",
            "/api/config/diff",
            "/api/config/flows/:flow_id",
            "/api/config/flows/:flow_id/stages/:stage_key",
        ];
        let mut server = WarpWebHost::new();
        for template in templates {
            server
                .register_endpoint(Box::new(RawConfigEndpoint(template)))
                .unwrap();
        }
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let probes = [
            "/api/config",
            "/api/config/overlay",
            "/api/config/effective",
            "/api/config/schema",
            "/api/config/diff",
            "/api/config/flows/f1",
            "/api/config/flows/f1/stages/s1",
        ];
        for probe in probes {
            let response = warp::test::request()
                .method("GET")
                .path(probe)
                .reply(&filter)
                .await;
            assert_eq!(response.status(), 401, "{probe} must reject without auth");

            let response = warp::test::request()
                .method("GET")
                .path(probe)
                .header("authorization", "Bearer sekret")
                .reply(&filter)
                .await;
            assert_eq!(response.status(), 200, "{probe} must admit with auth");
            assert_eq!(response.body(), "config");
        }
    }

    #[tokio::test]
    async fn control_plane_auth_covers_registered_studio_updates() {
        if !super::auth_tests::with_auth_env(
            "web::warp::warp_server::tests::control_plane_auth_covers_registered_studio_updates",
        ) {
            return;
        }

        let mut server = WarpWebHost::new();
        let (endpoint, _closing) = super::auth_tests::studio_updates_endpoint();
        server.register_endpoint(Box::new(endpoint)).unwrap();
        let filter = server
            .build_filter(HostPolicy {
                max_body_size_bytes: 10,
                request_timeout: None,
                control_plane_auth: Some(AuthPolicy::ApiKey {
                    header: "Authorization".to_string(),
                    value_env: "OBZENFLOW_TEST_CONTROL_PLANE_API_KEY".to_string(),
                }),
            })
            .unwrap();

        let response = warp::test::request()
            .method("GET")
            .path("/api/flow/events")
            .reply(&filter)
            .await;
        assert_eq!(response.status(), 401);
    }

    #[test]
    fn build_host_policy_requires_control_plane_auth_for_non_loopback_built_ins() {
        let config = HostConfig::new("0.0.0.0".to_string(), 9090);
        let endpoints: Vec<Arc<dyn HttpEndpoint>> = vec![Arc::new(RawMetricsEndpoint)];

        let err = build_host_policy(&config, &endpoints).unwrap_err();
        match err {
            ManagedWebHostError::StartupFailed { message, .. } => {
                assert!(
                    message.contains("requires control-plane auth"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn build_host_policy_allows_non_loopback_when_no_control_plane_routes_exist() {
        let config = HostConfig::new("0.0.0.0".to_string(), 9090);
        let endpoints: Vec<Arc<dyn HttpEndpoint>> = vec![Arc::new(EchoEndpoint)];

        let host_policy = build_host_policy(&config, &endpoints).unwrap();
        assert!(host_policy.control_plane_auth.is_none());
    }

    #[test]
    fn build_host_policy_validates_control_plane_auth_env_at_startup() {
        let mut config = HostConfig::new("127.0.0.1".to_string(), 9090);
        config.control_plane_auth = Some(AuthPolicy::ApiKey {
            header: "Authorization".to_string(),
            value_env: "OBZENFLOW_TEST_MISSING_CONTROL_PLANE_ENV".to_string(),
        });
        let endpoints: Vec<Arc<dyn HttpEndpoint>> = vec![Arc::new(RawMetricsEndpoint)];

        let err = build_host_policy(&config, &endpoints).unwrap_err();
        match err {
            ManagedWebHostError::StartupFailed { message, .. } => {
                assert!(
                    message.contains("OBZENFLOW_TEST_MISSING_CONTROL_PLANE_ENV"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }
}
