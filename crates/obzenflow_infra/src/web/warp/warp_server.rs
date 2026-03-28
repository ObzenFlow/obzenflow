// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Warp implementation of the WebServer trait

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use matchit::Router as MatchItRouter;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::sse::Event as SseEvent;
use warp::{filters::BoxedFilter, Filter, Rejection, Reply};

use obzenflow_core::event::event_envelope::SystemEventEnvelope;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::web::{
    server::ServerShutdownHandle, AuthPolicy, CorsMode, HttpEndpoint, HttpMethod, ManagedResponse,
    ManagedRouteInfo, Request, Response, RouteKind, ServerConfig, SseBody, SseFrame, WebError,
    WebServer,
};
use obzenflow_core::EventId;

use crate::web::endpoint_tags::SURFACE_NAME_TAG_PREFIX;
use crate::web::routing::{matchit_template_to_public, public_template_to_matchit};
use crate::web::surface_metrics::{HttpSurfaceMetricsCollector, HttpSurfaceObservation};

/// Warp-based web server implementation
pub struct WarpServer {
    endpoints: Vec<Arc<dyn HttpEndpoint>>,
    /// Optional system journal for SSE lifecycle events
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
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

impl WarpServer {
    /// Create a new Warp server
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
            system_journal: None,
            surface_metrics: None,
        }
    }

    /// Attach a system journal to enable SSE lifecycle streaming
    pub fn with_system_journal(&mut self, journal: Arc<dyn Journal<SystemEvent>>) {
        self.system_journal = Some(journal);
    }

    /// Attach an in-memory metrics collector for hosted web surfaces (FLOWIP-093a).
    pub fn with_surface_metrics(&mut self, collector: Arc<HttpSurfaceMetricsCollector>) {
        self.surface_metrics = Some(collector);
    }

    /// Build Warp filter from endpoints.
    fn build_filter(
        &self,
        host_policy: HostPolicy,
    ) -> Result<BoxedFilter<(Box<dyn Reply>,)>, WebError> {
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

        let mut combined_route = get_route
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

        // Add SSE /api/flow/events route if a system journal is available
        if let Some(journal) = &self.system_journal {
            let sse_route = self.build_flow_events_route(journal.clone(), host_policy.clone());
            combined_route = combined_route.or(sse_route).unify().boxed();
        }

        Ok(combined_route)
    }

    fn build_route_router(&self) -> Result<MatchItRouter<RouteDispatch>, WebError> {
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

            if let Some(managed) = endpoint.managed_route() {
                if let Some(surface_policy) = managed.surface_policy.as_ref() {
                    if let Some(surface_auth) = surface_policy.auth.as_ref() {
                        if !matches!(surface_auth, AuthPolicy::None)
                            && matches!(managed.route_policy.auth, Some(AuthPolicy::None))
                        {
                            return Err(WebError::EndpointRegistrationFailed {
                                path: template.clone(),
                                message: "Route-local AuthPolicy::None must not weaken a surface-level auth requirement; prefer leaving surface auth unset and declaring auth on protected routes explicitly"
                                    .to_string(),
                            });
                        }
                    }
                }
            }

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
                    endpoint.clone(),
                    surface_metrics.clone(),
                    &template,
                )?;
            }
        }

        let mut entries: Vec<(String, RouteDispatch)> = by_template.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut router = MatchItRouter::new();
        for (template, dispatch) in entries {
            let matchit_path = public_template_to_matchit(&template).map_err(|message| {
                WebError::EndpointRegistrationFailed {
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
                return Err(WebError::EndpointRegistrationFailed {
                    path: template,
                    message,
                });
            }
        }

        Ok(router)
    }

    /// Build SSE route for `/api/flow/events`
    fn build_flow_events_route(
        &self,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        host_policy: HostPolicy,
    ) -> impl Filter<Extract = (Box<dyn Reply>,), Error = Rejection> + Clone {
        let journal_filter = warp::any().map(move || system_journal.clone());

        warp::path!("api" / "flow" / "events")
            .and(warp::get())
            .and(warp::header::headers_cloned())
            .and(warp::header::optional::<String>("Last-Event-ID"))
            .and(journal_filter)
            .and_then(
                move |headers: warp::http::HeaderMap,
                      last_event_id: Option<String>,
                      journal: Arc<dyn Journal<SystemEvent>>| {
                    let host_policy = host_policy.clone();
                    async move {
                    let req_headers = headers_to_owned_map(&headers);
                    if let Err(response) = maybe_enforce_control_plane_auth(
                        &host_policy,
                        "/api/flow/events",
                        &req_headers,
                        &[],
                    ) {
                        return reply_from_response(response);
                    }

                    let (tx, rx) =
                        tokio::sync::mpsc::unbounded_channel::<Result<SseEvent, Infallible>>();

                    // Spawn a background task to stream system events into the channel
                    tokio::spawn(async move {
                        use tokio::time::{sleep, Duration};

                        let resume_event_id = match last_event_id {
                            Some(id_str) => match EventId::from_string(&id_str) {
                                Ok(event_id) => Some(event_id),
                                Err(e) => {
                                    let payload = serde_json::json!({
                                        "error_type": "invalid_last_event_id",
                                        "message": e.to_string(),
                                        "recoverable": false,
                                    });
                                    let err_ev =
                                        SseEvent::default().event("error").data(payload.to_string());
                                    let _ = tx.send(Ok(err_ev));
                                    None
                                }
                            },
                            None => None,
                        };

                        let mut middleware_state = MiddlewareSseState::default();
                        let mut stage_lifecycle_state = StageLifecycleSseState::default();
                        let mut last_pipeline_event: Option<&'static str> = None;
                        let mut checkpoint_event_id: Option<EventId> = None;

                        // When resuming, we fast-forward until we've passed the resume ID.
                        // When starting fresh (no Last-Event-ID), we fast-forward to EOF so the
                        // client receives only live events (plus a snapshot bootstrap).
                        let mut ready_to_stream = false;
                        let mut resume_seen = resume_event_id.is_none();

                        let mut reader = match journal.reader().await {
                            Ok(reader) => reader,
                            Err(e) => {
                                let payload = serde_json::json!({
                                    "error_type": "journal_open_error",
                                    "message": e.to_string(),
                                    "recoverable": false,
                                });
                                let err_ev =
                                    SseEvent::default().event("error").data(payload.to_string());
                                let _ = tx.send(Ok(err_ev));
                                return;
                            }
                        };

                        loop {
                            match reader.next().await {
                                Ok(Some(envelope)) => {
                                    checkpoint_event_id = Some(envelope.event.id);
                                    if !ready_to_stream {
                                        if let Some(resume_id) = &resume_event_id {
                                            if !resume_seen {
                                                middleware_state.observe(&envelope);
                                                stage_lifecycle_state.observe(&envelope);
                                                last_pipeline_event = last_pipeline_event_name(&envelope)
                                                    .or(last_pipeline_event);

                                                if envelope.event.id == *resume_id {
                                                    resume_seen = true;
                                                }
                                                continue;
                                            }

                                            // Found resume point; start streaming subsequent events.
                                            ready_to_stream = true;
                                        } else {
                                            // Fresh connect: discard history until we reach EOF once.
                                            middleware_state.observe(&envelope);
                                            stage_lifecycle_state.observe(&envelope);
                                            last_pipeline_event = last_pipeline_event_name(&envelope)
                                                .or(last_pipeline_event);
                                            continue;
                                        }
                                    }

                                    stage_lifecycle_state.observe(&envelope);
                                    if let Some(ev) =
                                        map_system_event_to_sse(&envelope, &mut middleware_state)
                                    {
                                        if tx.send(Ok(ev)).is_err() {
                                            break;
                                        }
                                    }

                                    if is_flow_running_event(&envelope) {
                                        if let Some(snapshot_ev) =
                                            middleware_state.build_snapshot_sse_event()
                                        {
                                            if tx.send(Ok(snapshot_ev)).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {
                                    if !ready_to_stream && resume_event_id.is_none() {
                                        // Fresh connect has now caught up to EOF.
                                        ready_to_stream = true;

                                        for snapshot_ev in stage_lifecycle_state
                                            .build_snapshot_sse_events()
                                            .into_iter()
                                        {
                                            let _ = tx.send(Ok(snapshot_ev));
                                        }

                                        // Emit a synthetic bootstrap event with a checkpoint cursor so
                                        // SSE clients can resume "at least once" after transient disconnects.
                                        //
                                        // The checkpoint id is the last *real* system journal event observed
                                        // while building the snapshot, so the server can safely honor it as
                                        // a resume cursor.
                                        {
                                            let payload = serde_json::json!({
                                                "system_event_type": "bootstrap",
                                                "event_type": "flow_bootstrap",
                                                "checkpoint_event_id": checkpoint_event_id.map(|id| id.to_string()),
                                            });
                                            let mut ev = SseEvent::default()
                                                .event("bootstrap")
                                                .data(payload.to_string());
                                            if let Some(id) = checkpoint_event_id {
                                                ev = ev.id(id.to_string());
                                            }
                                            let _ = tx.send(Ok(ev));
                                        }

                                        if matches!(
                                            last_pipeline_event,
                                            Some("flow_running" | "flow_draining" | "flow_stages_completed")
                                        ) {
                                            if let Some(snapshot_ev) =
                                                middleware_state.build_snapshot_sse_event()
                                            {
                                                let _ = tx.send(Ok(snapshot_ev));
                                            }
                                        }
                                    } else if !ready_to_stream && resume_event_id.is_some() && !resume_seen {
                                        // Resume ID was not found in journal history.
                                        ready_to_stream = true;
                                        let payload = serde_json::json!({
                                            "error_type": "journal_resume_not_found",
                                            "message": "Last-Event-ID was not found in the system journal; resuming from live tail",
                                            "recoverable": true,
                                        });
                                        let err_ev =
                                            SseEvent::default().event("error").data(payload.to_string());
                                        let _ = tx.send(Ok(err_ev));

                                        for snapshot_ev in stage_lifecycle_state
                                            .build_snapshot_sse_events()
                                            .into_iter()
                                        {
                                            let _ = tx.send(Ok(snapshot_ev));
                                        }

                                        // Best-effort: publish a bootstrap cursor even when we couldn't
                                        // find the requested resume id (the client can treat this as a new base).
                                        {
                                            let payload = serde_json::json!({
                                                "system_event_type": "bootstrap",
                                                "event_type": "flow_bootstrap",
                                                "checkpoint_event_id": checkpoint_event_id.map(|id| id.to_string()),
                                            });
                                            let mut ev = SseEvent::default()
                                                .event("bootstrap")
                                                .data(payload.to_string());
                                            if let Some(id) = checkpoint_event_id {
                                                ev = ev.id(id.to_string());
                                            }
                                            let _ = tx.send(Ok(ev));
                                        }
                                    } else {
                                        // No new events; back off briefly.
                                        sleep(Duration::from_millis(100)).await;
                                    }
                                }
                                Err(e) => {
                                    let payload = serde_json::json!({
                                        "error_type": "journal_read_error",
                                        "message": e.to_string(),
                                        "recoverable": false,
                                    });
                                    let err_ev =
                                        SseEvent::default().event("error").data(payload.to_string());
                                    let _ = tx.send(Ok(err_ev));
                                    break;
                                }
                            }
                        }
                    });

                    let stream = UnboundedReceiverStream::new(rx);
                    let reply =
                        warp::sse::reply(warp::sse::keep_alive().stream(stream));
                    Ok::<Box<dyn Reply>, Rejection>(Box::new(reply) as Box<dyn Reply>)
                    }
                },
            )
    }
}

impl Default for WarpServer {
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
        endpoint: Arc<dyn HttpEndpoint>,
        surface_metrics: Option<SurfaceMetricsRouteContext>,
        template_for_error: &str,
    ) -> Result<(), WebError> {
        if self.by_method.contains_key(&method) {
            return Err(WebError::EndpointRegistrationFailed {
                path: template_for_error.to_string(),
                message: format!(
                    "Duplicate endpoint registered for {} {}",
                    method.as_str(),
                    template_for_error
                ),
            });
        }

        self.by_method.insert(
            method,
            RoutedEndpoint {
                endpoint,
                surface_metrics,
            },
        );
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
                routed.endpoint,
                host_policy,
                method,
                headers,
                query_params,
                body,
                raw_path,
                matched_route,
                path_params,
                routed.surface_metrics,
            )
            .await
        }
        None => {
            handle_request_no_body(
                routed.endpoint,
                host_policy,
                method,
                headers,
                query_params,
                raw_path,
                matched_route,
                path_params,
                routed.surface_metrics,
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

fn resolve_effective_auth(managed: &ManagedRouteInfo) -> Option<AuthPolicy> {
    if let Some(route_auth) = &managed.route_policy.auth {
        return Some(route_auth.clone());
    }
    managed
        .surface_policy
        .as_ref()
        .and_then(|policy| policy.auth.clone())
}

fn is_control_plane_path(path: &str) -> bool {
    matches!(path, "/api/topology" | "/metrics")
        || path == "/api/flow"
        || path.starts_with("/api/flow/")
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

fn validate_auth_policy_startup(auth: &AuthPolicy) -> Result<(), WebError> {
    match auth {
        AuthPolicy::None => Ok(()),
        AuthPolicy::ApiKey { value_env, .. } => {
            std::env::var(value_env)
                .map(|_| ())
                .map_err(|err| WebError::StartupFailed {
                    message: format!(
                        "Control-plane AuthPolicy::ApiKey expects env var `{value_env}`: {err}"
                    ),
                    source: None,
                })
        }
        AuthPolicy::HmacSha256 {
            secret_env,
            timestamp_header,
            replay_window_secs,
            ..
        } => {
            if replay_window_secs.is_some() && timestamp_header.is_none() {
                return Err(WebError::StartupFailed {
                    message: "Control-plane AuthPolicy::HmacSha256 requires timestamp_header when replay_window_secs is configured".to_string(),
                    source: None,
                });
            }

            std::env::var(secret_env)
                .map(|_| ())
                .map_err(|err| WebError::StartupFailed {
                    message: format!(
                        "Control-plane AuthPolicy::HmacSha256 expects env var `{secret_env}`: {err}"
                    ),
                    source: None,
                })
        }
    }
}

fn build_host_policy(
    config: &ServerConfig,
    endpoints: &[Arc<dyn HttpEndpoint>],
    has_flow_events_route: bool,
) -> Result<HostPolicy, WebError> {
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

    let has_control_plane_routes = has_flow_events_route
        || endpoints.iter().any(|endpoint| {
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
        return Err(WebError::StartupFailed {
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

            let expected = match std::env::var(value_env) {
                Ok(v) => v,
                Err(err) => {
                    tracing::error!(
                        value_env = %value_env,
                        error = %err,
                        "AuthPolicy::ApiKey is misconfigured: expected env var not present"
                    );
                    return Err(Response::internal_error().with_text("Internal Server Error"));
                }
            };

            let provided = header_value(headers, header).unwrap_or_default();
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

            let secret = match std::env::var(secret_env) {
                Ok(v) => v,
                Err(err) => {
                    tracing::error!(
                        secret_env = %secret_env,
                        error = %err,
                        "AuthPolicy::HmacSha256 is misconfigured: expected env var not present"
                    );
                    return Err(Response::internal_error().with_text("Internal Server Error"));
                }
            };

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
                let window = i64::try_from(*window_secs).unwrap_or(i64::MAX);
                if (now - ts).abs() > window {
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

    let bytes: Option<Vec<u8>> = (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).ok())
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

/// Helper function to handle requests without body (GET, HEAD, DELETE, OPTIONS)
#[allow(clippy::too_many_arguments)]
async fn handle_request_no_body(
    endpoint: Arc<dyn HttpEndpoint>,
    host_policy: HostPolicy,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    path: String,
    matched_route: String,
    path_params: HashMap<String, String>,
    surface_metrics: Option<SurfaceMetricsRouteContext>,
) -> Result<Box<dyn Reply>, Rejection> {
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }

    let start = std::time::Instant::now();

    // Convert headers
    let req_headers = headers_to_owned_map(&headers);

    let managed = endpoint.managed_route();
    let effective_timeout =
        resolve_effective_timeout(host_policy.request_timeout, managed.as_ref());
    let log_ctx = if managed.is_some() {
        Some((path.clone(), matched_route.clone()))
    } else {
        None
    };

    if let Some(managed) = managed.as_ref() {
        if let Some(auth) = resolve_effective_auth(managed) {
            if let Err(response) = enforce_auth_policy(&auth, &req_headers, &[]) {
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

    match handler_result {
        Ok(ManagedResponse::Unary(mut response)) => {
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

            let response_bytes = response.body.len() as u64;
            if let Some(metrics) = &surface_metrics {
                metrics.observe(
                    method,
                    response.status,
                    start.elapsed().as_millis() as u64,
                    0,
                    response_bytes,
                );
            }

            let mut builder = warp::http::Response::builder().status(response.status);

            for (key, value) in response.headers {
                builder = builder.header(key, value);
            }

            let reply = builder
                .body(response.body)
                .map_err(|_| warp::reject::reject())?;

            Ok(Box::new(reply) as Box<dyn Reply>)
        }
        Ok(ManagedResponse::Sse(body)) => {
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
        Err(_) => {
            if let Some(metrics) = &surface_metrics {
                metrics.observe(method, 500, start.elapsed().as_millis() as u64, 0, 0);
            }
            Err(warp::reject::reject())
        }
    }
}

/// Helper function to handle requests with body (POST, PUT, PATCH)
#[allow(clippy::too_many_arguments)]
async fn handle_request_with_body(
    endpoint: Arc<dyn HttpEndpoint>,
    host_policy: HostPolicy,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    body: bytes::Bytes,
    path: String,
    matched_route: String,
    path_params: HashMap<String, String>,
    surface_metrics: Option<SurfaceMetricsRouteContext>,
) -> Result<Box<dyn Reply>, Rejection> {
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }

    let start = std::time::Instant::now();
    let request_bytes = body.len() as u64;

    // Convert headers
    let req_headers = headers_to_owned_map(&headers);

    let managed = endpoint.managed_route();
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

        if let Some(auth) = resolve_effective_auth(managed) {
            if let Err(response) = enforce_auth_policy(&auth, &req_headers, body.as_ref()) {
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

    match handler_result {
        Ok(ManagedResponse::Unary(mut response)) => {
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

            let response_bytes = response.body.len() as u64;
            if let Some(metrics) = &surface_metrics {
                metrics.observe(
                    method,
                    response.status,
                    start.elapsed().as_millis() as u64,
                    request_bytes,
                    response_bytes,
                );
            }

            let mut builder = warp::http::Response::builder().status(response.status);

            for (key, value) in response.headers {
                builder = builder.header(key, value);
            }

            let reply = builder
                .body(response.body)
                .map_err(|_| warp::reject::reject())?;

            Ok(Box::new(reply) as Box<dyn Reply>)
        }
        Ok(ManagedResponse::Sse(body)) => {
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
        Err(_) => {
            if let Some(metrics) = &surface_metrics {
                metrics.observe(
                    method,
                    500,
                    start.elapsed().as_millis() as u64,
                    request_bytes,
                    0,
                );
            }
            Err(warp::reject::reject())
        }
    }
}

fn sse_body_reply(body: SseBody) -> impl Reply {
    use tokio_stream::StreamExt;

    let stream = body.map(|frame: SseFrame| Ok::<SseEvent, Infallible>(sse_frame_to_warp(frame)));
    warp::sse::reply(warp::sse::keep_alive().stream(stream))
}

fn sse_frame_to_warp(frame: SseFrame) -> SseEvent {
    let mut ev = SseEvent::default().data(frame.data);
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

#[async_trait]
impl WebServer for WarpServer {
    fn register_endpoint(&mut self, endpoint: Box<dyn HttpEndpoint>) -> Result<(), WebError> {
        self.endpoints.push(Arc::from(endpoint));
        Ok(())
    }

    async fn start(self, config: ServerConfig) -> Result<(), WebError> {
        let addr: SocketAddr = config.address().parse().map_err(|e| WebError::BindFailed {
            address: config.address(),
            source: Some(Box::new(e)),
        })?;

        let host_policy =
            build_host_policy(&config, &self.endpoints, self.system_journal.is_some())?;

        let routes = self.build_filter(host_policy)?;

        let cors_config = config.cors.unwrap_or_default();
        let cors_mode = cors_config.mode;
        if matches!(&cors_mode, CorsMode::AllowAnyOrigin) && !cfg!(debug_assertions) {
            tracing::warn!(
                "CORS is configured as AllowAnyOrigin in a release build; prefer an explicit allow-list for production"
            );
        }

        // Add CORS support (configurable).
        //
        // Note: `CorsMode::SameOrigin` means "do not add CORS headers"; browsers will enforce
        // the same-origin policy by default.
        let routes_with_cors = if matches!(&cors_mode, CorsMode::SameOrigin) {
            routes
        } else {
            let mut cors = warp::cors();
            cors = match cors_mode {
                CorsMode::AllowAnyOrigin => cors.allow_any_origin(),
                CorsMode::AllowList(origins) => {
                    let origins: Vec<&str> = origins.iter().map(String::as_str).collect();
                    cors.allow_origins(origins)
                }
                CorsMode::SameOrigin => cors,
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

        // Start the server
        warp::serve(routes_with_cors).run(addr).await;

        Ok(())
    }

    fn shutdown_handle(&self) -> Option<Box<dyn ServerShutdownHandle>> {
        // Warp doesn't provide easy shutdown handles in this simple implementation
        // For production, we'd need to use warp::Server::bind_with_graceful_shutdown
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::MemoryJournal;
    use async_trait::async_trait;
    use obzenflow_core::web::{
        AuthPolicy, HttpMethod, ManagedResponse, ManagedRouteInfo, Request, Response, RouteKind,
        RoutePolicy, ServerConfig, SurfacePolicy, WebError,
    };

    fn test_host_policy() -> HostPolicy {
        HostPolicy {
            max_body_size_bytes: 10,
            request_timeout: None,
            control_plane_auth: None,
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
            Ok(Response::ok().with_text("OK").into())
        }
    }

    #[tokio::test]
    async fn build_filter_enforces_content_length_limit() {
        let mut server = WarpServer::new();
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

        async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
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
        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
            Ok(Response::ok().with_text("PARAM").into())
        }
    }

    #[tokio::test]
    async fn build_filter_prefers_static_routes_over_parameterised_routes() {
        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
            Ok(Response::ok().with_text("B").into())
        }
    }

    #[tokio::test]
    async fn build_filter_rejects_conflicting_parameterised_routes() {
        let mut server = WarpServer::new();
        server
            .register_endpoint(Box::new(ConflictEndpointA))
            .unwrap();
        server
            .register_endpoint(Box::new(ConflictEndpointB))
            .unwrap();

        let err = server.build_filter(test_host_policy()).unwrap_err();
        match err {
            WebError::EndpointRegistrationFailed { path, message } => {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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
        let mut server = WarpServer::new();
        server
            .register_endpoint(Box::new(WeakeningAuthEndpoint))
            .unwrap();

        let err = server.build_filter(test_host_policy()).unwrap_err();

        match err {
            WebError::EndpointRegistrationFailed { path, message } => {
                assert_eq!(path, "/weak");
                assert!(
                    message.contains("must not weaken"),
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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
        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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
        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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
        std::env::set_var("OBZENFLOW_TEST_API_KEY_V1", "sekret");

        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        std::env::set_var("OBZENFLOW_TEST_HMAC_SECRET_V1", "sekret");

        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Response::ok().with_text("OK").into())
        }
    }

    #[tokio::test]
    async fn host_policy_enforces_request_timeout() {
        let mut server = WarpServer::new();
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
            Ok(Response::ok().with_text("control").into())
        }
    }

    #[tokio::test]
    async fn control_plane_auth_enforces_raw_metrics_endpoint() {
        std::env::set_var("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");

        let mut server = WarpServer::new();
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
        std::env::set_var("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");

        let mut server = WarpServer::new();
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
        std::env::set_var("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");

        let mut server = WarpServer::new();
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
        std::env::set_var("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");

        let mut server = WarpServer::new();
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
        std::env::set_var("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");

        let mut server = WarpServer::new();
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

    #[tokio::test]
    async fn control_plane_auth_covers_flow_events_special_route() {
        std::env::set_var("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");

        let mut server = WarpServer::new();
        server.with_system_journal(Arc::new(MemoryJournal::<SystemEvent>::new()));
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
        let config = ServerConfig::new("0.0.0.0".to_string(), 9090);
        let endpoints: Vec<Arc<dyn HttpEndpoint>> = vec![Arc::new(RawMetricsEndpoint)];

        let err = build_host_policy(&config, &endpoints, false).unwrap_err();
        match err {
            WebError::StartupFailed { message, .. } => {
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
        let config = ServerConfig::new("0.0.0.0".to_string(), 9090);
        let endpoints: Vec<Arc<dyn HttpEndpoint>> = vec![Arc::new(EchoEndpoint)];

        let host_policy = build_host_policy(&config, &endpoints, false).unwrap();
        assert!(host_policy.control_plane_auth.is_none());
    }

    #[test]
    fn build_host_policy_validates_control_plane_auth_env_at_startup() {
        let mut config = ServerConfig::new("127.0.0.1".to_string(), 9090);
        config.control_plane_auth = Some(AuthPolicy::ApiKey {
            header: "Authorization".to_string(),
            value_env: "OBZENFLOW_TEST_MISSING_CONTROL_PLANE_ENV".to_string(),
        });
        let endpoints: Vec<Arc<dyn HttpEndpoint>> = vec![Arc::new(RawMetricsEndpoint)];

        let err = build_host_policy(&config, &endpoints, false).unwrap_err();
        match err {
            WebError::StartupFailed { message, .. } => {
                assert!(
                    message.contains("OBZENFLOW_TEST_MISSING_CONTROL_PLANE_ENV"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }
}

/// Map a SystemEvent into an SSE event with JSON payload
fn map_system_event_to_sse(
    envelope: &SystemEventEnvelope,
    middleware_state: &mut MiddlewareSseState,
) -> Option<SseEvent> {
    use obzenflow_core::event::system_event::StageLifecycleEvent;
    use obzenflow_core::event::SystemEventType;
    use serde_json::json;

    let event: &SystemEvent = &envelope.event;
    let id_str = event.id.to_string();
    let vector_clock_value = serde_json::to_value(&envelope.vector_clock).ok();
    middleware_state.last_vector_clock = Some(envelope.vector_clock.clone());

    match &event.event {
        SystemEventType::StageLifecycle {
            stage_id,
            event: lifecycle,
        } => {
            let (event_type, metrics_value, error, recoverable, reason) = match lifecycle {
                StageLifecycleEvent::Running => ("stage_running", None, None, None, None),
                StageLifecycleEvent::Draining { metrics } => (
                    "stage_draining",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                    None,
                ),
                StageLifecycleEvent::Drained => ("stage_drained", None, None, None, None),
                StageLifecycleEvent::Completed { metrics } => (
                    "stage_completed",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                    None,
                ),
                StageLifecycleEvent::Cancelled { reason, metrics } => (
                    "stage_cancelled",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                    Some(reason.clone()),
                ),
                StageLifecycleEvent::Failed {
                    error,
                    recoverable,
                    metrics,
                } => (
                    "stage_failed",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    Some(error.clone()),
                    *recoverable,
                    None,
                ),
            };

            let mut data = json!({
                "system_event_type": "stage_lifecycle",
                "event_type": event_type,
                "stage_id": stage_id.to_string(),
                "timestamp_ms": event.timestamp,
            });

            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            if let Some(m) = metrics_value {
                data["metrics"] = m;
            }
            if let Some(err) = error {
                data["error"] = serde_json::Value::String(err);
            }
            if let Some(r) = reason {
                data["reason"] = serde_json::Value::String(r);
            }
            if let Some(rec) = recoverable {
                data["recoverable"] = serde_json::Value::Bool(rec);
            }

            Some(
                SseEvent::default()
                    .id(id_str)
                    .event("stage_lifecycle")
                    .data(data.to_string()),
            )
        }
        SystemEventType::PipelineLifecycle(pipeline_event) => match pipeline_event {
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Starting => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_starting",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Running {
                stage_count,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_running",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(count) = stage_count {
                    data["stage_count"] = json!(count);
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::StopRequested {
                mode,
                timeout_ms,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_stop_requested",
                    "timestamp_ms": event.timestamp,
                    "mode": mode,
                });
                if let Some(ms) = timeout_ms {
                    data["timeout_ms"] = json!(ms);
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Draining { metrics } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_draining",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(m) = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()) {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::AllStagesCompleted {
                metrics,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_stages_completed",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(m) = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()) {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Drained => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_drained",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Completed {
                duration_ms,
                metrics,
            } => {
                let metrics_value = serde_json::to_value(metrics).ok();
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_completed",
                    "timestamp_ms": event.timestamp,
                    "duration_ms": duration_ms,
                });
                if let Some(m) = metrics_value {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Failed {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            } => {
                let metrics_value = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok());
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_failed",
                    "timestamp_ms": event.timestamp,
                    "reason": reason,
                    "duration_ms": duration_ms,
                });
                if let Some(cause) = failure_cause {
                    if let Ok(cause_value) = serde_json::to_value(cause) {
                        data["failure_cause"] = cause_value;
                    }
                }
                if let Some(m) = metrics_value {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Cancelled {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            } => {
                let metrics_value = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok());
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_cancelled",
                    "timestamp_ms": event.timestamp,
                    "reason": reason,
                    "duration_ms": duration_ms,
                });
                if let Some(cause) = failure_cause {
                    if let Ok(cause_value) = serde_json::to_value(cause) {
                        data["failure_cause"] = cause_value;
                    }
                }
                if let Some(m) = metrics_value {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("flow_lifecycle")
                        .data(data.to_string()),
                )
            }
        },
        SystemEventType::ReplayLifecycle(replay_event) => match replay_event {
            obzenflow_core::event::ReplayLifecycleEvent::Started {
                archive_path,
                archive_flow_id,
                archive_status,
                archive_status_derivation,
                allow_incomplete,
                source_stages,
            } => {
                let mut data = json!({
                    "system_event_type": "replay_lifecycle",
                    "event_type": "replay_started",
                    "timestamp_ms": event.timestamp,
                    "archive_path": archive_path,
                    "archive_flow_id": archive_flow_id,
                    "archive_status": archive_status,
                    "archive_status_derivation": archive_status_derivation,
                    "allow_incomplete": allow_incomplete,
                    "source_stages": source_stages,
                });
                if let Some(stage_id) = event.writer_id.as_stage() {
                    data["stage_id"] = json!(stage_id.to_string());
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("replay_lifecycle")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::ReplayLifecycleEvent::Completed {
                replayed_count,
                skipped_count,
                duration_ms,
            } => {
                let mut data = json!({
                    "system_event_type": "replay_lifecycle",
                    "event_type": "replay_completed",
                    "timestamp_ms": event.timestamp,
                    "replayed_count": replayed_count,
                    "skipped_count": skipped_count,
                    "duration_ms": duration_ms,
                });
                if let Some(stage_id) = event.writer_id.as_stage() {
                    data["stage_id"] = json!(stage_id.to_string());
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("replay_lifecycle")
                        .data(data.to_string()),
                )
            }
        },
        SystemEventType::MiddlewareLifecycle {
            stage_id,
            stage_name,
            flow_id,
            flow_name,
            origin,
            middleware,
        } => {
            middleware_state.observe_middleware_metadata(
                *stage_id,
                stage_name.as_deref(),
                flow_id.as_deref(),
                flow_name.as_deref(),
                Some(&envelope.vector_clock),
            );

            let mut data = json!({
                "system_event_type": "middleware_lifecycle",
                "stage_id": stage_id.to_string(),
                "timestamp_ms": event.timestamp,
                "origin": {
                    "event_id": origin.event_id.to_string(),
                    "writer_key": origin.writer_key,
                    "seq": origin.seq,
                },
                "revision": origin.seq,
            });

            if let Some(name) = stage_name {
                data["stage_name"] = json!(name);
            }
            if let Some(fid) = flow_id {
                data["flow_id"] = json!(fid);
            }
            if let Some(fname) = flow_name {
                data["flow_name"] = json!(fname);
            }
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            if let Some(payload) =
                middleware_state.project_middleware_event(*stage_id, origin.seq.0, middleware)
            {
                if let Some(payload_obj) = payload.as_object() {
                    for (key, value) in payload_obj {
                        data[key] = value.clone();
                    }
                }
            } else {
                return Some(SseEvent::default().comment("unsupported_middleware_event_skipped"));
            }

            Some(
                SseEvent::default()
                    .id(id_str)
                    .event("middleware_lifecycle")
                    .data(data.to_string()),
            )
        }
        SystemEventType::ContractStatus {
            upstream,
            reader,
            pass,
            reader_seq,
            advertised_writer_seq,
            reason,
        } => {
            let mut data = json!({
                "system_event_type": "contract_status",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "pass": pass,
                "timestamp_ms": event.timestamp,
            });

            if let Some(seq) = reader_seq {
                data["reader_seq"] = serde_json::json!(seq);
            }
            if let Some(seq) = advertised_writer_seq {
                data["advertised_writer_seq"] = serde_json::json!(seq);
            }
            if let Some(cause) = reason {
                data["reason"] = serde_json::json!(cause);
            }
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            let sse_event_name = if *pass {
                "contract_status"
            } else {
                "contract_violation"
            };

            Some(
                SseEvent::default()
                    .id(id_str)
                    .event(sse_event_name)
                    .data(data.to_string()),
            )
        }
        SystemEventType::ContractResult {
            upstream,
            reader,
            contract_name,
            status,
            cause,
            reader_seq,
            advertised_writer_seq,
        } => {
            let mut data = json!({
                "system_event_type": "contract_result",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "contract_name": contract_name,
                "status": status,
                "timestamp_ms": event.timestamp,
            });

            if let Some(cause) = cause {
                data["cause"] = serde_json::json!(cause);
            }
            if let Some(seq) = reader_seq {
                data["reader_seq"] = serde_json::json!(seq);
            }
            if let Some(seq) = advertised_writer_seq {
                data["advertised_writer_seq"] = serde_json::json!(seq);
            }
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            Some(
                SseEvent::default()
                    .id(id_str)
                    .event("contract_result")
                    .data(data.to_string()),
            )
        }
        SystemEventType::EdgeLiveness {
            upstream,
            reader,
            state,
            idle_ms,
            last_reader_seq,
            last_event_id,
        } => {
            let mut data = json!({
                "system_event_type": "edge_liveness",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "state": state,
                "idle_ms": idle_ms,
                "timestamp_ms": event.timestamp,
            });

            if let Some(seq) = last_reader_seq {
                data["last_reader_seq"] = serde_json::json!(seq);
            }
            if let Some(event_id) = last_event_id {
                data["last_event_id"] = serde_json::json!(event_id.to_string());
            }
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            Some(
                SseEvent::default()
                    .id(id_str)
                    .event("edge_liveness")
                    .data(data.to_string()),
            )
        }
        SystemEventType::StageHeartbeat { .. } => None,
        SystemEventType::MetricsCoordination(metrics_event) => match metrics_event {
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Exported {
                watermark,
            } => {
                let mut data = json!({
                    "timestamp_ms": event.timestamp,
                    "watermark": watermark,
                    "export_id": id_str,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("metrics_watermark")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Ready => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_ready",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("metrics_coordination")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::DrainRequested => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_drain_requested",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("metrics_coordination")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Drained => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_drained",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("metrics_coordination")
                        .data(data.to_string()),
                )
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Shutdown => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_shutdown",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(
                    SseEvent::default()
                        .id(id_str)
                        .event("metrics_coordination")
                        .data(data.to_string()),
                )
            }
        },
        SystemEventType::ContractOverrideByPolicy {
            upstream,
            reader,
            contract_name,
            original_cause,
            policy,
        } => {
            let mut data = json!({
                "system_event_type": "contract_override_by_policy",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "contract_name": contract_name,
                "original_cause": original_cause,
                "policy": policy,
                "timestamp_ms": event.timestamp,
            });
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            Some(
                SseEvent::default()
                    .id(id_str)
                    .event("contract_override_by_policy")
                    .data(data.to_string()),
            )
        }
        // FLOWIP-093a: keep system-level hosted-surface snapshot facts out of the SSE stream
        // by default to avoid turning /api/flow/events into a high-volume metrics pipe.
        SystemEventType::HttpSurfaceSnapshot { .. } => None,
    }
}

fn last_pipeline_event_name(envelope: &SystemEventEnvelope) -> Option<&'static str> {
    match &envelope.event.event {
        obzenflow_core::event::SystemEventType::PipelineLifecycle(event) => Some(match event {
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Starting => {
                "flow_starting"
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Running { .. } => {
                "flow_running"
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::StopRequested {
                ..
            } => "flow_stop_requested",
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Draining { .. } => {
                "flow_draining"
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::AllStagesCompleted {
                ..
            } => "flow_stages_completed",
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Drained => "flow_drained",
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Completed { .. } => {
                "flow_completed"
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Failed { .. } => {
                "flow_failed"
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Cancelled { .. } => {
                "flow_cancelled"
            }
        }),
        _ => None,
    }
}

fn is_flow_running_event(envelope: &SystemEventEnvelope) -> bool {
    matches!(
        &envelope.event.event,
        obzenflow_core::event::SystemEventType::PipelineLifecycle(
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Running { .. }
        )
    )
}

#[derive(Default)]
struct StageLifecycleSseState {
    /// Latest lifecycle envelope per stage (best-effort).
    ///
    /// Used to bootstrap new SSE clients so the UI can render stage state even
    /// if it connected after the original stage_running events were emitted.
    latest_by_stage: std::collections::BTreeMap<obzenflow_core::StageId, SystemEventEnvelope>,
}

impl StageLifecycleSseState {
    fn observe(&mut self, envelope: &SystemEventEnvelope) {
        use obzenflow_core::event::system_event::StageLifecycleEvent;
        use obzenflow_core::event::SystemEventType;

        let SystemEventType::StageLifecycle { stage_id, event } = &envelope.event.event else {
            return;
        };

        // Prefer terminal lifecycle events with metrics when duplicates exist
        // (some stages write both a supervisor completion marker and a later
        // metrics-enriched completion event).
        let should_replace = match (self.latest_by_stage.get(stage_id), event) {
            (None, _) => true,
            (Some(prev), StageLifecycleEvent::Completed { metrics: None }) => !matches!(
                prev.event.event,
                SystemEventType::StageLifecycle {
                    event: StageLifecycleEvent::Completed { metrics: Some(_) },
                    ..
                }
            ),
            (Some(prev), StageLifecycleEvent::Cancelled { metrics: None, .. }) => !matches!(
                prev.event.event,
                SystemEventType::StageLifecycle {
                    event: StageLifecycleEvent::Cancelled {
                        metrics: Some(_),
                        ..
                    },
                    ..
                }
            ),
            (Some(prev), StageLifecycleEvent::Failed { metrics: None, .. }) => !matches!(
                prev.event.event,
                SystemEventType::StageLifecycle {
                    event: StageLifecycleEvent::Failed {
                        metrics: Some(_),
                        ..
                    },
                    ..
                }
            ),
            _ => true,
        };

        if should_replace {
            self.latest_by_stage.insert(*stage_id, envelope.clone());
        }
    }

    fn build_snapshot_sse_events(&self) -> Vec<SseEvent> {
        let mut out = Vec::new();
        for envelope in self.latest_by_stage.values() {
            if let Some(ev) = map_stage_lifecycle_to_sse_snapshot(envelope) {
                out.push(ev);
            }
        }
        out
    }
}

fn map_stage_lifecycle_to_sse_snapshot(envelope: &SystemEventEnvelope) -> Option<SseEvent> {
    use obzenflow_core::event::system_event::StageLifecycleEvent;
    use obzenflow_core::event::SystemEventType;
    use serde_json::json;

    let event: &SystemEvent = &envelope.event;
    let vector_clock_value = serde_json::to_value(&envelope.vector_clock).ok();

    let SystemEventType::StageLifecycle {
        stage_id,
        event: lifecycle,
    } = &event.event
    else {
        return None;
    };

    let (event_type, metrics_value, error, recoverable, reason) = match lifecycle {
        StageLifecycleEvent::Running => ("stage_running", None, None, None, None),
        StageLifecycleEvent::Draining { metrics } => (
            "stage_draining",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            None,
            None,
            None,
        ),
        StageLifecycleEvent::Drained => ("stage_drained", None, None, None, None),
        StageLifecycleEvent::Completed { metrics } => (
            "stage_completed",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            None,
            None,
            None,
        ),
        StageLifecycleEvent::Cancelled { reason, metrics } => (
            "stage_cancelled",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            None,
            None,
            Some(reason.clone()),
        ),
        StageLifecycleEvent::Failed {
            error,
            recoverable,
            metrics,
        } => (
            "stage_failed",
            metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
            Some(error.clone()),
            *recoverable,
            None,
        ),
    };

    let mut data = json!({
        "system_event_type": "stage_lifecycle",
        "event_type": event_type,
        "stage_id": stage_id.to_string(),
        "timestamp_ms": event.timestamp,
    });

    if let Some(vc) = &vector_clock_value {
        data["vector_clock"] = vc.clone();
    }
    if let Some(m) = metrics_value {
        data["metrics"] = m;
    }
    if let Some(err) = error {
        data["error"] = serde_json::Value::String(err);
    }
    if let Some(r) = reason {
        data["reason"] = serde_json::Value::String(r);
    }
    if let Some(rec) = recoverable {
        data["recoverable"] = serde_json::Value::Bool(rec);
    }

    Some(
        SseEvent::default()
            .event("stage_lifecycle")
            .data(data.to_string()),
    )
}

#[derive(Default)]
struct MiddlewareSseState {
    flow_id: Option<String>,
    flow_name: Option<String>,
    stage_names: HashMap<obzenflow_core::StageId, String>,
    circuit_breakers: HashMap<obzenflow_core::StageId, CircuitBreakerSnapshot>,
    rate_limiters: HashMap<obzenflow_core::StageId, RateLimiterSnapshot>,
    last_vector_clock: Option<obzenflow_core::event::vector_clock::VectorClock>,
}

#[derive(Clone)]
struct CircuitBreakerSnapshot {
    state: String,
    revision: u64,
    successes_total: Option<u64>,
    failures_total: Option<u64>,
    opened_total: Option<u64>,
    requests_processed: Option<u64>,
    requests_rejected: Option<u64>,
    rejection_rate: Option<f64>,
    consecutive_failures: Option<usize>,
    time_in_closed_s: Option<f64>,
    time_in_open_s: Option<f64>,
    time_in_half_open_s: Option<f64>,
}

#[derive(Clone)]
struct RateLimiterSnapshot {
    mode: String,
    revision: u64,
    utilization_pct: Option<f64>,
    events_in_window: Option<u64>,
    window_size_ms: Option<u64>,
}

impl MiddlewareSseState {
    fn observe(&mut self, envelope: &SystemEventEnvelope) {
        self.last_vector_clock = Some(envelope.vector_clock.clone());

        let obzenflow_core::event::SystemEventType::MiddlewareLifecycle {
            stage_id,
            stage_name,
            flow_id,
            flow_name,
            origin,
            middleware,
        } = &envelope.event.event
        else {
            return;
        };

        self.observe_middleware_metadata(
            *stage_id,
            stage_name.as_deref(),
            flow_id.as_deref(),
            flow_name.as_deref(),
            None,
        );
        self.apply_middleware_event(*stage_id, origin.seq.0, middleware);
    }

    fn observe_middleware_metadata(
        &mut self,
        stage_id: obzenflow_core::StageId,
        stage_name: Option<&str>,
        flow_id: Option<&str>,
        flow_name: Option<&str>,
        vector_clock: Option<&obzenflow_core::event::vector_clock::VectorClock>,
    ) {
        if let Some(name) = stage_name {
            self.stage_names.insert(stage_id, name.to_string());
        }
        if self.flow_id.is_none() {
            if let Some(id) = flow_id {
                self.flow_id = Some(id.to_string());
            }
        }
        if self.flow_name.is_none() {
            if let Some(name) = flow_name {
                self.flow_name = Some(name.to_string());
            }
        }
        if let Some(vc) = vector_clock {
            self.last_vector_clock = Some(vc.clone());
        }
    }

    fn project_middleware_event(
        &mut self,
        stage_id: obzenflow_core::StageId,
        revision: u64,
        middleware: &obzenflow_core::event::payloads::observability_payload::MiddlewareLifecycle,
    ) -> Option<serde_json::Value> {
        use obzenflow_core::event::payloads::observability_payload::{
            BackpressureEvent, CircuitBreakerEvent, MiddlewareLifecycle, RateLimiterEvent,
        };
        use serde_json::json;

        match middleware {
            MiddlewareLifecycle::CircuitBreaker(cb) => {
                let previous_state = self
                    .circuit_breakers
                    .get(&stage_id)
                    .map(|snapshot| snapshot.state.clone());

                match cb {
                    CircuitBreakerEvent::Opened {
                        error_rate,
                        failure_count,
                        last_error,
                    } => {
                        let state_to = "open".to_string();

                        self.apply_middleware_event(stage_id, revision, middleware);

                        let mut payload = json!({
                            "middleware": "circuit_breaker",
                            "event_type": "state_change",
                            "state_to": state_to,
                            "context": {
                                "error_rate": error_rate,
                                "failure_count": failure_count,
                            }
                        });
                        if let Some(from) = previous_state {
                            payload["state_from"] = json!(from);
                        }
                        if let Some(err) = last_error {
                            payload["context"]["last_error"] = json!(err);
                        }
                        Some(payload)
                    }
                    CircuitBreakerEvent::Closed {
                        success_count,
                        recovery_duration_ms,
                    } => {
                        let state_to = "closed".to_string();

                        self.apply_middleware_event(stage_id, revision, middleware);

                        let mut payload = json!({
                            "middleware": "circuit_breaker",
                            "event_type": "state_change",
                            "state_to": state_to,
                            "context": {
                                "success_count": success_count,
                                "recovery_duration_ms": recovery_duration_ms,
                            }
                        });
                        if let Some(from) = previous_state {
                            payload["state_from"] = json!(from);
                        }
                        Some(payload)
                    }
                    CircuitBreakerEvent::HalfOpen { test_request_count } => {
                        let state_to = "half_open".to_string();

                        self.apply_middleware_event(stage_id, revision, middleware);

                        let mut payload = json!({
                            "middleware": "circuit_breaker",
                            "event_type": "state_change",
                            "state_to": state_to,
                            "context": {
                                "test_request_count": test_request_count,
                            }
                        });
                        if let Some(from) = previous_state {
                            payload["state_from"] = json!(from);
                        }
                        Some(payload)
                    }
                    CircuitBreakerEvent::Summary {
                        window_duration_s,
                        requests_processed,
                        requests_rejected,
                        state,
                        consecutive_failures,
                        rejection_rate,
                        successes_total,
                        failures_total,
                        opened_total,
                        time_in_closed_seconds,
                        time_in_open_seconds,
                        time_in_half_open_seconds,
                    } => {
                        let current_state = normalize_circuit_state(state);

                        self.apply_middleware_event(stage_id, revision, middleware);

                        Some(json!({
                            "middleware": "circuit_breaker",
                            "event_type": "summary",
                            "current_state": current_state,
                            "summary": {
                                "window_duration_s": window_duration_s,
                                "requests_processed": requests_processed,
                                "requests_rejected": requests_rejected,
                                "consecutive_failures": consecutive_failures,
                                "rejection_rate": rejection_rate,
                                "successes_total": successes_total,
                                "failures_total": failures_total,
                                "opened_total": opened_total,
                                "time_in_closed_s": time_in_closed_seconds,
                                "time_in_open_s": time_in_open_seconds,
                                "time_in_half_open_s": time_in_half_open_seconds,
                            }
                        }))
                    }
                    // High-volume (not mirrored) or unsupported variants.
                    _ => None,
                }
            }
            MiddlewareLifecycle::RateLimiter(rl) => match rl {
                RateLimiterEvent::ActivityPulse {
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    limit_rate,
                } => Some(json!({
                    "middleware": "rate_limiter",
                    "event_type": "activity_pulse",
                    "window_ms": window_ms,
                    "delayed_events": delayed_events,
                    "delay_ms_total": delay_ms_total,
                    "delay_ms_max": delay_ms_max,
                    "limit_rate": limit_rate,
                })),
                RateLimiterEvent::ModeChange {
                    mode_from,
                    mode_to,
                    limit_rate,
                } => {
                    self.apply_middleware_event(stage_id, revision, middleware);

                    Some(json!({
                        "middleware": "rate_limiter",
                        "event_type": "mode_change",
                        "mode_from": mode_from,
                        "mode_to": mode_to,
                        "limit_rate": limit_rate,
                    }))
                }
                RateLimiterEvent::WindowUtilization {
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                } => {
                    self.apply_middleware_event(stage_id, revision, middleware);

                    let mode = self
                        .rate_limiters
                        .get(&stage_id)
                        .map(|snapshot| snapshot.mode.clone())
                        .unwrap_or_else(|| "normal".to_string());

                    Some(json!({
                        "middleware": "rate_limiter",
                        "event_type": "window_utilization",
                        "utilization_pct": utilization_percent,
                        "events_in_window": events_in_window,
                        "window_size_ms": window_size_ms,
                        "mode": mode,
                    }))
                }
                _ => None,
            },
            MiddlewareLifecycle::Backpressure(bp) => match bp {
                BackpressureEvent::ActivityPulse {
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    min_credit,
                    limiting_downstream_stage_id,
                } => {
                    let mut payload = json!({
                        "middleware": "backpressure",
                        "event_type": "activity_pulse",
                        "window_ms": window_ms,
                        "delayed_events": delayed_events,
                        "delay_ms_total": delay_ms_total,
                        "delay_ms_max": delay_ms_max,
                    });
                    if let Some(v) = min_credit {
                        payload["min_credit"] = json!(v);
                    }
                    if let Some(v) = limiting_downstream_stage_id {
                        payload["limiting_downstream_stage_id"] = json!(v.to_string());
                    }
                    Some(payload)
                }
            },
            _ => None,
        }
    }

    fn apply_middleware_event(
        &mut self,
        stage_id: obzenflow_core::StageId,
        revision: u64,
        middleware: &obzenflow_core::event::payloads::observability_payload::MiddlewareLifecycle,
    ) {
        use obzenflow_core::event::payloads::observability_payload::{
            CircuitBreakerEvent, MiddlewareLifecycle, RateLimiterEvent,
        };

        match middleware {
            MiddlewareLifecycle::CircuitBreaker(cb) => {
                match cb {
                    CircuitBreakerEvent::Opened { .. } => {
                        let entry = self.circuit_breakers.entry(stage_id).or_insert(
                            CircuitBreakerSnapshot {
                                state: "open".to_string(),
                                revision,
                                successes_total: None,
                                failures_total: None,
                                opened_total: None,
                                requests_processed: None,
                                requests_rejected: None,
                                rejection_rate: None,
                                consecutive_failures: None,
                                time_in_closed_s: None,
                                time_in_open_s: None,
                                time_in_half_open_s: None,
                            },
                        );
                        entry.state = "open".to_string();
                        entry.revision = revision;
                    }
                    CircuitBreakerEvent::Closed { .. } => {
                        let entry = self.circuit_breakers.entry(stage_id).or_insert(
                            CircuitBreakerSnapshot {
                                state: "closed".to_string(),
                                revision,
                                successes_total: None,
                                failures_total: None,
                                opened_total: None,
                                requests_processed: None,
                                requests_rejected: None,
                                rejection_rate: None,
                                consecutive_failures: None,
                                time_in_closed_s: None,
                                time_in_open_s: None,
                                time_in_half_open_s: None,
                            },
                        );
                        entry.state = "closed".to_string();
                        entry.revision = revision;
                    }
                    CircuitBreakerEvent::HalfOpen { .. } => {
                        let entry = self.circuit_breakers.entry(stage_id).or_insert(
                            CircuitBreakerSnapshot {
                                state: "half_open".to_string(),
                                revision,
                                successes_total: None,
                                failures_total: None,
                                opened_total: None,
                                requests_processed: None,
                                requests_rejected: None,
                                rejection_rate: None,
                                consecutive_failures: None,
                                time_in_closed_s: None,
                                time_in_open_s: None,
                                time_in_half_open_s: None,
                            },
                        );
                        entry.state = "half_open".to_string();
                        entry.revision = revision;
                    }
                    CircuitBreakerEvent::Summary {
                        state,
                        requests_processed,
                        requests_rejected,
                        rejection_rate,
                        consecutive_failures,
                        successes_total,
                        failures_total,
                        opened_total,
                        time_in_closed_seconds,
                        time_in_open_seconds,
                        time_in_half_open_seconds,
                        ..
                    } => {
                        self.circuit_breakers.insert(
                            stage_id,
                            CircuitBreakerSnapshot {
                                state: normalize_circuit_state(state),
                                revision,
                                successes_total: Some(*successes_total),
                                failures_total: Some(*failures_total),
                                opened_total: Some(*opened_total),
                                requests_processed: Some(*requests_processed),
                                requests_rejected: Some(*requests_rejected),
                                rejection_rate: Some(*rejection_rate),
                                consecutive_failures: Some(*consecutive_failures),
                                time_in_closed_s: Some(*time_in_closed_seconds),
                                time_in_open_s: Some(*time_in_open_seconds),
                                time_in_half_open_s: Some(*time_in_half_open_seconds),
                            },
                        );
                    }
                    _ => {}
                }
            }
            MiddlewareLifecycle::RateLimiter(rl) => match rl {
                RateLimiterEvent::ModeChange { mode_to, .. } => {
                    let entry = self
                        .rate_limiters
                        .entry(stage_id)
                        .or_insert(RateLimiterSnapshot {
                            mode: mode_to.clone(),
                            revision,
                            utilization_pct: None,
                            events_in_window: None,
                            window_size_ms: None,
                        });
                    entry.mode = mode_to.clone();
                    entry.revision = revision;
                }
                RateLimiterEvent::WindowUtilization {
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                } => {
                    let mode = self
                        .rate_limiters
                        .get(&stage_id)
                        .map(|snapshot| snapshot.mode.clone())
                        .unwrap_or_else(|| "normal".to_string());

                    self.rate_limiters.insert(
                        stage_id,
                        RateLimiterSnapshot {
                            mode,
                            revision,
                            utilization_pct: Some(*utilization_percent),
                            events_in_window: Some(*events_in_window),
                            window_size_ms: Some(*window_size_ms),
                        },
                    );
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn build_snapshot_sse_event(&self) -> Option<SseEvent> {
        use serde_json::json;

        if self.circuit_breakers.is_empty() && self.rate_limiters.is_empty() {
            return None;
        }

        let mut middleware = Vec::new();

        let mut stage_ids: std::collections::BTreeSet<obzenflow_core::StageId> =
            std::collections::BTreeSet::new();
        stage_ids.extend(self.circuit_breakers.keys().copied());
        stage_ids.extend(self.rate_limiters.keys().copied());

        for stage_id in stage_ids {
            let mut stage_obj = json!({
                "stage_id": stage_id.to_string(),
            });

            if let Some(stage_name) = self.stage_names.get(&stage_id) {
                stage_obj["stage_name"] = json!(stage_name);
            }

            if let Some(cb) = self.circuit_breakers.get(&stage_id) {
                let mut cb_obj = json!({
                    "state": cb.state,
                    "revision": cb.revision,
                });
                if let Some(v) = cb.successes_total {
                    cb_obj["successes_total"] = json!(v);
                }
                if let Some(v) = cb.failures_total {
                    cb_obj["failures_total"] = json!(v);
                }
                if let Some(v) = cb.opened_total {
                    cb_obj["opened_total"] = json!(v);
                }
                if let Some(v) = cb.requests_processed {
                    cb_obj["requests_processed"] = json!(v);
                }
                if let Some(v) = cb.requests_rejected {
                    cb_obj["requests_rejected"] = json!(v);
                }
                if let Some(v) = cb.rejection_rate {
                    cb_obj["rejection_rate"] = json!(v);
                }
                if let Some(v) = cb.consecutive_failures {
                    cb_obj["consecutive_failures"] = json!(v);
                }
                if let Some(v) = cb.time_in_closed_s {
                    cb_obj["time_in_closed_s"] = json!(v);
                }
                if let Some(v) = cb.time_in_open_s {
                    cb_obj["time_in_open_s"] = json!(v);
                }
                if let Some(v) = cb.time_in_half_open_s {
                    cb_obj["time_in_half_open_s"] = json!(v);
                }
                stage_obj["circuit_breaker"] = cb_obj;
            }

            if let Some(rl) = self.rate_limiters.get(&stage_id) {
                let mut rl_obj = json!({
                    "mode": rl.mode,
                    "revision": rl.revision,
                });
                if let Some(v) = rl.utilization_pct {
                    rl_obj["utilization_pct"] = json!(v);
                }
                if let Some(v) = rl.events_in_window {
                    rl_obj["events_in_window"] = json!(v);
                }
                if let Some(v) = rl.window_size_ms {
                    rl_obj["window_size_ms"] = json!(v);
                }
                stage_obj["rate_limiter"] = rl_obj;
            }

            middleware.push(stage_obj);
        }

        let mut data = json!({
            "timestamp_ms": now_timestamp_ms(),
            "middleware": middleware,
        });

        if let Some(flow_id) = &self.flow_id {
            data["flow_id"] = json!(flow_id);
        }
        if let Some(flow_name) = &self.flow_name {
            data["flow_name"] = json!(flow_name);
        }
        if let Some(vc) = &self.last_vector_clock {
            if let Ok(v) = serde_json::to_value(vc) {
                data["vector_clock"] = v;
            }
        }

        Some(
            SseEvent::default()
                .event("middleware_state_snapshot")
                .data(data.to_string()),
        )
    }
}

fn normalize_circuit_state(state: &str) -> String {
    let lower = state.to_ascii_lowercase();
    match lower.as_str() {
        "closed" => "closed".to_string(),
        "open" => "open".to_string(),
        "halfopen" | "half_open" | "half-open" => "half_open".to_string(),
        other => other.to_string(),
    }
}

fn now_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
