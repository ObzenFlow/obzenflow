//! Warp implementation of the WebServer trait

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::sse::Event as SseEvent;
use warp::{filters::BoxedFilter, Filter, Rejection, Reply};

use obzenflow_core::event::event_envelope::SystemEventEnvelope;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
	use obzenflow_core::web::{
	    server::ServerShutdownHandle, CorsMode, HttpEndpoint, HttpMethod, Request, ServerConfig,
	    WebError, WebServer,
	};
use obzenflow_core::EventId;

/// Warp-based web server implementation
pub struct WarpServer {
    endpoints: Vec<Arc<dyn HttpEndpoint>>,
    /// Optional system journal for SSE lifecycle events
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
}

const DEFAULT_MAX_BODY_SIZE_BYTES: usize = 10 * 1024 * 1024;

impl WarpServer {
    /// Create a new Warp server
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
            system_journal: None,
        }
    }

    /// Attach a system journal to enable SSE lifecycle streaming
    pub fn with_system_journal(&mut self, journal: Arc<dyn Journal<SystemEvent>>) {
        self.system_journal = Some(journal);
    }

    /// Build path filter from string path
    fn build_path_filter(
        &self,
        path: &str,
    ) -> impl Filter<Extract = (), Error = Rejection> + Clone {
        if path == "/" {
            warp::path::end().boxed()
        } else {
            let segments: Vec<&str> = path
                .trim_start_matches('/')
                .split('/')
                .filter(|s| !s.is_empty())
                .collect();

            let mut filter = warp::path(segments[0].to_string()).boxed();
            for segment in &segments[1..] {
                filter = filter.and(warp::path(segment.to_string())).boxed();
            }
            filter.and(warp::path::end()).boxed()
        }
    }

    /// Build Warp filter from endpoints.
    fn build_filter(&self, max_body_size_bytes: u64) -> BoxedFilter<(Box<dyn Reply>,)> {
        let mut combined_route: Option<BoxedFilter<(Box<dyn Reply>,)>> = None;

        for endpoint in &self.endpoints {
            let endpoint = endpoint.clone();
            let path_str = endpoint.path().to_string();

            // Create simple route for this endpoint
            let route = self.build_simple_route(path_str, endpoint, max_body_size_bytes);

            combined_route = match combined_route {
                Some(existing) => Some(existing.or(route).unify().boxed()),
                None => Some(route.boxed()),
            };
        }

        // Add SSE /api/flow/events route if a system journal is available
        if let Some(journal) = &self.system_journal {
            let sse_route = self.build_flow_events_route(journal.clone());
            combined_route = match combined_route {
                Some(existing) => Some(existing.or(sse_route).unify().boxed()),
                None => Some(sse_route.boxed()),
            };
        }

        // Return combined routes or 404 if none
        combined_route.unwrap_or_else(|| {
            warp::any()
                .map(|| -> Box<dyn Reply> {
                    Box::new(
                        warp::http::Response::builder()
                            .status(404)
                            .body(Vec::new())
                            .unwrap(),
                    )
                })
                .boxed()
        })
    }

    /// Build a simple route for an endpoint
    fn build_simple_route(
        &self,
        path_str: String,
        endpoint: Arc<dyn HttpEndpoint>,
        max_body_size_bytes: u64,
    ) -> impl Filter<Extract = (Box<dyn Reply>,), Error = Rejection> + Clone {
        let path_filter = self.build_path_filter(&path_str);

        // Create GET/HEAD/DELETE routes (no body)
        let get_route = warp::get()
            .and(path_filter.clone())
            .and(
                warp::filters::query::query::<HashMap<String, String>>()
                    .or(warp::any().map(|| HashMap::new()))
                    .unify(),
            )
            .and(warp::header::headers_cloned())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>, headers: warp::http::HeaderMap| {
                    handle_request_no_body(
                        endpoint.clone(),
                        HttpMethod::Get,
                        headers,
                        query_params,
                        path_str.clone(),
                    )
                }
            });

        let head_route = warp::head()
            .and(path_filter.clone())
            .and(
                warp::filters::query::query::<HashMap<String, String>>()
                    .or(warp::any().map(|| HashMap::new()))
                    .unify(),
            )
            .and(warp::header::headers_cloned())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>, headers: warp::http::HeaderMap| {
                    handle_request_no_body(
                        endpoint.clone(),
                        HttpMethod::Head,
                        headers,
                        query_params,
                        path_str.clone(),
                    )
                }
            });

        let delete_route = warp::delete()
            .and(path_filter.clone())
            .and(
                warp::filters::query::query::<HashMap<String, String>>()
                    .or(warp::any().map(|| HashMap::new()))
                    .unify(),
            )
            .and(warp::header::headers_cloned())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>, headers: warp::http::HeaderMap| {
                    handle_request_no_body(
                        endpoint.clone(),
                        HttpMethod::Delete,
                        headers,
                        query_params,
                        path_str.clone(),
                    )
                }
            });

        // Create POST/PUT/PATCH routes (with body)
        let post_route = warp::post()
            .and(path_filter.clone())
            .and(
                warp::filters::query::query::<HashMap<String, String>>()
                    .or(warp::any().map(|| HashMap::new()))
                    .unify(),
            )
            .and(warp::header::headers_cloned())
            .and(warp::body::content_length_limit(max_body_size_bytes))
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap,
                      body: bytes::Bytes| {
                    handle_request_with_body(
                        endpoint.clone(),
                        HttpMethod::Post,
                        headers,
                        query_params,
                        body,
                        path_str.clone(),
                    )
                }
            });

        let put_route = warp::put()
            .and(path_filter.clone())
            .and(
                warp::filters::query::query::<HashMap<String, String>>()
                    .or(warp::any().map(|| HashMap::new()))
                    .unify(),
            )
            .and(warp::header::headers_cloned())
            .and(warp::body::content_length_limit(max_body_size_bytes))
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap,
                      body: bytes::Bytes| {
                    handle_request_with_body(
                        endpoint.clone(),
                        HttpMethod::Put,
                        headers,
                        query_params,
                        body,
                        path_str.clone(),
                    )
                }
            });

        let patch_route = warp::patch()
            .and(path_filter.clone())
            .and(
                warp::filters::query::query::<HashMap<String, String>>()
                    .or(warp::any().map(|| HashMap::new()))
                    .unify(),
            )
            .and(warp::header::headers_cloned())
            .and(warp::body::content_length_limit(max_body_size_bytes))
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>,
                      headers: warp::http::HeaderMap,
                      body: bytes::Bytes| {
                    handle_request_with_body(
                        endpoint.clone(),
                        HttpMethod::Patch,
                        headers,
                        query_params,
                        body,
                        path_str.clone(),
                    )
                }
            });

        // Combine all routes
        get_route
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
    }

    /// Build SSE route for `/api/flow/events`
    fn build_flow_events_route(
        &self,
        system_journal: Arc<dyn Journal<SystemEvent>>,
    ) -> impl Filter<Extract = (Box<dyn Reply>,), Error = Rejection> + Clone {
        let journal_filter = warp::any().map(move || system_journal.clone());

        warp::path!("api" / "flow" / "events")
            .and(warp::get())
            .and(warp::header::optional::<String>("Last-Event-ID"))
            .and(journal_filter)
            .and_then(
                |last_event_id: Option<String>,
                 journal: Arc<dyn Journal<SystemEvent>>| async move {
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
                                    let ev =
                                        map_system_event_to_sse(&envelope, &mut middleware_state);
                                    if tx.send(Ok(ev)).is_err() {
                                        break;
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
                },
            )
    }
}

impl Default for WarpServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to handle requests without body (GET, HEAD, DELETE, OPTIONS)
async fn handle_request_no_body(
    endpoint: Arc<dyn HttpEndpoint>,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    path: String,
) -> Result<Box<dyn Reply>, Rejection> {
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }

    // Convert headers
    let mut req_headers = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            req_headers.insert(name.to_string(), value_str.to_string());
        }
    }

    let request = Request {
        method,
        path,
        headers: req_headers,
        query_params,
        body: Vec::new(),
    };

    // Handle request
    match endpoint.handle(request).await {
        Ok(response) => {
            let mut builder = warp::http::Response::builder().status(response.status);

            for (key, value) in response.headers {
                builder = builder.header(key, value);
            }

            let reply = builder
                .body(response.body)
                .map_err(|_| warp::reject::reject())?;

            Ok(Box::new(reply) as Box<dyn Reply>)
        }
        Err(_) => Err(warp::reject::reject()),
    }
}

/// Helper function to handle requests with body (POST, PUT, PATCH)
async fn handle_request_with_body(
    endpoint: Arc<dyn HttpEndpoint>,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    query_params: HashMap<String, String>,
    body: bytes::Bytes,
    path: String,
) -> Result<Box<dyn Reply>, Rejection> {
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }

    // Convert headers
    let mut req_headers = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            req_headers.insert(name.to_string(), value_str.to_string());
        }
    }

    let request = Request {
        method,
        path,
        headers: req_headers,
        query_params,
        body: body.to_vec(),
    };

    // Handle request
    match endpoint.handle(request).await {
        Ok(response) => {
            let mut builder = warp::http::Response::builder().status(response.status);

            for (key, value) in response.headers {
                builder = builder.header(key, value);
            }

            let reply = builder
                .body(response.body)
                .map_err(|_| warp::reject::reject())?;

            Ok(Box::new(reply) as Box<dyn Reply>)
        }
        Err(_) => Err(warp::reject::reject()),
    }
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

        let max_body_size_bytes = config.max_body_size.unwrap_or(DEFAULT_MAX_BODY_SIZE_BYTES);
        let routes = self.build_filter(max_body_size_bytes as u64);

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
                .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
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
    use async_trait::async_trait;
    use obzenflow_core::web::{HttpMethod, Request, Response, WebError};

    struct EchoEndpoint;

    #[async_trait]
    impl HttpEndpoint for EchoEndpoint {
        fn path(&self) -> &str {
            "/echo"
        }

        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Post]
        }

        async fn handle(&self, _request: Request) -> Result<Response, WebError> {
            Ok(Response::ok().with_text("OK"))
        }
    }

    #[tokio::test]
    async fn build_filter_enforces_content_length_limit() {
        let mut server = WarpServer::new();
        server.register_endpoint(Box::new(EchoEndpoint)).unwrap();
        let filter = server.build_filter(10);

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
}

/// Map a SystemEvent into an SSE event with JSON payload
fn map_system_event_to_sse(
    envelope: &SystemEventEnvelope,
    middleware_state: &mut MiddlewareSseState,
) -> SseEvent {
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

            SseEvent::default()
                .id(id_str)
                .event("stage_lifecycle")
                .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("flow_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("replay_lifecycle")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("replay_lifecycle")
                    .data(data.to_string())
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
                middleware_state.project_middleware_event(*stage_id, origin.seq, middleware)
            {
                if let Some(payload_obj) = payload.as_object() {
                    for (key, value) in payload_obj {
                        data[key] = value.clone();
                    }
                }
            } else {
                return SseEvent::default().comment("unsupported_middleware_event_skipped");
            }

            SseEvent::default()
                .id(id_str)
                .event("middleware_lifecycle")
                .data(data.to_string())
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

            SseEvent::default()
                .id(id_str)
                .event(sse_event_name)
                .data(data.to_string())
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

            SseEvent::default()
                .id(id_str)
                .event("contract_result")
                .data(data.to_string())
        }
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
                SseEvent::default()
                    .id(id_str)
                    .event("metrics_watermark")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("metrics_coordination")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("metrics_coordination")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("metrics_coordination")
                    .data(data.to_string())
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
                SseEvent::default()
                    .id(id_str)
                    .event("metrics_coordination")
                    .data(data.to_string())
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

            SseEvent::default()
                .id(id_str)
                .event("contract_override_by_policy")
                .data(data.to_string())
        }
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
            obzenflow_core::event::system_event::PipelineLifecycleEvent::StopRequested { .. } => {
                "flow_stop_requested"
            }
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
            (Some(prev), StageLifecycleEvent::Completed { metrics: None }) => {
                !matches!(
                    prev.event.event,
                    SystemEventType::StageLifecycle {
                        event: StageLifecycleEvent::Completed { metrics: Some(_) },
                        ..
                    }
                )
            }
            (Some(prev), StageLifecycleEvent::Cancelled { metrics: None, .. }) => {
                !matches!(
                    prev.event.event,
                    SystemEventType::StageLifecycle {
                        event: StageLifecycleEvent::Cancelled { metrics: Some(_), .. },
                        ..
                    }
                )
            }
            (Some(prev), StageLifecycleEvent::Failed { metrics: None, .. }) => {
                !matches!(
                    prev.event.event,
                    SystemEventType::StageLifecycle {
                        event: StageLifecycleEvent::Failed { metrics: Some(_), .. },
                        ..
                    }
                )
            }
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

    Some(SseEvent::default().event("stage_lifecycle").data(data.to_string()))
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
        self.apply_middleware_event(*stage_id, origin.seq, middleware);
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
