//! Warp implementation of the WebServer trait

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::{filters::BoxedFilter, Filter, Rejection, Reply};
use warp::sse::Event as SseEvent;

use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::event_envelope::SystemEventEnvelope;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::EventId;
use obzenflow_core::web::{
    server::ServerShutdownHandle, HttpEndpoint, HttpMethod, Request, ServerConfig, WebError,
    WebServer,
};

/// Warp-based web server implementation
pub struct WarpServer {
    endpoints: Vec<Arc<dyn HttpEndpoint>>,
    /// Optional system journal for SSE lifecycle events
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
}

impl WarpServer {
    /// Create a new Warp server
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
            system_journal: None,
        }
    }

    /// Attach a system journal to enable SSE lifecycle streaming
    pub fn with_system_journal(
        &mut self,
        journal: Arc<dyn Journal<SystemEvent>>,
    ) {
        self.system_journal = Some(journal);
    }
    
    /// Build path filter from string path
    fn build_path_filter(&self, path: &str) -> impl Filter<Extract = (), Error = Rejection> + Clone {
        if path == "/" {
            warp::path::end().boxed()
        } else {
            let segments: Vec<&str> = path.trim_start_matches('/')
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
    
    /// Build Warp filter from endpoints  
    fn build_filter(&self) -> BoxedFilter<(Box<dyn Reply>,)> {
        let mut combined_route: Option<BoxedFilter<(Box<dyn Reply>,)>> = None;
        
        for endpoint in &self.endpoints {
            let endpoint = endpoint.clone();
            let path_str = endpoint.path().to_string();
            
            // Create simple route for this endpoint
            let route = self.build_simple_route(path_str, endpoint);
            
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
                    Box::new(warp::http::Response::builder()
                        .status(404)
                        .body(Vec::new())
                        .unwrap())
                })
                .boxed()
        })
    }
    
    /// Build a simple route for an endpoint
    fn build_simple_route(
        &self,
        path_str: String,
        endpoint: Arc<dyn HttpEndpoint>,
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
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>, headers: warp::http::HeaderMap, body: bytes::Bytes| {
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
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>, headers: warp::http::HeaderMap, body: bytes::Bytes| {
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
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |query_params: HashMap<String, String>, headers: warp::http::HeaderMap, body: bytes::Bytes| {
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

                        // Step 1: best-effort replay of events after Last-Event-ID (if provided)
                        if let Some(id_str) = last_event_id {
                            match EventId::from_string(&id_str) {
                                Ok(event_id) => match journal.read_causally_after(&event_id).await {
                                    Ok(events) => {
                                        for envelope in events {
                                            let ev = map_system_event_to_sse(&envelope);
                                            if tx.send(Ok(ev)).is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let payload = serde_json::json!({
                                            "error_type": "journal_resume_error",
                                            "message": e.to_string(),
                                            "recoverable": false,
                                        });
                                        let err_ev = SseEvent::default()
                                            .event("error")
                                            .data(payload.to_string());
                                        let _ = tx.send(Ok(err_ev));
                                        // On resume failure, fall through to live tail from current end.
                                    }
                                },
                                Err(e) => {
                                    let payload = serde_json::json!({
                                        "error_type": "invalid_last_event_id",
                                        "message": e.to_string(),
                                        "recoverable": false,
                                    });
                                    let err_ev = SseEvent::default()
                                        .event("error")
                                        .data(payload.to_string());
                                    let _ = tx.send(Ok(err_ev));
                                }
                            }
                        }

                        // Step 2: live tail from the journal using a reader
                        match journal.reader().await {
                            Ok(mut reader) => loop {
                                match reader.next().await {
                                    Ok(Some(envelope)) => {
                                        let ev = map_system_event_to_sse(&envelope);
                                        if tx.send(Ok(ev)).is_err() {
                                            break;
                                        }
                                    }
                                    Ok(None) => {
                                        // No new events; back off briefly
                                        sleep(Duration::from_millis(100)).await;
                                    }
                                    Err(e) => {
                                        let payload = serde_json::json!({
                                            "error_type": "journal_read_error",
                                            "message": e.to_string(),
                                            "recoverable": false,
                                        });
                                        let err_ev = SseEvent::default()
                                            .event("error")
                                            .data(payload.to_string());
                                        let _ = tx.send(Ok(err_ev));
                                        break;
                                    }
                                }
                            },
                            Err(e) => {
                                let payload = serde_json::json!({
                                    "error_type": "journal_open_error",
                                    "message": e.to_string(),
                                    "recoverable": false,
                                });
                                let err_ev =
                                    SseEvent::default().event("error").data(payload.to_string());
                                let _ = tx.send(Ok(err_ev));
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
            let mut builder = warp::http::Response::builder()
                .status(response.status);
            
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
            let mut builder = warp::http::Response::builder()
                .status(response.status);
            
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
        let addr: SocketAddr = config.address()
            .parse()
            .map_err(|e| WebError::BindFailed {
                address: config.address(),
                source: Some(Box::new(e)),
            })?;
        
        let routes = self.build_filter();
        
        // Add CORS support for development
        // This allows the UI (running on different port) to access the API
        let cors = warp::cors()
            .allow_any_origin()  // Allow any origin in development
            .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
            .allow_headers(vec!["Content-Type", "Accept"]);
        
        let routes_with_cors = routes.with(cors);
        
        // Start the server
        warp::serve(routes_with_cors)
            .run(addr)
            .await;
        
        Ok(())
    }
    
    fn shutdown_handle(&self) -> Option<Box<dyn ServerShutdownHandle>> {
        // Warp doesn't provide easy shutdown handles in this simple implementation
        // For production, we'd need to use warp::Server::bind_with_graceful_shutdown
        None
    }
}

/// Map a SystemEvent into an SSE event with JSON payload
fn map_system_event_to_sse(envelope: &SystemEventEnvelope) -> SseEvent {
    use obzenflow_core::event::system_event::StageLifecycleEvent;
    use obzenflow_core::event::SystemEventType;
    use serde_json::json;

    let event: &SystemEvent = &envelope.event;
    let id_str = event.id.to_string();
    let vector_clock_value = serde_json::to_value(&envelope.vector_clock).ok();

    match &event.event {
        SystemEventType::StageLifecycle { stage_id, event: lifecycle } => {
            let (event_type, metrics_value, error, recoverable) = match lifecycle {
                StageLifecycleEvent::Running => ("stage_running", None, None, None),
                StageLifecycleEvent::Draining { metrics } => (
                    "stage_draining",
                    metrics
                        .as_ref()
                        .and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                ),
                StageLifecycleEvent::Drained => ("stage_drained", None, None, None),
                StageLifecycleEvent::Completed { metrics } => (
                    "stage_completed",
                    metrics
                        .as_ref()
                        .and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                ),
                StageLifecycleEvent::Failed {
                    error,
                    recoverable,
                    metrics,
                } => (
                    "stage_failed",
                    metrics
                        .as_ref()
                        .and_then(|m| serde_json::to_value(m).ok()),
                    Some(error.clone()),
                    *recoverable,
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

            // Skip supervisor-only completion events without metrics to avoid
            // duplicate stage_completed events in the SSE stream.
            if event_type == "stage_completed" && metrics_value.is_none() {
                return SseEvent::default()
                    .comment("stage_completed_without_metrics_skipped");
            }

            if let Some(m) = metrics_value {
                data["metrics"] = m;
            }
            if let Some(err) = error {
                data["error"] = serde_json::Value::String(err);
            }
            if let Some(rec) = recoverable {
                data["recoverable"] = serde_json::Value::Bool(rec);
            }

            SseEvent::default()
                .id(id_str)
                .event("stage_lifecycle")
                .data(data.to_string())
        }
        SystemEventType::PipelineLifecycle(pipeline_event) => {
            match pipeline_event {
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
                obzenflow_core::event::system_event::PipelineLifecycleEvent::Draining {
                    metrics,
                } => {
                    let mut data = json!({
                        "system_event_type": "pipeline_lifecycle",
                        "event_type": "flow_draining",
                        "timestamp_ms": event.timestamp,
                    });
                    if let Some(m) = metrics
                        .as_ref()
                        .and_then(|m| serde_json::to_value(m).ok())
                    {
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
                    if let Some(m) = metrics
                        .as_ref()
                        .and_then(|m| serde_json::to_value(m).ok())
                    {
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
                } => {
                    let metrics_value = metrics
                        .as_ref()
                        .and_then(|m| serde_json::to_value(m).ok());
                    let mut data = json!({
                        "system_event_type": "pipeline_lifecycle",
                        "event_type": "flow_failed",
                        "timestamp_ms": event.timestamp,
                        "reason": reason,
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
            }
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
        SystemEventType::MetricsCoordination(metrics_event) => {
            let event_type = match metrics_event {
                obzenflow_core::event::system_event::MetricsCoordinationEvent::Ready => {
                    "metrics_ready"
                }
                obzenflow_core::event::system_event::MetricsCoordinationEvent::DrainRequested => {
                    "metrics_drain_requested"
                }
                obzenflow_core::event::system_event::MetricsCoordinationEvent::Drained => {
                    "metrics_drained"
                }
                obzenflow_core::event::system_event::MetricsCoordinationEvent::Shutdown => {
                    "metrics_shutdown"
                }
            };

            let data = json!({
                "system_event_type": "metrics_coordination",
                "event_type": event_type,
                "timestamp_ms": event.timestamp,
            });
            let mut data = data;
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            SseEvent::default()
                .id(id_str)
                .event("metrics_coordination")
                .data(data.to_string())
        }
        SystemEventType::ContractOverrideByPolicy {
            upstream,
            reader,
            original_cause,
            policy,
        } => {
            let mut data = json!({
                "system_event_type": "contract_override_by_policy",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
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
