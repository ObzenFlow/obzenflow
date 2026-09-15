// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::web::surface_metrics::HttpSurfaceMetricsEmitter;
use obzenflow_core::id::SystemId;
use obzenflow_core::web::{EndpointMetadata, RoutePolicy};
use obzenflow_core::FlowId;
use obzenflow_runtime::execution::{RuntimeExecution, RuntimeMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const SECRET: &str = "response-diagnostic-secret-sentinel";

#[derive(Clone, Copy)]
enum Outcome {
    Failure,
    BadStatus,
    BadName,
    BadValue,
    Intentional,
}

struct Endpoint {
    outcome: Outcome,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpEndpoint for Endpoint {
    fn path(&self) -> &str {
        "/probe/:id"
    }
    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get, HttpMethod::Post]
    }
    fn metadata(&self) -> Option<EndpointMetadata> {
        Some(
            EndpointMetadata::new("probe".into())
                .with_tags(vec![format!("{SURFACE_NAME_TAG_PREFIX}probe")]),
        )
    }
    fn managed_route(&self) -> Option<ManagedRouteInfo> {
        Some(ManagedRouteInfo {
            kind: RouteKind::Unary,
            surface_policy: None,
            route_policy: RoutePolicy::default(),
        })
    }
    async fn handle(&self, _: Request) -> Result<ManagedResponse, EndpointError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = match self.outcome {
            Outcome::Failure => {
                return Err(EndpointError::with_source(
                    "Reading probe response",
                    std::io::Error::other(SECRET),
                ))
            }
            Outcome::BadStatus => Response::new(0).with_text(SECRET),
            Outcome::BadName => {
                Response::ok().with_header(format!("{SECRET}\r\n"), "invalid".into())
            }
            Outcome::BadValue => {
                Response::ok().with_header("x-probe".into(), format!("{SECRET}\r\n"))
            }
            Outcome::Intentional => Response::new(503)
                .with_text("Busy")
                .with_header("Retry-After".into(), "7".into()),
        };
        Ok(response.into())
    }
}

#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn get_and_post_failures_select_safe_500_before_optional_measurements() {
    let logs = LogCapture::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _logging = tracing::subscriber::set_default(subscriber);
    for method in ["GET", "POST"] {
        for outcome in [
            Outcome::Failure,
            Outcome::BadStatus,
            Outcome::BadName,
            Outcome::BadValue,
            Outcome::Intentional,
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let metrics = Arc::new(HttpSurfaceMetricsCollector::new());
            let execution = RuntimeExecution::new(RuntimeMode::Live, None);
            let recorder = execution.observation_recorder(FlowId::new(), SystemId::new().into());
            let mut host = WarpWebHost::new();
            host.with_surface_metrics(metrics.clone());
            host.register_endpoint(Box::new(Endpoint {
                outcome,
                calls: calls.clone(),
            }))
            .unwrap();
            let routes = host
                .build_filter(HostPolicy {
                    max_body_size_bytes: 100,
                    request_timeout: None,
                    control_plane_auth: None,
                })
                .unwrap();
            let request = warp::test::request().method(method).path("/probe/one");
            let reply = if method == "POST" {
                request.body("abc")
            } else {
                request
            }
            .reply(&routes)
            .await;
            let intentional = matches!(outcome, Outcome::Intentional);
            let expected_body = if intentional {
                "Busy"
            } else {
                "Internal Server Error"
            };
            assert_eq!(reply.status(), if intentional { 503 } else { 500 });
            assert_eq!(reply.headers()["content-type"], "text/plain");
            assert_eq!(reply.body(), expected_body);
            if intentional {
                assert_eq!(reply.headers()["retry-after"], "7");
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(metrics.total_requests(), 1);
            HttpSurfaceMetricsEmitter::new(metrics, recorder).capture_final();
            use obzenflow_core::event::observation::{ObservationRecord, ObservationSource};
            let captures = execution.observations().snapshot();
            let ObservationRecord::HttpSurface { snapshot } = &captures[0].records[0] else {
                panic!("expected surface snapshot")
            };
            assert_eq!(snapshot.routes.len(), 1);
            let route = &snapshot.routes[0];
            assert_eq!(route.status_class, "5xx");
            assert_eq!(route.requests_total, 1);
            assert_eq!(
                route.request_bytes_total,
                if method == "POST" { 3 } else { 0 }
            );
            assert_eq!(route.response_bytes_total, expected_body.len() as u64);
        }
    }
    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Reading probe response"));
    assert!(logs.contains("/probe/:id"));
    assert!(!logs.contains(SECRET));
}
