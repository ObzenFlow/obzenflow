// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Optional hosted-surface HTTP measurements.
//!
//! The hosting layer maintains in-memory counters for low-overhead request accounting,
//! The runtime capture owner offers bounded samples to its retained view for `/metrics`.

use obzenflow_core::event::observability::{
    HttpSurfaceMetricsSnapshot, HttpSurfaceRouteMetricsSnapshot,
};
use obzenflow_core::web::HttpMethod;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct HttpSurfaceObservation {
    pub surface_name: Arc<str>,
    pub method: HttpMethod,
    pub path: Arc<str>,
    pub status: u16,
    pub duration_ms: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpStatusClass {
    S2xx,
    S3xx,
    S4xx,
    S5xx,
    Other,
}

impl HttpStatusClass {
    pub fn from_status(status: u16) -> Self {
        match status {
            200..=299 => Self::S2xx,
            300..=399 => Self::S3xx,
            400..=499 => Self::S4xx,
            500..=599 => Self::S5xx,
            _ => Self::Other,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::S2xx => "2xx",
            Self::S3xx => "3xx",
            Self::S4xx => "4xx",
            Self::S5xx => "5xx",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Debug, Eq)]
struct RouteKey {
    surface_name: Arc<str>,
    method: HttpMethod,
    path: Arc<str>,
    status_class: HttpStatusClass,
}

impl PartialEq for RouteKey {
    fn eq(&self, other: &Self) -> bool {
        self.method == other.method
            && self.status_class == other.status_class
            && self.surface_name.as_ref() == other.surface_name.as_ref()
            && self.path.as_ref() == other.path.as_ref()
    }
}

impl Hash for RouteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.surface_name.as_ref().hash(state);
        self.method.hash(state);
        self.path.as_ref().hash(state);
        self.status_class.hash(state);
    }
}

#[derive(Default)]
struct RouteCounters {
    requests_total: AtomicU64,
    request_duration_ms_total: AtomicU64,
    request_bytes_total: AtomicU64,
    response_bytes_total: AtomicU64,
}

/// In-memory request accounting for hosted web surfaces.
///
/// This collector is intentionally low-cardinality and only intended for surfaces that
/// explicitly attach to `FlowApplication` (not built-in framework endpoints).
#[derive(Default)]
pub struct HttpSurfaceMetricsCollector {
    routes: Mutex<HashMap<RouteKey, Arc<RouteCounters>>>,
    total_requests: AtomicU64,
}

impl HttpSurfaceMetricsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&self, observation: HttpSurfaceObservation) {
        let status_class = HttpStatusClass::from_status(observation.status);

        let key = RouteKey {
            surface_name: observation.surface_name,
            method: observation.method,
            path: observation.path,
            status_class,
        };

        let counters = {
            let Ok(mut map) = self.routes.try_lock() else {
                return;
            };
            if map.len() >= 1024 && !map.contains_key(&key) {
                return;
            }
            map.entry(key)
                .or_insert_with(|| Arc::new(RouteCounters::default()))
                .clone()
        };

        counters.requests_total.fetch_add(1, Ordering::Relaxed);
        counters
            .request_duration_ms_total
            .fetch_add(observation.duration_ms, Ordering::Relaxed);
        counters
            .request_bytes_total
            .fetch_add(observation.request_bytes, Ordering::Relaxed);
        counters
            .response_bytes_total
            .fetch_add(observation.response_bytes, Ordering::Relaxed);

        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    pub fn try_snapshot_routes(&self) -> Option<Vec<HttpSurfaceRouteMetricsSnapshot>> {
        let map = self.routes.try_lock().ok()?;

        let mut out = Vec::with_capacity(map.len());
        for (key, counters) in map.iter() {
            out.push(HttpSurfaceRouteMetricsSnapshot {
                surface_name: key.surface_name.to_string(),
                method: key.method,
                path: key.path.to_string(),
                status_class: key.status_class.as_str().to_string(),
                requests_total: counters.requests_total.load(Ordering::Relaxed),
                request_duration_ms_total: counters
                    .request_duration_ms_total
                    .load(Ordering::Relaxed),
                request_bytes_total: counters.request_bytes_total.load(Ordering::Relaxed),
                response_bytes_total: counters.response_bytes_total.load(Ordering::Relaxed),
            });
        }

        out.sort_by(|a, b| {
            (
                a.surface_name.as_str(),
                a.path.as_str(),
                a.method.as_str(),
                a.status_class.as_str(),
            )
                .cmp(&(
                    b.surface_name.as_str(),
                    b.path.as_str(),
                    b.method.as_str(),
                    b.status_class.as_str(),
                ))
        });

        Some(out)
    }
}

#[derive(Clone)]
pub struct HttpSurfaceMetricsEmitter {
    state: Arc<HttpSurfaceMetricsEmitterState>,
}

struct HttpSurfaceMetricsEmitterState {
    collector: Arc<HttpSurfaceMetricsCollector>,
    recorder: Arc<dyn obzenflow_core::event::observation::ObservationRecorder>,
    last_emitted_total_requests: AtomicU64,
}

impl HttpSurfaceMetricsEmitter {
    pub fn new(
        collector: Arc<HttpSurfaceMetricsCollector>,
        recorder: Arc<dyn obzenflow_core::event::observation::ObservationRecorder>,
    ) -> Self {
        Self {
            state: Arc::new(HttpSurfaceMetricsEmitterState {
                collector,
                recorder,
                last_emitted_total_requests: AtomicU64::new(0),
            }),
        }
    }

    pub fn spawn_periodic(&self, interval: Duration) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            if interval == Duration::ZERO {
                return;
            }

            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                this.emit_snapshot(false);
            }
        })
    }

    /// Best-effort flush on shutdown to reduce the "last interval" gap.
    pub fn capture_final(&self) {
        self.emit_snapshot(true);
    }

    fn emit_snapshot(&self, force: bool) {
        let current_total = self.state.collector.total_requests();
        let last_emitted = self
            .state
            .last_emitted_total_requests
            .load(Ordering::Relaxed);

        if !force && current_total == last_emitted {
            return;
        }

        self.state
            .last_emitted_total_requests
            .store(current_total, Ordering::Relaxed);

        let Some(routes) = self.state.collector.try_snapshot_routes() else {
            return;
        };
        self.state.recorder.observe_with_reason(
            obzenflow_core::event::observation::ObservationRecord::HttpSurface {
                snapshot: HttpSurfaceMetricsSnapshot { routes },
            },
            if force {
                obzenflow_core::event::observation::CaptureReason::Final
            } else {
                obzenflow_core::event::observation::CaptureReason::Periodic
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::SystemId;

    #[test]
    fn http_status_class_buckets_expected_ranges() {
        assert_eq!(HttpStatusClass::from_status(200).as_str(), "2xx");
        assert_eq!(HttpStatusClass::from_status(302).as_str(), "3xx");
        assert_eq!(HttpStatusClass::from_status(404).as_str(), "4xx");
        assert_eq!(HttpStatusClass::from_status(503).as_str(), "5xx");
        assert_eq!(HttpStatusClass::from_status(700).as_str(), "other");
    }

    #[test]
    fn collector_accumulates_and_returns_sorted_routes() {
        let collector = HttpSurfaceMetricsCollector::new();

        collector.observe(HttpSurfaceObservation {
            surface_name: Arc::from("b"),
            method: HttpMethod::Get,
            path: Arc::from("/z"),
            status: 200,
            duration_ms: 1,
            request_bytes: 0,
            response_bytes: 1,
        });
        collector.observe(HttpSurfaceObservation {
            surface_name: Arc::from("a"),
            method: HttpMethod::Post,
            path: Arc::from("/b"),
            status: 500,
            duration_ms: 2,
            request_bytes: 10,
            response_bytes: 0,
        });
        collector.observe(HttpSurfaceObservation {
            surface_name: Arc::from("a"),
            method: HttpMethod::Get,
            path: Arc::from("/a"),
            status: 204,
            duration_ms: 3,
            request_bytes: 0,
            response_bytes: 0,
        });
        collector.observe(HttpSurfaceObservation {
            surface_name: Arc::from("a"),
            method: HttpMethod::Get,
            path: Arc::from("/a"),
            status: 204,
            duration_ms: 5,
            request_bytes: 0,
            response_bytes: 7,
        });

        assert_eq!(collector.total_requests(), 4);

        let routes = collector.try_snapshot_routes().expect("uncontended routes");
        assert_eq!(routes.len(), 3, "expected 3 distinct route keys");

        assert_eq!(routes[0].surface_name, "a");
        assert_eq!(routes[0].path, "/a");
        assert_eq!(routes[0].method, HttpMethod::Get);
        assert_eq!(routes[0].status_class, "2xx");
        assert_eq!(routes[0].requests_total, 2);
        assert_eq!(routes[0].request_duration_ms_total, 8);
        assert_eq!(routes[0].response_bytes_total, 7);

        assert_eq!(routes[1].surface_name, "a");
        assert_eq!(routes[1].path, "/b");
        assert_eq!(routes[1].method, HttpMethod::Post);
        assert_eq!(routes[1].status_class, "5xx");
        assert_eq!(routes[1].requests_total, 1);
        assert_eq!(routes[1].request_bytes_total, 10);

        assert_eq!(routes[2].surface_name, "b");
        assert_eq!(routes[2].path, "/z");
        assert_eq!(routes[2].method, HttpMethod::Get);
        assert_eq!(routes[2].status_class, "2xx");
        assert_eq!(routes[2].requests_total, 1);
    }

    #[test]
    fn emitter_skips_unchanged_samples_and_final_contention_is_optional() {
        use obzenflow_core::event::observation::{
            CaptureReason, ObservationRecord, ObservationSource,
        };
        use obzenflow_runtime::execution::{RuntimeExecution, RuntimeMode};
        let collector = Arc::new(HttpSurfaceMetricsCollector::new());
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let recorder =
            execution.observation_recorder(obzenflow_core::FlowId::new(), SystemId::new().into());
        let emitter = HttpSurfaceMetricsEmitter::new(collector.clone(), recorder);
        let observe = || {
            collector.observe(HttpSurfaceObservation {
                surface_name: Arc::from("surf"),
                method: HttpMethod::Get,
                path: Arc::from("/x"),
                status: 200,
                duration_ms: 1,
                request_bytes: 0,
                response_bytes: 0,
            })
        };
        observe();
        emitter.emit_snapshot(false);
        let first = execution.observations().snapshot();
        assert_eq!(first.len(), 1);
        emitter.emit_snapshot(false);
        assert_eq!(
            execution.observations().snapshot()[0].capture,
            first[0].capture,
            "idle suppression does not allocate another capture"
        );
        observe();
        emitter.emit_snapshot(false);
        let second = execution.observations().snapshot();
        assert!(second[0].capture.capture_seq > first[0].capture.capture_seq);
        let ObservationRecord::HttpSurface { snapshot } = &second[0].records[0] else {
            panic!("surface measurement")
        };
        assert_eq!(snapshot.routes[0].requests_total, 2);
        let held = collector.routes.lock().unwrap();
        emitter.capture_final();
        assert_eq!(
            execution.observations().snapshot()[0].capture,
            second[0].capture
        );
        drop(held);
        emitter.capture_final();
        assert_eq!(
            execution.observations().snapshot()[0]
                .capture
                .capture_reason,
            CaptureReason::Final
        );
    }
}
