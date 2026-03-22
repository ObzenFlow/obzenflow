// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Web-ingress wiring helpers for the piggy bank demo.
//!
//! Keep this separate from `flow.rs` so the flow definition stays focused on topology and
//! domain logic rather than HTTP hosting/plumbing.

use obzenflow_adapters::sources::http::{HttpSource, HttpSourceTyped};
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Route, WebSurface};
use obzenflow_core::TypedPayload;
use obzenflow_infra::application::{
    WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext,
};
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, IngestionConfig, TypedValidator, ValidationConfig,
};
use std::sync::Arc;

pub const ACCOUNTS_BASE_PATH: &str = "/api/bank/accounts";
pub const TX_BASE_PATH: &str = "/api/bank/tx";

pub struct TypedIngress<T>
where
    T: TypedPayload + Send + Sync + std::fmt::Debug + 'static,
{
    pub surface: WebSurfaceAttachment,
    pub source: HttpSourceTyped<T>,
}

pub fn typed_ingress<T>(base_path: impl Into<String>) -> TypedIngress<T>
where
    T: TypedPayload + Send + Sync + std::fmt::Debug + 'static,
{
    fn find_endpoint(
        endpoints: &[Arc<dyn HttpEndpoint>],
        method: HttpMethod,
        path: &str,
    ) -> Arc<dyn HttpEndpoint> {
        endpoints
            .iter()
            .find(|ep| ep.path() == path && ep.methods().contains(&method))
            .cloned()
            .unwrap_or_else(|| panic!("ingestion endpoint not found: {method:?} {path}"))
    }

    let config = IngestionConfig {
        base_path: base_path.into(),
        validation: Some(ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<T>::new()),
        }),
        ..Default::default()
    };

    let (endpoints, rx, state) = create_ingestion_endpoints(config);
    let telemetry = state.telemetry();
    let base_path = state.config.base_path.clone();

    let endpoints: Vec<Arc<dyn HttpEndpoint>> = endpoints.into_iter().map(Arc::from).collect();
    let events = find_endpoint(&endpoints, HttpMethod::Post, &format!("{base_path}/events"));
    let batch = find_endpoint(&endpoints, HttpMethod::Post, &format!("{base_path}/batch"));
    let health = find_endpoint(&endpoints, HttpMethod::Get, &format!("{base_path}/health"));

    let surface = WebSurface::resource(format!("ingestion:{base_path}"))
        .base_path(base_path.clone())
        .route(Route::post("/events").endpoint(events))
        .route(Route::post("/batch").endpoint(batch))
        .route(Route::get("/health").endpoint(health));

    let state_for_wiring = state.clone();
    let surface =
        WebSurfaceAttachment::from(surface).with_wiring(move |ctx: WebSurfaceWiringContext| {
            let task = state_for_wiring.watch_pipeline_state(ctx.pipeline_state);
            Ok(WebSurfaceWiring::new(vec![task]))
        });

    let source = HttpSource::with_telemetry(rx, telemetry).typed::<T>();

    TypedIngress { surface, source }
}
