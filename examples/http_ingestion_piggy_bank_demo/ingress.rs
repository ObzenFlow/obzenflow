// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Web-ingress wiring helpers for the piggy bank demo.
//!
//! Keep this separate from `flow.rs` so the flow definition stays focused on topology and
//! domain logic rather than HTTP hosting/plumbing.

use obzenflow_adapters::sources::http::{HttpSource, HttpSourceTyped};
use obzenflow_core::TypedPayload;
use obzenflow_infra::application::WebSurfaceAttachment;
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_surface, IngestionConfig, TypedValidator, ValidationConfig,
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
    let config = IngestionConfig {
        base_path: base_path.into(),
        validation: Some(ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<T>::new()),
        }),
        ..Default::default()
    };

    let (surface, rx, telemetry) = create_ingestion_surface(config);
    let source = HttpSource::with_telemetry(rx, telemetry).typed::<T>();

    TypedIngress { surface, source }
}
