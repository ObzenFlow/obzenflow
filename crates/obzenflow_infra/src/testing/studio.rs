// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development fixtures enter the real Studio endpoint through its HTTP port.
//! No alternate reader, projection or settlement algorithm lives here.

use crate::web::endpoints::studio::StudioUpdatesEndpoint;
use obzenflow_adapters::studio::StudioProjection;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, SseBody};
use obzenflow_core::{ChainEvent, Journal, StageId};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

pub async fn connect(
    stages: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    systems: Vec<Arc<dyn Journal<SystemEvent>>>,
    projection: StudioProjection,
    closing: watch::Receiver<bool>,
    cursor: Option<&str>,
    observation_interval: Duration,
) -> SseBody {
    let endpoint = StudioUpdatesEndpoint::new(systems[0].clone(), projection, None, closing)
        .with_live_journals(stages, systems)
        .with_observation_interval(observation_interval);
    let mut request = Request::new(HttpMethod::Get, "/api/flow/events".into());
    if let Some(cursor) = cursor {
        request = request.with_header("last-event-id".into(), cursor.into());
    }
    let ManagedResponse::Sse(body) = endpoint.handle(request).await.expect("Studio endpoint")
    else {
        panic!("Studio fixture must connect before closing");
    };
    body
}
