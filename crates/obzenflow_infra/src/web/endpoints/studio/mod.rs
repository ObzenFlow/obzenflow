// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The Studio lifecycle stream, hosted through the portable HTTP endpoint contract.

mod stream;
#[cfg(feature = "warp-server")]
pub(crate) mod topology;

use async_trait::async_trait;
use obzenflow_adapters::studio::StudioProjection;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::web::{
    EndpointError, HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, SseBody,
};
use std::sync::Arc;
use tokio::sync::watch;

use crate::web::RuntimeInstanceId;

pub(crate) struct StudioUpdatesEndpoint {
    journal: Arc<dyn Journal<SystemEvent>>,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
}

impl StudioUpdatesEndpoint {
    pub(crate) fn new(
        journal: Arc<dyn Journal<SystemEvent>>,
        projection: StudioProjection,
        runtime_instance_id: Option<RuntimeInstanceId>,
        closing: watch::Receiver<bool>,
    ) -> Self {
        Self {
            journal,
            projection,
            runtime_instance_id,
            closing,
        }
    }
}

#[async_trait]
impl HttpEndpoint for StudioUpdatesEndpoint {
    fn path(&self) -> &str {
        "/api/flow/events"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    // Leave managed_route() absent, just like topology and metrics: the common
    // dispatcher applies the host's existing built-in control-plane policy.
    async fn handle(&self, request: Request) -> Result<ManagedResponse, EndpointError> {
        if *self.closing.borrow() {
            return Ok(Response::new(204).into());
        }
        let cursor = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("last-event-id"))
            .map(|(_, value)| value.as_str());
        Ok(ManagedResponse::Sse(SseBody::new(stream::connection(
            self.journal.clone(),
            self.projection.clone(),
            self.runtime_instance_id.clone(),
            self.closing.clone(),
            cursor,
        ))))
    }
}

#[cfg(all(test, feature = "warp-server"))]
mod contract_tests;
#[cfg(test)]
pub(crate) mod tests;
