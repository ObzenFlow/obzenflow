// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Serves Studio updates at `GET /api/flow/events`.
//!
//! Each connection gets its own journal reader and `StudioProjection`.
//! `stream.rs` handles reading and reconnects; the Studio adapters build the
//! messages sent to the browser.

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
    journals: Vec<obzenflow_runtime::supervised_base::SupervisorJournal>,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    observation_interval: std::time::Duration,
}

impl StudioUpdatesEndpoint {
    pub(crate) fn new(
        journal: Arc<dyn Journal<SystemEvent>>,
        projection: StudioProjection,
        runtime_instance_id: Option<RuntimeInstanceId>,
        closing: watch::Receiver<bool>,
    ) -> Self {
        Self {
            journals: vec![journal.into()],
            projection,
            runtime_instance_id,
            closing,
            observation_interval: std::time::Duration::from_millis(
                obzenflow_runtime::runtime_config::schema::DEFAULT_OBSERVATION_EXPORT_INTERVAL_MS,
            ),
        }
    }

    pub(crate) fn with_report_journals(
        mut self,
        journals: Vec<obzenflow_runtime::supervised_base::SupervisorJournal>,
    ) -> Self {
        self.journals = journals;
        self
    }

    pub(crate) fn with_observation_interval(mut self, interval: std::time::Duration) -> Self {
        self.observation_interval = interval;
        self
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

    // Omitting managed_route() retains the host's authentication policy for built-ins.
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
            self.journals.clone(),
            self.projection.clone(),
            self.runtime_instance_id.clone(),
            self.closing.clone(),
            cursor,
            self.observation_interval,
        ))))
    }
}

#[cfg(test)]
pub(crate) mod tests;
