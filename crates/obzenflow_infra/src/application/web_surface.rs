// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-owned attachment object for HTTP-facing capabilities that are hosted by
//! `FlowApplication` but do not participate in pipeline topology.

use obzenflow_core::web::HttpEndpoint;
use obzenflow_runtime::pipeline::fsm::PipelineState;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::error::ApplicationError;

pub struct WebSurfaceWiringContext {
    pub pipeline_state: watch::Receiver<PipelineState>,
}

#[derive(Default)]
pub struct WebSurfaceWiring {
    pub tasks: Vec<JoinHandle<()>>,
}

impl WebSurfaceWiring {
    pub fn new(tasks: Vec<JoinHandle<()>>) -> Self {
        Self { tasks }
    }
}

pub struct WebSurfaceAttachment {
    name: String,
    endpoints: Vec<Box<dyn HttpEndpoint>>,
    wiring: Option<
        Box<
            dyn FnOnce(WebSurfaceWiringContext) -> Result<WebSurfaceWiring, ApplicationError>
                + Send,
        >,
    >,
}

impl WebSurfaceAttachment {
    pub fn new(name: impl Into<String>, endpoints: Vec<Box<dyn HttpEndpoint>>) -> Self {
        Self {
            name: name.into(),
            endpoints,
            wiring: None,
        }
    }

    pub fn with_wiring<F>(mut self, wiring: F) -> Self
    where
        F: FnOnce(WebSurfaceWiringContext) -> Result<WebSurfaceWiring, ApplicationError>
            + Send
            + 'static,
    {
        self.wiring = Some(Box::new(wiring));
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Vec<Box<dyn HttpEndpoint>>,
        Option<
            Box<
                dyn FnOnce(WebSurfaceWiringContext) -> Result<WebSurfaceWiring, ApplicationError>
                    + Send,
            >,
        >,
    ) {
        (self.name, self.endpoints, self.wiring)
    }
}

