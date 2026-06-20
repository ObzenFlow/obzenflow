// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-owned attachment object for HTTP-facing capabilities that are hosted by
//! `FlowApplication` but do not participate in pipeline topology.

use async_trait::async_trait;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::journal::Journal;
use obzenflow_core::web::{
    EndpointMetadata, HttpEndpoint, ManagedResponse, ManagedRouteInfo, Request, WebError,
    WebSurface,
};
use obzenflow_runtime::pipeline::PipelineState;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::error::ApplicationError;
use crate::web::endpoint_tags::SURFACE_NAME_TAG_PREFIX;

type WebSurfaceWiringFn =
    Box<dyn FnOnce(WebSurfaceWiringContext) -> Result<WebSurfaceWiring, ApplicationError> + Send>;

pub(crate) type WebSurfaceAttachmentParts = (
    String,
    Vec<Box<dyn HttpEndpoint>>,
    Option<WebSurfaceWiringFn>,
    Option<HostedIngressBindingSlot>,
);

/// Narrow, framework-owned wiring context for hosted web surfaces.
///
/// Host-owned capabilities only; this never exposes adapter carrier objects. More
/// capabilities can be added when a concrete surface requires it.
pub struct WebSurfaceWiringContext {
    pub pipeline_state: watch::Receiver<PipelineState>,
    /// FLOWIP-115d: the host system journal, so a hosted ingress surface can
    /// install its refusal-fact writer. `None` when the flow has no system
    /// journal, in which case a surface with refusal recording enabled fails
    /// startup.
    pub system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
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

/// A framework-managed attachment describing a hosted HTTP-facing capability.
///
/// Surfaces contribute a set of `HttpEndpoint`s plus an optional wiring closure that can
/// spawn background tasks after the flow has been built (e.g. readiness watchers).
///
/// # Example
/// ```ignore
/// use obzenflow_infra::application::{FlowApplication, WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext};
/// use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
/// use async_trait::async_trait;
///
/// struct Hello;
/// #[async_trait]
/// impl HttpEndpoint for Hello {
///     fn path(&self) -> &str { "/hello" }
///     fn methods(&self) -> &[HttpMethod] { &[HttpMethod::Get] }
///     async fn handle(&self, _req: Request) -> Result<ManagedResponse, WebError> {
///         Ok(Response::ok().with_text("hello").into())
///     }
/// }
///
/// let surface = WebSurfaceAttachment::new("hello", vec![Box::new(Hello)])
///     .with_wiring(|_ctx: WebSurfaceWiringContext| Ok(WebSurfaceWiring::default()));
///
/// FlowApplication::builder()
///     .with_web_surface(surface)
///     .run_blocking(flow! { /* ... */ })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct WebSurfaceAttachment {
    name: String,
    endpoints: Vec<Box<dyn HttpEndpoint>>,
    wiring: Option<WebSurfaceWiringFn>,
    ingress_slot: Option<HostedIngressBindingSlot>,
}

impl WebSurfaceAttachment {
    /// Create a new surface attachment with a name and a set of endpoints.
    pub fn new(name: impl Into<String>, endpoints: Vec<Box<dyn HttpEndpoint>>) -> Self {
        Self {
            name: name.into(),
            endpoints,
            wiring: None,
            ingress_slot: None,
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

    /// FLOWIP-115d: attach the hosted-ingress binding slot so `FlowApplication`
    /// can verify it was filled (its source half was placed in flow topology)
    /// before serving endpoints.
    pub fn with_ingress_slot(mut self, slot: HostedIngressBindingSlot) -> Self {
        self.ingress_slot = Some(slot);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn into_parts(self) -> WebSurfaceAttachmentParts {
        (self.name, self.endpoints, self.wiring, self.ingress_slot)
    }
}

impl From<WebSurface> for WebSurfaceAttachment {
    fn from(surface: WebSurface) -> Self {
        let (name, endpoints) = surface.into_endpoints();
        Self::new(name, endpoints)
    }
}

pub(crate) fn label_endpoint(
    surface_name: &str,
    endpoint: Box<dyn HttpEndpoint>,
) -> Box<dyn HttpEndpoint> {
    Box::new(SurfaceTaggedEndpoint {
        surface_name: surface_name.to_string(),
        inner: endpoint,
    })
}

struct SurfaceTaggedEndpoint {
    surface_name: String,
    inner: Box<dyn HttpEndpoint>,
}

#[async_trait]
impl HttpEndpoint for SurfaceTaggedEndpoint {
    fn path(&self) -> &str {
        self.inner.path()
    }

    fn methods(&self) -> &[obzenflow_core::web::HttpMethod] {
        self.inner.methods()
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        self.inner.handle(request).await
    }

    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    fn metadata(&self) -> Option<EndpointMetadata> {
        let mut meta = self
            .inner
            .metadata()
            .unwrap_or_else(|| EndpointMetadata::new(self.inner.path().to_string()));
        meta.tags
            .push(format!("{SURFACE_NAME_TAG_PREFIX}{}", self.surface_name));
        Some(meta)
    }

    fn managed_route(&self) -> Option<ManagedRouteInfo> {
        self.inner.managed_route()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn web_surface_attachment_wiring_receives_pipeline_state_and_returns_tasks() {
        let (tx, rx) = watch::channel(PipelineState::Created);

        let surface = WebSurfaceAttachment::new("test", vec![]).with_wiring(
            |ctx: WebSurfaceWiringContext| {
                assert!(matches!(
                    *ctx.pipeline_state.borrow(),
                    PipelineState::Created
                ));
                let mut pipeline_state = ctx.pipeline_state;
                let task = tokio::spawn(async move {
                    let _ = pipeline_state.changed().await;
                });
                Ok(WebSurfaceWiring::new(vec![task]))
            },
        );

        let (_name, endpoints, wiring, _ingress_slot) = surface.into_parts();
        assert!(endpoints.is_empty());
        let wiring = wiring.expect("wiring should exist");

        let wired = wiring(WebSurfaceWiringContext {
            pipeline_state: rx,
            system_journal: None,
        })
        .unwrap();
        assert_eq!(wired.tasks.len(), 1);

        let _ = tx.send(PipelineState::Running);
    }
}
