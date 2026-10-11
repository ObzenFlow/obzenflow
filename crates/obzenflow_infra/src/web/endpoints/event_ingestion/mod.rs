// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle-first ingress plus optional HTTP event ingestion endpoints
//!
//! Applications use [`crate::application::FlowApplicationBuilder::http_ingress`]
//! to register HTTP ingress and receive its source for the flow. The builder owns
//! hosting and lifecycle wiring. For application-owned protocols, clone a handle
//! from [`ingress_source`] and pass the bundle to
//! [`crate::application::FlowApplicationBuilder::ingress`].
//! HTTP ingress provides the hosted routes:
//! - `POST {base_path}/events` (single event)
//! - `POST {base_path}/batch`  (batch)
//! - `GET  {base_path}/health` (ingestion readiness/backpressure)
//!
//! FLOWIP-084l: a success answer follows the member's own committed row. An
//! optional `Idempotency-Key` makes a retry resolve to that row instead of
//! admitting another copy.

mod auth;
mod batch_event;
mod contract;
mod health;
mod http;
mod settlement;
mod shared;
mod single_event;
mod validation;

#[cfg(test)]
mod tests;

pub use auth::{authorize_request, AuthConfig, AuthError};
pub use contract::{
    IngressErrorBody, IngressErrorCode, IngressOutcome, SubmissionMemberOutcome,
    SubmissionMemberResult, SubmissionResponse,
};
pub use shared::{IngestionConfig, IngestionState};
pub use validation::{
    validate_submission, SchemaValidator, TypedValidator, ValidationConfig, ValidationError,
};

pub use obzenflow_adapters::sources::http::{
    HostedIngressSource, IngressDecodeError, IngressDecoder, IngressRecord,
};
use obzenflow_core::ingress::{
    HostedIngressBindingSlot, IngressAttemptContext, IngressRequestId, OUTCOME_UNKNOWN_RETRY_AFTER,
};
use obzenflow_core::web::HttpEndpoint;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::prelude::FlowHandle;
use settlement::EvidenceState;
use shared::{admit_typed, AdmittedRequest, EdgeRefusal};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::application::{
    ApplicationError, WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext,
};

use batch_event::BatchEventEndpoint;
use health::IngestionHealthEndpoint;
use single_event::SingleEventEndpoint;

/// Cloneable developer-owned ingress sender.
pub struct IngressHandle<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    state: IngestionState<T>,
}

impl<T> Clone for IngressHandle<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> IngressHandle<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    /// Submit one typed value. The answer follows its committed row; without
    /// a request ID there is no deduplication promise.
    pub async fn submit(&self, payload: T) -> IngressOutcome {
        self.submit_with(None, payload).await
    }

    /// Submit one typed value under a stable request ID, so a retry of the
    /// same value resolves to the original row (FLOWIP-084l). The identity is
    /// the value's serde form; build it deterministically from your request.
    pub async fn submit_identified(&self, id: IngressRequestId, payload: T) -> IngressOutcome {
        self.submit_with(Some(id), payload).await
    }

    async fn submit_with(
        &self,
        request_id: Option<IngressRequestId>,
        payload: T,
    ) -> IngressOutcome {
        let started = Instant::now();
        let attempt = IngressAttemptContext {
            attempt_seq: self.state.next_attempt_seq(),
            request_count: 1,
            event_count: 1,
            batch_count: 0,
        };
        if !self.state.is_ready() {
            let refused = self
                .state
                .refuse_not_ready(&attempt, request_id.as_ref())
                .await;
            return refused_outcome(refused);
        }
        let admitted = match admit_typed(payload, request_id.as_ref()) {
            Ok(admitted) => admitted,
            Err(refusal) => {
                let recorded = self
                    .state
                    .record_validation_refusals(
                        attempt.attempt_seq,
                        false,
                        request_id.as_ref(),
                        &[(0, refusal)],
                    )
                    .await;
                return match recorded {
                    Ok(()) => IngressOutcome::Refused {
                        code: IngressErrorCode::ValidationFailed,
                        retry_after: None,
                    },
                    Err(_) => refused_outcome(EdgeRefusal::EvidenceUnavailable),
                };
            }
        };
        let request = AdmittedRequest {
            attempt,
            request_id: request_id.clone(),
            members: vec![(0, None, admitted)],
        };
        let settlement = match self.state.enqueue(request).await {
            Ok(settlement) => settlement,
            Err(edge) => return refused_outcome(edge),
        };
        let snapshot = settlement
            .wait_until(started + self.state.ack_deadline())
            .await;
        if snapshot.evidence == EvidenceState::Unavailable {
            return refused_outcome(EdgeRefusal::EvidenceUnavailable);
        }
        let (_, settled) = snapshot.members[0];
        match snapshot.outcome(settled, request_id.as_ref()) {
            SubmissionMemberOutcome::Committed { event_id } => {
                IngressOutcome::Committed { event_id }
            }
            SubmissionMemberOutcome::Duplicate { event_id } => {
                IngressOutcome::Duplicate { event_id }
            }
            SubmissionMemberOutcome::Rejected { code, .. } => IngressOutcome::Refused {
                code,
                retry_after: None,
            },
            SubmissionMemberOutcome::Unknown { .. } => IngressOutcome::Unknown {
                retry_after: OUTCOME_UNKNOWN_RETRY_AFTER,
            },
        }
    }

    /// Protocol-neutral ingress key used in provenance and attempt evidence.
    pub fn ingress_key(&self) -> &str {
        self.state.ingress_key().as_str()
    }

    pub(crate) fn state(&self) -> IngestionState<T> {
        self.state.clone()
    }

    pub(crate) fn ingress_slot(&self) -> HostedIngressBindingSlot {
        self.state.ingress_slot()
    }

    pub(crate) fn bind_flow_handle(
        &self,
        flow_handle: &Arc<FlowHandle>,
    ) -> Result<JoinHandle<()>, ApplicationError> {
        let slot = self.state.ingress_slot();
        if !slot.is_filled() {
            return Err(ApplicationError::FlowBuildFailed(format!(
                "hosted ingress handle '{}' was registered but its source half was not placed in \
                 the flow topology; place the ingress source in flow!",
                slot.ingress_key()
            )));
        }
        install_evidence_writer(&self.state, flow_handle.ingress_evidence(), "handle")?;
        Ok(self
            .state
            .watch_pipeline_state(flow_handle.state_receiver()))
    }
}

fn refused_outcome(edge: EdgeRefusal) -> IngressOutcome {
    IngressOutcome::Refused {
        code: edge.code(),
        retry_after: Some(edge.retry_after()),
    }
}

/// Install the host system journal's evidence writer, or fail startup when
/// recording is enabled and the flow has no system journal.
fn install_evidence_writer<T>(
    state: &IngestionState<T>,
    writer: Option<obzenflow_runtime::pipeline::ingress::IngressEvidenceWriter>,
    kind: &str,
) -> Result<(), ApplicationError> {
    if !state.evidence_recording_enabled() {
        return Ok(());
    }
    match writer {
        Some(writer) => {
            state.install_evidence_writer(writer);
            Ok(())
        }
        None => Err(ApplicationError::FlowBuildFailed(format!(
            "hosted ingress {kind} '{}' has evidence recording enabled but no system journal is \
             available; disable record_ingress_evidence or run with a system journal",
            state.ingress_key()
        ))),
    }
}

/// Typed source plus ingress handle for developer-owned ingress protocols.
pub struct Ingress<D>
where
    D: IngressDecoder,
{
    source: HostedIngressSource<D>,
    handle: IngressHandle<D::Output>,
    /// Maps protocol bodies for a hosted HTTP wrapper; typed handle
    /// submissions bypass it.
    decoder: D,
}

impl<D> Ingress<D>
where
    D: IngressDecoder,
{
    /// Consume the bundle into its source and cloneable submission handle.
    ///
    /// The receiver transfers once. Use topology fan-out for multiple consumers.
    /// Splitting preserves the queue and ingress binding without starting execution.
    ///
    /// ```compile_fail,E0382
    /// use obzenflow_infra::web::endpoints::event_ingestion::{Ingress, IngressDecoder};
    ///
    /// fn transfer_twice<D: IngressDecoder>(ingress: Ingress<D>) {
    ///     let (source, handle) = ingress.into_parts();
    ///     let (another_source, _) = ingress.into_parts(); // ingress was consumed
    /// }
    /// ```
    pub fn into_parts(self) -> (HostedIngressSource<D>, IngressHandle<D::Output>) {
        (self.source, self.handle)
    }

    pub fn handle(&self) -> IngressHandle<D::Output> {
        self.handle.clone()
    }
}

/// Framework-owned HTTP ingress bundle for a single typed payload.
pub struct HttpIngress<D>
where
    D: IngressDecoder,
{
    source: HostedIngressSource<D>,
    attachment: HttpIngressAttachment<D::Output>,
}

/// HTTP hosting and submission half returned by [`HttpIngress::into_parts`].
///
/// Pass this attachment to [`crate::application::FlowApplicationBuilder::with_http_ingress`]
/// and place the source half in the flow topology. Submission handles may be cloned;
/// the attachment retains ownership of its HTTP surface.
pub struct HttpIngressAttachment<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    surface: WebSurfaceAttachment,
    handle: IngressHandle<T>,
}

impl<D> HttpIngress<D>
where
    D: IngressDecoder,
{
    /// Consume the bundle into its source and HTTP hosting attachment.
    ///
    /// The receiver transfers once. Use topology fan-out for multiple consumers.
    /// Splitting preserves the queue and ingress binding without starting execution.
    ///
    /// ```compile_fail,E0382
    /// use obzenflow_infra::web::endpoints::event_ingestion::{HttpIngress, IngressDecoder};
    ///
    /// fn transfer_twice<D: IngressDecoder>(ingress: HttpIngress<D>) {
    ///     let (source, http) = ingress.into_parts();
    ///     let (another_source, _) = ingress.into_parts(); // ingress was consumed
    /// }
    /// ```
    pub fn into_parts(self) -> (HostedIngressSource<D>, HttpIngressAttachment<D::Output>) {
        (self.source, self.attachment)
    }

    /// Clone the underlying handle used by the hosted HTTP adaptor.
    pub fn handle(&self) -> IngressHandle<D::Output> {
        self.attachment.handle()
    }
}

impl<T> HttpIngressAttachment<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    /// Clone the underlying handle used by the hosted HTTP adaptor.
    pub fn handle(&self) -> IngressHandle<T> {
        self.handle.clone()
    }

    /// Consume the HTTP attachment and return its managed web surface.
    pub fn into_surface(self) -> WebSurfaceAttachment {
        self.surface
    }

    pub(crate) fn into_surface_and_handle(self) -> (WebSurfaceAttachment, IngressHandle<T>) {
        (self.surface, self.handle)
    }
}

/// Create a typed source plus protocol-neutral ingress handle.
///
/// The decoder value owns the source's output type. Clone the bundle's handle for
/// submission, then pass the bundle to [`crate::application::FlowApplicationBuilder::ingress`]
/// to register its lifecycle and receive the source.
pub fn ingress_source<D>(decoder: D, config: IngestionConfig) -> Ingress<D>
where
    D: IngressDecoder,
{
    let (state, rx) = IngestionState::new(config);
    let source = HostedIngressSource::new(rx, state.ingress_slot());
    Ingress {
        source,
        handle: IngressHandle { state },
        decoder,
    }
}

/// Create a zero-wiring HTTP ingress bundle.
///
/// Prefer [`crate::application::FlowApplicationBuilder::http_ingress`] for application
/// hosting. This bundle supports explicit source/attachment composition. The
/// decoder runs once per submission at admission; a configured validator adds
/// to it and never replaces it.
pub fn http_ingress<D>(decoder: D, config: IngestionConfig) -> HttpIngress<D>
where
    D: IngressDecoder,
{
    let Ingress {
        source,
        handle,
        decoder,
    } = ingress_source(decoder, config);
    let surface = create_ingestion_surface(handle.state(), decoder);
    HttpIngress {
        source,
        attachment: HttpIngressAttachment { surface, handle },
    }
}

fn create_ingestion_surface<D>(state: IngestionState<D::Output>, decoder: D) -> WebSurfaceAttachment
where
    D: IngressDecoder,
{
    let ingress_slot = state.ingress_slot();
    let base_path = state.config.base_path.clone();

    let state_for_wiring = state.clone();
    let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
        Box::new(SingleEventEndpoint::new(state.clone(), decoder.clone())),
        Box::new(BatchEventEndpoint::new(state.clone(), decoder)),
        Box::new(IngestionHealthEndpoint::new(state)),
    ];

    WebSurfaceAttachment::new(format!("ingestion:{base_path}"), endpoints)
        .with_ingress_slot(ingress_slot)
        .with_wiring(move |ctx: WebSurfaceWiringContext| {
            // FLOWIP-115d: install the evidence writer from the host system
            // journal. Recording is on by default; a surface that wants it but
            // has no system journal fails startup rather than silently dropping
            // replayable evidence.
            install_evidence_writer(&state_for_wiring, ctx.ingress_evidence, "surface")?;
            let task = state_for_wiring.watch_pipeline_state(ctx.pipeline_state);
            Ok(WebSurfaceWiring::new(vec![task]))
        })
}
