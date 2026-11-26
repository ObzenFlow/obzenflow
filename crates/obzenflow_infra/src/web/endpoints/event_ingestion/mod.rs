// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! `event_ingestion` holds hosted HTTP endpoints and a protocol-neutral ingress handle.
//!
//! [`crate::application::HostedIngress::http`] registers an HTTP endpoint that a
//! `subscribe` line reads. [`ingress_source`] creates an [`Ingress`] for an
//! application-owned protocol.
//!
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

use async_trait::async_trait;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::ingress::{
    HostedIngressBindingSlot, IngressAttemptContext, IngressRequestId, OUTCOME_UNKNOWN_RETRY_AFTER,
};
use obzenflow_core::web::HttpEndpoint;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::events::{Arrival, Events, SourceReceiver};
use obzenflow_runtime::execution::SourceExecutionPhases;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handlers::SourceError;
use settlement::EvidenceState;
use shared::{admit_typed, AdmittedRequest, EdgeRefusal};
use std::marker::PhantomData;
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
    state: IngestionState,
    payload: PhantomData<fn(T)>,
}

impl<T> Clone for IngressHandle<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            payload: PhantomData,
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
        let Some(decoder) = self.state.decoder() else {
            let refused = self
                .state
                .refuse_not_ready(&attempt, request_id.as_ref())
                .await;
            return refused_outcome(refused);
        };
        let admitted = match admit_typed(decoder.as_ref(), payload, request_id.as_ref()) {
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

    pub(crate) fn state(&self) -> IngestionState {
        self.state.clone()
    }
}

impl IngestionState {
    pub(crate) fn bind_flow_handle(
        &self,
        flow_handle: &Arc<FlowHandle>,
    ) -> Result<JoinHandle<()>, ApplicationError> {
        let slot = self.ingress_slot();
        if !slot.is_filled() {
            return Err(unbound_ingress(&slot));
        }
        install_evidence_writer(self, flow_handle.ingress_evidence(), "handle")?;
        install_source_phases(self, flow_handle.source_phases(), "handle")?;
        Ok(self.watch_pipeline_state(flow_handle.state_receiver()))
    }
}

pub(crate) fn unbound_ingress(slot: &HostedIngressBindingSlot) -> ApplicationError {
    ApplicationError::FlowBuildFailed(format!(
        "hosted ingress '{}' was registered but no subscribe line binds its port; bind it with \
         subscribe(…).through(port) in flow!",
        slot.ingress_key()
    ))
}

fn refused_outcome(edge: EdgeRefusal) -> IngressOutcome {
    IngressOutcome::Refused {
        code: edge.code(),
        retry_after: Some(edge.retry_after()),
    }
}

/// Install the host system journal's evidence writer, or fail startup when
/// recording is enabled and the flow has no system journal.
fn install_evidence_writer(
    state: &IngestionState,
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

fn install_source_phases(
    state: &IngestionState,
    phases: Option<SourceExecutionPhases>,
    kind: &str,
) -> Result<(), ApplicationError> {
    match phases {
        Some(phases) => {
            state.install_source_phases(phases);
            Ok(())
        }
        None => Err(ApplicationError::FlowBuildFailed(format!(
            "hosted ingress {kind} '{}' cannot read the execution phase of its source, because the \
             flow has no runtime execution. Use flow! to build the flow.",
            state.ingress_key()
        ))),
    }
}

/// `Ingress` is an ingress for an application-owned protocol. It holds the event
/// system that a `subscribe` line reads and the handle that submits each value.
pub struct Ingress<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    events: IngressEvents,
    handle: IngressHandle<T>,
}

impl<T> Ingress<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    /// `handle` returns a clone of the submission handle.
    pub fn handle(&self) -> IngressHandle<T> {
        self.handle.clone()
    }

    pub(crate) fn into_parts(self) -> (IngressEvents, IngressHandle<T>) {
        (self.events, self.handle)
    }
}

/// `ingress_source` creates an [`Ingress`] of `T` for an application-owned protocol.
/// [`crate::application::FlowApplicationBuilder::ingress`] registers it. The
/// [`IngressHandle`] runs the decoder of the `subscribe` line on each value that it submits.
pub fn ingress_source<T>(config: IngestionConfig) -> Ingress<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    let (state, rx) = IngestionState::new(config);
    Ingress {
        events: IngressEvents {
            rx,
            slot: state.ingress_slot(),
        },
        handle: IngressHandle {
            state,
            payload: PhantomData,
        },
    }
}

pub(crate) struct IngressEvents {
    rx: SourceReceiver<Arrival>,
    slot: HostedIngressBindingSlot,
}

#[async_trait]
impl Events for IngressEvents {
    async fn ready(&self) -> Result<(), SourceError> {
        self.rx
            .ready()
            .await
            .map_err(|_| SourceError::Transport(SourceDiagnosticReason::InputClosed.into()))
    }

    fn take(&self) -> Option<Arrival> {
        self.rx.take(1).pop()
    }

    fn hosted_ingress(&self) -> Option<HostedIngressBindingSlot> {
        Some(self.slot.clone())
    }
}

pub(crate) fn http_endpoint(
    config: IngestionConfig,
) -> (IngressEvents, IngestionState, WebSurfaceAttachment) {
    let (state, rx) = IngestionState::new(config);
    let events = IngressEvents {
        rx,
        slot: state.ingress_slot(),
    };
    let surface = create_ingestion_surface(state.clone());
    (events, state, surface)
}

fn create_ingestion_surface(state: IngestionState) -> WebSurfaceAttachment {
    let ingress_slot = state.ingress_slot();
    let base_path = state.config.base_path.clone();

    let state_for_wiring = state.clone();
    let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
        Box::new(SingleEventEndpoint::new(state.clone())),
        Box::new(BatchEventEndpoint::new(state.clone())),
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
            install_source_phases(&state_for_wiring, ctx.source_phases, "surface")?;
            let task = state_for_wiring.watch_pipeline_state(ctx.pipeline_state);
            Ok(WebSurfaceWiring::new(vec![task]))
        })
}
