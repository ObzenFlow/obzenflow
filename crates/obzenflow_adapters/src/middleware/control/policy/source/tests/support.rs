// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use super::super::retry::AttemptDisposition;
use crate::middleware::control::circuit_breaker::{
    CircuitBreakerMiddleware, CircuitBreakerSourcePolicy,
};
use crate::middleware::BoundaryRetryOwner;
use async_trait::async_trait;
use crate::middleware::{
    BoundaryRetryPolicy, MiddlewareAttachmentId, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareDeclarationIndex, MiddlewareOrigin, MiddlewareSurface,
    MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId, SourcePollSurface, SourcePollUnitId,
};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RetryEvent, RetryExhaustionCause, RetryInvocation,
    RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{ChainEventContent, ChainEventFactory};
use obzenflow_core::{ChainEvent, MiddlewareContextKey, StageId, Ulid, WriterId};
use obzenflow_runtime::prelude::SourceError;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::source::{
    SourceBoundary, SourceBoundaryOutcome, SourcePollCompletion, SourcePollExecution,
    SourcePollExecutor, SourcePollReport,
};
use serde_json::json;
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

struct RecordingPolicy {
    label: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SourcePolicy for RecordingPolicy {
    fn label(&self) -> &'static str {
        self.label
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        self.log
            .lock()
            .unwrap()
            .push(format!("admit:{}", self.label));
        SourceAdmission::Admit(None)
    }

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        self.log
            .lock()
            .unwrap()
            .push(format!("after:{}", self.label));
        SourceAfterPoll::Proceed
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        self.log
            .lock()
            .unwrap()
            .push(format!("observe:{}", self.label));
    }
}

fn test_event() -> ChainEvent {
    ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.source",
        json!({ "value": 1 }),
    )
}

fn source_retry_owner(
    max_attempts: u32,
    delay: Duration,
    max_single_delay: Duration,
) -> BoundaryRetryOwner {
    let stage_id = StageId::from_ulid(Ulid::from(0x115_0001_u128));
    let declaration = MiddlewareDeclaration::control(
        "test.source_retry_owner",
        vec![MiddlewareSurfaceKind::SourcePoll],
    );
    let surface = MiddlewareSurface::SourcePoll(SourcePollSurface { stage_id });
    let protected_unit = ProtectedUnitId {
        stage_id,
        unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin: &origin,
        declaration_index: MiddlewareDeclarationIndex::resolved(0),
    };
    BoundaryRetryOwner {
        attachment_id: MiddlewareAttachmentId::from_declaration_and_request(
            &declaration,
            &request,
        ),
        stage_id,
        writer_id: WriterId::from(stage_id),
        protected_unit_label: "orders_source".to_string(),
        sink_configured_target_id: None,
        policy: BoundaryRetryPolicy {
            max_attempts: NonZeroU32::new(max_attempts).expect("test attempts are non-zero"),
            backoff: BackoffStrategy::Fixed { delay },
            max_single_delay,
            max_total_wall_time: Duration::from_secs(30),
        },
    }
}

fn retry_event(event: &ChainEvent) -> &RetryEvent {
    match &event.content {
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::Retry(retry),
        )) => retry,
        other => panic!("expected retry lifecycle row, got {other:?}"),
    }
}

#[test]
fn source_error_disposition_matrix_is_exhaustive_and_preserves_retry_hints() {
    let retry_after = Duration::from_millis(725);
    let cases = [
        (
            SourceError::Timeout("timeout".to_string()),
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Timeout,
                retry_after: None,
            },
        ),
        (
            SourceError::Transport("transport".to_string()),
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Remote,
                retry_after: None,
            },
        ),
        (
            SourceError::RateLimited {
                message: "rate limited with hint".to_string(),
                retry_after: Some(retry_after),
            },
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::RateLimited,
                retry_after: Some(retry_after),
            },
        ),
        (
            SourceError::RateLimited {
                message: "rate limited without hint".to_string(),
                retry_after: None,
            },
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::RateLimited,
                retry_after: None,
            },
        ),
        (
            SourceError::Deserialization("malformed".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Deserialization,
            },
        ),
        (
            SourceError::PermanentFailure("permanent".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::PermanentFailure,
            },
        ),
        (
            SourceError::Other("unknown".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
    ];

    for (error, expected) in cases {
        let report = SourcePollReport {
            result: Err(error),
            poll_duration: Duration::ZERO,
        };
        assert_eq!(source_attempt_disposition(&report), expected);
    }

    for completion in [
        SourcePollCompletion::Batch(Vec::new()),
        SourcePollCompletion::Eof,
    ] {
        let report = SourcePollReport {
            result: Ok(completion),
            poll_duration: Duration::ZERO,
        };
        assert_eq!(
            source_attempt_disposition(&report),
            AttemptDisposition::Completed
        );
    }
}

