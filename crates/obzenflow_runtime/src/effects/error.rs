// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::binding::EffectPortSlotLabel;
use super::*;
use obzenflow_core::event::status::processing_status::ErrorKind;

#[derive(Debug, Clone, Error)]
pub enum EffectError {
    #[error("{fault}")]
    BindingAuthority { fault: BindingAuthorityFault },

    #[error("effect serialization failed: {0}")]
    Serialization(String),

    #[error("effect journal write failed: {0}")]
    Journal(String),

    #[error("missing recorded effect for cursor {cursor:?}")]
    MissingRecordedEffect { cursor: EffectCursor },

    /// A resumed stage authored an action other than the next one its archive
    /// recorded; `detail` names the archived action.
    #[error("{detail}")]
    RecordingDeparture { detail: String },

    /// A replay reached a cursor its recording holds no outcome for, or an
    /// attempt it left in doubt; a replay never executes an effect, so only a
    /// resume finishes this work (FLOWIP-134i B13).
    #[error(
        "replay reached the end of its recording at stage '{}', input {}; a replay never executes an effect",
        cursor.stage_key,
        cursor.input_seq
    )]
    RecordingEnded { cursor: EffectCursor },

    #[error("duplicate recorded effect for cursor {cursor:?}")]
    DuplicateRecordedEffect { cursor: EffectCursor },

    #[error("{}", descriptor_mismatch_message(cursor, expected, recorded, drift.as_deref()))]
    DescriptorMismatch {
        cursor: EffectCursor,
        expected: EffectDescriptorHash,
        recorded: EffectDescriptorHash,
        drift: Option<Box<DescriptorDrift>>,
    },

    #[error("recorded effect failure: {error_type}: {error_message}")]
    RecordedFailure {
        error_type: EffectFailureKind,
        error_message: String,
        retry: RetryDisposition,
        cause: Option<EffectFailureCause>,
        detail: Option<Box<EffectFailureDetail>>,
    },

    #[error("effect rejected at boundary by {rejected_by}: {code}: {message}")]
    BoundaryRejected {
        rejected_by: EffectFailureSource,
        code: EffectFailureCode,
        message: String,
        retry: RetryDisposition,
    },

    #[error("effect provenance mismatch: {0}")]
    EffectProvenanceMismatch(String),

    #[error(
        "incomplete effect outcome group for cursor {cursor:?}: {present} of {expected} facts present"
    )]
    IncompleteOutcomeGroup {
        cursor: EffectCursor,
        expected: usize,
        present: usize,
    },

    #[error("non-idempotent effect '{effect_type}' has no idempotency key")]
    MissingIdempotencyKey { effect_type: String },

    #[error("effectful stage '{stage_key}' performed undeclared effect '{effect_type}'")]
    UndeclaredEffect {
        stage_key: String,
        effect_type: String,
    },

    #[error("effectful stage '{stage_key}' emitted undeclared output fact '{event_type}'")]
    UndeclaredOutput {
        stage_key: String,
        event_type: String,
    },

    #[error("effectful stage '{stage_key}' cannot emit output facts from this handler context")]
    EmitUnsupported { stage_key: String },

    #[error("effectful stage '{stage_key}' completed without authoring an output fact")]
    CompletedWithoutOutput { stage_key: String },

    #[error(
        "effectful stage '{stage_key}' used complete_empty after committing {committed} output facts"
    )]
    CompletedEmptyWithOutput { stage_key: String, committed: usize },

    #[error("effect target invariant failed for slot '{slot}'")]
    EffectTargetInvariantViolation { slot: EffectPortSlotLabel },

    #[error("effect dependency '{failure_source}' failed ({code}): {message}")]
    DependencyFailed {
        failure_source: EffectFailureSource,
        code: EffectFailureCode,
        message: String,
        retry: RetryDisposition,
    },

    #[error(
        "effect recovery abandoned after attempt {last_started_attempt} by {failure_source} ({code}): {message}"
    )]
    RecoveryAbandoned {
        last_started_attempt: EffectAttemptOrdinal,
        failure_source: EffectFailureSource,
        code: EffectFailureCode,
        message: String,
        boundary_retry: RetryDisposition,
    },

    #[error("transactional effect '{effect_type}' returned without committing an effect result")]
    TransactionalCommitMissing { effect_type: String },

    /// Legacy opaque call failure. Circuit-breaker recovery treats it as
    /// permanent and ineligible; effect ports should author a typed variant.
    #[error("effect execution failed: {0}")]
    Execution(String),

    /// The physical call exceeded its operation timeout.
    #[error("effect call timed out: {0}")]
    Timeout(String),

    /// The physical call failed at the transport boundary.
    #[error("effect call transport failed: {0}")]
    Transport(String),

    /// The dependency refused the call and supplied a minimum retry delay.
    #[error("effect call was rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Duration,
    },

    /// The physical call failed in a way another call cannot repair.
    #[error("effect call failed permanently: {0}")]
    Permanent(String),

    /// The dependency rejected invalid call input.
    #[error("effect call validation failed: {0}")]
    Validation(String),

    /// The dependency returned a domain-level failure.
    #[error("effect call domain failure: {0}")]
    Domain(String),

    #[error("effect replay archive failed: {0}")]
    ReplayArchive(String),
}

/// A part of an effect's descriptor that a replayed or resumed call compares
/// with its recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescriptorComponent {
    /// The effect type, label or schema version.
    Contract,
    /// The canonical request.
    Request,
    /// The binding, which for an AI effect is its model.
    Binding,
    StageLogicVersion,
}

/// The kind of binding a drifted effect has, which words its diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftBinding {
    /// A model chosen by configuration, as an AI effect has.
    Model,
    /// A named binding the stage's handler holds.
    Named,
}

impl DriftBinding {
    fn noun(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Named => "binding",
        }
    }

    fn request_changed(self) -> &'static str {
        match self {
            Self::Model => "the request differs from the recording (prompt or parameters changed)",
            Self::Named => "the request differs from the recording",
        }
    }
}

/// Which recorded descriptor components an offered call changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorDrift {
    pub differs: Vec<DescriptorComponent>,
    pub binding: DriftBinding,
}

impl DescriptorDrift {
    pub(crate) fn between(
        offered: &EffectDescriptor,
        recorded: &EffectDescriptor,
        binding: DriftBinding,
    ) -> Self {
        let mut differs = Vec::new();
        if offered.effect_type != recorded.effect_type
            || offered.label != recorded.label
            || offered.schema_version != recorded.schema_version
        {
            differs.push(DescriptorComponent::Contract);
        }
        if offered.binding != recorded.binding {
            differs.push(DescriptorComponent::Binding);
        } else if offered.canonical_input_hash != recorded.canonical_input_hash {
            differs.push(DescriptorComponent::Request);
        }
        if offered.stage_logic_version != recorded.stage_logic_version {
            differs.push(DescriptorComponent::StageLogicVersion);
        }
        Self { differs, binding }
    }
}

impl std::fmt::Display for DescriptorDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let changed = self
            .differs
            .iter()
            .map(|component| match component {
                DescriptorComponent::Contract => {
                    "the effect's type, label or schema version differs from the recording"
                        .to_string()
                }
                DescriptorComponent::Request => self.binding.request_changed().to_string(),
                DescriptorComponent::Binding => {
                    format!("the {} differs from the recording", self.binding.noun())
                }
                DescriptorComponent::StageLogicVersion => {
                    "the stage logic version differs from the recording".to_string()
                }
            })
            .collect::<Vec<_>>();
        let matching = [
            (DescriptorComponent::Binding, self.binding.noun()),
            (
                DescriptorComponent::StageLogicVersion,
                "stage logic version",
            ),
        ]
        .into_iter()
        .filter(|(component, _)| !self.differs.contains(component))
        .map(|(_, name)| name)
        .collect::<Vec<_>>();
        f.write_str(&changed.join("; "))?;
        match matching.as_slice() {
            [] => Ok(()),
            [one] => write!(f, "; {one} matches"),
            [first, second] => write!(f, "; {first} and {second} match"),
            _ => Ok(()),
        }
    }
}

fn descriptor_mismatch_message(
    cursor: &EffectCursor,
    expected: &EffectDescriptorHash,
    recorded: &EffectDescriptorHash,
    drift: Option<&DescriptorDrift>,
) -> String {
    match drift {
        Some(drift) if !drift.differs.is_empty() => format!(
            "effect {}#{}/{}: {drift}",
            cursor.stage_key.as_str(),
            cursor.input_seq,
            cursor.effect_ordinal
        ),
        _ => format!(
            "effect descriptor mismatch for cursor {cursor:?}: expected {expected}, recorded {recorded}"
        ),
    }
}

impl EffectError {
    /// The named system did not act on the call, and a later call may succeed.
    /// A call that may have reached the system is a maybe, not a no, and maps to
    /// `Timeout` or `Transport`.
    pub fn unavailable(source: &str, code: &str, error: impl std::fmt::Display) -> Self {
        Self::DependencyFailed {
            failure_source: EffectFailureSource::new(source),
            code: EffectFailureCode::new(code),
            message: error.to_string(),
            retry: RetryDisposition::Retryable,
        }
    }

    /// Construct a target-invariant failure from a validated typed slot.
    pub fn target_invariant_violation<P, M>(slot: EffectPortSlot<P, M>) -> Self
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        Self::EffectTargetInvariantViolation {
            slot: slot.diagnostic_label(),
        }
    }

    fn recovery_eligible_by_default(&self) -> bool {
        match self {
            EffectError::BindingAuthority { .. } => false,
            EffectError::RecordedFailure { retry, .. }
            | EffectError::BoundaryRejected { retry, .. }
            | EffectError::DependencyFailed { retry, .. } => retry.is_retryable(),
            EffectError::RecoveryAbandoned { .. }
            | EffectError::EffectTargetInvariantViolation { .. } => false,
            EffectError::EffectProvenanceMismatch(_) => false,
            EffectError::IncompleteOutcomeGroup { .. } => false,
            EffectError::MissingRecordedEffect { .. }
            | EffectError::RecordingDeparture { .. }
            | EffectError::RecordingEnded { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::CompletedWithoutOutput { .. }
            | EffectError::CompletedEmptyWithOutput { .. }
            | EffectError::TransactionalCommitMissing { .. } => false,
            EffectError::Timeout(_)
            | EffectError::Transport(_)
            | EffectError::RateLimited { .. }
            | EffectError::Serialization(_)
            | EffectError::Journal(_)
            | EffectError::Execution(_)
            | EffectError::ReplayArchive(_) => true,
            EffectError::Permanent(_) | EffectError::Validation(_) | EffectError::Domain(_) => {
                false
            }
        }
    }

    pub(super) fn retry_disposition(&self) -> RetryDisposition {
        RetryDisposition::from_bool(self.recovery_eligible_by_default())
    }

    /// The failure's kind, read from its recorded type so a live failure and
    /// its replay agree.
    pub fn error_kind(&self) -> ErrorKind {
        match self.error_type().as_str() {
            "timeout" => ErrorKind::Timeout,
            "transport" | "remote" => ErrorKind::Remote,
            "rate_limited" => ErrorKind::RateLimited,
            "validation" | "invalid_request" | "unsupported" => ErrorKind::Validation,
            "domain" => ErrorKind::Domain,
            "authentication"
            | "binding_authority_fault"
            | "serialization"
            | "journal"
            | "missing_recorded_effect"
            | "recording_departure"
            | "recording_ended"
            | "duplicate_recorded_effect"
            | "descriptor_mismatch"
            | "boundary_rejected"
            | "effect_provenance_mismatch"
            | "incomplete_outcome_group"
            | "missing_idempotency_key"
            | "undeclared_effect"
            | "undeclared_output"
            | "emit_unsupported"
            | "completed_without_output"
            | "completed_empty_with_output"
            | "target_invariant_violation"
            | "recovery_abandoned"
            | "transactional_commit_missing"
            | "execution"
            | "permanent"
            | "replay_archive" => ErrorKind::PermanentFailure,
            _ => ErrorKind::Unknown,
        }
    }

    pub(super) fn error_type(&self) -> EffectFailureKind {
        match self {
            EffectError::BindingAuthority { .. } => "binding_authority_fault",
            EffectError::Serialization(_) => "serialization",
            EffectError::Journal(_) => "journal",
            EffectError::MissingRecordedEffect { .. } => "missing_recorded_effect",
            EffectError::RecordingDeparture { .. } => "recording_departure",
            EffectError::RecordingEnded { .. } => "recording_ended",
            EffectError::DuplicateRecordedEffect { .. } => "duplicate_recorded_effect",
            EffectError::DescriptorMismatch { .. } => "descriptor_mismatch",
            EffectError::RecordedFailure { error_type, .. } => return error_type.clone(),
            EffectError::BoundaryRejected { .. } => "boundary_rejected",
            EffectError::EffectProvenanceMismatch(_) => "effect_provenance_mismatch",
            EffectError::IncompleteOutcomeGroup { .. } => "incomplete_outcome_group",
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
            EffectError::UndeclaredEffect { .. } => "undeclared_effect",
            EffectError::UndeclaredOutput { .. } => "undeclared_output",
            EffectError::EmitUnsupported { .. } => "emit_unsupported",
            EffectError::CompletedWithoutOutput { .. } => "completed_without_output",
            EffectError::CompletedEmptyWithOutput { .. } => "completed_empty_with_output",
            EffectError::EffectTargetInvariantViolation { .. } => "target_invariant_violation",
            EffectError::DependencyFailed { code, .. } => return code.as_str().into(),
            EffectError::RecoveryAbandoned { .. } => "recovery_abandoned",
            EffectError::TransactionalCommitMissing { .. } => "transactional_commit_missing",
            EffectError::Execution(_) => "execution",
            EffectError::Timeout(_) => "timeout",
            EffectError::Transport(_) => "transport",
            EffectError::RateLimited { .. } => "rate_limited",
            EffectError::Permanent(_) => "permanent",
            EffectError::Validation(_) => "validation",
            EffectError::Domain(_) => "domain",
            EffectError::ReplayArchive(_) => "replay_archive",
        }
        .into()
    }

    /// The business-facing failure reason, identical live and replayed
    /// (FLOWIP-120i).
    ///
    /// A live `Execution("gateway_timeout_simulated")` and the
    /// `RecordedFailure` its journaled outcome rehydrates to on replay both
    /// project to `"gateway_timeout_simulated"`, so failure-mapping code has
    /// one path and never parses Display wrappers. Variants carrying a
    /// semantic message borrow it; infrastructure variants with structured
    /// fields fall back to their Display rendering.
    pub fn semantic_reason(&self) -> std::borrow::Cow<'_, str> {
        match self {
            EffectError::Execution(message)
            | EffectError::Timeout(message)
            | EffectError::Transport(message)
            | EffectError::Permanent(message)
            | EffectError::Validation(message)
            | EffectError::Domain(message)
            | EffectError::Serialization(message)
            | EffectError::Journal(message)
            | EffectError::ReplayArchive(message)
            | EffectError::EffectProvenanceMismatch(message) => std::borrow::Cow::Borrowed(message),
            EffectError::RecordedFailure { error_message, .. } => {
                std::borrow::Cow::Borrowed(error_message)
            }
            EffectError::RateLimited { message, .. } => std::borrow::Cow::Borrowed(message),
            EffectError::BoundaryRejected { message, .. } => std::borrow::Cow::Borrowed(message),
            EffectError::DependencyFailed { message, .. }
            | EffectError::RecoveryAbandoned { message, .. } => std::borrow::Cow::Borrowed(message),
            EffectError::BindingAuthority { .. }
            | EffectError::MissingRecordedEffect { .. }
            | EffectError::RecordingDeparture { .. }
            | EffectError::RecordingEnded { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::IncompleteOutcomeGroup { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::CompletedWithoutOutput { .. }
            | EffectError::CompletedEmptyWithOutput { .. }
            | EffectError::EffectTargetInvariantViolation { .. }
            | EffectError::TransactionalCommitMissing { .. } => {
                std::borrow::Cow::Owned(self.to_string())
            }
        }
    }

    /// The message recorded into a `Failed` outcome payload. The semantic
    /// reason, never the Display wrapper: recording `self.to_string()` here
    /// baked "effect execution failed: " into journaled facts, which replay
    /// then surfaced to user code as a different business payload than the
    /// live run produced (the FLOWIP-120i wrapper leak).
    pub(super) fn error_message(&self) -> String {
        self.semantic_reason().into_owned()
    }

    /// Structured cause carried into a recorded `Failed` outcome, present when
    /// execution was rejected by a named component such as boundary middleware.
    /// Live `BoundaryRejected` errors and replayed `RecordedFailure` errors
    /// return the same source/code pair.
    pub fn failure_cause(&self) -> Option<EffectFailureCause> {
        match self {
            EffectError::BoundaryRejected {
                rejected_by, code, ..
            } => Some(EffectFailureCause {
                source: rejected_by.clone(),
                code: code.clone(),
            }),
            EffectError::RecordedFailure { cause, .. } => cause.clone(),
            EffectError::DependencyFailed {
                failure_source,
                code,
                ..
            }
            | EffectError::RecoveryAbandoned {
                failure_source,
                code,
                ..
            } => Some(EffectFailureCause {
                source: failure_source.clone(),
                code: code.clone(),
            }),
            _ => None,
        }
    }

    pub fn failure_detail(&self) -> Option<EffectFailureDetail> {
        match self {
            Self::EffectTargetInvariantViolation { slot } => {
                Some(EffectFailureDetail::PortBindingInvariantViolation {
                    port: slot.as_str().to_string(),
                    expected: "<not disclosed>".to_string(),
                    observed: "<not disclosed>".to_string(),
                })
            }
            Self::RecordedFailure { detail, .. } => detail.as_deref().cloned(),
            _ => None,
        }
    }
}

impl From<ReplayError> for EffectError {
    fn from(value: ReplayError) -> Self {
        Self::ReplayArchive(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handler_error::HandlerError;

    /// FLOWIP-120i: the semantic reason is identical across the live error,
    /// what gets recorded, and the replay-side rehydration, with no Display
    /// wrapper anywhere in the chain.
    #[test]
    fn semantic_reason_is_stable_across_live_recording_and_replay() {
        let live = EffectError::Execution("gateway_timeout_simulated".to_string());
        assert_eq!(live.semantic_reason(), "gateway_timeout_simulated");
        assert_eq!(live.error_message(), "gateway_timeout_simulated");

        // What replay rehydrates from the recorded Failed payload.
        let replayed = EffectError::RecordedFailure {
            error_type: live.error_type(),
            error_message: live.error_message(),
            retry: live.retry_disposition(),
            cause: live.failure_cause(),
            detail: live.failure_detail().map(Box::new),
        };
        assert_eq!(replayed.semantic_reason(), live.semantic_reason());
        assert_eq!(
            replayed.error_message(),
            "gateway_timeout_simulated",
            "re-recording a recorded failure must not wrap again"
        );
        assert!(
            !replayed
                .semantic_reason()
                .contains("effect execution failed"),
            "no Display wrapper may leak into the semantic reason"
        );
    }

    #[test]
    fn semantic_reason_projects_boundary_rejections_to_their_message() {
        let rejected = EffectError::BoundaryRejected {
            rejected_by: EffectFailureSource::new("effect_boundary"),
            code: EffectFailureCode::new("breaker_open"),
            message: "circuit breaker open".to_string(),
            retry: RetryDisposition::from_bool(true),
        };
        assert_eq!(rejected.semantic_reason(), "circuit breaker open");
        assert_eq!(
            rejected.error_message(),
            "circuit breaker open",
            "structured source and code live in the recorded cause, never the message"
        );
        assert!(rejected.failure_cause().is_some());
    }

    #[test]
    fn semantic_reason_falls_back_to_display_for_infrastructure_variants() {
        let infra = EffectError::MissingIdempotencyKey {
            effect_type: "demo.effect".to_string(),
        };
        assert_eq!(infra.semantic_reason(), infra.to_string());
    }

    fn replayed(live: &EffectError) -> EffectError {
        EffectError::RecordedFailure {
            error_type: live.error_type(),
            error_message: live.error_message(),
            retry: live.retry_disposition(),
            cause: live.failure_cause(),
            detail: live.failure_detail().map(Box::new),
        }
    }

    fn dependency(code: &str) -> EffectError {
        EffectError::DependencyFailed {
            failure_source: EffectFailureSource::new("chat_client"),
            code: EffectFailureCode::new(code),
            message: "provider".to_string(),
            retry: RetryDisposition::NotRetryable,
        }
    }

    #[test]
    fn a_live_failure_and_its_replay_have_one_kind() {
        let cases = [
            (EffectError::Timeout("timeout".into()), ErrorKind::Timeout),
            (EffectError::Transport("offline".into()), ErrorKind::Remote),
            (
                EffectError::RateLimited {
                    message: "slow down".into(),
                    retry_after: Duration::from_millis(250),
                },
                ErrorKind::RateLimited,
            ),
            (
                EffectError::Permanent("denied".into()),
                ErrorKind::PermanentFailure,
            ),
            (
                EffectError::Validation("invalid".into()),
                ErrorKind::Validation,
            ),
            (EffectError::Domain("declined".into()), ErrorKind::Domain),
            (
                EffectError::Execution("opaque".into()),
                ErrorKind::PermanentFailure,
            ),
            (
                EffectError::Serialization("garbled".into()),
                ErrorKind::PermanentFailure,
            ),
            (
                EffectError::BoundaryRejected {
                    rejected_by: EffectFailureSource::new("effect_boundary"),
                    code: EffectFailureCode::new("breaker_open"),
                    message: "circuit breaker open".into(),
                    retry: RetryDisposition::from_bool(true),
                },
                ErrorKind::PermanentFailure,
            ),
            (dependency("timeout"), ErrorKind::Timeout),
            (dependency("remote"), ErrorKind::Remote),
            (dependency("rate_limited"), ErrorKind::RateLimited),
            (dependency("authentication"), ErrorKind::PermanentFailure),
            (dependency("invalid_request"), ErrorKind::Validation),
            (dependency("unsupported"), ErrorKind::Validation),
            (dependency("other"), ErrorKind::Unknown),
        ];
        for (live, kind) in cases {
            assert_eq!(live.error_kind(), kind, "{live:?}");
            assert_eq!(replayed(&live).error_kind(), kind, "{live:?}");
        }
    }

    #[test]
    fn a_handler_error_from_a_failure_matches_its_replay() {
        for live in [
            EffectError::Permanent("denied".into()),
            EffectError::Transport("offline".into()),
            dependency("authentication"),
            dependency("other"),
        ] {
            let from_live = HandlerError::from(live.clone());
            let from_replay = HandlerError::from(replayed(&live));
            assert_eq!(from_live.kind(), from_replay.kind(), "{live:?}");
            assert_eq!(from_live.message(), from_replay.message(), "{live:?}");
            assert_eq!(from_live.message(), live.semantic_reason(), "{live:?}");
        }
    }
}
