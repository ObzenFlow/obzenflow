// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

pub struct EffectContext {
    pub(super) is_replaying: bool,
    pub(super) flow_id: FlowId,
    pub(super) stage_key: String,
    pub(super) input_seq: StageInputPosition,
    pub(super) ports: EffectPortRegistry,
}

impl EffectContext {
    pub fn is_replaying(&self) -> bool {
        self.is_replaying
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    pub fn stage_key(&self) -> &str {
        &self.stage_key
    }

    pub fn input_seq(&self) -> StageInputPosition {
        self.input_seq
    }

    pub fn now(&self) -> u64 {
        self.input_seq.0
    }

    pub fn deterministic_id(
        &self,
        label: &str,
        ordinal: impl Into<EffectOutputOrdinal>,
    ) -> EventId {
        deterministic_event_id(
            self.flow_id.to_string(),
            format!("{}:{label}", self.stage_key),
            self.input_seq,
            ordinal,
        )
    }

    pub fn rng(&self, label: &str) -> fastrand::Rng {
        let material = format!(
            "{}:{}:{}:{label}",
            self.flow_id, self.stage_key, self.input_seq.0
        );
        let hash = digest(&SHA256, material.as_bytes());
        let mut seed = [0u8; 8];
        seed.copy_from_slice(&hash.as_ref()[..8]);
        fastrand::Rng::with_seed(u64::from_be_bytes(seed))
    }

    pub fn port<T>(&self, name: &str) -> Result<Arc<T>, EffectError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.ports
            .get(name)
            .ok_or_else(|| EffectError::MissingEffectPort {
                type_name: std::any::type_name::<T>(),
                name: name.to_string(),
            })
    }

    pub fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

/// The shape of a synthesized-outcome registration (FLOWIP-120m).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynthesizedOutcomeKind {
    /// FLOWIP-120h: the middleware authors branch facts disjoint from the
    /// effect's own fact set, decoded through the `Guarded` wrapper's lifted
    /// carrier.
    BranchShaped,
    /// FLOWIP-120m: the middleware may synthesize the protected effect's own
    /// outcome facts (a cached decision, a stubbed authorization). The
    /// handler performs the plain effect, with no `Guarded` wrapper.
    OutcomeShaped,
}

/// Registration made by type-shaping middleware declared in the
/// `output_middleware:` macro lane (FLOWIP-120h). It names the fact types
/// the middleware may synthesize at the effect boundary, so `perform` can
/// validate wrapper coordination before any I/O.
#[derive(Debug, Clone)]
pub struct SynthesizedOutcomeRegistration {
    /// The protected effect's `EFFECT_TYPE`. `None` means the stage's single
    /// declared effect (v1 rejects typed-outcome middleware on multi-effect
    /// stages at build time); outcome-shaped registrations always name it.
    pub effect_type: Option<String>,
    /// Fact types the middleware may author: branch facts disjoint from the
    /// effect's set for `BranchShaped`, the effect's own outcome facts for
    /// `OutcomeShaped`.
    pub fact_types: Vec<TypedFactType>,
    /// Label of the registering middleware, for error messages.
    pub source_label: String,
    /// Which validation and coordination regime applies (FLOWIP-120m).
    pub kind: SynthesizedOutcomeKind,
}

pub struct EffectInvocationContext {
    pub flow_id: FlowId,
    pub stage_id: StageId,
    pub stage_key: String,
    pub writer_id: WriterId,
    pub input_seq: StageInputPosition,
    pub stage_logic_version: String,
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    pub flow_context: Option<FlowContext>,
    pub system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    pub instrumentation: Option<Arc<StageInstrumentation>>,
    pub heartbeat_state: Option<Arc<HeartbeatState>>,
    pub parent: EventEnvelope<ChainEvent>,
    pub effect_history: Option<Arc<EffectHistory>>,
    pub effect_runtime_mode: EffectRuntimeMode,
    pub effect_ports: EffectPortRegistry,
    pub effect_declarations: Vec<EffectDeclaration>,
    pub synthesized_outcomes: Vec<SynthesizedOutcomeRegistration>,
    pub output_contract: StageOutputContract,
    pub backpressure_writer: BackpressureWriter,
    pub emit_enabled: bool,
    pub effect_boundary: Option<Arc<dyn EffectBoundary>>,
    pub boundary_control_events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl EffectInvocationContext {
    pub fn push_boundary_control_events(&self, mut events: Vec<ChainEvent>) {
        if events.is_empty() {
            return;
        }

        let mut buffer = self
            .boundary_control_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        buffer.append(&mut events);
    }

    pub fn drain_boundary_control_events(&self) -> Vec<ChainEvent> {
        let mut buffer = self
            .boundary_control_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *buffer)
    }

    pub fn effect_declaration(
        &self,
        effect_type: &'static str,
    ) -> Result<EffectDeclaration, EffectError> {
        self.effect_declarations
            .iter()
            .find(|declaration| declaration.effect_type == effect_type)
            .cloned()
            .ok_or_else(|| EffectError::UndeclaredEffect {
                stage_key: self.stage_key.clone(),
                effect_type: effect_type.to_string(),
            })
    }

    /// The typed-outcome registration covering the given effect, if any.
    pub fn synthesized_outcome_registration(
        &self,
        effect_type: &str,
    ) -> Option<&SynthesizedOutcomeRegistration> {
        self.synthesized_outcomes.iter().find(|registration| {
            registration
                .effect_type
                .as_deref()
                .is_none_or(|guarded| guarded == effect_type)
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum EffectRuntimeMode {
    #[default]
    Live,
    ReplayStrict,
    ResumeIncomplete,
}

impl EffectRuntimeMode {
    pub fn from_replay_archive(archive: Option<&dyn ReplayArchive>) -> Self {
        let Some(archive) = archive else {
            return Self::Live;
        };

        if matches!(
            archive.archive_status(),
            ArchiveStatus::Completed | ArchiveStatus::Cancelled
        ) {
            Self::ReplayStrict
        } else if archive.allow_incomplete_archive() {
            Self::ResumeIncomplete
        } else {
            Self::ReplayStrict
        }
    }
}

/// Compute the middleware execution scope for one dispatched event
/// (FLOWIP-120c H3).
///
/// Supervisors hold the delivered position at dispatch and call this per
/// event, replacing the build-time-static `with_execution_scope` binding on
/// the middleware wrappers. Today the decision depends only on the stage's
/// runtime mode; FLOWIP-120n's resume phase predicate adds the
/// position-versus-seeded-high-water-mark comparison here, which is why the
/// delivered position already rides the signature.
pub fn scope_for_dispatch(
    mode: EffectRuntimeMode,
    position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
) -> obzenflow_core::MiddlewareExecutionScope {
    let _ = position;
    mode.into()
}

/// Map a stage's effect runtime mode onto the handler-level middleware execution
/// scope (FLOWIP-120a). Live runs reconstruct nothing, so handler middleware runs
/// live. Strict replay and incomplete-archive resume both reconstruct the handler
/// shell from recorded events, so handler-level control middleware must suppress
/// its side effects. Live work that resume performs happens at the effect boundary,
/// which is scoped separately as `LiveEffectBoundary`.
impl From<EffectRuntimeMode> for obzenflow_core::MiddlewareExecutionScope {
    fn from(mode: EffectRuntimeMode) -> Self {
        match mode {
            EffectRuntimeMode::Live => Self::LiveHandler,
            EffectRuntimeMode::ReplayStrict => Self::StrictReplayHandler,
            EffectRuntimeMode::ResumeIncomplete => Self::ResumeHandler,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::upstream_subscription::StageInputPosition;

    #[test]
    fn resume_scope_remains_reconstruction_until_phase_predicate_lands() {
        assert_eq!(
            scope_for_dispatch(
                EffectRuntimeMode::ResumeIncomplete,
                Some(StageInputPosition(42))
            ),
            obzenflow_core::MiddlewareExecutionScope::ResumeHandler
        );
    }
}
