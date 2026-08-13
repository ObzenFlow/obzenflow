// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use crate::dsl::backpressure_clause::BackpressureClause;
use crate::dsl::typing::StageTypingMetadata;
use crate::dsl::StageCreationResult;
use crate::stage_handle_adapter::StageHandleAdapter;
use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::StageObserverSet;
use obzenflow_adapters::middleware::{
    validate_middleware_safety, CheckedMiddlewareSurfaceAttachment, MiddlewareDeclaration,
    MiddlewareDeclarationIndex, MiddlewareDeclarationPosition, MiddlewareFactory,
    MiddlewareSurfaceKind, PerEffectPolicyBoundary, PerSinkDeliveryPolicyBoundary,
    PerSourcePolicyBoundary, SinkPolicy, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::__private::{SinkWriterAdapter, UnifiedJoinHandler};
use obzenflow_runtime::{
    effects::{
        EffectBoundary, EffectDeclaration, EffectPortRegistry, EffectSafety, IdempotencyKeyPolicy,
    },
    metrics::instrumentation::{InstrumentationConfig, StageInstrumentation},
    stages::StageResources,
};
use obzenflow_runtime::{
    pipeline::config::StageConfig,
    stages::{
        common::{
            control_strategies::{JonestownSignalStrategy, SignalGate},
            handlers::{
                EffectfulStatefulHandler, EffectfulStatefulHandlerAdapter,
                EffectfulTransformHandler, EffectfulTransformHandlerAdapter, SinkConnector,
                SinkDescription, SinkWriterInitContext, TransformHandler,
                UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
                UnifiedFiniteSourceHandler, UnifiedInfiniteSourceHandler, UnifiedStatefulHandler,
            },
            stage_handle::{BoxedStageHandle, StageEvent, FORCE_SHUTDOWN_MESSAGE},
        },
        join::{JoinBuilder, JoinConfig, JoinEvent, JoinReferenceMode, JoinState},
        sink::journal_sink::{
            JournalSinkBuilder, JournalSinkConfig, JournalSinkEvent, JournalSinkState,
            SinkDeliveryBoundary,
        },
        source::{
            finite::{
                AsyncFiniteSourceBuilder, FiniteSourceBuilder, FiniteSourceConfig,
                FiniteSourceEvent, FiniteSourceState,
            },
            infinite::{
                AsyncInfiniteSourceBuilder, InfiniteSourceBuilder, InfiniteSourceConfig,
                InfiniteSourceEvent, InfiniteSourceState,
            },
            strategies::CompletionGate,
            SourceBoundary,
        },
        stateful::{StatefulBuilder, StatefulConfig, StatefulEvent, StatefulState},
        transform::{TransformBuilder, TransformConfig, TransformEvent, TransformState},
    },
    supervised_base::SupervisorBuilder as SupervisorBuilderTrait,
};
use std::sync::Arc;
use std::time::Duration;

pub(crate) mod sealed {
    pub trait Sealed {}
}

fn factory_declares_circuit_breaker(factory: &dyn MiddlewareFactory) -> bool {
    factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::CircuitBreaker)
}

/// Marker name used by stage macros when the runtime name should be derived from the enclosing
/// `flow!` binding.
///
/// This is intentionally a weird, non-user-facing value. `flow!` resolves it to the left-hand
/// binding before any uniqueness checks or topology build steps run.
#[doc(hidden)]
pub const BINDING_DERIVED_NAME_SENTINEL: &str = "__obzenflow_binding_derived_name__";

fn create_system_observers(_config: &StageConfig) -> StageObserverSet {
    // No built-in observers (FLOWIP-115f): `processing_time` is stamped by the
    // runtime output committer from the instrumentation timer, not by an
    // observer, and the user-facing observation middleware are `indicator()` and
    // `indicator(..)`. User-attached observers are merged onto this empty default.
    StageObserverSet::default()
}

fn observer_surfaces_for_stage(stage_type: StageType) -> &'static [MiddlewareSurfaceKind] {
    match stage_type {
        StageType::FiniteSource | StageType::InfiniteSource => &[
            MiddlewareSurfaceKind::SourcePoll,
            MiddlewareSurfaceKind::OutputCommit,
            MiddlewareSurfaceKind::StageLifecycle,
        ],
        StageType::Transform => &[
            MiddlewareSurfaceKind::Handler,
            MiddlewareSurfaceKind::OutputCommit,
            MiddlewareSurfaceKind::StageLifecycle,
        ],
        StageType::Stateful => &[
            MiddlewareSurfaceKind::Stateful,
            MiddlewareSurfaceKind::OutputCommit,
            MiddlewareSurfaceKind::StageLifecycle,
        ],
        StageType::Join => &[
            MiddlewareSurfaceKind::Join,
            MiddlewareSurfaceKind::OutputCommit,
            MiddlewareSurfaceKind::StageLifecycle,
        ],
        StageType::Sink => &[
            MiddlewareSurfaceKind::SinkDelivery,
            MiddlewareSurfaceKind::StageLifecycle,
        ],
    }
}

fn push_observer_attachment(
    observers: &mut StageObserverSet,
    attachment: CheckedMiddlewareSurfaceAttachment,
) -> StageCreationResult<()> {
    observers.push_attachment(attachment).map_err(|e| e.into())
}

fn declaration_has_stage_observer_surface(
    declaration: &MiddlewareDeclaration,
    stage_type: StageType,
) -> bool {
    observer_surfaces_for_stage(stage_type)
        .iter()
        .any(|surface| declaration.supports(*surface))
}

struct EffectObserverMaterialization<'a> {
    config: &'a StageConfig,
    stage_type: StageType,
    control_middleware: &'a Arc<ControlMiddlewareAggregator>,
    declaration_index: MiddlewareDeclarationIndex,
    effect_declarations: &'a [EffectDeclaration],
}

fn materialize_effect_observers_for_declarations(
    observers: &mut StageObserverSet,
    factory: &dyn MiddlewareFactory,
    materialization: EffectObserverMaterialization<'_>,
) -> StageCreationResult<()> {
    for effect in materialization.effect_declarations {
        let attachment = crate::dsl::binder::materialize_effect_observer(
            factory,
            materialization.config,
            materialization.stage_type,
            materialization.control_middleware,
            effect,
            materialization.declaration_index,
        )?;
        push_observer_attachment(observers, attachment)?;
    }
    Ok(())
}

struct SourceMiddlewareBinding {
    observers: StageObserverSet,
    source_boundary: Option<Arc<dyn SourceBoundary>>,
    /// FLOWIP-115b: the source completion gate companion supplied by a
    /// hook-bound source control middleware (the circuit breaker), sharing its
    /// state view. Replaces the old `has_circuit_breaker` + `try_new` lookup.
    completion_gate: Option<Arc<dyn CompletionGate>>,
    expects_circuit_breaker: bool,
    expects_rate_limiter: bool,
}

struct MiddlewarePlacement {
    observers: StageObserverSet,
    expects_circuit_breaker: bool,
    expects_rate_limiter: bool,
}

/// Grammar-owned source middleware inputs kept together so the source planner
/// receives one structural description instead of an expanding positional
/// argument list.
struct SourceMiddlewarePlan {
    source_policy_factories: Vec<Box<dyn MiddlewareFactory>>,
    ingress_policy_factory: Option<Box<dyn MiddlewareFactory>>,
    observer_factories: Vec<Box<dyn MiddlewareFactory>>,
    hosted_ingress_slot: Option<obzenflow_core::ingress::HostedIngressBindingSlot>,
}

fn reject_control_in_observers(factory: &dyn MiddlewareFactory) -> StageCreationResult<()> {
    let declaration = factory.declaration();
    if declaration.is_control() {
        return Err(format!(
            "'observers:' accepts observer middleware only; attach control middleware '{}' in the 'with [...]' clause of the live I/O unit it protects (FLOWIP-115s)",
            declaration.label
        )
        .into());
    }
    Ok(())
}

fn plan_positioned_stage_observers(
    config: &StageConfig,
    stage_type: StageType,
    observer_factories: Vec<(usize, Box<dyn MiddlewareFactory>)>,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
) -> StageCreationResult<MiddlewarePlacement> {
    let mut observers = create_system_observers(config);
    let observer_surfaces = observer_surfaces_for_stage(stage_type);

    for (observer_index, factory) in observer_factories {
        let declaration = factory.declaration();
        reject_control_in_observers(factory.as_ref())?;
        let mut placed = false;
        for surface in observer_surfaces {
            if !declaration.supports(*surface) {
                continue;
            }
            let attachment = crate::dsl::binder::materialize_observer(
                factory.as_ref(),
                config,
                stage_type,
                control_middleware,
                *surface,
                MiddlewareDeclarationIndex::observers(observer_index),
            )?;
            push_observer_attachment(&mut observers, attachment)?;
            placed = true;
        }
        if !placed {
            return Err(format!(
                "observer middleware '{}' declares capability {:?} and surfaces {:?}, but stage '{}' ({stage_type:?}) has no compatible observer surface",
                declaration.label,
                declaration.capability,
                declaration.surfaces,
                config.name
            )
            .into());
        }
    }

    Ok(MiddlewarePlacement {
        observers,
        expects_circuit_breaker: false,
        expects_rate_limiter: false,
    })
}

fn plan_stage_observers(
    config: &StageConfig,
    stage_type: StageType,
    observer_factories: Vec<Box<dyn MiddlewareFactory>>,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
) -> StageCreationResult<MiddlewarePlacement> {
    plan_positioned_stage_observers(
        config,
        stage_type,
        observer_factories.into_iter().enumerate().collect(),
        control_middleware,
    )
}

fn build_source_middleware_and_register_policies(
    config: &StageConfig,
    stage_type: StageType,
    writer_id: WriterId,
    plan: SourceMiddlewarePlan,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
) -> StageCreationResult<SourceMiddlewareBinding> {
    let SourceMiddlewarePlan {
        source_policy_factories,
        ingress_policy_factory,
        observer_factories,
        hosted_ingress_slot,
    } = plan;
    let observer_placement =
        plan_stage_observers(config, stage_type, observer_factories, control_middleware)?;
    let observers = observer_placement.observers;
    let expects_circuit_breaker = source_policy_factories
        .iter()
        .chain(ingress_policy_factory.iter())
        .any(|factory| factory_declares_circuit_breaker(factory.as_ref()));
    let expects_rate_limiter = source_policy_factories
        .iter()
        .chain(ingress_policy_factory.iter())
        .any(|factory| {
            factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::RateLimiter)
        });

    let mut completion_gate: Option<Arc<dyn CompletionGate>> = None;
    // Source-policy composition is binder-local. The flow-scoped control
    // aggregator publishes read-only snapshots only and is not a policy
    // registration backchannel available to factories.
    let mut source_policies: Vec<Arc<dyn obzenflow_adapters::middleware::SourcePolicy>> =
        Vec::new();
    // FLOWIP-115d: the ingress boundary materialized for a source-backed hosted
    // ingress source, filled into the shared binding slot below.
    let mut ingress_boundary: Option<Arc<dyn obzenflow_core::ingress::IngressBoundaryMiddleware>> =
        None;

    for (source_policy_index, factory) in source_policy_factories.into_iter().enumerate() {
        if hosted_ingress_slot.is_some()
            && factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::RateLimiter)
        {
            return Err(format!(
                "stage '{}' hosts an ingress route; attach its rate limiter as 'ingress with <policy>', not to the post-admission drain in 'with [...]' (FLOWIP-115s)",
                config.name,
            )
            .into());
        }
        let binding = crate::dsl::binder::materialize_source_poll(
            factory.as_ref(),
            config,
            stage_type,
            control_middleware,
            MiddlewareDeclarationIndex::source_with(source_policy_index),
        )?;
        source_policies.push(binding.policy);
        if binding.completion_gate.is_some() {
            completion_gate = binding.completion_gate;
        }
    }

    if let Some(factory) = ingress_policy_factory {
        let slot = hosted_ingress_slot.as_ref().ok_or_else(|| {
            format!(
                "'ingress with <policy>' requires a hosted ingress route on stage '{}' (FLOWIP-115s)",
                config.name,
            )
        })?;
        ingress_boundary = Some(crate::dsl::binder::materialize_ingress(
            factory.as_ref(),
            config,
            stage_type,
            control_middleware,
            slot.ingress_key(),
            MiddlewareDeclarationIndex::ingress_with(),
        )?);
    }

    // FLOWIP-115d: fill the hosted-ingress binding slot during source-stage
    // materialization, even when no ingress middleware is attached (boundary
    // None), so startup can verify every registered hosted surface was placed in
    // flow topology. A second source stage binding the same slot fails the build.
    if let Some(slot) = hosted_ingress_slot.as_ref() {
        slot.fill(obzenflow_core::ingress::FilledHostedIngress {
            stage_id: config.stage_id,
            stage_key: config.name.clone().into(),
            boundary: ingress_boundary,
        })
        .map_err(|e| format!("Stage '{}': {e}", config.name))?;
        // FLOWIP-120n F12: under resume, hosted ingress refuses until the
        // source supervisor marks the slot live at the catch-up handoff.
        if obzenflow_runtime::bootstrap::replay_bootstrap()
            .is_some_and(|replay| replay.verb == obzenflow_runtime::bootstrap::ReplayVerb::Resume)
        {
            slot.hold_for_resume_catch_up();
        }
    }

    let source_boundary = if source_policies.is_empty() {
        None
    } else {
        Some(
            Arc::new(PerSourcePolicyBoundary::new(source_policies, writer_id))
                as Arc<dyn SourceBoundary>,
        )
    };

    Ok(SourceMiddlewareBinding {
        observers,
        source_boundary,
        completion_gate,
        expects_circuit_breaker,
        expects_rate_limiter,
    })
}

/// The signal strategy attached to every stage. FLOWIP-115c retired the dead
/// `create_control_strategy` middleware lane (no factory ever overrode it), so
/// every stage gets the default Jonestown poison-pill signal strategy. Policies
/// bind to typed runtime control points instead of synthesizing a strategy.
fn create_default_signal_strategy() -> Arc<dyn SignalGate> {
    Arc::new(JonestownSignalStrategy)
}

/// Handler-surface classification for the FLOWIP-120c H1 policy-middleware
/// guards. Coarser than handler types, finer than `StageType`: it names the
/// surfaces whose policy placement differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyGuardSurface {
    /// Sync transforms, sync stateful stages, joins: deterministic handler
    /// shells with no live I/O unit. Policy middleware is build-rejected.
    PureSync,
    /// Effectful stages: per-effect policy placement (FLOWIP-120c phase 3).
    Effectful,
    /// Effectful stateful stages do not install the stateful effect boundary
    /// until FLOWIP-120l, so policy declarations must reject for now.
    EffectfulStatefulPendingBoundary,
    /// Sources are live I/O units; policy middleware attaches legitimately.
    Source,
    /// Sink delivery placement is deferred until FLOWIP-095g.
    Sink,
}

/// Trait for stage descriptors that know how to create their supervisors
#[async_trait]
pub trait StageDescriptor: sealed::Sealed + Send + Sync {
    /// Get the stage name
    fn name(&self) -> &str;

    /// The stage's `backpressure:` clause, when declared (FLOWIP-115e).
    /// Pure DSL-tier candidate sugar; enforcement is the resolved mode.
    fn backpressure_clause(&self) -> Option<&crate::dsl::backpressure_clause::BackpressureClause> {
        None
    }

    /// Update the stage's runtime name.
    ///
    /// Implementations that carry an internal name field should override this.
    /// Default: no-op (for descriptors that do not own a mutable name).
    fn set_name(&mut self, _name: String) {
        // Default: no-op
    }

    /// Get the stage type
    fn stage_type(&self) -> StageType;

    /// Get the reference stage ID (only for join stages)
    /// Returns None for non-join stages
    fn reference_stage_id(&self) -> Option<StageId> {
        None
    }

    /// Get the reference stage name for DSL resolution (only for join stages)
    /// Returns None for non-join stages or programmatic join stages
    fn reference_stage_name(&self) -> Option<&str> {
        None
    }

    /// Set the reference stage ID (only for join stages, used by DSL)
    fn set_reference_stage_id(&mut self, _id: StageId) {
        // Default: no-op for non-join stages
    }

    /// Create the handle for this stage with the flow's shared control-plane
    /// aggregation carrier.
    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle>;

    /// Structural: return configured stage-level middleware names (for topology)
    ///
    /// Default implementation returns an empty list; concrete descriptors that
    /// carry middleware should override this.
    fn stage_middleware_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Structural: return configured stage-level middleware factories (for topology config extraction)
    ///
    /// Each descriptor exposes the union of its grammar-owned attachment
    /// positions for configuration/topology bookkeeping. Placement never uses
    /// this view; lowering passes each position to its typed binder directly.
    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        Vec::new()
    }

    /// The grammar-owned position of every stage-scoped middleware factory.
    /// This is bookkeeping and validation evidence only: materialisation uses
    /// the descriptor's separate typed fields and never routes this union.
    fn positioned_stage_middleware_factories(
        &self,
    ) -> Vec<(MiddlewareDeclarationPosition, &dyn MiddlewareFactory)> {
        self.stage_middleware_factories()
            .into_iter()
            .map(|factory| (MiddlewareDeclarationPosition::Observers, factory))
            .collect()
    }

    /// Surviving inline effect-policy attachments. Configuration collection
    /// uses this narrow view to qualify factory defaults with the exact effect
    /// subject; it does not build a general consumer registry.
    fn effect_policy_attachments(&self) -> &[EffectPolicyAttachment] {
        &[]
    }

    /// Get a debug representation
    fn debug_info(&self) -> String {
        format!("Stage[{}]", self.name())
    }

    /// Optional types-first metadata captured by typed stage macros.
    fn typing_metadata(&self) -> Option<&StageTypingMetadata> {
        None
    }

    /// Whether this stage can perform replay-suppressed user effects.
    fn is_effectful(&self) -> bool {
        false
    }

    /// Handler-surface classification for the FLOWIP-120c H1 policy guards.
    ///
    /// The default derives from `is_effectful()` and `stage_type()`. Typed
    /// wrappers must forward to their inner descriptor.
    fn policy_guard_surface(&self) -> PolicyGuardSurface {
        if self.is_effectful() {
            return PolicyGuardSurface::Effectful;
        }
        match self.stage_type() {
            StageType::FiniteSource | StageType::InfiniteSource => PolicyGuardSurface::Source,
            StageType::Sink => PolicyGuardSurface::Sink,
            _ => PolicyGuardSurface::PureSync,
        }
    }

    /// Whether this stage imposes total deterministic order on N:1 input.
    fn is_deterministic_input_orderer(&self) -> bool {
        false
    }

    fn stage_logic_version(&self) -> String {
        "1".to_string()
    }

    /// The sink's one pre-erasure connector-description snapshot. Every
    /// descriptor and wrapper must state whether it carries one so metadata
    /// cannot silently attenuate while crossing a wrapper (FLOWIP-134h).
    fn sink_description(&self) -> Option<&SinkDescription>;

    fn effect_declarations(&self) -> Vec<EffectDeclaration> {
        Vec::new()
    }

    /// Descriptor-owned exact-input proof for generated bounded direct facts.
    #[doc(hidden)]
    fn direct_fact_plan(
        &self,
    ) -> Option<&obzenflow_runtime::stages::resources_builder::DirectFactPlan> {
        None
    }
}

fn validate_effect_declarations(
    stage_name: &str,
    declarations: &[EffectDeclaration],
    effect_ports: &EffectPortRegistry,
    port_registration_policy: obzenflow_runtime::execution::EffectPortRegistrationPolicy,
) -> Result<(), String> {
    let mut effect_types = std::collections::HashSet::new();

    for declaration in declarations {
        if !effect_types.insert(declaration.effect_type) {
            return Err(format!(
                "Effectful stage '{stage_name}' declares effect '{}' more than once",
                declaration.effect_type
            ));
        }

        if matches!(declaration.safety, EffectSafety::NonIdempotentRequiresKey)
            && !matches!(
                declaration.idempotency_key_policy,
                IdempotencyKeyPolicy::Required
            )
        {
            return Err(format!(
                "Effectful stage '{stage_name}' declares non-idempotent effect '{}' without an idempotency-key strategy",
                declaration.effect_type
            ));
        }

        if matches!(declaration.safety, EffectSafety::NonIdempotentAtLeastOnce)
            && !matches!(
                declaration.idempotency_key_policy,
                IdempotencyKeyPolicy::AtLeastOnceAcknowledged
            )
        {
            return Err(format!(
                "Effectful stage '{stage_name}' declares paid non-idempotent effect '{}' without explicit at_least_once(...) acknowledgement",
                declaration.effect_type
            ));
        }

        if matches!(declaration.safety, EffectSafety::Transactional)
            && declaration.transactional_executor.is_none()
        {
            return Err(format!(
                "Effectful stage '{stage_name}' declares transactional effect '{}' without a transactional executor",
                declaration.effect_type
            ));
        }

        for requirement in &declaration.required_ports {
            if matches!(
                port_registration_policy,
                obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required
            ) && !effect_ports.contains_requirement(requirement)
            {
                return Err(format!(
                    "Effectful stage '{stage_name}' requires effect port '{}' for type '{}' but it is not registered",
                    requirement.name, requirement.type_name
                ));
            }
        }
    }

    Ok(())
}

/// Crate-private erased descriptor for finite source stages.
pub(crate) struct FiniteSourceDescriptor<H: UnifiedFiniteSourceHandler + 'static> {
    pub(crate) name: String,
    pub(crate) handler: H,
    pub(crate) source_policies: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) ingress_policy: Option<Box<dyn MiddlewareFactory>>,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) backpressure: Option<BackpressureClause>,
}

#[async_trait]
impl<H: UnifiedFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StageDescriptor for FiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.stage_middleware_factories()
            .into_iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.source_policies
            .iter()
            .map(Box::as_ref)
            .chain(self.ingress_policy.iter().map(Box::as_ref))
            .chain(self.observers.iter().map(Box::as_ref))
            .collect()
    }

    fn positioned_stage_middleware_factories(
        &self,
    ) -> Vec<(MiddlewareDeclarationPosition, &dyn MiddlewareFactory)> {
        self.source_policies
            .iter()
            .map(|factory| (MiddlewareDeclarationPosition::SourceWith, factory.as_ref()))
            .chain(
                self.ingress_policy
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::IngressWith, factory.as_ref())),
            )
            .chain(
                self.observers
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::Observers, factory.as_ref())),
            )
            .collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::FiniteSource,
            writer_id,
            SourceMiddlewarePlan {
                source_policy_factories: self.source_policies,
                ingress_policy_factory: self.ingress_policy,
                observer_factories: self.observers,
                hosted_ingress_slot: None,
            },
            &control_middleware,
        )?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                source_binding.expects_circuit_breaker,
                source_binding.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Install the stage writer id at the erased runtime boundary.
        let mut handler = self.handler;
        handler.install_writer_id(writer_id);

        // Create the stage configuration
        let source_config = FiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: source_binding.completion_gate,
            source_boundary: source_binding.source_boundary,
            observers: source_binding.observers.build(),
        };

        // Use the builder to create the handle
        let handle = FiniteSourceBuilder::new(handler, source_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build finite source: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::FiniteSource,
            translate_stage_event_to_finite_source,
            check_finite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Crate-private erased descriptor for async finite source stages.
pub(crate) struct AsyncFiniteSourceDescriptor<H: UnifiedAsyncFiniteSourceHandler + 'static> {
    pub(crate) name: String,
    pub(crate) handler: H,
    poll_timeout: Option<Duration>,
    pub(crate) source_policies: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) ingress_policy: Option<Box<dyn MiddlewareFactory>>,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) backpressure: Option<BackpressureClause>,
}

impl<H: UnifiedAsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncFiniteSourceDescriptor<H>
{
    /// Create a new async finite source descriptor carrying the handler's configured timeout.
    pub(crate) fn new(name: impl Into<String>, handler: H) -> Self {
        let poll_timeout = handler.poll_timeout();
        Self {
            name: name.into(),
            handler,
            poll_timeout,
            source_policies: Vec::new(),
            ingress_policy: None,
            observers: Vec::new(),
            backpressure: None,
        }
    }
}

#[async_trait]
impl<H: UnifiedAsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StageDescriptor for AsyncFiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.stage_middleware_factories()
            .into_iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.source_policies
            .iter()
            .map(Box::as_ref)
            .chain(self.ingress_policy.iter().map(Box::as_ref))
            .chain(self.observers.iter().map(Box::as_ref))
            .collect()
    }

    fn positioned_stage_middleware_factories(
        &self,
    ) -> Vec<(MiddlewareDeclarationPosition, &dyn MiddlewareFactory)> {
        self.source_policies
            .iter()
            .map(|factory| (MiddlewareDeclarationPosition::SourceWith, factory.as_ref()))
            .chain(
                self.ingress_policy
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::IngressWith, factory.as_ref())),
            )
            .chain(
                self.observers
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::Observers, factory.as_ref())),
            )
            .collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);
        let poll_timeout = self.poll_timeout;

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::FiniteSource,
            writer_id,
            SourceMiddlewarePlan {
                source_policy_factories: self.source_policies,
                ingress_policy_factory: self.ingress_policy,
                observer_factories: self.observers,
                hosted_ingress_slot: None,
            },
            &control_middleware,
        )?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                source_binding.expects_circuit_breaker,
                source_binding.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Install the stage writer id at the erased runtime boundary.
        let mut handler = self.handler;
        handler.install_writer_id(writer_id);

        // Create the stage configuration
        let source_config = FiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: source_binding.completion_gate,
            source_boundary: source_binding.source_boundary,
            observers: source_binding.observers.build(),
        };

        // Use the builder to create the handle
        let handle = AsyncFiniteSourceBuilder::new(handler, source_config, resources)
            .with_poll_timeout(poll_timeout)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build async finite source: {e:?}"))?;

        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::FiniteSource,
            translate_stage_event_to_finite_source,
            check_finite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Crate-private erased descriptor for infinite source stages.
pub(crate) struct InfiniteSourceDescriptor<H: UnifiedInfiniteSourceHandler + 'static> {
    pub(crate) name: String,
    pub(crate) handler: H,
    pub(crate) source_policies: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) ingress_policy: Option<Box<dyn MiddlewareFactory>>,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) backpressure: Option<BackpressureClause>,
}

#[async_trait]
impl<H: UnifiedInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StageDescriptor for InfiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.stage_middleware_factories()
            .into_iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.source_policies
            .iter()
            .map(Box::as_ref)
            .chain(self.ingress_policy.iter().map(Box::as_ref))
            .chain(self.observers.iter().map(Box::as_ref))
            .collect()
    }

    fn positioned_stage_middleware_factories(
        &self,
    ) -> Vec<(MiddlewareDeclarationPosition, &dyn MiddlewareFactory)> {
        self.source_policies
            .iter()
            .map(|factory| (MiddlewareDeclarationPosition::SourceWith, factory.as_ref()))
            .chain(
                self.ingress_policy
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::IngressWith, factory.as_ref())),
            )
            .chain(
                self.observers
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::Observers, factory.as_ref())),
            )
            .collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            writer_id,
            SourceMiddlewarePlan {
                source_policy_factories: self.source_policies,
                ingress_policy_factory: self.ingress_policy,
                observer_factories: self.observers,
                hosted_ingress_slot: None,
            },
            &control_middleware,
        )?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                source_binding.expects_circuit_breaker,
                source_binding.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Install the stage writer id at the erased runtime boundary.
        let mut handler = self.handler;
        handler.install_writer_id(writer_id);

        // Create the stage configuration
        let source_config = InfiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: source_binding.completion_gate,
            source_boundary: source_binding.source_boundary,
            observers: source_binding.observers.build(),
        };

        // Use the builder to create the handle
        let handle = InfiniteSourceBuilder::new(handler, source_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build infinite source: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::InfiniteSource,
            translate_stage_event_to_infinite_source,
            check_infinite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Crate-private erased descriptor for async infinite source stages.
pub(crate) struct AsyncInfiniteSourceDescriptor<H: UnifiedAsyncInfiniteSourceHandler + 'static> {
    pub(crate) name: String,
    pub(crate) handler: H,
    poll_timeout: Option<Duration>,
    pub(crate) source_policies: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) ingress_policy: Option<Box<dyn MiddlewareFactory>>,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) backpressure: Option<BackpressureClause>,
}

impl<H: UnifiedAsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncInfiniteSourceDescriptor<H>
{
    /// Create a new async infinite source descriptor.
    ///
    /// The handler contract defaults infinite sources to no poll timeout so push
    /// sources can block efficiently (e.g. `recv().await`).
    pub(crate) fn new(name: impl Into<String>, handler: H) -> Self {
        let poll_timeout = handler.poll_timeout();
        Self {
            name: name.into(),
            handler,
            poll_timeout,
            source_policies: Vec::new(),
            ingress_policy: None,
            observers: Vec::new(),
            backpressure: None,
        }
    }
}

#[async_trait]
impl<H: UnifiedAsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StageDescriptor for AsyncInfiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.stage_middleware_factories()
            .into_iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.source_policies
            .iter()
            .map(Box::as_ref)
            .chain(self.ingress_policy.iter().map(Box::as_ref))
            .chain(self.observers.iter().map(Box::as_ref))
            .collect()
    }

    fn positioned_stage_middleware_factories(
        &self,
    ) -> Vec<(MiddlewareDeclarationPosition, &dyn MiddlewareFactory)> {
        self.source_policies
            .iter()
            .map(|factory| (MiddlewareDeclarationPosition::SourceWith, factory.as_ref()))
            .chain(
                self.ingress_policy
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::IngressWith, factory.as_ref())),
            )
            .chain(
                self.observers
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::Observers, factory.as_ref())),
            )
            .collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);
        let poll_timeout = self.poll_timeout;

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // FLOWIP-115d: a source-backed hosted ingress source (e.g. http_ingress)
        // exposes its binding slot here; the DSL fills it during this source
        // stage's materialization with the stage id, replay-stable key, and the
        // materialized ingress boundary.
        let hosted_ingress_slot = self.handler.hosted_ingress_slot();

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            writer_id,
            SourceMiddlewarePlan {
                source_policy_factories: self.source_policies,
                ingress_policy_factory: self.ingress_policy,
                observer_factories: self.observers,
                hosted_ingress_slot,
            },
            &control_middleware,
        )?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                source_binding.expects_circuit_breaker,
                source_binding.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Install the stage writer id at the erased runtime boundary.
        let mut handler = self.handler;
        handler.install_writer_id(writer_id);

        let source_config = InfiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: source_binding.completion_gate,
            source_boundary: source_binding.source_boundary,
            observers: source_binding.observers.build(),
        };

        let handle = AsyncInfiniteSourceBuilder::new(handler, source_config, resources)
            .with_poll_timeout(poll_timeout)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build async infinite source: {e:?}"))?;

        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::InfiniteSource,
            translate_stage_event_to_infinite_source,
            check_infinite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for transform stages
pub(crate) struct TransformDescriptor<H: TransformHandler + 'static> {
    pub(crate) name: String,
    pub(crate) handler: H,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) backpressure: Option<BackpressureClause>,
}

#[async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for TransformDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.observers
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.observers.iter().map(Box::as_ref).collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.observers {
            // Validate safety
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Transform, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
                // Could choose to panic here for critical errors
            }
        }

        // Create control strategy before moving observer factories.
        let observers = self.observers;
        let control_strategy = create_default_signal_strategy();

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement = plan_stage_observers(
            &config,
            StageType::Transform,
            observers,
            &control_middleware,
        )?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Create the stage configuration
        let transform_config = TransformConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            observers: placement.observers.build(),
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
            cycle_guard: config.cycle_guard,
        };

        // Use the builder to create the handle
        let handle = TransformBuilder::new(self.handler, transform_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build transform: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Transform,
            translate_stage_event_to_transform,
            check_transform_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// The single control policy declared inline on one `effects:` entry.
pub struct EffectPolicyAttachment {
    pub effect_type: &'static str,
    pub factory: Box<dyn MiddlewareFactory>,
}

fn validate_effect_policy_attachments(
    stage_name: &str,
    effect_declarations: &[EffectDeclaration],
    attachments: &[EffectPolicyAttachment],
) -> Result<(), String> {
    let declared_effect_types = effect_declarations
        .iter()
        .map(|declaration| declaration.effect_type)
        .collect::<std::collections::HashSet<_>>();

    let mut attached_effect_types = std::collections::HashSet::new();
    for attachment in attachments {
        if !declared_effect_types.contains(attachment.effect_type) {
            return Err(format!(
                "Effectful stage '{stage_name}' attaches policy middleware to undeclared effect '{}'",
                attachment.effect_type
            ));
        }
        if !attached_effect_types.insert(attachment.effect_type) {
            return Err(format!(
                "Effectful stage '{stage_name}' attaches more than one policy to effect '{}'; each effect has one bare `with` position",
                attachment.effect_type
            ));
        }
    }

    Ok(())
}

/// Descriptor for replay-safe effectful async transform stages.
pub struct EffectfulTransformDescriptor<H: EffectfulTransformHandler + 'static> {
    name: String,
    handler: H,
    effects: Vec<EffectDeclaration>,
    observers: Vec<Box<dyn MiddlewareFactory>>,
    /// Per-effect policy attachments from the `effects:` clause
    /// (FLOWIP-120c H7).
    effect_policies: Vec<EffectPolicyAttachment>,
    direct_fact_plan: obzenflow_runtime::stages::resources_builder::DirectFactPlan,
    pass_through_event_type: Option<obzenflow_core::EventType>,
    generated_surface: Option<&'static str>,
    generated_owner_kind: &'static str,
    backpressure: Option<BackpressureClause>,
}

impl<H: EffectfulTransformHandler + 'static> EffectfulTransformDescriptor<H> {
    /// Construct an ordinary effectful transform. Generated direct-fact
    /// admission and raw physical pass-through are intentionally absent.
    pub fn new(
        name: impl Into<String>,
        handler: H,
        effects: Vec<EffectDeclaration>,
        observers: Vec<Box<dyn MiddlewareFactory>>,
        effect_policies: Vec<EffectPolicyAttachment>,
        backpressure: Option<BackpressureClause>,
    ) -> Self {
        Self {
            name: name.into(),
            handler,
            effects,
            observers,
            effect_policies,
            direct_fact_plan: obzenflow_runtime::stages::resources_builder::DirectFactPlan::default(
            ),
            pass_through_event_type: None,
            generated_surface: None,
            generated_owner_kind: "role",
            backpressure,
        }
    }

    pub(crate) fn generated<Input>(
        name: impl Into<String>,
        handler: H,
        effects: Vec<EffectDeclaration>,
        effect_policies: Vec<EffectPolicyAttachment>,
        direct_bound: std::num::NonZeroU64,
    ) -> Self
    where
        Input: obzenflow_core::TypedPayload,
    {
        let mut descriptor = Self::new(name, handler, effects, Vec::new(), effect_policies, None);
        descriptor.direct_fact_plan =
            obzenflow_runtime::stages::resources_builder::DirectFactPlan::generated::<Input>(
                direct_bound,
            );
        descriptor.generated_surface = Some("ai_map_reduce!");
        descriptor
    }

    pub(crate) fn generated_for_surface<Input>(
        surface: &'static str,
        owner_kind: &'static str,
        name: impl Into<String>,
        handler: H,
        effects: Vec<EffectDeclaration>,
        effect_policies: Vec<EffectPolicyAttachment>,
        direct_bound: std::num::NonZeroU64,
    ) -> Self
    where
        Input: obzenflow_core::TypedPayload,
    {
        let mut descriptor =
            Self::generated::<Input>(name, handler, effects, effect_policies, direct_bound);
        descriptor.generated_surface = Some(surface);
        descriptor.generated_owner_kind = owner_kind;
        descriptor
    }

    pub(crate) fn generated_with_pass_through<Input, PassThrough>(
        name: impl Into<String>,
        handler: H,
        effects: Vec<EffectDeclaration>,
        effect_policies: Vec<EffectPolicyAttachment>,
        direct_bound: std::num::NonZeroU64,
    ) -> Self
    where
        Input: obzenflow_core::TypedPayload,
        PassThrough: obzenflow_core::TypedPayload,
    {
        let mut descriptor =
            Self::generated::<Input>(name, handler, effects, effect_policies, direct_bound);
        descriptor.pass_through_event_type = Some(obzenflow_core::EventType::from(
            PassThrough::versioned_event_type(),
        ));
        descriptor
    }
}

#[async_trait]
impl<H: EffectfulTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for EffectfulTransformDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn is_effectful(&self) -> bool {
        true
    }

    fn stage_logic_version(&self) -> String {
        self.handler.stage_logic_version().to_string()
    }

    fn effect_declarations(&self) -> Vec<EffectDeclaration> {
        self.effects.clone()
    }

    fn direct_fact_plan(
        &self,
    ) -> Option<&obzenflow_runtime::stages::resources_builder::DirectFactPlan> {
        Some(&self.direct_fact_plan)
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.observers
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.observers.iter().map(Box::as_ref).collect()
    }

    fn effect_policy_attachments(&self) -> &[EffectPolicyAttachment] {
        &self.effect_policies
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        mut resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        if let Some(surface) = self.generated_surface {
            crate::dsl::ai_effect::require_generated_chat_resilience(
                surface,
                self.generated_owner_kind,
                &self.name,
                self.effect_policies
                    .iter()
                    .filter(|attachment| {
                        attachment.effect_type
                            == <obzenflow_adapters::ai::ChatCompletion as obzenflow_runtime::effects::Effect>::EFFECT_TYPE
                    })
                    .map(|attachment| attachment.factory.as_ref()),
            )?;
        }
        if let Some(bound) = self.direct_fact_plan.maximum_bound() {
            let surface = self.generated_surface.unwrap_or("generated stage");
            resources
                .backpressure_writer
                .validate_generated_direct_bound_for(
                    bound,
                    self.generated_owner_kind,
                    Some(&self.name),
                )
                .map_err(|message| format!("{surface}: {message}"))?;
        }
        let effect_declarations = self.effects.clone();
        validate_effect_declarations(
            &self.name,
            &effect_declarations,
            &resources.effect_ports,
            resources
                .runtime_execution
                .effect_port_registration_policy(),
        )?;
        validate_effect_policy_attachments(
            &self.name,
            &effect_declarations,
            &self.effect_policies,
        )?;
        resources.effect_declarations = effect_declarations.clone();
        resources.direct_fact_plan = self.direct_fact_plan.clone();

        for factory in &self.observers {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Transform, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        let observer_factories = self.observers;
        let control_strategy = create_default_signal_strategy();

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // `observers:` is already an observation-only lane. Effect observers
        // fan out over the declared effects while the same declaration index is
        // reused for every surface/subject produced by that one list entry.
        let mut shell_specs = Vec::new();
        let mut effect_observers = StageObserverSet::default();
        for (observer_index, factory) in observer_factories.into_iter().enumerate() {
            let declaration = factory.declaration();
            reject_control_in_observers(factory.as_ref())?;
            let observes_effect = declaration.supports(MiddlewareSurfaceKind::Effect);
            let observes_shell =
                declaration_has_stage_observer_surface(&declaration, StageType::Transform);
            if observes_effect {
                materialize_effect_observers_for_declarations(
                    &mut effect_observers,
                    factory.as_ref(),
                    EffectObserverMaterialization {
                        config: &config,
                        stage_type: StageType::Transform,
                        control_middleware: &control_middleware,
                        declaration_index: MiddlewareDeclarationIndex::observers(observer_index),
                        effect_declarations: &effect_declarations,
                    },
                )?;
            }
            if observes_shell {
                shell_specs.push((observer_index, factory));
            } else if !observes_effect {
                return Err(format!(
                    "observer middleware '{}' has no compatible surface on effectful stage '{}'",
                    declaration.label, self.name
                )
                .into());
            }
        }

        let inline_policy_declarations: Vec<MiddlewareDeclaration> = self
            .effect_policies
            .iter()
            .map(|attachment| attachment.factory.declaration())
            .collect();

        // Validate before materialising any factory.
        for effect in &effect_declarations {
            let declarations = self
                .effect_policies
                .iter()
                .zip(&inline_policy_declarations)
                .filter(|(attachment, _)| attachment.effect_type == effect.effect_type)
                .map(|(_, declaration)| declaration.clone())
                .collect::<Vec<_>>();
            obzenflow_adapters::middleware::validate_effect_control_composition(
                &self.name,
                effect.effect_type,
                &declarations,
            )
            .map_err(|error| error.to_string())?;
        }

        let mut effect_chains: std::collections::HashMap<
            &'static str,
            Vec<obzenflow_adapters::middleware::EffectPolicyAttachment>,
        > = std::collections::HashMap::new();

        for (attachment, declaration) in
            self.effect_policies.iter().zip(&inline_policy_declarations)
        {
            let effect_declaration = effect_declarations
                .iter()
                .find(|effect| effect.effect_type == attachment.effect_type)
                .ok_or_else(|| {
                    format!(
                        "Effectful stage '{}' attaches policy middleware to undeclared effect '{}'",
                        self.name, attachment.effect_type
                    )
                })?;
            let policy = crate::dsl::binder::bind_effect_policy(
                crate::dsl::binder::DeclaredMiddlewareFactory::new(
                    attachment.factory.as_ref(),
                    declaration,
                ),
                &config,
                StageType::Transform,
                &control_middleware,
                effect_declaration,
                MiddlewareDeclarationIndex::effect_with(),
            )?;
            effect_chains
                .entry(attachment.effect_type)
                .or_default()
                .push(policy);
        }

        let effect_policy_chains: std::collections::HashMap<
            &'static str,
            Arc<Vec<obzenflow_adapters::middleware::EffectPolicyAttachment>>,
        > = effect_chains
            .into_iter()
            .map(|(effect_type, chain)| (effect_type, Arc::new(chain)))
            .collect();

        let placement = plan_positioned_stage_observers(
            &config,
            StageType::Transform,
            shell_specs,
            &control_middleware,
        )?;
        let mut observers = placement.observers;
        observers.extend(effect_observers);

        // Stage-level control binding covers shell instances only; per-effect
        // instances register under their effect key and surface through the
        // per-effect snapshot extension (FLOWIP-120c phase 4).
        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        let transform_config = TransformConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            observers: observers.build(),
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
            cycle_guard: config.cycle_guard,
        };

        let effect_boundary: Arc<dyn EffectBoundary> =
            Arc::new(PerEffectPolicyBoundary::new(effect_policy_chains));
        let mut effectful_handler =
            EffectfulTransformHandlerAdapter::new(self.handler, effect_boundary);
        if let Some(event_type) = self.pass_through_event_type {
            effectful_handler = effectful_handler.with_exact_pass_through_event_type(event_type);
        }

        let handle = TransformBuilder::new(effectful_handler, transform_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build effectful async transform: {e:?}"))?;

        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Transform,
            translate_stage_event_to_transform,
            check_transform_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for sink stages
pub(crate) struct SinkDescriptor<C: SinkConnector + 'static> {
    pub(crate) name: String,
    pub(crate) connector: C,
    pub(crate) description: SinkDescription,
    pub(crate) sink_policies: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<C: SinkConnector + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for SinkDescriptor<C>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Sink
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        Some(&self.description)
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.stage_middleware_factories()
            .into_iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.sink_policies
            .iter()
            .map(Box::as_ref)
            .chain(self.observers.iter().map(Box::as_ref))
            .collect()
    }

    fn positioned_stage_middleware_factories(
        &self,
    ) -> Vec<(MiddlewareDeclarationPosition, &dyn MiddlewareFactory)> {
        self.sink_policies
            .iter()
            .map(|factory| (MiddlewareDeclarationPosition::SinkWith, factory.as_ref()))
            .chain(
                self.observers
                    .iter()
                    .map(|factory| (MiddlewareDeclarationPosition::Observers, factory.as_ref())),
            )
            .collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in self.sink_policies.iter().chain(self.observers.iter()) {
            // Validate safety
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Sink, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
                // Could choose to panic here for critical errors
            }
        }

        let sink_policy_factories = self.sink_policies;
        let observer_factories = self.observers;
        let control_strategy = create_default_signal_strategy();

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let expects_circuit_breaker = sink_policy_factories
            .iter()
            .any(|factory| factory_declares_circuit_breaker(factory.as_ref()));
        let expects_rate_limiter = sink_policy_factories.iter().any(|factory| {
            factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::RateLimiter)
        });

        // FLOWIP-115b: hook-bound control middleware that attaches to the
        // sink-delivery surface is materialized into a sink policy composed at
        // the delivery boundary. Observers arrive through their own lane.
        let mut sink_policies: Vec<Arc<dyn SinkPolicy>> = Vec::new();
        let observer_placement = plan_stage_observers(
            &config,
            StageType::Sink,
            observer_factories,
            &control_middleware,
        )?;
        let observers = observer_placement.observers;
        for (sink_policy_index, factory) in sink_policy_factories.into_iter().enumerate() {
            let policy = crate::dsl::binder::materialize_sink_delivery(
                factory.as_ref(),
                &config,
                StageType::Sink,
                &control_middleware,
                MiddlewareDeclarationIndex::sink_with(sink_policy_index),
            )?;
            sink_policies.push(policy);
        }
        let sink_delivery_boundary: Option<Arc<dyn SinkDeliveryBoundary>> =
            if sink_policies.is_empty() {
                None
            } else {
                Some(Arc::new(PerSinkDeliveryPolicyBoundary::new(sink_policies))
                    as Arc<dyn SinkDeliveryBoundary>)
            };

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // The typed factory captured this exactly once before erasure and
        // middleware wrapping. The binding-derived fallback is resolved only
        // now, after the final stage name exists.
        let receipt_destination = self.description.destination_name().map(str::to_owned);
        let default_delivery_method = self.description.default_method().cloned();

        // Create the stage configuration
        let sink_config = JournalSinkConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            upstream_stages: resources.upstream_stages.clone(),
            buffer_size: None,
            flush_interval_ms: None,
            control_strategy: Some(control_strategy),
            sink_delivery_boundary,
            observers: observers.build(),
            receipt_destination,
            default_delivery_method: default_delivery_method.clone(),
        };

        // Open the configured connector only at stage materialisation, then
        // erase its unique mutable writer behind the journal sink boundary.
        let writer_context = SinkWriterInitContext::new(
            config.stage_id,
            config.name.clone(),
            config.flow_name.clone(),
        );
        let writer = self
            .connector
            .open(writer_context)
            .await
            .map_err(|error| format!("Failed to open sink connector: {error}"))?;
        let handler = SinkWriterAdapter::with_default_method(
            writer,
            config.stage_id,
            default_delivery_method,
        );
        let handle = JournalSinkBuilder::new(handler, sink_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build sink: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Sink,
            translate_stage_event_to_sink,
            check_sink_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

// Event translation functions

fn translate_stage_event_to_finite_source<H>(
    event: StageEvent,
) -> Result<FiniteSourceEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(FiniteSourceEvent::Initialize),
        StageEvent::Ready => Ok(FiniteSourceEvent::Ready),
        StageEvent::Start => Ok(FiniteSourceEvent::Start),
        StageEvent::BeginDrain => Ok(FiniteSourceEvent::BeginDrain),
        StageEvent::ForceShutdown => {
            Ok(FiniteSourceEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string()))
        }
        _ => Err(format!(
            "Unsupported stage event for finite source: {event:?}"
        )),
    }
}

fn check_finite_source_state<H>(
    state: &FiniteSourceState<H>,
) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        FiniteSourceState::Created => StageStatus::Created,
        FiniteSourceState::Initialized | FiniteSourceState::WaitingForGun => StageStatus::Ready,
        FiniteSourceState::Running => StageStatus::Running,
        FiniteSourceState::Draining => StageStatus::Draining,
        FiniteSourceState::Drained => StageStatus::Drained,
        FiniteSourceState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

fn translate_stage_event_to_infinite_source<H>(
    event: StageEvent,
) -> Result<InfiniteSourceEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(InfiniteSourceEvent::Initialize),
        StageEvent::Ready => Ok(InfiniteSourceEvent::Ready),
        StageEvent::Start => Ok(InfiniteSourceEvent::Start),
        StageEvent::BeginDrain => Ok(InfiniteSourceEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(InfiniteSourceEvent::Error(
            FORCE_SHUTDOWN_MESSAGE.to_string(),
        )),
        _ => Err(format!(
            "Unsupported stage event for infinite source: {event:?}"
        )),
    }
}

fn check_infinite_source_state<H>(
    state: &InfiniteSourceState<H>,
) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        InfiniteSourceState::Created => StageStatus::Created,
        InfiniteSourceState::Initialized | InfiniteSourceState::WaitingForGun => StageStatus::Ready,
        InfiniteSourceState::Running => StageStatus::Running,
        InfiniteSourceState::Draining => StageStatus::Draining,
        InfiniteSourceState::Drained => StageStatus::Drained,
        InfiniteSourceState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

fn translate_stage_event_to_transform<H>(event: StageEvent) -> Result<TransformEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(TransformEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(TransformEvent::Ready), // Transforms don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(TransformEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(TransformEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string())),
        _ => Err(format!("Unsupported stage event for transform: {event:?}")),
    }
}

fn check_transform_state<H>(state: &TransformState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        TransformState::Created => StageStatus::Created,
        TransformState::Initialized => StageStatus::Ready,
        TransformState::Running => StageStatus::Running,
        TransformState::Draining => StageStatus::Draining,
        TransformState::Drained => StageStatus::Drained,
        TransformState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

fn translate_stage_event_to_sink<H>(event: StageEvent) -> Result<JournalSinkEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(JournalSinkEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(JournalSinkEvent::Ready), // Sinks don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(JournalSinkEvent::BeginDrain),
        StageEvent::ForceShutdown => {
            Ok(JournalSinkEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string()))
        }
        _ => Err(format!("Unsupported stage event for sink: {event:?}")),
    }
}

fn check_sink_state<H>(state: &JournalSinkState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        JournalSinkState::Created => StageStatus::Created,
        JournalSinkState::Initialized => StageStatus::Ready,
        JournalSinkState::Running => StageStatus::Running,
        JournalSinkState::Flushing | JournalSinkState::Draining => StageStatus::Draining,
        JournalSinkState::Drained => StageStatus::Drained,
        JournalSinkState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

// ============================================================================
// Stateful Descriptor (FLOWIP-080b)
// ============================================================================

/// Descriptor for stateful transform stages
pub(crate) struct StatefulDescriptor<H: UnifiedStatefulHandler + 'static> {
    pub(crate) name: String,
    pub(crate) handler: H,
    pub(crate) emit_interval: Option<Duration>,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
    pub(crate) backpressure: Option<BackpressureClause>,
}

#[async_trait]
impl<H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for StatefulDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Stateful
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.observers
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.observers.iter().map(Box::as_ref).collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.observers {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Stateful, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        let observers = self.observers;
        let control_strategy = create_default_signal_strategy();

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement =
            plan_stage_observers(&config, StageType::Stateful, observers, &control_middleware)?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Create the stage configuration
        let stateful_config = StatefulConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            observers: placement.observers.build(),
            emit_interval: self.emit_interval,
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
        };

        // Use the builder to create the handle
        let handle = StatefulBuilder::new(self.handler, stateful_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build stateful stage: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Stateful,
            translate_stage_event_to_stateful,
            check_stateful_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for replay-safe effectful stateful stages.
///
/// Input-driven only (FLOWIP-120z): there is no emission interval, because
/// the effectful surface has no periodic-emission position.
pub struct EffectfulStatefulDescriptor<H: EffectfulStatefulHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub effects: Vec<EffectDeclaration>,
    pub observers: Vec<Box<dyn MiddlewareFactory>>,
    pub backpressure: Option<BackpressureClause>,
}

impl<H: EffectfulStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    EffectfulStatefulDescriptor<H>
{
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        Self {
            name: name.into(),
            handler,
            effects: Vec::new(),
            observers: Vec::new(),
            backpressure: None,
        }
    }

    pub fn with_observer<M: MiddlewareFactory + 'static>(mut self, observer: M) -> Self {
        self.observers.push(Box::new(observer));
        self
    }

    pub fn with_effect_declarations(mut self, effects: Vec<EffectDeclaration>) -> Self {
        self.effects = effects;
        self
    }

    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: EffectfulStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for EffectfulStatefulDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn backpressure_clause(&self) -> Option<&BackpressureClause> {
        self.backpressure.as_ref()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Stateful
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn is_effectful(&self) -> bool {
        true
    }

    fn policy_guard_surface(&self) -> PolicyGuardSurface {
        PolicyGuardSurface::EffectfulStatefulPendingBoundary
    }

    fn stage_logic_version(&self) -> String {
        self.handler.stage_logic_version().to_string()
    }

    fn effect_declarations(&self) -> Vec<EffectDeclaration> {
        self.effects.clone()
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.observers
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.observers.iter().map(Box::as_ref).collect()
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        mut resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let effect_declarations = self.effects.clone();
        validate_effect_declarations(
            &self.name,
            &effect_declarations,
            &resources.effect_ports,
            resources
                .runtime_execution
                .effect_port_registration_policy(),
        )?;
        resources.effect_declarations = effect_declarations.clone();

        for factory in &self.observers {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Stateful, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        let observer_factories = self.observers;
        let control_strategy = create_default_signal_strategy();

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let mut shell_specs = Vec::new();
        let mut effect_observers = StageObserverSet::default();
        for (observer_index, factory) in observer_factories.into_iter().enumerate() {
            let declaration = factory.declaration();
            reject_control_in_observers(factory.as_ref())?;
            if declaration.supports(MiddlewareSurfaceKind::Effect) {
                materialize_effect_observers_for_declarations(
                    &mut effect_observers,
                    factory.as_ref(),
                    EffectObserverMaterialization {
                        config: &config,
                        stage_type: StageType::Stateful,
                        control_middleware: &control_middleware,
                        declaration_index: MiddlewareDeclarationIndex::observers(observer_index),
                        effect_declarations: &effect_declarations,
                    },
                )?;
                if declaration_has_stage_observer_surface(&declaration, StageType::Stateful) {
                    shell_specs.push((observer_index, factory));
                }
            } else {
                shell_specs.push((observer_index, factory));
            }
        }

        let placement = plan_positioned_stage_observers(
            &config,
            StageType::Stateful,
            shell_specs,
            &control_middleware,
        )?;
        let mut observers = placement.observers;
        observers.extend(effect_observers);

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        let stateful_config = StatefulConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            observers: observers.build(),
            // Input-driven only (FLOWIP-120z): the effectful surface never
            // arms the supervisor's emission timer.
            emit_interval: None,
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
        };

        let handle = StatefulBuilder::new(
            EffectfulStatefulHandlerAdapter(self.handler),
            stateful_config,
            resources,
        )
        .with_instrumentation(instrumentation)
        .build()
        .await
        .map_err(|e| format!("Failed to build effectful stateful stage: {e:?}"))?;

        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Stateful,
            translate_stage_event_to_stateful,
            check_stateful_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

fn translate_stage_event_to_stateful<H>(event: StageEvent) -> Result<StatefulEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(StatefulEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(StatefulEvent::Ready), // Stateful stages use Ready like transforms
        StageEvent::BeginDrain => Ok(StatefulEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(StatefulEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string())),
        _ => Err(format!("Unsupported stage event for stateful: {event:?}")),
    }
}

fn check_stateful_state<H>(state: &StatefulState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        StatefulState::Created => StageStatus::Created,
        StatefulState::Initialized => StageStatus::Ready,
        StatefulState::Accumulating | StatefulState::Emitting => StageStatus::Running,
        StatefulState::Draining => StageStatus::Draining,
        StatefulState::Drained => StageStatus::Drained,
        StatefulState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

// ============================================================================
// Join Descriptor (FLOWIP-080l)
// ============================================================================

/// Descriptor for join stages
pub(crate) struct JoinDescriptor<H: UnifiedJoinHandler + 'static> {
    pub(crate) name: String,
    pub(crate) reference_stage_id: StageId,
    pub(crate) reference_stage_var: Option<&'static str>, // For DSL resolution - stage variable name
    pub(crate) handler: H,
    pub(crate) observers: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for JoinDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Join
    }

    fn sink_description(&self) -> Option<&SinkDescription> {
        None
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.observers
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> Vec<&dyn MiddlewareFactory> {
        self.observers.iter().map(Box::as_ref).collect()
    }

    fn reference_stage_id(&self) -> Option<StageId> {
        Some(self.reference_stage_id)
    }

    fn reference_stage_name(&self) -> Option<&str> {
        self.reference_stage_var
    }

    fn set_reference_stage_id(&mut self, id: StageId) {
        self.reference_stage_id = id;
    }

    /// FLOWIP-095d: a hydrating join is a structural deterministic orderer.
    /// The phase boundary (reference consumed to authored EOF, then stream)
    /// pins the reference-versus-stream order with no merge machinery. The
    /// claim is per-boundary: a multi-upstream stream side still needs the
    /// subscription-level canonical merge within that side, which the join
    /// builder wires when the flow build marks the stage.
    fn is_deterministic_input_orderer(&self) -> bool {
        self.handler.reference_mode() == JoinReferenceMode::FiniteEof
    }

    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.observers {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Join, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        let observers = self.observers;
        let control_strategy = create_default_signal_strategy();

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement =
            plan_stage_observers(&config, StageType::Join, observers, &control_middleware)?;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Extract join-mode configuration from the handler before moving it into the runtime.
        let reference_mode = self.handler.reference_mode();
        let reference_batch_cap = self.handler.reference_batch_cap();

        // Create the stage configuration
        // reference_stage_id comes from the builder (stored in self)
        // Stream stages come from topology (in upstream_stages, after DSL adds reference)
        let reference_source_id = self.reference_stage_id;

        // Get stream sources - all upstreams after the reference (which DSL prepended)
        let stream_sources: Vec<StageId> = resources
            .upstream_stages
            .iter()
            .skip(1) // Skip reference which is at index 0
            .copied()
            .collect();

        // For now, we support single stream source
        let stream_source_id = stream_sources
            .first()
            .copied()
            .ok_or_else(|| "Join stage requires at least one stream source".to_string())?;

        let join_config = JoinConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            observers: placement.observers.build(),
            flow_name: config.flow_name.clone(),
            reference_source_id,
            stream_source_id,
            reference_mode,
            reference_batch_cap,
            control_strategy: Some(control_strategy.clone()),
            upstream_stages: resources.upstream_stages.clone(),
        };

        // Separate reference and stream journals
        // First upstream is reference, rest are streams
        let (reference_journal, stream_journals) =
            if let Some((first, rest)) = resources.upstream_journals.split_first() {
                (first.1.clone(), rest.to_vec())
            } else {
                return Err("Join stage requires at least one upstream journal".into());
            };

        // Use the builder to create the handle
        // NOTE: For join stages, the pre-built subscription in resources is stale
        // because DSL mutates upstream_journals AFTER subscription was built
        let handle = JoinBuilder::new(
            self.handler,
            join_config,
            resources,
            reference_journal,
            stream_journals,
            control_strategy,
        )
        .map_err(|e| format!("Failed to create join builder: {e}"))?
        .with_instrumentation(instrumentation)
        .build()
        .await
        .map_err(|e| format!("Failed to build join stage: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Join,
            translate_stage_event_to_join,
            check_join_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

fn translate_stage_event_to_join<H>(event: StageEvent) -> Result<JoinEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(JoinEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(JoinEvent::Ready), // Join stages use Ready like transforms
        StageEvent::BeginDrain => Ok(JoinEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(JoinEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string())),
        _ => Err(format!("Unsupported stage event for join: {event:?}")),
    }
}

fn check_join_state<H>(state: &JoinState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        JoinState::Created => StageStatus::Created,
        JoinState::Initialized => StageStatus::Ready,
        JoinState::Hydrating | JoinState::Live | JoinState::Enriching => StageStatus::Running,
        JoinState::Draining => StageStatus::Draining,
        JoinState::Drained => StageStatus::Drained,
        JoinState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

impl<H: UnifiedFiniteSourceHandler + 'static> sealed::Sealed for FiniteSourceDescriptor<H> {}
impl<H: UnifiedAsyncFiniteSourceHandler + 'static> sealed::Sealed
    for AsyncFiniteSourceDescriptor<H>
{
}
impl<H: UnifiedInfiniteSourceHandler + 'static> sealed::Sealed for InfiniteSourceDescriptor<H> {}
impl<H: UnifiedAsyncInfiniteSourceHandler + 'static> sealed::Sealed
    for AsyncInfiniteSourceDescriptor<H>
{
}
impl<H: TransformHandler + 'static> sealed::Sealed for TransformDescriptor<H> {}
impl<H: EffectfulTransformHandler + 'static> sealed::Sealed for EffectfulTransformDescriptor<H> {}
impl<C: SinkConnector + 'static> sealed::Sealed for SinkDescriptor<C> {}
impl<H: UnifiedStatefulHandler + 'static> sealed::Sealed for StatefulDescriptor<H> {}
impl<H: EffectfulStatefulHandler + 'static> sealed::Sealed for EffectfulStatefulDescriptor<H> {}
impl<H: UnifiedJoinHandler + 'static> sealed::Sealed for JoinDescriptor<H> {}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_adapters::middleware::CircuitBreaker;
    use obzenflow_core::event::{JournalEvent, SystemEvent};
    use obzenflow_core::{ChainEvent, EventEnvelope, FlowId, StageKey, TypedPayload};
    use obzenflow_runtime::control_plane::ControlPlaneProvider;
    use obzenflow_runtime::effects::{
        Effect, EffectCommitHandle, EffectContext, EffectError, TransactionalEffectPort,
    };
    use obzenflow_runtime::message_bus::FsmMessageBus;
    use obzenflow_runtime::stages::common::handlers::source::traits::FiniteSourceHandler;
    use obzenflow_runtime::stages::common::handlers::source::traits::{
        AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler,
    };
    use obzenflow_runtime::stages::resources_builder::SubscriptionFactory;
    use obzenflow_runtime::stages::LivenessSnapshots;
    use serde_json::json;

    fn effective_config_for_stage(
        stage_name: &str,
        factories: &[&dyn obzenflow_adapters::middleware::MiddlewareFactory],
    ) -> Arc<obzenflow_runtime::runtime_config::FlowEffectiveConfig> {
        use obzenflow_core::config::ConfigScope;
        use obzenflow_runtime::runtime_config::{
            materialize_flow_config, DslCandidates, FlowResolutionContext, ResolvedRuntimeConfig,
        };

        let mut dsl = DslCandidates::default();
        for factory in factories {
            for key_path in factory.consumed_config_keys() {
                dsl.declare_stage_consumption(key_path, StageKey::from(stage_name));
            }
            for default in factory.dsl_config_defaults() {
                dsl.declare(
                    default.key_path,
                    ConfigScope::stage(stage_name),
                    default.value,
                );
            }
        }
        Arc::new(
            materialize_flow_config(
                &ResolvedRuntimeConfig::builtin_defaults(),
                FlowResolutionContext {
                    flow_name: "stage_descriptor_test".to_string(),
                    stages: std::collections::BTreeSet::from([StageKey::from(stage_name)]),
                    edges: std::collections::BTreeSet::new(),
                    declared_effects: Default::default(),
                    dsl,
                },
            )
            .expect("factory defaults materialize"),
        )
    }

    trait DemoEffectPort: Send + Sync {}

    struct DemoEffectPortImpl;

    impl DemoEffectPort for DemoEffectPortImpl {}

    #[derive(Clone, Debug)]
    struct DemoTransactionalEffect;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct DemoTransactionalOutput;

    impl TypedPayload for DemoTransactionalOutput {
        const EVENT_TYPE: &'static str = "test.transactional_declared_output";
    }

    #[async_trait]
    impl Effect for DemoTransactionalEffect {
        const EFFECT_TYPE: &'static str = "test.transactional_declared";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Transactional;

        type Outcome = DemoTransactionalOutput;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "declared"
        }

        fn canonical_input(&self) -> serde_json::Value {
            json!({})
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(DemoTransactionalOutput)
        }
    }

    struct DemoTransactionalPort;

    #[async_trait]
    impl TransactionalEffectPort<DemoTransactionalEffect> for DemoTransactionalPort {
        async fn execute_and_commit(
            &self,
            _effect: DemoTransactionalEffect,
            _ctx: &mut EffectContext,
            commit: EffectCommitHandle<DemoTransactionalOutput>,
        ) -> Result<DemoTransactionalOutput, EffectError> {
            let output = DemoTransactionalOutput;
            commit.commit_success(&output).await?;
            Ok(output)
        }
    }

    #[test]
    fn effect_declaration_validation_rejects_missing_key_strategy() {
        let declaration = EffectDeclaration {
            effect_type: "test.non_idempotent",
            safety: EffectSafety::NonIdempotentRequiresKey,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: None,
            outcome_kind: obzenflow_runtime::effects::EffectOutcomeKind::DomainFacts,
            public_outcome_fact_types: Vec::new(),
        };

        let err = validate_effect_declarations(
            "effectful",
            &[declaration],
            &EffectPortRegistry::new(),
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect_err("missing key strategy must fail materialisation");

        assert!(err.contains("without an idempotency-key strategy"));
    }

    #[test]
    fn effect_policy_attachment_validation_rejects_undeclared_effect() {
        let attachment = EffectPolicyAttachment {
            effect_type: "test.undeclared",
            factory: Box::new(
                obzenflow_adapters::middleware::IndicatorMiddlewareFactory::new()
                    .operation("test.undeclared_effect")
                    .indicator("test.latency"),
            ),
        };

        let error = validate_effect_policy_attachments("effectful", &[], &[attachment])
            .expect_err("an attachment for an undeclared effect must fail materialisation");

        assert_eq!(
            error,
            "Effectful stage 'effectful' attaches policy middleware to undeclared effect 'test.undeclared'"
        );
    }

    #[test]
    fn effect_declaration_validation_requires_explicit_at_least_once_acknowledgement() {
        let declaration = EffectDeclaration {
            effect_type: "obzenflow.ai.chat_completion",
            safety: EffectSafety::NonIdempotentAtLeastOnce,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: None,
            outcome_kind: obzenflow_runtime::effects::EffectOutcomeKind::RecordedReply,
            public_outcome_fact_types: Vec::new(),
        };

        let error = validate_effect_declarations(
            "standalone_chat",
            &[declaration],
            &EffectPortRegistry::new(),
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect_err("a bare paid effect must be rejected");
        assert_eq!(
            error,
            "Effectful stage 'standalone_chat' declares paid non-idempotent effect \
             'obzenflow.ai.chat_completion' without explicit at_least_once(...) acknowledgement"
        );
    }

    #[derive(Clone, Debug)]
    struct DemoDuplicateEffect;

    #[async_trait]
    impl Effect for DemoDuplicateEffect {
        const EFFECT_TYPE: &'static str = "test.duplicate";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Outcome = DemoTransactionalOutput;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "duplicate"
        }

        fn canonical_input(&self) -> serde_json::Value {
            json!({})
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(DemoTransactionalOutput)
        }
    }

    #[derive(Clone, Debug)]
    struct DemoPortedEffect;

    #[async_trait]
    impl Effect for DemoPortedEffect {
        const EFFECT_TYPE: &'static str = "test.ported";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Outcome = DemoTransactionalOutput;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "ported"
        }

        fn canonical_input(&self) -> serde_json::Value {
            json!({})
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(DemoTransactionalOutput)
        }
    }

    #[test]
    fn effect_declaration_validation_rejects_duplicate_effect_type() {
        let declarations = [
            EffectDeclaration::of::<DemoDuplicateEffect>(),
            EffectDeclaration::of::<DemoDuplicateEffect>(),
        ];

        let err = validate_effect_declarations(
            "effectful",
            &declarations,
            &EffectPortRegistry::new(),
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect_err("duplicate effect type must fail materialisation");

        assert!(err.contains("more than once"));
    }

    #[test]
    fn effect_declaration_validation_checks_required_ports() {
        let declaration = EffectDeclaration::of::<DemoPortedEffect>()
            .require_port::<dyn DemoEffectPort>("primary");

        let missing = validate_effect_declarations(
            "effectful",
            std::slice::from_ref(&declaration),
            &EffectPortRegistry::new(),
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect_err("missing required port must fail materialisation");
        assert!(missing.contains("requires effect port"));

        let mut registry = EffectPortRegistry::new();
        registry
            .insert::<dyn DemoEffectPort>("primary", Arc::new(DemoEffectPortImpl))
            .expect("unique effect port");

        validate_effect_declarations(
            "effectful",
            &[declaration],
            &registry,
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect("registered required port should pass");
    }

    #[test]
    fn effect_declaration_validation_transactional_effect_requires_typed_port() {
        let declaration = EffectDeclaration::transactional_effect::<DemoTransactionalEffect>("tx");

        let missing = validate_effect_declarations(
            "effectful",
            std::slice::from_ref(&declaration),
            &EffectPortRegistry::new(),
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect_err("missing transactional port must fail materialisation");
        assert!(missing.contains("requires effect port"));

        let mut registry = EffectPortRegistry::new();
        registry
            .insert::<dyn TransactionalEffectPort<DemoTransactionalEffect>>(
                "tx",
                Arc::new(DemoTransactionalPort),
            )
            .expect("unique transactional port");

        validate_effect_declarations(
            "effectful",
            &[declaration],
            &registry,
            obzenflow_runtime::execution::EffectPortRegistrationPolicy::Required,
        )
        .expect("registered transactional typed port should pass");
    }

    #[derive(Clone, Debug)]
    struct DummyFiniteSource;

    impl FiniteSourceHandler for DummyFiniteSource {
        fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            // This dummy source never emits data; it's used only to verify that the
            // circuit breaker middleware wires up a CircuitBreakerSourceStrategy.
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncFiniteSourceDefault;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceDefault {
        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncFiniteSourceConfigured;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceConfigured {
        fn poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(123))
        }

        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncFiniteSourceDisabled;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceDisabled {
        fn poll_timeout(&self) -> Option<Duration> {
            None
        }

        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncInfiniteSourceDefault;

    #[async_trait]
    impl AsyncInfiniteSourceHandler for DummyAsyncInfiniteSourceDefault {
        async fn next(
            &mut self,
        ) -> Result<Vec<ChainEvent>, obzenflow_runtime::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncInfiniteSourceConfigured;

    #[async_trait]
    impl AsyncInfiniteSourceHandler for DummyAsyncInfiniteSourceConfigured {
        fn poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(7))
        }

        async fn next(
            &mut self,
        ) -> Result<Vec<ChainEvent>, obzenflow_runtime::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn async_finite_source_descriptor_carries_configured_poll_timeout() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("configured", DummyAsyncFiniteSourceConfigured);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(123)));
    }

    #[test]
    fn async_finite_source_descriptor_uses_handler_contract_default() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("defaulted", DummyAsyncFiniteSourceDefault);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn async_finite_source_descriptor_preserves_configured_disabled_timeout() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("disabled", DummyAsyncFiniteSourceDisabled);
        assert_eq!(descriptor.poll_timeout, None);
    }

    #[test]
    fn async_infinite_source_descriptor_carries_configured_poll_timeout() {
        let descriptor =
            AsyncInfiniteSourceDescriptor::new("configured", DummyAsyncInfiniteSourceConfigured);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(7)));
    }

    #[test]
    fn async_infinite_source_descriptor_uses_handler_contract_default() {
        let descriptor =
            AsyncInfiniteSourceDescriptor::new("defaulted", DummyAsyncInfiniteSourceDefault);
        assert_eq!(descriptor.poll_timeout, None);
    }

    #[test]
    fn hosted_ingress_source_fills_slot_and_binds_limiter_to_ingress() {
        use obzenflow_core::ingress::{
            HostedIngressBindingSlot, IngressAdmissionDecision, IngressAttemptContext,
            IngressAttemptSeq,
        };

        let stage_id = StageId::new();
        let limiter = obzenflow_adapters::middleware::control::rate_limiter::rate_limit(1.0);
        let config = StageConfig {
            stage_id,
            name: "accounts".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: effective_config_for_stage("accounts", &[limiter.as_ref()]),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let slot = HostedIngressBindingSlot::new("bank.accounts");

        // A hosted ingress source with a rate limiter in the explicit ingress
        // control position.
        build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            WriterId::from(stage_id),
            SourceMiddlewarePlan {
                source_policy_factories: vec![],
                ingress_policy_factory: Some(limiter),
                observer_factories: vec![],
                hosted_ingress_slot: Some(slot.clone()),
            },
            &control,
        )
        .expect("source middleware build");

        // The DSL fills the slot with the materialized ingress boundary.
        assert!(slot.is_filled(), "the DSL fills the hosted-ingress slot");
        let filled = slot.filled().expect("slot filled");
        assert_eq!(filled.stage_key, "accounts");
        let boundary = filled
            .boundary
            .as_ref()
            .expect("the ingress boundary is materialized");

        // FLOWIP-115d AC42: the limiter binds to Ingress, not source poll. The
        // binder-local source policy list is empty, so only this slot boundary
        // can pace the hosted request.

        // The materialized boundary rate-limits: the burst token admits, then a
        // fail-fast reject (never waiting).
        let attempt = IngressAttemptContext {
            attempt_seq: IngressAttemptSeq(0),
            request_count: 1,
            event_count: 1,
            batch_count: 0,
        };
        assert!(matches!(
            boundary.on_ingress(&attempt),
            IngressAdmissionDecision::Accept
        ));
        assert!(matches!(
            boundary.on_ingress(&attempt),
            IngressAdmissionDecision::Reject { .. }
        ));
    }

    #[test]
    fn ingress_position_requires_a_hosted_ingress_route() {
        let stage_id = StageId::new();
        let limiter = obzenflow_adapters::middleware::control::rate_limiter::rate_limit(1.0);
        let config = StageConfig {
            stage_id,
            name: "pull_source".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: effective_config_for_stage("pull_source", &[limiter.as_ref()]),
        };

        let error = match build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            WriterId::from(stage_id),
            SourceMiddlewarePlan {
                source_policy_factories: vec![],
                ingress_policy_factory: Some(limiter),
                observer_factories: vec![],
                hosted_ingress_slot: None,
            },
            &Arc::new(ControlMiddlewareAggregator::new()),
        ) {
            Ok(_) => panic!("an ingress position without a hosted route must fail"),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "'ingress with <policy>' requires a hosted ingress route on stage 'pull_source' (FLOWIP-115s)"
        );
    }

    #[test]
    fn hosted_ingress_rejects_a_source_poll_rate_limiter_without_an_ingress_policy() {
        use obzenflow_core::ingress::HostedIngressBindingSlot;

        let stage_id = StageId::new();
        let limiter = obzenflow_adapters::middleware::control::rate_limiter::rate_limit(1.0);
        let config = StageConfig {
            stage_id,
            name: "accounts".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: effective_config_for_stage("accounts", &[limiter.as_ref()]),
        };
        let slot = HostedIngressBindingSlot::new("bank.accounts");

        let error = match build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            WriterId::from(stage_id),
            SourceMiddlewarePlan {
                source_policy_factories: vec![limiter],
                ingress_policy_factory: None,
                observer_factories: vec![],
                hosted_ingress_slot: Some(slot.clone()),
            },
            &Arc::new(ControlMiddlewareAggregator::new()),
        ) {
            Ok(_) => panic!("a hosted drain must not acquire a source-poll limiter"),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "stage 'accounts' hosts an ingress route; attach its rate limiter as 'ingress with <policy>', not to the post-admission drain in 'with [...]' (FLOWIP-115s)"
        );
        assert!(
            !slot.is_filled(),
            "a rejected placement must not fill the slot"
        );
    }

    /// FLOWIP-115d AC55: a third-party (non-framework) control middleware that
    /// admits the first attempt then refuses, declaring only the `Ingress`
    /// surface. It implements the core boundary trait and materializes through the
    /// same public carrier as the built-in limiter, with no framework enum branch,
    /// downcast, or legacy shell route.
    struct AllowOnceIngressFactory;
    struct AllowOnceIngressFamily;
    struct AllowOnceBoundary {
        admitted: std::sync::atomic::AtomicBool,
    }

    impl obzenflow_core::ingress::IngressBoundaryMiddleware for AllowOnceBoundary {
        fn label(&self) -> &'static str {
            "allow_once"
        }
        fn on_ingress(
            &self,
            _attempt: &obzenflow_core::ingress::IngressAttemptContext,
        ) -> obzenflow_core::ingress::IngressAdmissionDecision {
            if self
                .admitted
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                obzenflow_core::ingress::IngressAdmissionDecision::Reject { retry_after: None }
            } else {
                obzenflow_core::ingress::IngressAdmissionDecision::Accept
            }
        }
        fn observe(
            &self,
            _attempt: &obzenflow_core::ingress::IngressAttemptContext,
            _outcome: obzenflow_core::ingress::IngressAdmissionOutcome,
        ) {
        }
    }

    impl MiddlewareFactory for AllowOnceIngressFactory {
        fn label(&self) -> &'static str {
            "allow_once_ingress"
        }
        fn override_key(&self) -> obzenflow_adapters::middleware::MiddlewareOverrideKey {
            obzenflow_adapters::middleware::MiddlewareOverrideKey::of::<AllowOnceIngressFamily>(
                "allow_once_ingress",
            )
        }
        fn declaration(&self) -> obzenflow_adapters::middleware::MiddlewareDeclaration {
            obzenflow_adapters::middleware::MiddlewareDeclaration::control_with_family(
                self.label(),
                "allow_once_ingress",
                vec![MiddlewareSurfaceKind::Ingress],
            )
        }
        fn materialize(
            &self,
            request: obzenflow_adapters::middleware::MiddlewareAttachmentRequest<'_>,
            context: &obzenflow_adapters::middleware::MiddlewareMaterializationContext<'_>,
        ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<
            obzenflow_adapters::middleware::MiddlewareSurfaceAttachment,
        > {
            use obzenflow_adapters::middleware::{
                validate_attachment_request, MiddlewareFactoryError, MiddlewareSurface,
                MiddlewareSurfaceAttachment,
            };
            validate_attachment_request(&self.declaration(), &request).map_err(|e| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    e,
                )
            })?;
            match request.surface {
                MiddlewareSurface::Ingress(_) => Ok(MiddlewareSurfaceAttachment::ingress(
                    std::sync::Arc::new(AllowOnceBoundary {
                        admitted: std::sync::atomic::AtomicBool::new(false),
                    }),
                )),
                other => Err(MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    std::io::Error::other(format!(
                        "unsupported allow-once ingress surface {:?}",
                        other.kind()
                    )),
                )),
            }
        }
    }

    #[test]
    fn hosted_ingress_routes_third_party_control_middleware_through_carrier() {
        use obzenflow_core::ingress::{
            HostedIngressBindingSlot, IngressAdmissionDecision, IngressAttemptContext,
            IngressAttemptSeq,
        };

        let stage_id = StageId::new();
        let config = StageConfig {
            stage_id,
            name: "accounts".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: std::sync::Arc::new(
                obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
            ),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let slot = HostedIngressBindingSlot::new("bank.accounts");

        // The same build -> materialize carrier path as the built-in limiter,
        // but with a user-authored factory.
        build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            WriterId::from(stage_id),
            SourceMiddlewarePlan {
                source_policy_factories: vec![],
                ingress_policy_factory: Some(Box::new(AllowOnceIngressFactory)),
                observer_factories: vec![],
                hosted_ingress_slot: Some(slot.clone()),
            },
            &control,
        )
        .expect("source middleware build");

        // The ingress position bound the third-party factory to Ingress and
        // filled the slot with its own boundary; no source-poll policy and no
        // framework-specific branch was needed.
        let filled = slot.filled().expect("slot filled");
        let boundary = filled.boundary.as_ref().expect("third-party boundary");
        assert_eq!(boundary.label(), "allow_once");

        let attempt = IngressAttemptContext {
            attempt_seq: IngressAttemptSeq(0),
            request_count: 1,
            event_count: 1,
            batch_count: 0,
        };
        assert!(matches!(
            boundary.on_ingress(&attempt),
            IngressAdmissionDecision::Accept
        ));
        assert!(matches!(
            boundary.on_ingress(&attempt),
            IngressAdmissionDecision::Reject { .. }
        ));
    }

    #[tokio::test]
    async fn finite_source_with_circuit_breaker_uses_cb_strategy() {
        let stage_id = StageId::new();
        let breaker = CircuitBreaker::builder()
            .consecutive_failures(1)
            .build()
            .expect("source breaker configuration");
        let config = StageConfig {
            stage_id,
            name: "cb_source".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: effective_config_for_stage("cb_source", &[&breaker]),
        };

        // Minimal StageResources: journals are never actually written in this unit test.
        use obzenflow_core::id::JournalId;
        use obzenflow_core::journal::journal_error::JournalError;
        use obzenflow_core::journal::journal_owner::JournalOwner;
        use obzenflow_core::journal::journal_reader::JournalReader;
        use obzenflow_core::journal::Journal;

        struct NoopJournal<T: JournalEvent> {
            id: JournalId,
            owner: Option<JournalOwner>,
            _marker: std::marker::PhantomData<T>,
        }

        impl<T: JournalEvent> NoopJournal<T> {
            fn new(owner: JournalOwner) -> Self {
                Self {
                    id: JournalId::new(),
                    owner: Some(owner),
                    _marker: std::marker::PhantomData,
                }
            }
        }

        struct NoopReader;

        #[async_trait]
        impl<T: JournalEvent + 'static> Journal<T> for NoopJournal<T> {
            fn id(&self) -> &JournalId {
                &self.id
            }

            fn owner(&self) -> Option<&JournalOwner> {
                self.owner.as_ref()
            }

            async fn append(
                &self,
                _event: T,
                _parent: Option<&EventEnvelope<T>>,
            ) -> Result<EventEnvelope<T>, JournalError> {
                Err(JournalError::Implementation {
                    message: "noop journal".to_string(),
                    source: "noop".into(),
                })
            }

            async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_event(
                &self,
                _event_id: &obzenflow_core::EventId,
            ) -> Result<Option<EventEnvelope<T>>, JournalError> {
                Ok(None)
            }

            async fn reader_from(
                &self,
                _position: u64,
            ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
                Ok(Box::new(NoopReader))
            }

            async fn read_last_n(
                &self,
                _count: usize,
            ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
                // NoopJournal never stores events; always return empty.
                Ok(Vec::new())
            }
        }

        #[async_trait]
        impl<T: JournalEvent + 'static> JournalReader<T> for NoopReader {
            async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
                Ok(None)
            }

            fn position(&self) -> u64 {
                0
            }

            fn is_at_end(&self) -> bool {
                true
            }
        }

        let system_owner = JournalOwner::system(obzenflow_core::SystemId::new());
        let stage_owner = JournalOwner::stage(stage_id);

        let data_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(NoopJournal::new(stage_owner.clone()));
        let error_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(NoopJournal::new(stage_owner.clone()));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(NoopJournal::new(system_owner));

        let topology_stage_id = obzenflow_topology::StageId::from_ulid(stage_id.as_ulid());
        let topology_stage = obzenflow_topology::StageInfo::new(
            topology_stage_id,
            "cb_source",
            obzenflow_topology::StageType::FiniteSource,
        );
        let dummy_stage_id =
            obzenflow_topology::StageId::from_ulid(obzenflow_core::StageId::new().as_ulid());
        let dummy_stage = obzenflow_topology::StageInfo::new(
            dummy_stage_id,
            "dummy",
            obzenflow_topology::StageType::Sink,
        );
        let topology = obzenflow_topology::Topology::new_unvalidated(
            vec![topology_stage, dummy_stage],
            vec![obzenflow_topology::DirectedEdge::new(
                topology_stage_id,
                dummy_stage_id,
                obzenflow_topology::EdgeKind::Forward,
            )],
        )
        .expect("topology");
        let backpressure_registry =
            std::sync::Arc::new(obzenflow_runtime::backpressure::BackpressureRegistry::new(
                &topology,
                &obzenflow_runtime::backpressure::BackpressurePlan::disabled(),
            ));

        let resources = StageResources {
            flow_id: FlowId::new(),
            lineage_policy: obzenflow_core::config::LineagePolicy::default(),
            heartbeat_interval: 1000,
            data_journal,
            error_journal,
            system_journal,
            upstream_journals: Vec::new(),
            upstream_stage_names: std::collections::HashMap::new(),
            output_contract: Default::default(),
            input_feeds: Vec::new(),
            subscription_factory: SubscriptionFactory::new(std::collections::HashMap::new()),
            upstream_subscription_factory: SubscriptionFactory::new(
                std::collections::HashMap::new(),
            )
            .bind(&[]),
            message_bus: Arc::new(FsmMessageBus::new()),
            upstream_stages: Vec::new(),
            error_journals: Vec::new(),
            backpressure_writer: Default::default(),
            backpressure_readers: Default::default(),
            backpressure_registry,
            liveness_snapshots: LivenessSnapshots::new(),
            runtime_execution: obzenflow_runtime::execution::RuntimeExecution::new(
                obzenflow_runtime::execution::RuntimeMode::Live,
                None,
            ),
            effect_ports: obzenflow_runtime::effects::EffectPortRegistry::new(),
            effect_declarations: Vec::new(),
            direct_fact_plan: obzenflow_runtime::stages::resources_builder::DirectFactPlan::default(
            ),
            deterministic_fan_in: false,
            seq_ordered_fan_in: false,
        };

        let descriptor = FiniteSourceDescriptor {
            name: "cb_source".to_string(),
            handler: DummyFiniteSource,
            source_policies: vec![Box::new(breaker)],
            ingress_policy: None,
            observers: vec![],
            backpressure: None,
        };

        let control_middleware = Arc::new(ControlMiddlewareAggregator::new());
        let boxed: Box<dyn StageDescriptor> = Box::new(descriptor);
        let handle = boxed
            .create_handle(config, resources, control_middleware.clone())
            .await
            .expect("handle creation should succeed");

        // Ensure the breaker state is registered via the flow-scoped provider.
        let cb_state = control_middleware.circuit_breaker_state_view(&stage_id);
        assert!(
            cb_state.is_some(),
            "circuit breaker state view should be registered for source with circuit_breaker middleware"
        );

        // Avoid unused variable warning
        drop(handle);
    }
}

// ============================================================================
// ErrorSink Descriptor (FLOWIP-082e)
// ============================================================================

/// FLOWIP-115f AC 12 / AC 25: observer middleware binds through the placement
/// planner's observer path on every stage type.
#[cfg(test)]
mod observer_placement_negative_tests {
    use super::*;
    use obzenflow_adapters::middleware::{
        validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
        MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
        MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
        MiddlewareSurfaceKind, SourceAdmission, SourcePolicy, SourcePolicyCtx,
        SourcePollAttachment, SourcePollOutcome,
    };
    use obzenflow_runtime::stages::observer::{
        HandlerObserver, JoinObserver, SinkDeliveryObserver, SourcePollObserver,
        StageLifecycleObserver, StatefulObserver,
    };
    use std::sync::Mutex;

    /// A no-op observer that supports every observer surface this factory may be
    /// placed on. All hooks use the trait defaults (record nothing).
    struct NoopObserver;
    impl HandlerObserver for NoopObserver {
        fn label(&self) -> &'static str {
            "loud-observer"
        }
    }
    impl StatefulObserver for NoopObserver {
        fn label(&self) -> &'static str {
            "loud-observer"
        }
    }
    impl JoinObserver for NoopObserver {
        fn label(&self) -> &'static str {
            "loud-observer"
        }
    }
    impl SourcePollObserver for NoopObserver {
        fn label(&self) -> &'static str {
            "loud-observer"
        }
    }
    impl SinkDeliveryObserver for NoopObserver {
        fn label(&self) -> &'static str {
            "loud-observer"
        }
    }
    impl StageLifecycleObserver for NoopObserver {
        fn label(&self) -> &'static str {
            "loud-observer"
        }
    }

    /// An observer factory used to prove the planner reaches typed materialization.
    struct LoudObserverFactory;
    struct LoudObserverFamily;

    type MaterializationCall = (
        &'static str,
        MiddlewareSurfaceKind,
        MiddlewareDeclarationPosition,
        u64,
    );

    struct RecordingObserverFactory {
        label: &'static str,
        calls: Arc<Mutex<Vec<MaterializationCall>>>,
    }
    struct RecordingObserverFamily;

    struct RecordingSourceControlFactory<Family> {
        label: &'static str,
        calls: Arc<Mutex<Vec<MaterializationCall>>>,
        _family: std::marker::PhantomData<Family>,
    }
    struct FirstRecordingSourceFamily;
    struct SecondRecordingSourceFamily;
    struct NoopSourceControl;

    #[async_trait::async_trait]
    impl SourcePolicy for NoopSourceControl {
        fn label(&self) -> &'static str {
            "recording-source-control"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            SourceAdmission::Admit(None)
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {}
    }

    impl<Family: Send + Sync + 'static> MiddlewareFactory for RecordingSourceControlFactory<Family> {
        fn label(&self) -> &'static str {
            self.label
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<Family>(self.label())
        }

        fn declaration(&self) -> MiddlewareDeclaration {
            MiddlewareDeclaration::control_with_family(
                self.label(),
                self.override_key().family_label(),
                vec![MiddlewareSurfaceKind::SourcePoll],
            )
        }

        fn materialize(
            &self,
            request: MiddlewareAttachmentRequest<'_>,
            context: &MiddlewareMaterializationContext<'_>,
        ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
            validate_attachment_request(&self.declaration(), &request).map_err(|err| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    err,
                )
            })?;
            self.calls
                .lock()
                .expect("recording source-control call lock poisoned")
                .push((
                    self.label,
                    request.surface.kind(),
                    request.declaration_index.position(),
                    request.declaration_index.ordinal(),
                ));
            Ok(MiddlewareSurfaceAttachment::source_poll(
                SourcePollAttachment {
                    policy: Arc::new(NoopSourceControl),
                    completion_gate: None,
                },
            ))
        }
    }

    impl MiddlewareFactory for RecordingObserverFactory {
        fn label(&self) -> &'static str {
            self.label
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<RecordingObserverFamily>(self.label())
        }

        fn declaration(&self) -> MiddlewareDeclaration {
            MiddlewareDeclaration::observer_with_family(
                self.label(),
                self.override_key().family_label(),
                vec![
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareSurfaceKind::StageLifecycle,
                ],
            )
        }

        fn materialize(
            &self,
            request: MiddlewareAttachmentRequest<'_>,
            context: &MiddlewareMaterializationContext<'_>,
        ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
            let declaration = self.declaration();
            validate_attachment_request(&declaration, &request).map_err(|err| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    err,
                )
            })?;
            self.calls
                .lock()
                .expect("recording observer call lock poisoned")
                .push((
                    self.label,
                    request.surface.kind(),
                    request.declaration_index.position(),
                    request.declaration_index.ordinal(),
                ));

            let observer = Arc::new(NoopObserver);
            match request.surface.kind() {
                MiddlewareSurfaceKind::SourcePoll => {
                    Ok(MiddlewareSurfaceAttachment::source_poll_observer(observer))
                }
                MiddlewareSurfaceKind::StageLifecycle => Ok(
                    MiddlewareSurfaceAttachment::stage_lifecycle_observer(observer),
                ),
                other => Err(MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    obzenflow_runtime::stages::observer::ObserverCommitError::new(format!(
                        "unsupported recording observer surface {other:?}"
                    )),
                )),
            }
        }
    }

    impl MiddlewareFactory for LoudObserverFactory {
        fn label(&self) -> &'static str {
            "loud-observer"
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<LoudObserverFamily>(self.label())
        }

        fn declaration(&self) -> MiddlewareDeclaration {
            MiddlewareDeclaration::observer_with_family(
                self.label(),
                self.override_key().family_label(),
                vec![
                    MiddlewareSurfaceKind::Handler,
                    MiddlewareSurfaceKind::Stateful,
                    MiddlewareSurfaceKind::Join,
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareSurfaceKind::SinkDelivery,
                ],
            )
        }

        fn materialize(
            &self,
            request: MiddlewareAttachmentRequest<'_>,
            context: &MiddlewareMaterializationContext<'_>,
        ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
            let declaration = self.declaration();
            validate_attachment_request(&declaration, &request).map_err(|err| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    err,
                )
            })?;
            let observer = Arc::new(NoopObserver);
            match request.surface.kind() {
                MiddlewareSurfaceKind::Handler => {
                    Ok(MiddlewareSurfaceAttachment::handler_observer(observer))
                }
                MiddlewareSurfaceKind::Stateful => {
                    Ok(MiddlewareSurfaceAttachment::stateful_observer(observer))
                }
                MiddlewareSurfaceKind::Join => {
                    Ok(MiddlewareSurfaceAttachment::join_observer(observer))
                }
                MiddlewareSurfaceKind::SourcePoll => {
                    Ok(MiddlewareSurfaceAttachment::source_poll_observer(observer))
                }
                MiddlewareSurfaceKind::SinkDelivery => Ok(
                    MiddlewareSurfaceAttachment::sink_delivery_observer(observer),
                ),
                other => Err(MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    obzenflow_runtime::stages::observer::ObserverCommitError::new(format!(
                        "unsupported loud observer surface {other:?}"
                    )),
                )),
            }
        }
    }

    struct WrongSurfaceObserverFactory;
    struct WrongSurfaceObserverFamily;

    impl MiddlewareFactory for WrongSurfaceObserverFactory {
        fn label(&self) -> &'static str {
            "wrong-surface-observer"
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<WrongSurfaceObserverFamily>(self.label())
        }

        fn declaration(&self) -> MiddlewareDeclaration {
            MiddlewareDeclaration::observer(self.label(), vec![MiddlewareSurfaceKind::SourcePoll])
        }

        fn materialize(
            &self,
            _request: MiddlewareAttachmentRequest<'_>,
            _context: &MiddlewareMaterializationContext<'_>,
        ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
            Ok(MiddlewareSurfaceAttachment::handler_observer(Arc::new(
                NoopObserver,
            )))
        }
    }

    struct ObserverReturningControlFactory;
    struct ObserverReturningControlFamily;
    struct NoopEffectControl;

    #[async_trait::async_trait]
    impl obzenflow_adapters::middleware::EffectPolicy for NoopEffectControl {
        fn label(&self) -> &'static str {
            "noop-effect-control"
        }

        async fn admit(
            &self,
            _ctx: &mut obzenflow_adapters::middleware::MiddlewareContext,
        ) -> obzenflow_adapters::middleware::PolicyAdmission {
            obzenflow_adapters::middleware::PolicyAdmission::Admit
        }

        fn observe(
            &self,
            _attempt: &obzenflow_adapters::middleware::EffectAttemptOutcome<'_>,
            _ctx: &mut obzenflow_adapters::middleware::MiddlewareContext,
        ) {
        }
    }

    impl MiddlewareFactory for ObserverReturningControlFactory {
        fn label(&self) -> &'static str {
            "observer-returning-control"
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<ObserverReturningControlFamily>(self.label())
        }

        fn declaration(&self) -> MiddlewareDeclaration {
            MiddlewareDeclaration::observer(self.label(), vec![MiddlewareSurfaceKind::SourcePoll])
        }

        fn materialize(
            &self,
            _request: MiddlewareAttachmentRequest<'_>,
            _context: &MiddlewareMaterializationContext<'_>,
        ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
            Ok(MiddlewareSurfaceAttachment::effect(Arc::new(
                NoopEffectControl,
            )))
        }
    }

    #[test]
    fn observer_binds_through_placement_on_every_stage_type_without_legacy_create() {
        // StageType collapses pure and effectful transform descriptors to
        // `Transform`, so the six variants cover every planner stage kind.
        for stage_type in [
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ] {
            let stage_id = StageId::new();
            let config = StageConfig {
                stage_id,
                name: format!("loud_{stage_type:?}"),
                flow_name: "observer_placement_negative".to_string(),
                cycle_guard: None,
                lineage: obzenflow_core::config::LineagePolicy::default(),
                effective_config: std::sync::Arc::new(
                    obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
                ),
            };
            let control = Arc::new(ControlMiddlewareAggregator::new());
            plan_stage_observers(
                &config,
                stage_type,
                vec![Box::new(LoudObserverFactory)],
                &control,
            )
            .unwrap_or_else(|err| {
                panic!("observer placement must succeed for {stage_type:?}: {err}")
            });
        }
    }

    #[test]
    fn observer_fan_out_reuses_the_original_observers_lane_ordinal() {
        let config = StageConfig {
            stage_id: StageId::new(),
            name: "observer_fan_out".to_string(),
            flow_name: "observer_placement_negative".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: Arc::new(
                obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
            ),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let calls = Arc::new(Mutex::new(Vec::new()));

        plan_positioned_stage_observers(
            &config,
            StageType::FiniteSource,
            vec![
                (
                    2,
                    Box::new(RecordingObserverFactory {
                        label: "first-recording-observer",
                        calls: calls.clone(),
                    }),
                ),
                (
                    5,
                    Box::new(RecordingObserverFactory {
                        label: "second-recording-observer",
                        calls: calls.clone(),
                    }),
                ),
            ],
            &control,
        )
        .expect("observer fan-out should materialize");

        assert_eq!(
            *calls.lock().expect("recording observer call lock poisoned"),
            vec![
                (
                    "first-recording-observer",
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareDeclarationPosition::Observers,
                    2,
                ),
                (
                    "first-recording-observer",
                    MiddlewareSurfaceKind::StageLifecycle,
                    MiddlewareDeclarationPosition::Observers,
                    2,
                ),
                (
                    "second-recording-observer",
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareDeclarationPosition::Observers,
                    5,
                ),
                (
                    "second-recording-observer",
                    MiddlewareSurfaceKind::StageLifecycle,
                    MiddlewareDeclarationPosition::Observers,
                    5,
                ),
            ]
        );
    }

    #[test]
    fn source_control_and_observer_lanes_number_independently() {
        let stage_id = StageId::new();
        let config = StageConfig {
            stage_id,
            name: "lane_local_source".to_string(),
            flow_name: "observer_placement_negative".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: Arc::new(
                obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
            ),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let calls = Arc::new(Mutex::new(Vec::new()));

        build_source_middleware_and_register_policies(
            &config,
            StageType::FiniteSource,
            WriterId::from(stage_id),
            SourceMiddlewarePlan {
                source_policy_factories: vec![
                    Box::new(
                        RecordingSourceControlFactory::<FirstRecordingSourceFamily> {
                            label: "first-source-control",
                            calls: calls.clone(),
                            _family: std::marker::PhantomData,
                        },
                    ),
                    Box::new(
                        RecordingSourceControlFactory::<SecondRecordingSourceFamily> {
                            label: "second-source-control",
                            calls: calls.clone(),
                            _family: std::marker::PhantomData,
                        },
                    ),
                ],
                ingress_policy_factory: None,
                observer_factories: vec![Box::new(RecordingObserverFactory {
                    label: "source-observer",
                    calls: calls.clone(),
                })],
                hosted_ingress_slot: None,
            },
            &control,
        )
        .expect("positioned source middleware should materialize");

        assert_eq!(
            *calls
                .lock()
                .expect("recording middleware call lock poisoned"),
            vec![
                (
                    "source-observer",
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareDeclarationPosition::Observers,
                    0,
                ),
                (
                    "source-observer",
                    MiddlewareSurfaceKind::StageLifecycle,
                    MiddlewareDeclarationPosition::Observers,
                    0,
                ),
                (
                    "first-source-control",
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareDeclarationPosition::SourceWith,
                    0,
                ),
                (
                    "second-source-control",
                    MiddlewareSurfaceKind::SourcePoll,
                    MiddlewareDeclarationPosition::SourceWith,
                    1,
                ),
            ]
        );
    }

    #[test]
    fn binder_rejects_wrong_observer_surface_and_control_attachment() {
        let config = StageConfig {
            stage_id: StageId::new(),
            name: "malicious_observer".to_string(),
            flow_name: "observer_placement_negative".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: Arc::new(
                obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
            ),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let wrong_surface = match crate::dsl::binder::materialize_observer(
            &WrongSurfaceObserverFactory,
            &config,
            StageType::InfiniteSource,
            &control,
            MiddlewareSurfaceKind::SourcePoll,
            MiddlewareDeclarationIndex::observers(0),
        ) {
            Ok(_) => panic!("a source observer cannot return a handler observer"),
            Err(error) => error,
        };
        assert!(wrong_surface.contains("returned Observer/Handler"));

        let wrong_capability = match crate::dsl::binder::materialize_observer(
            &ObserverReturningControlFactory,
            &config,
            StageType::InfiniteSource,
            &control,
            MiddlewareSurfaceKind::SourcePoll,
            MiddlewareDeclarationIndex::observers(1),
        ) {
            Ok(_) => panic!("an observer cannot return a control attachment"),
            Err(error) => error,
        };
        assert!(wrong_capability.contains("returned Control/Effect"));
    }
}
