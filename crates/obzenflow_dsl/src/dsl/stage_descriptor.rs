// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use crate::dsl::typing::StageTypingMetadata;
use crate::dsl::StageCreationResult;
use crate::stage_handle_adapter::StageHandleAdapter;
use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::StageObserverSet;
use obzenflow_adapters::middleware::{
    validate_middleware_safety, AsyncFiniteSourceHandlerExt, AsyncInfiniteSourceHandlerExt,
    AsyncTransformHandlerExt, ControlMiddlewareRole, FiniteSourceHandlerExt,
    InfiniteSourceHandlerExt, JoinHandlerMiddlewareExt, Middleware, MiddlewareDeclaration,
    MiddlewareDeclarationIndex, MiddlewareFactory, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, PerSinkDeliveryPolicyBoundary, PerSourcePolicyBoundary, SinkHandlerExt,
    SinkPolicy, StatefulHandlerMiddlewareExt, TopologyMiddlewareConfigSlot, TransformHandlerExt,
    UnifiedMiddlewareTransform,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::{
    effects::{
        EffectDeclaration, EffectPortRegistry, EffectSafety, IdempotencyKeyPolicy,
        SynthesizedOutcomeRegistration,
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
                AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
                DestinationOrder, EffectfulSinkHandler, EffectfulStatefulHandler,
                EffectfulStatefulHandlerAdapter, EffectfulTransformHandler,
                EffectfulTransformHandlerAdapter, FiniteSourceHandler, InfiniteSourceHandler,
                InputOrderSemantics, JoinHandler, SinkHandler, StatefulHandler, TransformHandler,
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

const DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT: Duration = Duration::from_secs(30);

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

fn create_system_middleware(
    _config: &StageConfig,
    _stage_type: StageType,
) -> Vec<Box<dyn Middleware>> {
    Vec::new()
}

fn create_system_observers(_config: &StageConfig) -> StageObserverSet {
    // No built-in observers (FLOWIP-115f): `processing_time` is stamped by the
    // runtime output committer from the instrumentation timer, not by an
    // observer, and the user-facing observation middleware are `indicator()` and
    // `log()`. User-attached observers are merged onto this empty default.
    StageObserverSet::default()
}

fn create_legacy_shell(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control_middleware: Arc<ControlMiddlewareAggregator>,
) -> StageCreationResult<Box<dyn Middleware>> {
    let declaration = factory.declaration();
    if !declaration.is_legacy_shell() {
        return Err(format!(
            "middleware '{}' declares hook surfaces {:?} and cannot fall back to legacy create() on stage '{}'",
            factory.label(),
            declaration.surfaces,
            config.name
        )
        .into());
    }
    Ok(factory.create(config, control_middleware)?)
}

fn create_deprecated_async_policy_shell(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control_middleware: Arc<ControlMiddlewareAggregator>,
) -> StageCreationResult<Box<dyn Middleware>> {
    let declaration = factory.declaration();
    if factory.kind() != obzenflow_adapters::middleware::MiddlewareKind::Policy
        || declaration.is_observer()
    {
        return Err(format!(
            "middleware '{}' declares hook surfaces {:?} and cannot use the deprecated async policy shell on stage '{}'",
            factory.label(),
            declaration.surfaces,
            config.name
        )
        .into());
    }
    Ok(factory.create(config, control_middleware)?)
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
    attachment: MiddlewareSurfaceAttachment,
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
    origin: &'a obzenflow_adapters::middleware::MiddlewareOrigin,
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
            effect.effect_type,
            materialization.origin,
            materialization.declaration_index,
        )?;
        push_observer_attachment(observers, attachment)?;
    }
    Ok(())
}

struct SourceMiddlewareBinding {
    all_middleware: Vec<Box<dyn Middleware>>,
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
    legacy_shell: Vec<Box<dyn Middleware>>,
    observers: StageObserverSet,
    expects_circuit_breaker: bool,
    expects_rate_limiter: bool,
}

fn plan_stage_middleware(
    config: &StageConfig,
    stage_type: StageType,
    resolved: crate::middleware_resolution::ResolvedMiddleware,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    allow_deprecated_async_policy_shell: bool,
) -> StageCreationResult<MiddlewarePlacement> {
    let expects_circuit_breaker = resolved
        .middleware
        .iter()
        .any(|spec| factory_declares_circuit_breaker(spec.factory.as_ref()));
    let expects_rate_limiter = resolved.middleware.iter().any(|spec| {
        spec.factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::RateLimiter)
    });

    let mut legacy_shell = create_system_middleware(config, stage_type);
    let mut observers = create_system_observers(config);
    let observer_surfaces = observer_surfaces_for_stage(stage_type);

    for (middleware_index, spec) in resolved.middleware.into_iter().enumerate() {
        let declaration = spec.factory.declaration();
        if declaration.is_observer() {
            let mut placed = false;
            let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
            for surface in observer_surfaces {
                if !declaration.supports(*surface) {
                    continue;
                }
                let attachment = crate::dsl::binder::materialize_observer(
                    spec.factory.as_ref(),
                    config,
                    stage_type,
                    control_middleware,
                    *surface,
                    &origin,
                    MiddlewareDeclarationIndex::resolved(middleware_index),
                )?;
                push_observer_attachment(&mut observers, attachment)?;
                placed = true;
            }
            if !placed {
                return Err(format!(
                    "observer middleware '{}' declares surfaces {:?}, but stage '{}' ({stage_type:?}) has no compatible observer surface",
                    declaration.label,
                    declaration.surfaces,
                    config.name
                )
                .into());
            }
            continue;
        }

        if declaration.is_legacy_shell() {
            legacy_shell.push(create_legacy_shell(
                spec.factory.as_ref(),
                config,
                control_middleware.clone(),
            )?);
            continue;
        }

        if allow_deprecated_async_policy_shell
            && declaration.is_control()
            && spec.factory.kind() == obzenflow_adapters::middleware::MiddlewareKind::Policy
        {
            legacy_shell.push(create_deprecated_async_policy_shell(
                spec.factory.as_ref(),
                config,
                control_middleware.clone(),
            )?);
            continue;
        }

        return Err(format!(
            "middleware '{}' declares hook surfaces {:?}, but stage '{}' ({stage_type:?}) has no compatible placement",
            declaration.label,
            declaration.surfaces,
            config.name
        )
        .into());
    }

    Ok(MiddlewarePlacement {
        legacy_shell,
        observers,
        expects_circuit_breaker,
        expects_rate_limiter,
    })
}

fn build_source_middleware_and_register_policies(
    config: &StageConfig,
    stage_type: StageType,
    writer_id: WriterId,
    resolved: crate::middleware_resolution::ResolvedMiddleware,
    hosted_ingress_slot: Option<obzenflow_core::ingress::HostedIngressBindingSlot>,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
) -> StageCreationResult<SourceMiddlewareBinding> {
    let mut all_middleware = create_system_middleware(config, stage_type);
    let mut observers = create_system_observers(config);
    let expects_circuit_breaker = resolved
        .middleware
        .iter()
        .any(|spec| factory_declares_circuit_breaker(spec.factory.as_ref()));
    let expects_rate_limiter = resolved.middleware.iter().any(|spec| {
        spec.factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::RateLimiter)
    });

    let mut completion_gate: Option<Arc<dyn CompletionGate>> = None;
    // FLOWIP-115d: the ingress boundary materialized for a source-backed hosted
    // ingress source, filled into the shared binding slot below.
    let mut ingress_boundary: Option<Arc<dyn obzenflow_core::ingress::IngressBoundaryMiddleware>> =
        None;

    for (middleware_index, spec) in resolved.middleware.into_iter().enumerate() {
        let declaration = spec.factory.declaration();
        if declaration.is_observer() {
            let mut placed = false;
            let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
            for surface in observer_surfaces_for_stage(stage_type) {
                if !declaration.supports(*surface) {
                    continue;
                }
                let attachment = crate::dsl::binder::materialize_observer(
                    spec.factory.as_ref(),
                    config,
                    stage_type,
                    control_middleware,
                    *surface,
                    &origin,
                    MiddlewareDeclarationIndex::resolved(middleware_index),
                )?;
                push_observer_attachment(&mut observers, attachment)?;
                placed = true;
            }
            if !placed {
                return Err(format!(
                    "observer middleware '{}' declares surfaces {:?}, but source stage '{}' has no compatible observer surface",
                    declaration.label,
                    declaration.surfaces,
                    config.name
                )
                .into());
            }
            continue;
        }

        // FLOWIP-115d AC42: a source-backed hosted ingress source binds a control
        // middleware that declares Ingress to the ingress boundary, not source
        // poll, and does not also pace the internal mpsc-drain source poll. This
        // branch precedes the source-poll branch so a limiter declaring both
        // surfaces routes to Ingress for hosted sources.
        if let Some(slot) = hosted_ingress_slot.as_ref() {
            if declaration.is_control() && declaration.supports(MiddlewareSurfaceKind::Ingress) {
                let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
                let boundary = crate::dsl::binder::materialize_ingress(
                    spec.factory.as_ref(),
                    config,
                    stage_type,
                    control_middleware,
                    slot.ingress_key(),
                    &origin,
                    MiddlewareDeclarationIndex::resolved(middleware_index),
                )?;
                ingress_boundary = Some(boundary);
                continue;
            }
        }

        // FLOWIP-115b: hook-bound control middleware that attaches to the source
        // poll surface is placed by its declaration, not by ControlMiddlewareRole.
        // It is materialized into a source policy registered in declared order so
        // it composes with any still-legacy source policy (the rate limiter), and
        // it supplies the completion-gate companion that shares its state view.
        if declaration.is_control() && declaration.supports(MiddlewareSurfaceKind::SourcePoll) {
            let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
            let binding = crate::dsl::binder::materialize_source_poll(
                spec.factory.as_ref(),
                config,
                stage_type,
                control_middleware,
                &origin,
                MiddlewareDeclarationIndex::resolved(middleware_index),
            )?;
            control_middleware.register_source_policy(config.stage_id, binding.policy);
            if binding.completion_gate.is_some() {
                completion_gate = binding.completion_gate;
            }
            continue;
        }

        match spec.factory.control_role() {
            ControlMiddlewareRole::RateLimiter => {
                spec.factory
                    .register_source_policy(config, stage_type, control_middleware)?;
            }
            ControlMiddlewareRole::CircuitBreaker => {
                // FLOWIP-115b AC59: a breaker reaching the legacy source role path
                // did not declare source hooks. Fail closed rather than silently
                // registering it through the retired route.
                return Err(format!(
                    "Stage {:?}: circuit breaker reached the legacy source role path \
                     without declaring source hooks; hook-bound control must \
                     materialize onto the SourcePoll surface (FLOWIP-115b)",
                    config.stage_id
                )
                .into());
            }
            ControlMiddlewareRole::None => {
                all_middleware.push(create_legacy_shell(
                    spec.factory.as_ref(),
                    config,
                    control_middleware.clone(),
                )?);
            }
        }
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
    }

    let source_policies = control_middleware.source_policies(&config.stage_id);
    let source_boundary = if source_policies.is_empty() {
        None
    } else {
        Some(
            Arc::new(PerSourcePolicyBoundary::new(source_policies, writer_id))
                as Arc<dyn SourceBoundary>,
        )
    };

    Ok(SourceMiddlewareBinding {
        all_middleware,
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
fn create_default_signal_strategy(
    _middleware: &[crate::middleware_resolution::MiddlewareSpec],
) -> Arc<dyn SignalGate> {
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
    /// Async non-effectful transforms: deprecated surface; policy middleware
    /// warns naming FLOWIP-120p and FLOWIP-120f until 120f deletes it.
    AsyncNonEffectful,
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

/// FLOWIP-095l. A stage's relationship to concurrent fan-in delivery order: the
/// input to canonical-merge enablement, as `is_deterministic_input_orderer()` is
/// its output. Observers seed the merge, carriers relay order, barriers absorb
/// it. Illegal states are unrepresentable; consumers query `order_role()`, never
/// matching on `stage_type()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRole {
    /// Reconstruction reads an order-dependent value here (a non-commutative
    /// state fold, or an effect whose cursor is the merged input position).
    Observer,
    /// Relays input order to its output word but reads nothing order-dependent, so
    /// the order is pinned only when a downstream observer requires it. FLOWIP-095l
    /// Gap 13: transforms, sources, and sinks are Carriers by their descriptor type
    /// with no declaration, which trusts that a transform is genuinely stateless (its
    /// output a pure function of each single input). A transform that smuggles state
    /// (`Arc<Mutex>`, an atomic, `thread_local!`) is order-sensitive yet typed Carrier;
    /// enforcing that is FLOWIP-120d's determinism scope, and a CI guard flags the
    /// obvious cases.
    Carrier,
    /// Output is invariant under input permutation, so it absorbs reordering.
    Barrier,
}

/// Trait for stage descriptors that know how to create their supervisors
#[async_trait]
pub trait StageDescriptor: Send + Sync {
    /// Get the stage name
    fn name(&self) -> &str;

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

    /// Create the handle for this stage
    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Default implementation without flow middleware
        self.create_handle_with_flow_middleware(
            config,
            resources,
            vec![],
            Arc::new(ControlMiddlewareAggregator::new()),
        )
        .await
    }

    /// Create the handle for this stage with flow-level middleware
    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
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
    /// Default implementation returns an empty slice; concrete descriptors that
    /// carry middleware should override this to expose their factories for `config_snapshot()`.
    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
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
    /// The default derives from `is_effectful()` and `stage_type()`; the
    /// async-non-effectful transform descriptor overrides, because
    /// `StageType` cannot distinguish it from the sync surface. Typed
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

    /// FLOWIP-095l. This stage's role in fan-in order propagation, the input to
    /// merge enablement. Conservative by default (Observer over-orders, which is
    /// always correctness-safe); carriers and barriers override per descriptor.
    fn order_role(&self) -> OrderRole {
        OrderRole::Observer
    }

    /// FLOWIP-095l Gap 4. Whether the author has explicitly accepted that this
    /// order-sensitive stage runs non-deterministically below a cycle (forfeiting
    /// resume). Default: not accepted, so the build refuses.
    fn accepts_cycle_nondeterminism(&self) -> bool {
        false
    }

    /// FLOWIP-095l Gap 2. Whether this stage is a stateful or symmetric-join order
    /// observer that has not declared its input-order semantics. Such a stage in a
    /// multi-source fan-in cone is a build error: the author must choose
    /// `order_insensitive` or `OrderSensitive`. Default false (effects, transforms,
    /// sources, sinks, and hydrating joins are exempt; their role is inferred).
    fn is_undeclared_order_observer(&self) -> bool {
        false
    }

    fn stage_logic_version(&self) -> String {
        "1".to_string()
    }

    fn effect_declarations(&self) -> Vec<EffectDeclaration> {
        Vec::new()
    }

    /// Typed-outcome middleware registrations from the `output_middleware:`
    /// lane (FLOWIP-120h). Default: none.
    fn synthesized_outcome_registrations(&self) -> Vec<SynthesizedOutcomeRegistration> {
        Vec::new()
    }

    /// Configuration errors detected by the `output_middleware:` lane while
    /// branch types were nameable. Default: none.
    fn type_shaping_config_errors(&self) -> Vec<String> {
        Vec::new()
    }

    /// Whether this descriptor is a composite that must be lowered during `flow!` materialisation.
    ///
    /// Default: `false` for ordinary stages.
    fn is_composite(&self) -> bool {
        false
    }

    /// Lower a composite descriptor into concrete stages + edges.
    ///
    /// Default: returns `Ok(None)` for non-composite descriptors.
    #[allow(clippy::result_large_err)]
    fn try_lower_composite(
        self: Box<Self>,
        _binding: &str,
    ) -> Result<Option<crate::dsl::composites::CompositeLowering>, crate::dsl::FlowBuildError> {
        Ok(None)
    }
}

fn validate_effect_declarations(
    stage_name: &str,
    declarations: &[EffectDeclaration],
    effect_ports: &EffectPortRegistry,
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

        if matches!(declaration.safety, EffectSafety::Transactional)
            && declaration.transactional_executor.is_none()
        {
            return Err(format!(
                "Effectful stage '{stage_name}' declares transactional effect '{}' without a transactional executor",
                declaration.effect_type
            ));
        }

        for requirement in &declaration.required_ports {
            if !effect_ports.contains_requirement(requirement) {
                return Err(format!(
                    "Effectful stage '{stage_name}' requires effect port '{}' for type '{}' but it is not registered",
                    requirement.name, requirement.type_name
                ));
            }
        }
    }

    Ok(())
}

/// Descriptor for finite source stages
pub struct FiniteSourceDescriptor<H: FiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for FiniteSourceDescriptor<H>
{
    fn order_role(&self) -> OrderRole {
        OrderRole::Carrier
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::FiniteSource,
            writer_id,
            resolved,
            None,
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

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        // Apply all middleware
        let mut builder = handler.middleware(writer_id);
        for mw in source_binding.all_middleware {
            builder = builder.with_boxed(mw);
        }
        let handler_with_middleware = builder.build();

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
        let handle = FiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
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

/// Descriptor for async finite source stages.
pub struct AsyncFiniteSourceDescriptor<H: AsyncFiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub poll_timeout: Option<Duration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncFiniteSourceDescriptor<H>
{
    /// Create a new async finite source descriptor with a default 30s poll timeout.
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        let poll_timeout = handler
            .suggested_poll_timeout()
            .or(Some(DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT));
        Self {
            name: name.into(),
            handler,
            poll_timeout,
            middleware: Vec::new(),
        }
    }

    /// Override poll timeout. `None` disables enforcement (handler manages its own).
    pub fn with_poll_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    /// Build into a boxed StageDescriptor for DSL compatibility.
    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for AsyncFiniteSourceDescriptor<H>
{
    fn order_role(&self) -> OrderRole {
        OrderRole::Carrier
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);
        let poll_timeout = self.poll_timeout;

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::FiniteSource,
            writer_id,
            resolved,
            None,
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

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        // Apply all middleware.
        let mut builder = handler
            .middleware(writer_id)
            .with_poll_timeout(poll_timeout);
        for mw in source_binding.all_middleware {
            builder = builder.with_boxed(mw);
        }
        let handler_with_middleware = builder.build();

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
        let handle =
            AsyncFiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
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

/// Descriptor for infinite source stages
pub struct InfiniteSourceDescriptor<H: InfiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for InfiniteSourceDescriptor<H>
{
    fn order_role(&self) -> OrderRole {
        OrderRole::Carrier
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            writer_id,
            resolved,
            None,
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

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081d).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        // Apply all middleware
        let mut builder = handler.middleware(writer_id);
        for mw in source_binding.all_middleware {
            builder = builder.with_boxed(mw);
        }
        let handler_with_middleware = builder.build();

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
        let handle = InfiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
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

/// Descriptor for async infinite source stages.
pub struct AsyncInfiniteSourceDescriptor<H: AsyncInfiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub poll_timeout: Option<Duration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncInfiniteSourceDescriptor<H>
{
    /// Create a new async infinite source descriptor.
    ///
    /// Defaults to no poll timeout so push sources can block efficiently (e.g. `recv().await`).
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        let poll_timeout = handler.suggested_poll_timeout();
        Self {
            name: name.into(),
            handler,
            poll_timeout,
            middleware: Vec::new(),
        }
    }

    /// Override poll timeout. `None` disables enforcement (handler manages its own).
    pub fn with_poll_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    /// Build into a boxed StageDescriptor for DSL compatibility.
    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StageDescriptor for AsyncInfiniteSourceDescriptor<H>
{
    fn order_role(&self) -> OrderRole {
        OrderRole::Carrier
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let writer_id = WriterId::from(config.stage_id);
        let poll_timeout = self.poll_timeout;

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        // FLOWIP-115d: a source-backed hosted ingress source (e.g. http_ingress)
        // exposes its binding slot here; the DSL fills it during this source
        // stage's materialization with the stage id, replay-stable key, and the
        // materialized ingress boundary.
        let hosted_ingress_slot = self.handler.hosted_ingress_slot();

        let source_binding = build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            writer_id,
            resolved,
            hosted_ingress_slot,
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

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081d).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        let mut builder = handler
            .middleware(writer_id)
            .with_poll_timeout(poll_timeout);
        for mw in source_binding.all_middleware {
            builder = builder.with_boxed(mw);
        }
        let handler_with_middleware = builder.build();

        let source_config = InfiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: source_binding.completion_gate,
            source_boundary: source_binding.source_boundary,
            observers: source_binding.observers.build(),
        };

        let handle =
            AsyncInfiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
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
pub struct TransformDescriptor<H: TransformHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for TransformDescriptor<H>
{
    fn order_role(&self) -> OrderRole {
        OrderRole::Carrier
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.middleware {
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

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement = plan_stage_middleware(
            &config,
            StageType::Transform,
            resolved,
            &control_middleware,
            false,
        )?;
        let all_middleware = placement.legacy_shell;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware. The middleware execution scope is computed
        // per event by the supervisor at dispatch (FLOWIP-120c H3).
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

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
        let handle = TransformBuilder::new(handler_with_middleware, transform_config, resources)
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

/// Descriptor for async transform stages.
pub struct AsyncTransformDescriptor<H: AsyncTransformHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: AsyncTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for AsyncTransformDescriptor<H>
{
    fn order_role(&self) -> OrderRole {
        OrderRole::Carrier
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
    }

    fn policy_guard_surface(&self) -> PolicyGuardSurface {
        PolicyGuardSurface::AsyncNonEffectful
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Transform, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement = plan_stage_middleware(
            &config,
            StageType::Transform,
            resolved,
            &control_middleware,
            true,
        )?;
        let all_middleware = placement.legacy_shell;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware. The middleware execution scope is computed
        // per event by the supervisor at dispatch (FLOWIP-120c H3).
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

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

        // Use the builder to create the handle. The async middleware wrapper
        // implements the unified handler surface directly, so async stages
        // build through the same TransformBuilder as every other transform.
        let handle = TransformBuilder::new(handler_with_middleware, transform_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build async transform: {e:?}"))?;

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

/// Per-effect policy attachment (FLOWIP-120c H7): the policies declared
/// inline on one `effects:` entry (`Effect with [...]`), plus the
/// typed-outcome registrations their builders carry.
pub struct EffectPolicyAttachment {
    pub effect_type: &'static str,
    pub factories: Vec<Box<dyn MiddlewareFactory>>,
    pub synthesized: Vec<SynthesizedOutcomeRegistration>,
    pub config_errors: Vec<String>,
}

/// Descriptor for replay-safe effectful async transform stages.
pub struct EffectfulTransformDescriptor<H: EffectfulTransformHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub effects: Vec<EffectDeclaration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
    /// Per-effect policy attachments from the `effects:` clause
    /// (FLOWIP-120c H7).
    pub effect_policies: Vec<EffectPolicyAttachment>,
    /// Typed-outcome registrations from the `output_middleware:` lane
    /// (FLOWIP-120h). Their factories are already in `middleware`.
    pub synthesized_outcomes: Vec<SynthesizedOutcomeRegistration>,
    /// Configuration errors detected by the lane while branch types were
    /// nameable; surfaced as flow build failures.
    pub type_shaping_errors: Vec<String>,
}

#[async_trait]
impl<H: EffectfulTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for EffectfulTransformDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
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

    fn synthesized_outcome_registrations(&self) -> Vec<SynthesizedOutcomeRegistration> {
        let mut registrations = self.synthesized_outcomes.clone();
        for attachment in &self.effect_policies {
            registrations.extend(attachment.synthesized.clone());
        }
        registrations
    }

    fn type_shaping_config_errors(&self) -> Vec<String> {
        let mut errors = self.type_shaping_errors.clone();
        for attachment in &self.effect_policies {
            errors.extend(attachment.config_errors.clone());
        }
        errors
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        mut resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let effect_declarations = self.effects.clone();
        validate_effect_declarations(&self.name, &effect_declarations, &resources.effect_ports)?;
        resources.effect_declarations = effect_declarations.clone();
        resources.synthesized_outcomes = {
            let mut registrations = self.synthesized_outcomes.clone();
            for attachment in &self.effect_policies {
                registrations.extend(attachment.synthesized.clone());
            }
            registrations
        };

        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Transform, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // FLOWIP-120c placement split: policy kinds guard individual effects
        // at the boundary; observation and structural kinds stay on the
        // handler shell. Policy factories still arriving through the stage
        // `middleware:` lane (the transitional `output_middleware:` surface)
        // attach to the stage's single declared effect; a multi-effect stage
        // must name the guarded effect per entry.
        let mut shell_specs = Vec::new();
        let mut transitional_policy_specs = Vec::new();
        let mut effect_observers = StageObserverSet::default();
        for (middleware_index, spec) in resolved.middleware.into_iter().enumerate() {
            let declaration = spec.factory.declaration();
            if declaration.is_observer() && declaration.supports(MiddlewareSurfaceKind::Effect) {
                let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
                materialize_effect_observers_for_declarations(
                    &mut effect_observers,
                    spec.factory.as_ref(),
                    EffectObserverMaterialization {
                        config: &config,
                        stage_type: StageType::Transform,
                        control_middleware: &control_middleware,
                        origin: &origin,
                        declaration_index: MiddlewareDeclarationIndex::resolved(middleware_index),
                        effect_declarations: &effect_declarations,
                    },
                )?;
                if declaration_has_stage_observer_surface(&declaration, StageType::Transform) {
                    shell_specs.push(spec);
                }
            } else if spec.factory.kind() == obzenflow_adapters::middleware::MiddlewareKind::Policy
            {
                transitional_policy_specs.push((middleware_index, spec));
            } else {
                shell_specs.push(spec);
            }
        }

        let mut effect_chains: std::collections::HashMap<
            &'static str,
            Vec<obzenflow_adapters::middleware::EffectPolicyAttachment>,
        > = std::collections::HashMap::new();

        if !transitional_policy_specs.is_empty() {
            if effect_declarations.len() != 1 {
                return Err(format!(
                    "Stage '{}' declares policy middleware in `middleware:` but {} effects; \
                     attach each policy to the effect it guards (FLOWIP-120c H7)",
                    self.name,
                    effect_declarations.len()
                )
                .into());
            }
            let effect_type = effect_declarations[0].effect_type;
            for (middleware_index, spec) in transitional_policy_specs {
                let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
                let policy = crate::dsl::binder::bind_effect_policy(
                    spec.factory.as_ref(),
                    &config,
                    StageType::Transform,
                    &control_middleware,
                    effect_type,
                    &origin,
                    MiddlewareDeclarationIndex::resolved(middleware_index),
                )?;
                effect_chains.entry(effect_type).or_default().push(policy);
            }
        }

        for attachment in &self.effect_policies {
            for (middleware_index, factory) in attachment.factories.iter().enumerate() {
                let policy = crate::dsl::binder::bind_effect_policy(
                    factory.as_ref(),
                    &config,
                    StageType::Transform,
                    &control_middleware,
                    attachment.effect_type,
                    &obzenflow_adapters::middleware::MiddlewareOrigin::Stage,
                    MiddlewareDeclarationIndex::effect_policy(middleware_index),
                )?;
                effect_chains
                    .entry(attachment.effect_type)
                    .or_default()
                    .push(policy);
            }
        }

        let effect_policy_chains: std::collections::HashMap<
            &'static str,
            Arc<Vec<obzenflow_adapters::middleware::EffectPolicyAttachment>>,
        > = effect_chains
            .into_iter()
            .map(|(effect_type, chain)| (effect_type, Arc::new(chain)))
            .collect();

        let placement = plan_stage_middleware(
            &config,
            StageType::Transform,
            crate::middleware_resolution::ResolvedMiddleware {
                middleware: shell_specs,
                overrides: Vec::new(),
                warnings: Vec::new(),
            },
            &control_middleware,
            false,
        )?;
        let mut observers = placement.observers;
        observers.extend(effect_observers);
        let all_middleware = placement.legacy_shell;

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

        // The middleware execution scope is computed per event by the
        // supervisor at dispatch (FLOWIP-120c H3).
        let mut handler_with_middleware =
            UnifiedMiddlewareTransform::new(EffectfulTransformHandlerAdapter(self.handler));
        for mw in all_middleware {
            handler_with_middleware = handler_with_middleware.with_middleware(mw);
        }
        let handler_with_middleware =
            handler_with_middleware.with_effect_policies(effect_policy_chains);

        let handle = TransformBuilder::new(handler_with_middleware, transform_config, resources)
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
pub struct SinkDescriptor<H: SinkHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for SinkDescriptor<H>
{
    /// FLOWIP-095l Gap 14: a sink is terminal for ordering by default (a Carrier),
    /// so delivery-order fidelity is FLOWIP-090f/095g's contract. A sink whose
    /// destination is order-sensitive declares `DestinationOrder::Ordered` and
    /// becomes an Observer, seeding the canonical merge at its upstream fan-in.
    fn order_role(&self) -> OrderRole {
        match self.handler.destination_order() {
            DestinationOrder::Insensitive => OrderRole::Carrier,
            DestinationOrder::Ordered => OrderRole::Observer,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Sink
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.middleware {
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

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(&config, StageType::Sink);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| factory_declares_circuit_breaker(spec.factory.as_ref()));
        let expects_rate_limiter = resolved.middleware.iter().any(|spec| {
            spec.factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::RateLimiter)
        });

        // FLOWIP-115b: hook-bound control middleware that attaches to the
        // sink-delivery surface is materialized into a sink policy composed at
        // the delivery boundary; everything else stays on the handler shell.
        let mut sink_policies: Vec<Arc<dyn SinkPolicy>> = Vec::new();
        let mut observers = StageObserverSet::default();
        for (middleware_index, spec) in resolved.middleware.into_iter().enumerate() {
            let declaration = spec.factory.declaration();
            if declaration.is_observer() {
                let mut placed = false;
                let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
                for surface in observer_surfaces_for_stage(StageType::Sink) {
                    if !declaration.supports(*surface) {
                        continue;
                    }
                    let attachment = crate::dsl::binder::materialize_observer(
                        spec.factory.as_ref(),
                        &config,
                        StageType::Sink,
                        &control_middleware,
                        *surface,
                        &origin,
                        MiddlewareDeclarationIndex::resolved(middleware_index),
                    )?;
                    push_observer_attachment(&mut observers, attachment)?;
                    placed = true;
                }
                if !placed {
                    return Err(format!(
                        "observer middleware '{}' declares surfaces {:?}, but sink stage '{}' has no compatible observer surface",
                        declaration.label,
                        declaration.surfaces,
                        config.name
                    )
                    .into());
                }
            } else if declaration.is_control()
                && declaration.supports(MiddlewareSurfaceKind::SinkDelivery)
            {
                let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
                let policy = crate::dsl::binder::materialize_sink_delivery(
                    spec.factory.as_ref(),
                    &config,
                    StageType::Sink,
                    &control_middleware,
                    &origin,
                    MiddlewareDeclarationIndex::resolved(middleware_index),
                )?;
                sink_policies.push(policy);
            } else {
                all_middleware.push(create_legacy_shell(
                    spec.factory.as_ref(),
                    &config,
                    control_middleware.clone(),
                )?);
            }
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

        // Apply all middleware. The middleware execution scope is computed
        // per event by the supervisor at dispatch (FLOWIP-120c H3).
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

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
        };

        // Use the builder to create the handle
        let handle = JournalSinkBuilder::new(handler_with_middleware, sink_config, resources)
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

/// Descriptor for replay-safe effectful sink stages.
pub struct EffectfulSinkDescriptor<H: EffectfulSinkHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub effects: Vec<EffectDeclaration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: EffectfulSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for EffectfulSinkDescriptor<H>
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

    fn is_effectful(&self) -> bool {
        true
    }

    fn stage_logic_version(&self) -> String {
        self.handler.stage_logic_version().to_string()
    }

    fn effect_declarations(&self) -> Vec<EffectDeclaration> {
        self.effects.clone()
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        _resources: StageResources,
        _flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        _control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        Err(format!(
            "effectful sink stage `{}` is retired by FLOWIP-120b; use an effectful transform or effectful stateful stage to author facts, then consume them with a non-effectful sink",
            config.name
        )
        .into())
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
pub struct StatefulDescriptor<H: StatefulHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub emit_interval: Option<Duration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StatefulDescriptor<H> {
    /// Create a new stateful descriptor with no emit interval.
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        Self {
            name: name.into(),
            handler,
            emit_interval: None,
            middleware: Vec::new(),
        }
    }

    /// Configure a supervisor-driven emit interval for timer-driven emission while idle.
    pub fn with_emit_interval(mut self, emit_interval: Duration) -> Self {
        self.emit_interval = Some(emit_interval);
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    /// Build into a boxed StageDescriptor for DSL compatibility.
    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for StatefulDescriptor<H>
{
    /// FLOWIP-095l: a stateful fold takes its order role from its declared input
    /// semantics. Undeclared defaults to Observer (safe); a multi-source fan-in
    /// cone forces an explicit declaration via the build-time check.
    fn order_role(&self) -> OrderRole {
        match self.handler.declared_input_order() {
            InputOrderSemantics::OrderInsensitive(_) => OrderRole::Barrier,
            InputOrderSemantics::OrderSensitive | InputOrderSemantics::Undeclared => {
                OrderRole::Observer
            }
        }
    }

    fn is_undeclared_order_observer(&self) -> bool {
        matches!(
            self.handler.declared_input_order(),
            InputOrderSemantics::Undeclared
        )
    }

    fn accepts_cycle_nondeterminism(&self) -> bool {
        self.handler.accepts_cycle_nondeterminism()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Stateful
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Stateful, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement = plan_stage_middleware(
            &config,
            StageType::Stateful,
            resolved,
            &control_middleware,
            false,
        )?;
        let all_middleware = placement.legacy_shell;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware (FLOWIP-080o-part-2: MiddlewareStateful now
        // exists). The middleware execution scope is computed per event by
        // the supervisor at dispatch (FLOWIP-120c H3).
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

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
        let handle = StatefulBuilder::new(handler_with_middleware, stateful_config, resources)
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
pub struct EffectfulStatefulDescriptor<H: EffectfulStatefulHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub emit_interval: Option<Duration>,
    pub effects: Vec<EffectDeclaration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: EffectfulStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    EffectfulStatefulDescriptor<H>
{
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        Self {
            name: name.into(),
            handler,
            emit_interval: None,
            effects: Vec::new(),
            middleware: Vec::new(),
        }
    }

    pub fn with_emit_interval(mut self, emit_interval: Duration) -> Self {
        self.emit_interval = Some(emit_interval);
        self
    }

    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
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

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Stateful
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
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        mut resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        let effect_declarations = self.effects.clone();
        validate_effect_declarations(&self.name, &effect_declarations, &resources.effect_ports)?;
        resources.effect_declarations = effect_declarations.clone();

        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Stateful, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let mut shell_specs = Vec::new();
        let mut effect_observers = StageObserverSet::default();
        for (middleware_index, spec) in resolved.middleware.into_iter().enumerate() {
            let declaration = spec.factory.declaration();
            if declaration.is_observer() && declaration.supports(MiddlewareSurfaceKind::Effect) {
                let origin = crate::dsl::binder::middleware_origin_from_source(&spec.source);
                materialize_effect_observers_for_declarations(
                    &mut effect_observers,
                    spec.factory.as_ref(),
                    EffectObserverMaterialization {
                        config: &config,
                        stage_type: StageType::Stateful,
                        control_middleware: &control_middleware,
                        origin: &origin,
                        declaration_index: MiddlewareDeclarationIndex::resolved(middleware_index),
                        effect_declarations: &effect_declarations,
                    },
                )?;
                if declaration_has_stage_observer_surface(&declaration, StageType::Stateful) {
                    shell_specs.push(spec);
                }
            } else {
                shell_specs.push(spec);
            }
        }

        let placement = plan_stage_middleware(
            &config,
            StageType::Stateful,
            crate::middleware_resolution::ResolvedMiddleware {
                middleware: shell_specs,
                overrides: Vec::new(),
                warnings: Vec::new(),
            },
            &control_middleware,
            false,
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
            emit_interval: self.emit_interval,
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
pub struct JoinDescriptor<H: JoinHandler + 'static> {
    pub name: String,
    pub reference_stage_id: StageId,
    pub reference_stage_var: Option<&'static str>, // For DSL resolution - stage variable name
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
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

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.label().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
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

    /// FLOWIP-095l: a hydrating join self-orders its fan-in and relays the
    /// ordering requirement upward, so it is a Carrier, which keeps 095d
    /// correction 4 intact. A symmetric (Live) join observes the interleaving of
    /// its two sides, so it is an Observer.
    ///
    /// FLOWIP-095l Gap 12: a join is never a Barrier in v1. The commutativity
    /// trial proves only `StatefulHandler` folds, so there is no proof path for a
    /// join; a hand-declared `OrderInsensitive` is ignored here rather than trusted.
    /// A genuinely commutative symmetric join waits on a `JoinHandler` trial that
    /// drives `process_event` permutations and `on_source_eof`.
    fn order_role(&self) -> OrderRole {
        if self.handler.reference_mode() == JoinReferenceMode::FiniteEof {
            return OrderRole::Carrier;
        }
        OrderRole::Observer
    }

    fn is_undeclared_order_observer(&self) -> bool {
        // A hydrating join is a structural orderer, exempt from declaration.
        self.handler.reference_mode() != JoinReferenceMode::FiniteEof
            && matches!(
                self.handler.declared_input_order(),
                InputOrderSemantics::Undeclared
            )
    }

    fn accepts_cycle_nondeterminism(&self) -> bool {
        self.handler.accepts_cycle_nondeterminism()
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> StageCreationResult<BoxedStageHandle> {
        // Validate middleware safety
        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Join, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        )?;

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_default_signal_strategy(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_runtime::control_plane::ControlPlaneProvider> =
            control_middleware.clone();

        let placement = plan_stage_middleware(
            &config,
            StageType::Join,
            resolved,
            &control_middleware,
            false,
        )?;
        let all_middleware = placement.legacy_shell;

        instrumentation
            .bind_control_plane(
                &config.stage_id,
                &control_provider,
                placement.expects_circuit_breaker,
                placement.expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware (FLOWIP-080o-part-2: MiddlewareJoin now exists)
        // Same middleware is applied to both reference and stream sides
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        // FLOWIP-120a: bind the stage's replay scope so handler-level control
        // middleware suppresses its side effects during deterministic replay.
        // FLOWIP-120r: sourced from the runtime execution strategy. The join's
        // handler scope is stage-independent in 120r (constant per run), so it
        // stays build-time-static here; FLOWIP-120n adds the per-call
        // UnifiedJoinHandler seam when the resume predicate makes it positional.
        let handler_with_middleware = builder
            .build()
            .with_execution_scope(resources.runtime_execution.stage_scope(config.stage_id));

        // Extract join-mode configuration from the handler before moving it into the runtime.
        let reference_mode = handler_with_middleware.reference_mode();
        let reference_batch_cap = handler_with_middleware.reference_batch_cap();

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
            handler_with_middleware,
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

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_adapters::middleware::control::circuit_breaker::circuit_breaker;
    use obzenflow_core::event::{JournalEvent, SystemEvent};
    use obzenflow_core::{ChainEvent, EventEnvelope, FlowId, TypedPayload};
    use obzenflow_runtime::control_plane::ControlPlaneProvider;
    use obzenflow_runtime::effects::{
        Effect, EffectCommitHandle, EffectContext, EffectError, TransactionalEffectPort,
    };
    use obzenflow_runtime::message_bus::FsmMessageBus;
    use obzenflow_runtime::stages::resources_builder::SubscriptionFactory;
    use obzenflow_runtime::stages::LivenessSnapshots;
    use serde_json::json;

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
            outcome_fact_types: Vec::new(),
        };

        let err =
            validate_effect_declarations("effectful", &[declaration], &EffectPortRegistry::new())
                .expect_err("missing key strategy must fail materialisation");

        assert!(err.contains("without an idempotency-key strategy"));
    }

    #[derive(Clone, Debug)]
    struct DemoDuplicateEffect;

    #[async_trait]
    impl Effect for DemoDuplicateEffect {
        const EFFECT_TYPE: &'static str = "test.duplicate";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Outcome = DemoTransactionalOutput;

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

        let err =
            validate_effect_declarations("effectful", &declarations, &EffectPortRegistry::new())
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
        )
        .expect_err("missing required port must fail materialisation");
        assert!(missing.contains("requires effect port"));

        let mut registry = EffectPortRegistry::new();
        registry.insert::<dyn DemoEffectPort>("primary", Arc::new(DemoEffectPortImpl));

        validate_effect_declarations("effectful", &[declaration], &registry)
            .expect("registered required port should pass");
    }

    #[test]
    fn effect_declaration_validation_transactional_effect_requires_typed_port() {
        let declaration = EffectDeclaration::transactional_effect::<DemoTransactionalEffect>("tx");

        let missing = validate_effect_declarations(
            "effectful",
            std::slice::from_ref(&declaration),
            &EffectPortRegistry::new(),
        )
        .expect_err("missing transactional port must fail materialisation");
        assert!(missing.contains("requires effect port"));

        let mut registry = EffectPortRegistry::new();
        registry.insert::<dyn TransactionalEffectPort<DemoTransactionalEffect>>(
            "tx",
            Arc::new(DemoTransactionalPort),
        );

        validate_effect_declarations("effectful", &[declaration], &registry)
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
    struct DummyAsyncFiniteSourceNoHint;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceNoHint {
        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncFiniteSourceWithHint;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceWithHint {
        fn suggested_poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(123))
        }

        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncInfiniteSourceNoHint;

    #[async_trait]
    impl AsyncInfiniteSourceHandler for DummyAsyncInfiniteSourceNoHint {
        async fn next(
            &mut self,
        ) -> Result<Vec<ChainEvent>, obzenflow_runtime::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncInfiniteSourceWithHint;

    #[async_trait]
    impl AsyncInfiniteSourceHandler for DummyAsyncInfiniteSourceWithHint {
        fn suggested_poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(7))
        }

        async fn next(
            &mut self,
        ) -> Result<Vec<ChainEvent>, obzenflow_runtime::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn async_finite_source_descriptor_seeds_poll_timeout_from_handler_hint() {
        let descriptor = AsyncFiniteSourceDescriptor::new("hinted", DummyAsyncFiniteSourceWithHint);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(123)));
    }

    #[test]
    fn async_finite_source_descriptor_uses_default_poll_timeout_without_hint() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("defaulted", DummyAsyncFiniteSourceNoHint);
        assert_eq!(
            descriptor.poll_timeout,
            Some(DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT)
        );
    }

    #[test]
    fn async_finite_source_descriptor_explicit_poll_timeout_override_wins() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("override", DummyAsyncFiniteSourceWithHint)
                .with_poll_timeout(Some(Duration::from_secs(1)));
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(1)));
    }

    #[test]
    fn async_infinite_source_descriptor_seeds_poll_timeout_from_handler_hint() {
        let descriptor =
            AsyncInfiniteSourceDescriptor::new("hinted", DummyAsyncInfiniteSourceWithHint);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(7)));
    }

    #[test]
    fn async_infinite_source_descriptor_keeps_default_poll_timeout_without_hint() {
        let descriptor =
            AsyncInfiniteSourceDescriptor::new("defaulted", DummyAsyncInfiniteSourceNoHint);
        assert_eq!(descriptor.poll_timeout, None);
    }

    #[test]
    fn hosted_ingress_source_fills_slot_and_binds_limiter_to_ingress() {
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
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let slot = HostedIngressBindingSlot::new("bank.accounts");

        // A hosted ingress source with a rate limiter as its stage middleware.
        let resolved = crate::middleware_resolution::resolve_middleware(
            vec![],
            vec![obzenflow_adapters::middleware::control::rate_limiter::rate_limit(1.0)],
            &config.name,
        )
        .expect("middleware resolves");

        build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            WriterId::from(stage_id),
            resolved,
            Some(slot.clone()),
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

        // FLOWIP-115d AC42: the limiter binds to Ingress, not source poll, so it
        // does not also pace the internal mpsc-drain source poll.
        assert!(
            control.source_policies(&stage_id).is_empty(),
            "a hosted ingress limiter is not also registered as a source-poll policy"
        );

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
        fn control_role(&self) -> ControlMiddlewareRole {
            ControlMiddlewareRole::None
        }
        fn kind(&self) -> obzenflow_adapters::middleware::MiddlewareKind {
            obzenflow_adapters::middleware::MiddlewareKind::Policy
        }
        fn plan_contribution(&self) -> obzenflow_adapters::middleware::MiddlewarePlanContribution {
            obzenflow_adapters::middleware::MiddlewarePlanContribution::None
        }
        fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
            None
        }
        fn declaration(&self) -> obzenflow_adapters::middleware::MiddlewareDeclaration {
            obzenflow_adapters::middleware::MiddlewareDeclaration::control_with_family(
                self.label(),
                "allow_once_ingress",
                vec![MiddlewareSurfaceKind::Ingress],
            )
        }
        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
        ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
            // Hook-bound: this control middleware is placed through `materialize`,
            // never the legacy handler shell.
            Err(
                obzenflow_adapters::middleware::MiddlewareFactoryError::not_hook_bound(
                    self.label(),
                ),
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
                MiddlewareSurface::Ingress(_) => Ok(MiddlewareSurfaceAttachment::Ingress(
                    std::sync::Arc::new(AllowOnceBoundary {
                        admitted: std::sync::atomic::AtomicBool::new(false),
                    }),
                )),
                _ => Err(MiddlewareFactoryError::not_hook_bound(self.label())),
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
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let slot = HostedIngressBindingSlot::new("bank.accounts");

        // The same resolve -> build -> materialize carrier path as the built-in
        // limiter, but with a user-authored factory.
        let resolved = crate::middleware_resolution::resolve_middleware(
            vec![],
            vec![Box::new(AllowOnceIngressFactory)],
            &config.name,
        )
        .expect("middleware resolves");

        build_source_middleware_and_register_policies(
            &config,
            StageType::InfiniteSource,
            WriterId::from(stage_id),
            resolved,
            Some(slot.clone()),
            &control,
        )
        .expect("source middleware build");

        // The carrier routed the third-party Ingress-declaring factory to Ingress
        // and filled the slot with its own boundary; no source-poll policy and no
        // framework-specific branch was needed.
        let filled = slot.filled().expect("slot filled");
        let boundary = filled.boundary.as_ref().expect("third-party boundary");
        assert_eq!(boundary.label(), "allow_once");
        assert!(
            control.source_policies(&stage_id).is_empty(),
            "an ingress-only third party is not registered as a source-poll policy"
        );

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
        let config = StageConfig {
            stage_id,
            name: "cb_source".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
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
            synthesized_outcomes: Vec::new(),
            deterministic_fan_in: false,
        };

        let descriptor = FiniteSourceDescriptor {
            name: "cb_source".to_string(),
            handler: DummyFiniteSource,
            middleware: vec![circuit_breaker(1)],
        };

        let control_middleware = Arc::new(ControlMiddlewareAggregator::new());
        let boxed: Box<dyn StageDescriptor> = Box::new(descriptor);
        let handle = boxed
            .create_handle_with_flow_middleware(
                config,
                resources,
                vec![],
                control_middleware.clone(),
            )
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
/// planner's observer path on every stage type and never falls back to legacy
/// `create()` / `create_for_effect()`.
#[cfg(test)]
mod observer_placement_negative_tests {
    use super::*;
    use obzenflow_adapters::middleware::{
        validate_attachment_request, ControlMiddlewareRole, Middleware,
        MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory,
        MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareMaterializationContext,
        MiddlewareOverrideKey, MiddlewarePlanContribution, MiddlewareSurfaceAttachment,
        MiddlewareSurfaceKind, TopologyMiddlewareConfigSlot,
    };
    use obzenflow_runtime::stages::observer::{
        HandlerObserver, JoinObserver, SinkDeliveryObserver, SourcePollObserver, StatefulObserver,
    };

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

    /// An observer factory whose legacy-shell constructors panic. If the planner
    /// ever routed it through `create()` instead of observer placement, the test
    /// would abort, so reaching a successful placement proves the planner used
    /// `materialize()`.
    struct LoudObserverFactory;
    struct LoudObserverFamily;

    impl MiddlewareFactory for LoudObserverFactory {
        fn label(&self) -> &'static str {
            "loud-observer"
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<LoudObserverFamily>(self.label())
        }

        fn control_role(&self) -> ControlMiddlewareRole {
            ControlMiddlewareRole::None
        }

        fn plan_contribution(&self) -> MiddlewarePlanContribution {
            MiddlewarePlanContribution::None
        }

        fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
            None
        }

        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
        ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
            panic!("legacy create() must never be called for an observer middleware");
        }

        fn create_for_effect(
            &self,
            _config: &StageConfig,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
            _effect_type: &str,
        ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
            panic!("legacy create_for_effect() must never be called for an observer middleware");
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
                    Ok(MiddlewareSurfaceAttachment::HandlerObserver(observer))
                }
                MiddlewareSurfaceKind::Stateful => {
                    Ok(MiddlewareSurfaceAttachment::StatefulObserver(observer))
                }
                MiddlewareSurfaceKind::Join => {
                    Ok(MiddlewareSurfaceAttachment::JoinObserver(observer))
                }
                MiddlewareSurfaceKind::SourcePoll => {
                    Ok(MiddlewareSurfaceAttachment::SourcePollObserver(observer))
                }
                MiddlewareSurfaceKind::SinkDelivery => {
                    Ok(MiddlewareSurfaceAttachment::SinkDeliveryObserver(observer))
                }
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

    #[test]
    fn observer_binds_through_placement_on_every_stage_type_without_legacy_create() {
        // StageType collapses transform / async transform / effectful transform
        // to `Transform`, so the six variants cover all seven descriptor paths
        // the planner serves.
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
            };
            let control = Arc::new(ControlMiddlewareAggregator::new());
            let resolved = crate::middleware_resolution::resolve_middleware(
                vec![],
                vec![Box::new(LoudObserverFactory)],
                &config.name,
            )
            .expect("loud observer middleware resolves");

            let placement = plan_stage_middleware(&config, stage_type, resolved, &control, false)
                .unwrap_or_else(|err| {
                    panic!("observer placement must succeed for {stage_type:?}: {err}")
                });

            // The observer never lands in the legacy shell: that bucket holds only
            // the (now empty) system middleware, never a hook-bound observer.
            assert!(
                placement.legacy_shell.is_empty(),
                "observer middleware must not be placed in the legacy shell for {stage_type:?}"
            );
        }
    }
}
