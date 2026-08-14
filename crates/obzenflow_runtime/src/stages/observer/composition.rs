// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hardened composition for ordinary observer ports.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::effects::EffectDeclaration;
use obzenflow_core::event::context::StageType;
use obzenflow_core::ChainEvent;

use super::ports::{
    EffectObserver, EffectObserverContext, HandlerObserver, HandlerObserverContext, JoinObserver,
    JoinObserverContext, SinkDeliveryObserver, SinkDeliveryObserverContext, SourcePollObserver,
    SourcePollObserverContext, StageLifecycleObserver, StageLifecycleObserverContext,
    StatefulObserver, StatefulObserverContext,
};

/// A runtime-owned ordinary observer surface.
///
/// This closed vocabulary is shared with outer-layer diagnostics, while the
/// runtime remains authoritative for whether a concrete stage target accepts a
/// surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObserverSurface {
    Handler,
    Stateful,
    Join,
    SourcePoll,
    Effect,
    SinkDelivery,
    StageLifecycle,
}

impl fmt::Display for ObserverSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Handler => "handler",
            Self::Stateful => "stateful",
            Self::Join => "join",
            Self::SourcePoll => "source_poll",
            Self::Effect => "effect",
            Self::SinkDelivery => "sink_delivery",
            Self::StageLifecycle => "stage_lifecycle",
        };
        f.write_str(name)
    }
}

const SOURCE_SURFACES: &[ObserverSurface] =
    &[ObserverSurface::SourcePoll, ObserverSurface::StageLifecycle];
const TRANSFORM_SURFACES: &[ObserverSurface] =
    &[ObserverSurface::Handler, ObserverSurface::StageLifecycle];
const EFFECTFUL_TRANSFORM_SURFACES: &[ObserverSurface] = &[
    ObserverSurface::Handler,
    ObserverSurface::Effect,
    ObserverSurface::StageLifecycle,
];
const STATEFUL_SURFACES: &[ObserverSurface] =
    &[ObserverSurface::Stateful, ObserverSurface::StageLifecycle];
const EFFECTFUL_STATEFUL_SURFACES: &[ObserverSurface] = &[
    ObserverSurface::Stateful,
    ObserverSurface::Effect,
    ObserverSurface::StageLifecycle,
];
const JOIN_SURFACES: &[ObserverSurface] = &[ObserverSurface::Join, ObserverSurface::StageLifecycle];
const SINK_SURFACES: &[ObserverSurface] = &[
    ObserverSurface::SinkDelivery,
    ObserverSurface::StageLifecycle,
];

/// Runtime-owned shell compatibility used by the DSL for early diagnostics.
///
/// Effect compatibility additionally depends on the concrete stage's declared
/// effect subjects and is therefore decided only by runtime stage builders.
pub fn observer_shell_surfaces_for_stage(stage_type: StageType) -> &'static [ObserverSurface] {
    match stage_type {
        StageType::FiniteSource | StageType::InfiniteSource => SOURCE_SURFACES,
        StageType::Transform => TRANSFORM_SURFACES,
        StageType::Stateful => STATEFUL_SURFACES,
        StageType::Join => JOIN_SURFACES,
        StageType::Sink => SINK_SURFACES,
    }
}

#[derive(Clone)]
enum ObserverBindingKind {
    Handler(Arc<dyn HandlerObserver>),
    Stateful(Arc<dyn StatefulObserver>),
    Join(Arc<dyn JoinObserver>),
    SourcePoll(Arc<dyn SourcePollObserver>),
    Effect(Vec<EffectObserverSubject>),
    SinkDelivery(Arc<dyn SinkDeliveryObserver>),
    StageLifecycle(Arc<dyn StageLifecycleObserver>),
}

#[derive(Clone)]
struct EffectObserverSubject {
    effect_type: &'static str,
    observer: Arc<dyn EffectObserver>,
}

/// One labelled, surface-specific, safe input to runtime observer composition.
///
/// The private closed sum prevents a caller from injecting a bundle or control
/// attachment. Runtime still validates the binding against the concrete stage
/// target before materialising any observer ports.
#[derive(Clone)]
pub struct ObserverBinding {
    label: &'static str,
    kind: ObserverBindingKind,
}

impl ObserverBinding {
    fn new(label: &'static str, kind: ObserverBindingKind) -> Result<Self, ObserverBindingError> {
        if label.trim().is_empty() {
            return Err(ObserverBindingError::EmptyLabel);
        }
        Ok(Self { label, kind })
    }

    pub fn handler(
        label: &'static str,
        observer: Arc<dyn HandlerObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::new(label, ObserverBindingKind::Handler(observer))
    }

    pub fn stateful(
        label: &'static str,
        observer: Arc<dyn StatefulObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::new(label, ObserverBindingKind::Stateful(observer))
    }

    pub fn join(
        label: &'static str,
        observer: Arc<dyn JoinObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::new(label, ObserverBindingKind::Join(observer))
    }

    pub fn source_poll(
        label: &'static str,
        observer: Arc<dyn SourcePollObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::new(label, ObserverBindingKind::SourcePoll(observer))
    }

    pub fn effect(
        label: &'static str,
        effect_type: &'static str,
        observer: Arc<dyn EffectObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::effects(label, vec![(effect_type, observer)])
    }

    /// Bind one logical effect-observer declaration to its concrete declared
    /// subjects. Runtime gives the whole declaration one quarantine latch.
    pub fn effects(
        label: &'static str,
        subjects: Vec<(&'static str, Arc<dyn EffectObserver>)>,
    ) -> Result<Self, ObserverBindingError> {
        if subjects.is_empty() {
            return Err(ObserverBindingError::EmptyEffectSubjects { label });
        }
        for (index, (effect_type, _)) in subjects.iter().enumerate() {
            if subjects[..index]
                .iter()
                .any(|(prior, _)| prior == effect_type)
            {
                return Err(ObserverBindingError::DuplicateEffectSubject {
                    label,
                    effect_type: *effect_type,
                });
            }
        }
        Self::new(
            label,
            ObserverBindingKind::Effect(
                subjects
                    .into_iter()
                    .map(|(effect_type, observer)| EffectObserverSubject {
                        effect_type,
                        observer,
                    })
                    .collect(),
            ),
        )
    }

    pub fn sink_delivery(
        label: &'static str,
        observer: Arc<dyn SinkDeliveryObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::new(label, ObserverBindingKind::SinkDelivery(observer))
    }

    pub fn stage_lifecycle(
        label: &'static str,
        observer: Arc<dyn StageLifecycleObserver>,
    ) -> Result<Self, ObserverBindingError> {
        Self::new(label, ObserverBindingKind::StageLifecycle(observer))
    }

    pub fn label(&self) -> &'static str {
        self.label
    }

    pub fn surface(&self) -> ObserverSurface {
        match self.kind {
            ObserverBindingKind::Handler(_) => ObserverSurface::Handler,
            ObserverBindingKind::Stateful(_) => ObserverSurface::Stateful,
            ObserverBindingKind::Join(_) => ObserverSurface::Join,
            ObserverBindingKind::SourcePoll(_) => ObserverSurface::SourcePoll,
            ObserverBindingKind::Effect(_) => ObserverSurface::Effect,
            ObserverBindingKind::SinkDelivery(_) => ObserverSurface::SinkDelivery,
            ObserverBindingKind::StageLifecycle(_) => ObserverSurface::StageLifecycle,
        }
    }

    pub fn effect_types(&self) -> impl ExactSizeIterator<Item = &'static str> + '_ {
        let subjects: &[EffectObserverSubject] = match &self.kind {
            ObserverBindingKind::Effect(subjects) => subjects,
            _ => &[],
        };
        subjects.iter().map(|subject| subject.effect_type)
    }
}

impl fmt::Debug for ObserverBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserverBinding")
            .field("label", &self.label)
            .field("surface", &self.surface())
            .field("effect_types", &self.effect_types().collect::<Vec<_>>())
            .finish()
    }
}

/// Ordered collection of closed observer bindings accepted by runtime configs.
#[derive(Clone, Default)]
pub struct StageObserverBindings {
    bindings: Vec<ObserverBinding>,
}

impl StageObserverBindings {
    pub fn push(&mut self, binding: ObserverBinding) {
        self.bindings.push(binding);
    }

    pub fn extend(&mut self, other: Self) {
        self.bindings.extend(other.bindings);
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ObserverBinding> {
        self.bindings.iter()
    }
}

impl fmt::Debug for StageObserverBindings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(&self.bindings).finish()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ObserverBindingError {
    #[error("observer declaration label must not be empty")]
    EmptyLabel,
    #[error("effect observer '{label}' must name at least one declared effect subject")]
    EmptyEffectSubjects { label: &'static str },
    #[error("effect observer '{label}' repeats effect subject '{effect_type}'")]
    DuplicateEffectSubject {
        label: &'static str,
        effect_type: &'static str,
    },
    #[error(
        "observer '{label}' requests surface {requested} on stage '{stage}' ({stage_kind}), which permits {permitted:?}"
    )]
    IncompatibleSurface {
        stage: String,
        stage_kind: &'static str,
        label: &'static str,
        requested: ObserverSurface,
        permitted: Vec<ObserverSurface>,
    },
    #[error(
        "observer '{label}' targets undeclared effect '{effect_type}' on stage '{stage}' ({stage_kind}); declared effects are {declared_effects:?}"
    )]
    UndeclaredEffect {
        stage: String,
        stage_kind: &'static str,
        label: &'static str,
        effect_type: &'static str,
        declared_effects: Vec<&'static str>,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum ObserverTarget<'a> {
    FiniteSource,
    InfiniteSource,
    Transform { effects: &'a [EffectDeclaration] },
    Stateful { effects: &'a [EffectDeclaration] },
    Join,
    Sink,
}

impl<'a> ObserverTarget<'a> {
    fn kind(self) -> &'static str {
        match self {
            Self::FiniteSource => "finite source",
            Self::InfiniteSource => "infinite source",
            Self::Transform { effects } if effects.is_empty() => "transform",
            Self::Transform { .. } => "effectful transform",
            Self::Stateful { effects } if effects.is_empty() => "stateful",
            Self::Stateful { .. } => "effectful stateful",
            Self::Join => "join",
            Self::Sink => "sink",
        }
    }

    fn permitted_surfaces(self) -> &'static [ObserverSurface] {
        match self {
            Self::FiniteSource | Self::InfiniteSource => SOURCE_SURFACES,
            Self::Transform { effects } if effects.is_empty() => TRANSFORM_SURFACES,
            Self::Transform { .. } => EFFECTFUL_TRANSFORM_SURFACES,
            Self::Stateful { effects } if effects.is_empty() => STATEFUL_SURFACES,
            Self::Stateful { .. } => EFFECTFUL_STATEFUL_SURFACES,
            Self::Join => JOIN_SURFACES,
            Self::Sink => SINK_SURFACES,
        }
    }

    fn effects(self) -> &'a [EffectDeclaration] {
        match self {
            Self::Transform { effects } | Self::Stateful { effects } => effects,
            _ => &[],
        }
    }
}

struct ObserverChild<T: ?Sized> {
    label: &'static str,
    observer: Arc<T>,
    quarantined: AtomicBool,
}

impl<T: ?Sized> ObserverChild<T> {
    fn new(label: &'static str, observer: Arc<T>) -> Self {
        Self {
            label,
            observer,
            quarantined: AtomicBool::new(false),
        }
    }

    fn invoke<F>(&self, stage: &str, surface: &'static str, phase: &'static str, invoke: F)
    where
        F: FnOnce(&T),
    {
        if self.quarantined.load(Ordering::Acquire) {
            return;
        }

        if catch_unwind(AssertUnwindSafe(|| invoke(self.observer.as_ref()))).is_err()
            && self
                .quarantined
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn_quarantined(self.label, stage, surface, phase);
        }
    }
}

pub(crate) struct HardenedObserverPort<T: ?Sized> {
    observer: Arc<T>,
    quarantined: Arc<AtomicBool>,
}

impl<T: ?Sized> Clone for HardenedObserverPort<T> {
    fn clone(&self) -> Self {
        Self {
            observer: Arc::clone(&self.observer),
            quarantined: Arc::clone(&self.quarantined),
        }
    }
}

impl<T: ?Sized> HardenedObserverPort<T> {
    fn new(observer: Arc<T>) -> Self {
        Self {
            observer,
            quarantined: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn invoke<F>(
        &self,
        stage: &str,
        surface: &'static str,
        phase: &'static str,
        invoke: F,
    ) where
        F: FnOnce(&T),
    {
        if self.quarantined.load(Ordering::Acquire) {
            return;
        }

        if catch_unwind(AssertUnwindSafe(|| invoke(self.observer.as_ref()))).is_err()
            && self
                .quarantined
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn_quarantined("observer-composite", stage, surface, phase);
        }
    }
}

fn warn_quarantined(label: &'static str, stage: &str, surface: &'static str, phase: &'static str) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        tracing::warn!(
            observer = label,
            stage,
            surface,
            phase,
            "observer panicked and was quarantined for the remainder of the stage run"
        );
    }));
}

struct HandlerObserverChain(Vec<ObserverChild<dyn HandlerObserver>>);

impl HandlerObserver for HandlerObserverChain {
    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "handler", "before_handle", |observer| {
                observer.before_handle(ctx);
            });
        }
    }

    fn after_handle(&self, ctx: &HandlerObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "handler", "after_handle", |observer| {
                observer.after_handle(ctx, outputs);
            });
        }
    }
}

struct StatefulObserverChain(Vec<ObserverChild<dyn StatefulObserver>>);

impl StatefulObserver for StatefulObserverChain {
    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stateful",
                "before_state_accumulate",
                |observer| observer.before_state_accumulate(ctx),
            );
        }
    }

    fn after_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stateful",
                "after_state_accumulate",
                |observer| observer.after_state_accumulate(ctx),
            );
        }
    }

    fn after_state_emit(&self, ctx: &StatefulObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stateful",
                "after_state_emit",
                |observer| observer.after_state_emit(ctx, outputs),
            );
        }
    }
}

struct JoinObserverChain(Vec<ObserverChild<dyn JoinObserver>>);

impl JoinObserver for JoinObserverChain {
    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "join", "before_join_input", |observer| {
                observer.before_join_input(ctx)
            });
        }
    }

    fn after_join_output(&self, ctx: &JoinObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "join", "after_join_output", |observer| {
                observer.after_join_output(ctx, outputs)
            });
        }
    }
}

struct SourcePollObserverChain(Vec<ObserverChild<dyn SourcePollObserver>>);

impl SourcePollObserver for SourcePollObserverChain {
    fn after_source_poll(&self, ctx: &SourcePollObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "source_poll",
                "after_source_poll",
                |observer| observer.after_source_poll(ctx, outputs),
            );
        }
    }
}

struct EffectObserverChild {
    label: &'static str,
    subjects: Vec<EffectObserverSubject>,
    quarantined: AtomicBool,
}

impl EffectObserverChild {
    fn invoke(&self, ctx: &EffectObserverContext<'_>) {
        if self.quarantined.load(Ordering::Acquire) {
            return;
        }
        let Some(subject) = self
            .subjects
            .iter()
            .find(|subject| subject.effect_type == ctx.effect_type())
        else {
            return;
        };

        if catch_unwind(AssertUnwindSafe(|| subject.observer.after_effect(ctx))).is_err()
            && self
                .quarantined
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn_quarantined(self.label, ctx.stage_name(), "effect", "after_effect");
        }
    }
}

struct EffectObserverChain(Vec<EffectObserverChild>);

impl EffectObserver for EffectObserverChain {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(ctx);
        }
    }
}

struct SinkDeliveryObserverChain(Vec<ObserverChild<dyn SinkDeliveryObserver>>);

impl SinkDeliveryObserver for SinkDeliveryObserverChain {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "sink_delivery",
                "after_sink_delivery",
                |observer| observer.after_sink_delivery(ctx),
            );
        }
    }
}

struct StageLifecycleObserverChain(Vec<ObserverChild<dyn StageLifecycleObserver>>);

impl StageLifecycleObserver for StageLifecycleObserverChain {
    fn on_stage_lifecycle(&self, ctx: &StageLifecycleObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stage_lifecycle",
                "on_stage_lifecycle",
                |observer| observer.on_stage_lifecycle(ctx),
            );
        }
    }
}

/// Private raw materialiser reached only after runtime target validation.
///
/// Fields and construction stay inside the runtime crate so cross-crate
/// callers can supply only closed [`ObserverBinding`] values.
#[derive(Default)]
pub(crate) struct StageObserverBundleBuilder {
    handler: Vec<ObserverChild<dyn HandlerObserver>>,
    stateful: Vec<ObserverChild<dyn StatefulObserver>>,
    join: Vec<ObserverChild<dyn JoinObserver>>,
    source_poll: Vec<ObserverChild<dyn SourcePollObserver>>,
    effect: Vec<EffectObserverChild>,
    sink_delivery: Vec<ObserverChild<dyn SinkDeliveryObserver>>,
    stage_lifecycle: Vec<ObserverChild<dyn StageLifecycleObserver>>,
}

impl StageObserverBundleBuilder {
    pub(crate) fn push_handler(&mut self, label: &'static str, observer: Arc<dyn HandlerObserver>) {
        self.handler.push(ObserverChild::new(label, observer));
    }

    pub(crate) fn push_stateful(
        &mut self,
        label: &'static str,
        observer: Arc<dyn StatefulObserver>,
    ) {
        self.stateful.push(ObserverChild::new(label, observer));
    }

    pub(crate) fn push_join(&mut self, label: &'static str, observer: Arc<dyn JoinObserver>) {
        self.join.push(ObserverChild::new(label, observer));
    }

    pub(crate) fn push_source_poll(
        &mut self,
        label: &'static str,
        observer: Arc<dyn SourcePollObserver>,
    ) {
        self.source_poll.push(ObserverChild::new(label, observer));
    }

    fn push_effect(&mut self, label: &'static str, subjects: Vec<EffectObserverSubject>) {
        self.effect.push(EffectObserverChild {
            label,
            subjects,
            quarantined: AtomicBool::new(false),
        });
    }

    pub(crate) fn push_sink_delivery(
        &mut self,
        label: &'static str,
        observer: Arc<dyn SinkDeliveryObserver>,
    ) {
        self.sink_delivery.push(ObserverChild::new(label, observer));
    }

    pub(crate) fn push_stage_lifecycle(
        &mut self,
        label: &'static str,
        observer: Arc<dyn StageLifecycleObserver>,
    ) {
        self.stage_lifecycle
            .push(ObserverChild::new(label, observer));
    }

    pub(crate) fn build(self) -> StageObserverBundle {
        StageObserverBundle {
            handler: compose(self.handler, HandlerObserverChain),
            stateful: compose(self.stateful, StatefulObserverChain),
            join: compose(self.join, JoinObserverChain),
            source_poll: compose(self.source_poll, SourcePollObserverChain),
            effect: compose_effect(self.effect),
            sink_delivery: compose(self.sink_delivery, SinkDeliveryObserverChain),
            stage_lifecycle: compose(self.stage_lifecycle, StageLifecycleObserverChain),
        }
    }
}

fn compose_effect(
    children: Vec<EffectObserverChild>,
) -> Option<HardenedObserverPort<dyn EffectObserver>> {
    if children.is_empty() {
        None
    } else {
        Some(HardenedObserverPort::new(Arc::new(EffectObserverChain(
            children,
        ))))
    }
}

fn compose<T: ?Sized, C>(
    children: Vec<ObserverChild<T>>,
    chain: fn(Vec<ObserverChild<T>>) -> C,
) -> Option<HardenedObserverPort<T>>
where
    C: IntoObserverArc<T>,
{
    if children.is_empty() {
        None
    } else {
        Some(HardenedObserverPort::new(
            chain(children).into_observer_arc(),
        ))
    }
}

trait IntoObserverArc<T: ?Sized> {
    fn into_observer_arc(self) -> Arc<T>;
}

macro_rules! impl_into_observer_arc {
    ($chain:ty, $port:ty) => {
        impl IntoObserverArc<$port> for $chain {
            fn into_observer_arc(self) -> Arc<$port> {
                Arc::new(self)
            }
        }
    };
}

impl_into_observer_arc!(HandlerObserverChain, dyn HandlerObserver);
impl_into_observer_arc!(StatefulObserverChain, dyn StatefulObserver);
impl_into_observer_arc!(JoinObserverChain, dyn JoinObserver);
impl_into_observer_arc!(SourcePollObserverChain, dyn SourcePollObserver);
impl_into_observer_arc!(SinkDeliveryObserverChain, dyn SinkDeliveryObserver);
impl_into_observer_arc!(StageLifecycleObserverChain, dyn StageLifecycleObserver);

/// Opaque, per-stage composed observer ports.
///
/// Only the empty/default value is directly constructible. Runtime stage
/// builders create non-empty values through a private compositor that always
/// applies labelled per-child quarantine.
#[derive(Clone, Default)]
pub struct StageObserverBundle {
    handler: Option<HardenedObserverPort<dyn HandlerObserver>>,
    stateful: Option<HardenedObserverPort<dyn StatefulObserver>>,
    join: Option<HardenedObserverPort<dyn JoinObserver>>,
    source_poll: Option<HardenedObserverPort<dyn SourcePollObserver>>,
    effect: Option<HardenedObserverPort<dyn EffectObserver>>,
    sink_delivery: Option<HardenedObserverPort<dyn SinkDeliveryObserver>>,
    stage_lifecycle: Option<HardenedObserverPort<dyn StageLifecycleObserver>>,
}

impl StageObserverBundle {
    pub(crate) fn compose_checked(
        stage: &str,
        target: ObserverTarget<'_>,
        bindings: StageObserverBindings,
    ) -> Result<Self, ObserverBindingError> {
        let mut builder = StageObserverBundleBuilder::default();
        let permitted = target.permitted_surfaces();

        for binding in bindings.bindings {
            let requested = binding.surface();
            if !permitted.contains(&requested) {
                return Err(ObserverBindingError::IncompatibleSurface {
                    stage: stage.to_string(),
                    stage_kind: target.kind(),
                    label: binding.label,
                    requested,
                    permitted: permitted.to_vec(),
                });
            }

            match binding.kind {
                ObserverBindingKind::Handler(observer) => {
                    builder.push_handler(binding.label, observer)
                }
                ObserverBindingKind::Stateful(observer) => {
                    builder.push_stateful(binding.label, observer)
                }
                ObserverBindingKind::Join(observer) => builder.push_join(binding.label, observer),
                ObserverBindingKind::SourcePoll(observer) => {
                    builder.push_source_poll(binding.label, observer)
                }
                ObserverBindingKind::Effect(subjects) => {
                    for subject in &subjects {
                        if !target
                            .effects()
                            .iter()
                            .any(|effect| effect.effect_type == subject.effect_type)
                        {
                            return Err(ObserverBindingError::UndeclaredEffect {
                                stage: stage.to_string(),
                                stage_kind: target.kind(),
                                label: binding.label,
                                effect_type: subject.effect_type,
                                declared_effects: target
                                    .effects()
                                    .iter()
                                    .map(|effect| effect.effect_type)
                                    .collect(),
                            });
                        }
                    }
                    builder.push_effect(binding.label, subjects);
                }
                ObserverBindingKind::SinkDelivery(observer) => {
                    builder.push_sink_delivery(binding.label, observer)
                }
                ObserverBindingKind::StageLifecycle(observer) => {
                    builder.push_stage_lifecycle(binding.label, observer)
                }
            }
        }

        Ok(builder.build())
    }

    pub fn is_empty(&self) -> bool {
        self.handler.is_none()
            && self.stateful.is_none()
            && self.join.is_none()
            && self.source_poll.is_none()
            && self.effect.is_none()
            && self.sink_delivery.is_none()
            && self.stage_lifecycle.is_none()
    }

    pub(crate) fn has_join(&self) -> bool {
        self.join.is_some()
    }

    pub(crate) fn has_source_poll(&self) -> bool {
        self.source_poll.is_some()
    }

    pub(crate) fn has_sink_delivery(&self) -> bool {
        self.sink_delivery.is_some()
    }

    pub(crate) fn handler(&self) -> Option<&HardenedObserverPort<dyn HandlerObserver>> {
        self.handler.as_ref()
    }

    pub(crate) fn stateful(&self) -> Option<&HardenedObserverPort<dyn StatefulObserver>> {
        self.stateful.as_ref()
    }

    pub(crate) fn join(&self) -> Option<&HardenedObserverPort<dyn JoinObserver>> {
        self.join.as_ref()
    }

    pub(crate) fn source_poll(&self) -> Option<&HardenedObserverPort<dyn SourcePollObserver>> {
        self.source_poll.as_ref()
    }

    pub(crate) fn effect(&self) -> Option<&HardenedObserverPort<dyn EffectObserver>> {
        self.effect.as_ref()
    }

    pub(crate) fn sink_delivery(&self) -> Option<&HardenedObserverPort<dyn SinkDeliveryObserver>> {
        self.sink_delivery.as_ref()
    }

    pub(crate) fn stage_lifecycle(
        &self,
    ) -> Option<&HardenedObserverPort<dyn StageLifecycleObserver>> {
        self.stage_lifecycle.as_ref()
    }
}

impl fmt::Debug for StageObserverBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageObserverBundle")
            .field("has_observers", &!self.is_empty())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::{EffectOutcomeKind, EffectSafety, IdempotencyKeyPolicy};
    use crate::stages::observer::EffectObserverOutcome;
    use obzenflow_core::event::context::{FlowContext, StageType};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{FlowId, StageId, WriterId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    struct PanicsOnce {
        calls: AtomicUsize,
    }

    impl HandlerObserver for PanicsOnce {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("observer panic");
        }
    }

    struct Counts(AtomicUsize);

    impl HandlerObserver for Counts {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Records {
        id: usize,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    struct PanickingWarningSubscriber {
        warnings: Arc<AtomicUsize>,
    }

    struct CountingWarningSubscriber {
        warnings: Arc<AtomicUsize>,
    }

    impl Subscriber for PanickingWarningSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {
            self.warnings.fetch_add(1, Ordering::SeqCst);
            panic!("warning subscriber panic");
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    impl Subscriber for CountingWarningSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {
            self.warnings.fetch_add(1, Ordering::SeqCst);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    impl HandlerObserver for Records {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.calls
                .lock()
                .expect("recording observer lock")
                .push(self.id);
        }
    }

    struct NoopObserver;

    impl HandlerObserver for NoopObserver {}
    impl StatefulObserver for NoopObserver {}
    impl JoinObserver for NoopObserver {}
    impl SourcePollObserver for NoopObserver {}
    impl EffectObserver for NoopObserver {}
    impl SinkDeliveryObserver for NoopObserver {}
    impl StageLifecycleObserver for NoopObserver {}

    fn declared_effect(effect_type: &'static str) -> EffectDeclaration {
        EffectDeclaration {
            effect_type,
            safety: EffectSafety::Idempotent,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: None,
            outcome_kind: EffectOutcomeKind::RecordedReply,
            public_outcome_fact_types: Vec::new(),
        }
    }

    fn binding_for(surface: ObserverSurface) -> ObserverBinding {
        match surface {
            ObserverSurface::Handler => {
                ObserverBinding::handler("observer", Arc::new(NoopObserver))
            }
            ObserverSurface::Stateful => {
                ObserverBinding::stateful("observer", Arc::new(NoopObserver))
            }
            ObserverSurface::Join => ObserverBinding::join("observer", Arc::new(NoopObserver)),
            ObserverSurface::SourcePoll => {
                ObserverBinding::source_poll("observer", Arc::new(NoopObserver))
            }
            ObserverSurface::Effect => {
                ObserverBinding::effect("observer", "effect.a", Arc::new(NoopObserver))
            }
            ObserverSurface::SinkDelivery => {
                ObserverBinding::sink_delivery("observer", Arc::new(NoopObserver))
            }
            ObserverSurface::StageLifecycle => {
                ObserverBinding::stage_lifecycle("observer", Arc::new(NoopObserver))
            }
        }
        .expect("valid test binding")
    }

    fn effect_context(effect_type: &'static str) -> EffectObserverContext<'static> {
        EffectObserverContext::new(
            FlowId::new(),
            StageId::new(),
            "stage",
            effect_type,
            EffectObserverOutcome::Succeeded,
        )
    }

    fn context() -> (FlowContext, ChainEvent) {
        let flow_context = FlowContext {
            flow_name: "flow".to_string(),
            flow_id: "run".to_string(),
            stage_name: "stage".to_string(),
            stage_id: StageId::new(),
            stage_type: StageType::Transform,
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            serde_json::json!({}),
        );
        (flow_context, event)
    }

    #[test]
    fn panicking_child_is_quarantined_and_siblings_continue() {
        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts(AtomicUsize::new(0)));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("panics", panics.clone());
        builder.push_handler("counts", counts.clone());
        let bundle = builder.build();
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        for _ in 0..3 {
            bundle.handler().expect("handler port").invoke(
                "stage",
                "handler",
                "before_handle",
                |observer| {
                    observer.before_handle(&ctx);
                },
            );
        }

        assert_eq!(panics.calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn composed_port_preserves_declaration_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut builder = StageObserverBundleBuilder::default();
        for id in [1, 2, 3] {
            builder.push_handler(
                "ordered",
                Arc::new(Records {
                    id,
                    calls: calls.clone(),
                }),
            );
        }
        let bundle = builder.build();
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        bundle.handler().expect("one composed handler port").invoke(
            "stage",
            "handler",
            "before_handle",
            |observer| {
                observer.before_handle(&ctx);
            },
        );

        assert_eq!(*calls.lock().expect("recording observer lock"), [1, 2, 3]);
    }

    #[test]
    fn panicking_warning_subscriber_cannot_skip_siblings_or_repeat_warning() {
        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts(AtomicUsize::new(0)));
        let warnings = Arc::new(AtomicUsize::new(0));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("panics", panics.clone());
        builder.push_handler("counts", counts.clone());
        let bundle = builder.build();
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        tracing::subscriber::with_default(
            PanickingWarningSubscriber {
                warnings: warnings.clone(),
            },
            || {
                for _ in 0..2 {
                    bundle.handler().expect("handler port").invoke(
                        "stage",
                        "handler",
                        "before_handle",
                        |observer| {
                            observer.before_handle(&ctx);
                        },
                    );
                }
            },
        );

        assert_eq!(panics.calls.load(Ordering::SeqCst), 1);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn concurrent_panics_make_one_quarantine_transition_and_later_calls_skip_child() {
        const WORKERS: usize = 8;

        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts(AtomicUsize::new(0)));
        let warnings = Arc::new(AtomicUsize::new(0));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("panics", panics.clone());
        builder.push_handler("counts", counts.clone());
        let bundle = Arc::new(builder.build());
        let (flow_context, event) = context();
        let flow_context = Arc::new(flow_context);
        let event = Arc::new(event);
        let barrier = Arc::new(Barrier::new(WORKERS));
        let dispatch = tracing::Dispatch::new(CountingWarningSubscriber {
            warnings: warnings.clone(),
        });

        let handles = (0..WORKERS)
            .map(|_| {
                let bundle = bundle.clone();
                let flow_context = flow_context.clone();
                let event = event.clone();
                let barrier = barrier.clone();
                let dispatch = dispatch.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    tracing::dispatcher::with_default(&dispatch, || {
                        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));
                        bundle.handler().expect("handler port").invoke(
                            "stage",
                            "handler",
                            "before_handle",
                            |observer| observer.before_handle(&ctx),
                        );
                    });
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().expect("observer dispatch thread");
        }

        let calls_after_race = panics.calls.load(Ordering::SeqCst);
        assert!((1..=WORKERS).contains(&calls_after_race));
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), WORKERS);

        tracing::dispatcher::with_default(&dispatch, || {
            let ctx = HandlerObserverContext::new(&flow_context, &event, Some(2));
            bundle.handler().expect("handler port").invoke(
                "stage",
                "handler",
                "before_handle",
                |observer| observer.before_handle(&ctx),
            );
        });

        assert_eq!(panics.calls.load(Ordering::SeqCst), calls_after_race);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), WORKERS + 1);
    }

    #[test]
    fn runtime_owns_the_complete_stage_surface_matrix() {
        let effects = [declared_effect("effect.a")];
        let cases = [
            (ObserverTarget::FiniteSource, SOURCE_SURFACES),
            (ObserverTarget::InfiniteSource, SOURCE_SURFACES),
            (
                ObserverTarget::Transform { effects: &[] },
                TRANSFORM_SURFACES,
            ),
            (
                ObserverTarget::Transform { effects: &effects },
                EFFECTFUL_TRANSFORM_SURFACES,
            ),
            (ObserverTarget::Stateful { effects: &[] }, STATEFUL_SURFACES),
            (
                ObserverTarget::Stateful { effects: &effects },
                EFFECTFUL_STATEFUL_SURFACES,
            ),
            (ObserverTarget::Join, JOIN_SURFACES),
            (ObserverTarget::Sink, SINK_SURFACES),
        ];
        let all_surfaces = [
            ObserverSurface::Handler,
            ObserverSurface::Stateful,
            ObserverSurface::Join,
            ObserverSurface::SourcePoll,
            ObserverSurface::Effect,
            ObserverSurface::SinkDelivery,
            ObserverSurface::StageLifecycle,
        ];

        for (target, permitted) in cases {
            for surface in all_surfaces {
                let mut bindings = StageObserverBindings::default();
                bindings.push(binding_for(surface));
                let accepted =
                    StageObserverBundle::compose_checked("stage", target, bindings).is_ok();
                assert_eq!(
                    accepted,
                    permitted.contains(&surface),
                    "{} unexpectedly handled {surface}",
                    target.kind()
                );
            }
        }
    }

    #[test]
    fn runtime_rejects_an_undeclared_effect_subject() {
        let effects = [declared_effect("effect.a")];
        let mut bindings = StageObserverBindings::default();
        bindings.push(
            ObserverBinding::effect("wrong-subject", "effect.b", Arc::new(NoopObserver))
                .expect("valid closed binding"),
        );

        let error = StageObserverBundle::compose_checked(
            "orders",
            ObserverTarget::Transform { effects: &effects },
            bindings,
        )
        .expect_err("undeclared subject must fail before execution");

        assert!(matches!(
            error,
            ObserverBindingError::UndeclaredEffect {
                stage,
                stage_kind: "effectful transform",
                label: "wrong-subject",
                effect_type: "effect.b",
                declared_effects,
            } if stage == "orders" && declared_effects == ["effect.a"]
        ));
    }

    #[test]
    fn closed_bindings_reject_invalid_labels_and_duplicate_effect_subjects() {
        assert!(matches!(
            ObserverBinding::handler("  ", Arc::new(NoopObserver)),
            Err(ObserverBindingError::EmptyLabel)
        ));
        let observer: Arc<dyn EffectObserver> = Arc::new(NoopObserver);
        assert!(matches!(
            ObserverBinding::effects(
                "effects",
                vec![("effect.a", observer.clone()), ("effect.a", observer),],
            ),
            Err(ObserverBindingError::DuplicateEffectSubject {
                label: "effects",
                effect_type: "effect.a",
            })
        ));
    }

    struct RecordsEffect {
        materialization: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl EffectObserver for RecordsEffect {
        fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
            self.calls.lock().expect("effect call lock").push(format!(
                "{}:{}",
                self.materialization,
                ctx.effect_type()
            ));
        }
    }

    #[test]
    fn effect_dispatch_invokes_only_the_matching_materialised_subject() {
        let effects = [declared_effect("effect.a"), declared_effect("effect.b")];
        let calls = Arc::new(Mutex::new(Vec::new()));
        let binding = ObserverBinding::effects(
            "effects",
            vec![
                (
                    "effect.a",
                    Arc::new(RecordsEffect {
                        materialization: "a",
                        calls: calls.clone(),
                    }),
                ),
                (
                    "effect.b",
                    Arc::new(RecordsEffect {
                        materialization: "b",
                        calls: calls.clone(),
                    }),
                ),
            ],
        )
        .expect("valid effect binding");
        let mut bindings = StageObserverBindings::default();
        bindings.push(binding);
        let bundle = StageObserverBundle::compose_checked(
            "stage",
            ObserverTarget::Transform { effects: &effects },
            bindings,
        )
        .expect("compatible effect observers");

        for effect_type in ["effect.a", "effect.b"] {
            let ctx = effect_context(effect_type);
            bundle.effect().expect("effect port").invoke(
                "stage",
                "effect",
                "after_effect",
                |observer| observer.after_effect(&ctx),
            );
        }

        assert_eq!(
            *calls.lock().expect("effect call lock"),
            ["a:effect.a", "b:effect.b"]
        );
    }

    struct PanicsEffect;

    impl EffectObserver for PanicsEffect {
        fn after_effect(&self, _ctx: &EffectObserverContext<'_>) {
            panic!("effect observer panic");
        }
    }

    struct CountsEffect(Arc<AtomicUsize>);

    impl EffectObserver for CountsEffect {
        fn after_effect(&self, _ctx: &EffectObserverContext<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn effect_declaration_shares_quarantine_across_subjects_but_not_siblings() {
        let effects = [declared_effect("effect.a"), declared_effect("effect.b")];
        let grouped_b_calls = Arc::new(AtomicUsize::new(0));
        let sibling_calls = Arc::new(AtomicUsize::new(0));
        let mut bindings = StageObserverBindings::default();
        bindings.push(
            ObserverBinding::effects(
                "panics",
                vec![
                    ("effect.a", Arc::new(PanicsEffect)),
                    ("effect.b", Arc::new(CountsEffect(grouped_b_calls.clone()))),
                ],
            )
            .expect("valid grouped effect binding"),
        );
        bindings.push(
            ObserverBinding::effect(
                "sibling",
                "effect.b",
                Arc::new(CountsEffect(sibling_calls.clone())),
            )
            .expect("valid sibling binding"),
        );
        let bundle = StageObserverBundle::compose_checked(
            "stage",
            ObserverTarget::Transform { effects: &effects },
            bindings,
        )
        .expect("compatible effect observers");

        for effect_type in ["effect.a", "effect.b"] {
            let ctx = effect_context(effect_type);
            bundle.effect().expect("effect port").invoke(
                "stage",
                "effect",
                "after_effect",
                |observer| observer.after_effect(&ctx),
            );
        }

        assert_eq!(grouped_b_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sibling_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn each_bundle_materialisation_gets_fresh_quarantine_state() {
        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let mut bindings = StageObserverBindings::default();
        bindings.push(
            ObserverBinding::handler("panics", panics.clone()).expect("valid handler binding"),
        );
        let first = StageObserverBundle::compose_checked(
            "first",
            ObserverTarget::Transform { effects: &[] },
            bindings.clone(),
        )
        .expect("first bundle");
        let second = StageObserverBundle::compose_checked(
            "second",
            ObserverTarget::Transform { effects: &[] },
            bindings,
        )
        .expect("second bundle");
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        for bundle in [&first, &first, &second] {
            bundle.handler().expect("handler port").invoke(
                "stage",
                "handler",
                "before_handle",
                |observer| observer.before_handle(&ctx),
            );
        }

        assert_eq!(panics.calls.load(Ordering::SeqCst), 2);
    }
}
