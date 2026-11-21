// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Checked authoring and attachment of ordinary observer ports.

use std::any::{type_name, TypeId};
use std::sync::Arc;

use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::observer::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, HandlerObserver, JoinObserver,
    ObserverBinding, ObserverError, ObserverResult, SourcePollObserver, StageLifecycleObserver,
    StageObserverBindings, StatefulObserver,
};
use thiserror::Error;

use crate::middleware::{
    CheckedMiddlewareSurfaceAttachment, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceAttachmentKind, MiddlewareSurfaceKind,
};

/// The complete ordinary observer surface set. Control surfaces are not
/// inferred into this list, and output commit is intentionally absent.
pub(crate) const OBSERVER_SURFACE_KINDS: &[MiddlewareSurfaceKind] = &[
    MiddlewareSurfaceKind::Handler,
    MiddlewareSurfaceKind::Stateful,
    MiddlewareSurfaceKind::Join,
    MiddlewareSurfaceKind::SourcePoll,
    MiddlewareSurfaceKind::Effect,
    MiddlewareSurfaceKind::StageLifecycle,
];

/// Adapter-owned translation of checked attachments into closed runtime
/// bindings. Bundle materialisation remains runtime-private.
#[derive(Default)]
pub struct StageObserverSet {
    bindings: StageObserverBindings,
}

impl StageObserverSet {
    pub fn push_attachment(
        &mut self,
        attachment: CheckedMiddlewareSurfaceAttachment,
    ) -> Result<(), String> {
        let (label, kind) = attachment.into_labelled_kind();
        let binding = match kind {
            MiddlewareSurfaceAttachmentKind::HandlerObserver(observer) => {
                ObserverBinding::handler(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::StatefulObserver(observer) => {
                ObserverBinding::stateful(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::JoinObserver(observer) => {
                ObserverBinding::join(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::SourcePollObserver(observer) => {
                ObserverBinding::source_poll(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::EffectObserver(_) => {
                return Err(format!(
                    "effect observer '{label}' requires its checked effect subject"
                ))
            }
            MiddlewareSurfaceAttachmentKind::StageLifecycleObserver(observer) => {
                ObserverBinding::stage_lifecycle(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::SourcePoll(_)
            | MiddlewareSurfaceAttachmentKind::Effect(_)
            | MiddlewareSurfaceAttachmentKind::Ingress(_) => {
                return Err(
                    "middleware materialized a control attachment while planning observers".into(),
                )
            }
        }
        .map_err(|error| error.to_string())?;
        self.bindings.push(binding);
        Ok(())
    }

    /// Regroup every checked subject product from one authored observer attachment
    /// into one runtime attachment. This preserves subject-aware factory
    /// materialisation without multiplying dispatch or quarantine identities.
    pub fn push_effect_attachments(
        &mut self,
        attachments: Vec<(&'static str, CheckedMiddlewareSurfaceAttachment)>,
    ) -> Result<(), String> {
        let mut label = None;
        let mut subjects = Vec::with_capacity(attachments.len());
        for (effect_type, attachment) in attachments {
            let (attachment_label, kind) = attachment.into_labelled_kind();
            if let Some(expected) = label {
                if expected != attachment_label {
                    return Err(format!(
                        "effect observer declaration changed label from '{expected}' to '{attachment_label}' across subjects"
                    ));
                }
            } else {
                label = Some(attachment_label);
            }
            let MiddlewareSurfaceAttachmentKind::EffectObserver(observer) = kind else {
                return Err(format!(
                    "binder expected an effect observer attachment for '{attachment_label}'"
                ));
            };
            subjects.push((effect_type, observer));
        }
        let label = label.ok_or_else(|| {
            "effect observer declaration has no concrete declared effect subject".to_string()
        })?;
        let binding =
            ObserverBinding::effects(label, subjects).map_err(|error| error.to_string())?;
        self.bindings.push(binding);
        Ok(())
    }

    /// Preserve one authored declaration's quarantine across shell and effect
    /// products. Calls for different authored sites remain independent.
    pub fn push_logical_attachments(
        &mut self,
        shell: Vec<CheckedMiddlewareSurfaceAttachment>,
        effects: Vec<(&'static str, CheckedMiddlewareSurfaceAttachment)>,
    ) -> Result<(), String> {
        let mut products = Self::default();
        for attachment in shell {
            products.push_attachment(attachment)?;
        }
        if !effects.is_empty() {
            products.push_effect_attachments(effects)?;
        }
        if let Some(first) = products.bindings.iter().next() {
            if products
                .bindings
                .iter()
                .any(|binding| binding.label() != first.label())
            {
                return Err("observer declaration changed its label across surfaces".into());
            }
        }
        self.bindings.extend_logical_attachment(products.bindings);
        Ok(())
    }

    pub fn extend(&mut self, other: StageObserverSet) {
        self.bindings.extend(other.bindings);
    }

    pub fn into_bindings(self) -> StageObserverBindings {
        self.bindings
    }
}

#[derive(Debug, Error)]
#[error("observer factory expected surface {expected:?}, received {actual:?}")]
struct ObserverFactorySurfaceError {
    expected: MiddlewareSurfaceKind,
    actual: MiddlewareSurfaceKind,
}

/// Typed application diagnostics for the input an effect call settled.
///
/// `on_delivered` runs after a live call succeeds, with the stage's input
/// decoded as `Input`. Callbacks are synchronous and suppressed during replay.
/// They are best-effort diagnostics with no journal acknowledgement, retry or
/// exactly-once guarantee; an error is logged and changes nothing.
pub trait TypedEffectObserver: Send + Sync {
    type Input: TypedPayload + 'static;

    fn on_delivered(&self, _input: &Self::Input) -> ObserverResult {
        Ok(())
    }
}

struct TypedEffectObserverAdapter<T> {
    label: &'static str,
    observer: Arc<T>,
}

impl<T: TypedEffectObserver> TypedEffectObserverAdapter<T> {
    fn deliver(&self, event: &ChainEvent) -> ObserverResult {
        let input =
            T::Input::try_from_event(event).map_err(|error| Box::new(error) as ObserverError)?;
        self.observer.on_delivered(&input)
    }
}

impl<T: TypedEffectObserver> EffectObserver for TypedEffectObserverAdapter<T> {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        if ctx.outcome() != EffectObserverOutcome::Succeeded {
            return;
        }
        let Some(event) = ctx.input() else {
            return;
        };
        if let Err(error) = self.deliver(event) {
            tracing::warn!(
                observer = self.label,
                stage = ctx.stage_name(),
                error = %error,
                "typed effect observer failed"
            );
        }
    }
}

pub struct TypedEffectObserverFactory<T: TypedEffectObserver> {
    label: &'static str,
    observer: Arc<T>,
}

impl<T: TypedEffectObserver + 'static> MiddlewareFactory for TypedEffectObserverFactory<T> {
    fn label(&self) -> &'static str {
        self.label
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<Self>(self.label)
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(self.label, vec![MiddlewareSurfaceKind::Effect])
    }

    fn observer_input_type(&self) -> Option<(TypeId, &'static str)> {
        Some((TypeId::of::<T::Input>(), type_name::<T::Input>()))
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let actual = request.surface.kind();
        if actual != MiddlewareSurfaceKind::Effect {
            return Err(MiddlewareFactoryError::materialization_failed(
                self.label,
                &context.config.name,
                ObserverFactorySurfaceError {
                    expected: MiddlewareSurfaceKind::Effect,
                    actual,
                },
            ));
        }
        Ok(MiddlewareSurfaceAttachment::effect_observer(Arc::new(
            TypedEffectObserverAdapter {
                label: self.label,
                observer: self.observer.clone(),
            },
        )))
    }
}

/// Attach a typed effect observer. Flow construction checks its `Input`
/// against the stage's input.
pub fn typed_effect_observer<T>(label: &'static str, observer: T) -> TypedEffectObserverFactory<T>
where
    T: TypedEffectObserver + 'static,
{
    TypedEffectObserverFactory {
        label,
        observer: Arc::new(observer),
    }
}

macro_rules! observer_factory {
    (
        $factory:ident,
        $helper:ident,
        $observer_trait:ident,
        $surface:ident,
        $attachment:ident
    ) => {
        pub struct $factory<T>
        where
            T: $observer_trait + 'static,
        {
            label: &'static str,
            observer: Arc<T>,
        }

        impl<T> $factory<T>
        where
            T: $observer_trait + 'static,
        {
            pub fn new(label: &'static str, observer: T) -> Self {
                Self {
                    label,
                    observer: Arc::new(observer),
                }
            }
        }

        impl<T> MiddlewareFactory for $factory<T>
        where
            T: $observer_trait + 'static,
        {
            fn label(&self) -> &'static str {
                self.label
            }

            fn override_key(&self) -> MiddlewareOverrideKey {
                MiddlewareOverrideKey::of::<Self>(self.label)
            }

            fn declaration(&self) -> MiddlewareDeclaration {
                MiddlewareDeclaration::observer(self.label, vec![MiddlewareSurfaceKind::$surface])
            }

            fn materialize(
                &self,
                request: MiddlewareAttachmentRequest<'_>,
                context: &MiddlewareMaterializationContext<'_>,
            ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
                let actual = request.surface.kind();
                if actual != MiddlewareSurfaceKind::$surface {
                    return Err(MiddlewareFactoryError::materialization_failed(
                        self.label,
                        &context.config.name,
                        ObserverFactorySurfaceError {
                            expected: MiddlewareSurfaceKind::$surface,
                            actual,
                        },
                    ));
                }
                let observer: Arc<dyn $observer_trait> = self.observer.clone();
                Ok(MiddlewareSurfaceAttachment::$attachment(observer))
            }
        }

        pub fn $helper<T>(label: &'static str, observer: T) -> $factory<T>
        where
            T: $observer_trait + 'static,
        {
            $factory::new(label, observer)
        }
    };
}

observer_factory!(
    HandlerObserverFactory,
    handler_observer,
    HandlerObserver,
    Handler,
    handler_observer
);
observer_factory!(
    StatefulObserverFactory,
    stateful_observer,
    StatefulObserver,
    Stateful,
    stateful_observer
);
observer_factory!(
    JoinObserverFactory,
    join_observer,
    JoinObserver,
    Join,
    join_observer
);
observer_factory!(
    SourcePollObserverFactory,
    source_poll_observer,
    SourcePollObserver,
    SourcePoll,
    source_poll_observer
);
observer_factory!(
    EffectObserverFactory,
    effect_observer,
    EffectObserver,
    Effect,
    effect_observer
);
observer_factory!(
    StageLifecycleObserverFactory,
    stage_lifecycle_observer,
    StageLifecycleObserver,
    StageLifecycle,
    stage_lifecycle_observer
);

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::stages::observer::HandlerObserverContext;

    struct Noop;

    impl HandlerObserver for Noop {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {}
    }
    impl StatefulObserver for Noop {}
    impl JoinObserver for Noop {}
    impl SourcePollObserver for Noop {}
    impl EffectObserver for Noop {}
    impl TypedEffectObserver for Noop {
        type Input = TestInput;
    }
    impl StageLifecycleObserver for Noop {}

    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestInput {
        sequence: u64,
    }

    impl TypedPayload for TestInput {
        const EVENT_TYPE: &'static str = "observer.test_input";
    }

    #[test]
    fn typed_effect_observer_undecodable_input_skipped() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct RecordsDelivery(Arc<AtomicUsize>);
        impl TypedEffectObserver for RecordsDelivery {
            type Input = TestInput;

            fn on_delivered(&self, input: &TestInput) -> ObserverResult {
                assert_eq!(input.sequence, 7);
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let adapter = TypedEffectObserverAdapter {
            label: "delivery-log",
            observer: Arc::new(RecordsDelivery(calls.clone())),
        };
        let mut input = TestInput { sequence: 7 }.to_event(obzenflow_core::WriterId::from(
            obzenflow_core::StageId::new(),
        ));
        adapter
            .deliver(&input)
            .expect("valid input reaches observer");
        input.payload = obzenflow_core::event::ChainPayload::Fact(serde_json::json!({
            "sequence": "invalid"
        }));
        let error = adapter
            .deliver(&input)
            .expect_err("decode error is observable");
        assert!(error.to_string().contains("Failed to deserialize"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn every_helper_declares_exactly_its_one_observer_surface() {
        let cases: Vec<(Box<dyn MiddlewareFactory>, MiddlewareSurfaceKind)> = vec![
            (
                Box::new(handler_observer("handler", Noop)),
                MiddlewareSurfaceKind::Handler,
            ),
            (
                Box::new(stateful_observer("stateful", Noop)),
                MiddlewareSurfaceKind::Stateful,
            ),
            (
                Box::new(join_observer("join", Noop)),
                MiddlewareSurfaceKind::Join,
            ),
            (
                Box::new(source_poll_observer("source-poll", Noop)),
                MiddlewareSurfaceKind::SourcePoll,
            ),
            (
                Box::new(effect_observer("effect", Noop)),
                MiddlewareSurfaceKind::Effect,
            ),
            (
                Box::new(typed_effect_observer("typed-effect", Noop)),
                MiddlewareSurfaceKind::Effect,
            ),
            (
                Box::new(stage_lifecycle_observer("lifecycle", Noop)),
                MiddlewareSurfaceKind::StageLifecycle,
            ),
        ];

        for (factory, expected_surface) in cases {
            let declaration = factory.declaration();
            assert_eq!(declaration.label, factory.label());
            assert_eq!(declaration.surfaces, vec![expected_surface]);
            assert!(declaration.is_observer());
        }
    }

    #[test]
    fn empty_set_builds_an_empty_closed_binding_collection() {
        let bindings = StageObserverSet::default().into_bindings();
        assert!(bindings.is_empty());
    }

    #[test]
    fn translation_preserves_declaration_labels_and_order() {
        let mut observers = StageObserverSet::default();
        for label in ["first", "second", "third"] {
            let observer: Arc<dyn HandlerObserver> = Arc::new(Noop);
            let attachment = CheckedMiddlewareSurfaceAttachment::from_validated(
                label,
                MiddlewareSurfaceAttachment::handler_observer(observer),
            );
            observers
                .push_attachment(attachment)
                .expect("translate checked observer");
        }

        let bindings = observers.into_bindings();
        assert_eq!(
            bindings
                .iter()
                .map(ObserverBinding::label)
                .collect::<Vec<_>>(),
            ["first", "second", "third"]
        );
    }

    #[test]
    fn effect_subject_materialisations_become_one_logical_binding() {
        let attachments = ["effect.a", "effect.b"]
            .into_iter()
            .map(|effect_type| {
                let observer: Arc<dyn EffectObserver> = Arc::new(Noop);
                (
                    effect_type,
                    CheckedMiddlewareSurfaceAttachment::from_validated(
                        "effects",
                        MiddlewareSurfaceAttachment::effect_observer(observer),
                    ),
                )
            })
            .collect();
        let mut observers = StageObserverSet::default();
        observers
            .push_effect_attachments(attachments)
            .expect("translate checked effect observers");

        let bindings = observers.into_bindings();
        assert_eq!(bindings.len(), 1);
        let binding = bindings.iter().next().expect("one binding");
        assert_eq!(binding.label(), "effects");
        assert_eq!(
            binding.effect_types().collect::<Vec<_>>(),
            ["effect.a", "effect.b"]
        );
    }

    #[test]
    fn surface_catalog_excludes_output_commit() {
        assert_eq!(
            OBSERVER_SURFACE_KINDS,
            &[
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::Stateful,
                MiddlewareSurfaceKind::Join,
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::StageLifecycle,
            ]
        );
    }
}
