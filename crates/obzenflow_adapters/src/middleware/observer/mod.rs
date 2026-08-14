// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Checked authoring and attachment of ordinary observer ports.

use std::sync::Arc;

use obzenflow_runtime::stages::observer::{
    EffectObserver, HandlerObserver, JoinObserver, SinkDeliveryObserver, SourcePollObserver,
    StageLifecycleObserver, StageObserverBundle, StageObserverBundleBuilder, StatefulObserver,
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
    MiddlewareSurfaceKind::SinkDelivery,
    MiddlewareSurfaceKind::StageLifecycle,
];

/// Adapter-owned routing of checked attachments into the runtime-owned safe
/// compositor.
#[derive(Default)]
pub struct StageObserverSet {
    builder: StageObserverBundleBuilder,
}

impl StageObserverSet {
    pub fn push_attachment(
        &mut self,
        attachment: CheckedMiddlewareSurfaceAttachment,
    ) -> Result<(), String> {
        let (label, kind) = attachment.into_labelled_kind();
        match kind {
            MiddlewareSurfaceAttachmentKind::HandlerObserver(observer) => {
                self.builder.push_handler(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::StatefulObserver(observer) => {
                self.builder.push_stateful(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::JoinObserver(observer) => {
                self.builder.push_join(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::SourcePollObserver(observer) => {
                self.builder.push_source_poll(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::EffectObserver(observer) => {
                self.builder.push_effect(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::SinkDeliveryObserver(observer) => {
                self.builder.push_sink_delivery(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::StageLifecycleObserver(observer) => {
                self.builder.push_stage_lifecycle(label, observer)
            }
            MiddlewareSurfaceAttachmentKind::SourcePoll(_)
            | MiddlewareSurfaceAttachmentKind::Effect(_)
            | MiddlewareSurfaceAttachmentKind::SinkDelivery(_)
            | MiddlewareSurfaceAttachmentKind::Ingress(_) => {
                return Err(
                    "middleware materialized a control attachment while planning observers".into(),
                )
            }
        }
        Ok(())
    }

    pub fn extend(&mut self, other: StageObserverSet) {
        self.builder.extend(other.builder);
    }

    pub fn build(self) -> StageObserverBundle {
        self.builder.build()
    }
}

#[derive(Debug, Error)]
#[error("observer factory expected surface {expected:?}, received {actual:?}")]
struct ObserverFactorySurfaceError {
    expected: MiddlewareSurfaceKind,
    actual: MiddlewareSurfaceKind,
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
    SinkDeliveryObserverFactory,
    sink_delivery_observer,
    SinkDeliveryObserver,
    SinkDelivery,
    sink_delivery_observer
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
    use obzenflow_runtime::stages::observer::{HandlerObserverContext, StageObserverBundle};

    struct Noop;

    impl HandlerObserver for Noop {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {}
    }
    impl StatefulObserver for Noop {}
    impl JoinObserver for Noop {}
    impl SourcePollObserver for Noop {}
    impl EffectObserver for Noop {}
    impl SinkDeliveryObserver for Noop {}
    impl StageLifecycleObserver for Noop {}

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
                Box::new(sink_delivery_observer("sink-delivery", Noop)),
                MiddlewareSurfaceKind::SinkDelivery,
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
    fn empty_set_builds_the_public_empty_bundle() {
        let bundle: StageObserverBundle = StageObserverSet::default().build();
        assert!(bundle.is_empty());
    }

    #[test]
    fn surface_catalogue_excludes_output_commit() {
        assert_eq!(
            OBSERVER_SURFACE_KINDS,
            &[
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::Stateful,
                MiddlewareSurfaceKind::Join,
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::SinkDelivery,
                MiddlewareSurfaceKind::StageLifecycle,
            ]
        );
    }
}
