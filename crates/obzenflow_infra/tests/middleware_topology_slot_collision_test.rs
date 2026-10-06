// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115v: resolved attachments keep observers independent and reject real conflicts.

use obzenflow_adapters::middleware::{
    circuit_breaker, rate_limit, validate_attachment_request, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareHints, MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::{FlowBuildError, MiddlewarePlanError};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_runtime::stages::observer::StageLifecycleObserver;
use obzenflow_topology::{MiddlewareAuthoredSite, MiddlewareFamily, MiddlewareOperation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestEvent;
impl TypedPayload for TestEvent {
    const EVENT_TYPE: &'static str = "test.topology_slot_collision";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug)]
struct NoopObserver;
impl StageLifecycleObserver for NoopObserver {}

struct FamilyA;
struct FamilyB;

#[derive(Clone)]
struct SlotFactory {
    label: &'static str,
    key: MiddlewareOverrideKey,
    slot: TopologyMiddlewareConfigSlot,
    materialisations: Arc<AtomicUsize>,
}

impl MiddlewareFactory for SlotFactory {
    fn label(&self) -> &'static str {
        self.label
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        self.key
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(self.label, vec![MiddlewareSurfaceKind::StageLifecycle])
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(self.slot)
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;
        self.materialisations.fetch_add(1, Ordering::SeqCst);
        match request.surface.kind() {
            MiddlewareSurfaceKind::StageLifecycle => Ok(
                MiddlewareSurfaceAttachment::stage_lifecycle_observer(Arc::new(NoopObserver)),
            ),
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!("unsupported observer surface {other:?}")),
            )),
        }
    }

    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Safe
    }

    fn hints(&self) -> MiddlewareHints {
        MiddlewareHints::default()
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        Some(json!({"label": self.label}))
    }
}

#[tokio::test]
async fn legacy_slots_and_override_families_do_not_merge_observer_attachments() {
    let materialisations = Arc::new(AtomicUsize::new(0));
    let factory_materialisations = materialisations.clone();
    let built = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "independent_observer_attachments",
            journals: obzenflow_infra::journal::memory_journals(),

            stages: {
                src = source!(TestEvent => placeholder!() with {
                    SlotFactory {
                        label: "slot.a",
                        key: MiddlewareOverrideKey::of::<FamilyA>("family.a"),
                        slot: TopologyMiddlewareConfigSlot::CircuitBreaker,
                        materialisations: factory_materialisations.clone(),
                    },
                    SlotFactory {
                        label: "slot.b",
                        key: MiddlewareOverrideKey::of::<FamilyB>("family.b"),
                        slot: TopologyMiddlewareConfigSlot::CircuitBreaker,
                        materialisations: factory_materialisations.clone(),
                    },
                    SlotFactory {
                        label: "family.second",
                        key: MiddlewareOverrideKey::of::<FamilyA>("family.a"),
                        slot: TopologyMiddlewareConfigSlot::RateLimiter,
                        materialisations: factory_materialisations.clone(),
                    },
                    circuit_breaker().consecutive_failures(3)
                });
                snk = sink!(TestEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("distinct observer labels define independent attachments");

    assert_eq!(materialisations.load(Ordering::SeqCst), 3);
    let topology = built.topology().expect("built topology");
    let source = topology.stages().find(|stage| stage.name == "src").unwrap();
    let attachments = &source.middleware.as_ref().unwrap().attachments;
    assert_eq!(attachments.len(), 4);
    assert_eq!(
        attachments
            .iter()
            .map(|attachment| &attachment.key)
            .collect::<HashSet<_>>()
            .len(),
        4,
        "legacy slot and override hints must not collapse resolved attachment identities"
    );
    let observers: Vec<_> = attachments
        .iter()
        .filter(|attachment| attachment.family() == MiddlewareFamily::Observer)
        .collect();
    assert_eq!(observers.len(), 3);
    assert_eq!(
        observers
            .iter()
            .map(|attachment| attachment.label.as_str())
            .collect::<HashSet<_>>(),
        HashSet::from(["slot.a", "slot.b", "family.second"])
    );
    assert!(observers.iter().all(|attachment| {
        attachment.authored_site == MiddlewareAuthoredSite::Implementation
            && attachment.operation == MiddlewareOperation::Lifecycle
    }));
    let breaker = attachments
        .iter()
        .find(|attachment| attachment.family() == MiddlewareFamily::CircuitBreaker)
        .expect("the real built-in breaker retains its own binding");
    assert_eq!(breaker.label, "circuit_breaker");
    assert_eq!(breaker.operation, MiddlewareOperation::SourcePoll);
    assert!(!attachments
        .iter()
        .any(|attachment| attachment.family() == MiddlewareFamily::RateLimiter));
}

#[tokio::test]
async fn duplicate_builtin_controls_are_rejected_before_materialisation() {
    let materialisations = Arc::new(AtomicUsize::new(0));
    let factory_materialisations = materialisations.clone();
    let built = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "duplicate_source_controls",
            journals: obzenflow_infra::journal::memory_journals(),

            stages: {
                src = source!(TestEvent => placeholder!() with {
                    SlotFactory {
                        label: "observer",
                        key: MiddlewareOverrideKey::of::<FamilyA>("family.a"),
                        slot: TopologyMiddlewareConfigSlot::CircuitBreaker,
                        materialisations: factory_materialisations,
                    },
                    rate_limit(10.0),
                    rate_limit(20.0)
                });
                snk = sink!(TestEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let error = built.err().expect("duplicate source controls must fail");
    assert!(
        error.run.is_none(),
        "validation must precede substrate selection"
    );
    assert_eq!(materialisations.load(Ordering::SeqCst), 0);
    assert!(matches!(
        error.error,
        FlowBuildError::MiddlewarePlan(MiddlewarePlanError::InvalidAttachment {
            stage,
            effect,
            message,
        }) if stage == "src" && effect.is_empty()
            && message.contains("duplicate 'rate_limiter' controls")
            && message.contains("SourcePoll")
    ));
}

#[tokio::test]
async fn repeated_observer_labels_are_rejected_before_materialisation() {
    let materialisations = Arc::new(AtomicUsize::new(0));
    let factory_materialisations = materialisations.clone();
    let built = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "duplicate_observer_labels",
            journals: obzenflow_infra::journal::memory_journals(),

            stages: {
                src = source!(TestEvent => placeholder!() with {
                    SlotFactory {
                        label: "same_label",
                        key: MiddlewareOverrideKey::of::<FamilyA>("family.a"),
                        slot: TopologyMiddlewareConfigSlot::CircuitBreaker,
                        materialisations: factory_materialisations.clone(),
                    },
                    SlotFactory {
                        label: "same_label",
                        key: MiddlewareOverrideKey::of::<FamilyB>("family.b"),
                        slot: TopologyMiddlewareConfigSlot::RateLimiter,
                        materialisations: factory_materialisations,
                    }
                });
                snk = sink!(TestEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let error = built
        .err()
        .expect("same-site observer labels must be unique");
    assert!(
        error.run.is_none(),
        "validation must precede substrate selection"
    );
    assert_eq!(materialisations.load(Ordering::SeqCst), 0);
    assert!(matches!(
        error.error,
        FlowBuildError::MiddlewarePlan(MiddlewarePlanError::InvalidAttachment {
            stage,
            effect,
            message,
        }) if stage == "src" && effect.is_empty()
            && message.contains("observer label 'same_label' occurs twice at the same attachment site")
    ));
}
