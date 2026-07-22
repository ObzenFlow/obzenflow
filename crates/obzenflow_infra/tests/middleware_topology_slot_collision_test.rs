// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for FLOWIP-114p: topology config slot collisions are errors.

use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareHints,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_runtime::stages::observer::StageLifecycleObserver;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestEvent;
impl TypedPayload for TestEvent {
    const EVENT_TYPE: &'static str = "test.topology_slot_collision";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug)]
struct NoopObserver;
impl StageLifecycleObserver for NoopObserver {
    fn label(&self) -> &'static str {
        "noop"
    }
}

struct FamilyA;
struct FamilyB;

#[derive(Clone)]
struct SlotFactory {
    label: &'static str,
    key: MiddlewareOverrideKey,
    slot: TopologyMiddlewareConfigSlot,
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
async fn topology_config_slot_collisions_are_configuration_errors() {
    let built = flow! {
        name: "topology_slot_collision",
        journals: obzenflow_infra::journal::memory_journals(),
        middleware: [
            SlotFactory {
                label: "slot.a",
                key: MiddlewareOverrideKey::of::<FamilyA>("family.a"),
                slot: TopologyMiddlewareConfigSlot::CircuitBreaker,
            },
            SlotFactory {
                label: "slot.b",
                key: MiddlewareOverrideKey::of::<FamilyB>("family.b"),
                slot: TopologyMiddlewareConfigSlot::CircuitBreaker,
            }
        ],

        stages: {
            src = source!(TestEvent => placeholder!());
            snk = sink!(TestEvent => placeholder!());
        },

        topology: {
            src |> snk;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let err = built.err().expect("expected flow build to fail");
    match err.error {
        obzenflow_dsl::dsl::FlowBuildError::StageResourcesFailed(msg) => {
            assert!(msg
                .contains("multiple middleware claiming the CircuitBreaker topology config slot"));
        }
        other => panic!("expected StageResourcesFailed, got {other:?}"),
    }
}
