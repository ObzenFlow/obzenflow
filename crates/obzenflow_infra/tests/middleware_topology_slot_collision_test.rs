// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for FLOWIP-114p: topology config slot collisions are errors.

use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    ControlMiddlewareRole, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
    MiddlewareFactoryResult, MiddlewareHints, MiddlewareOverrideKey, MiddlewarePlanContribution,
    MiddlewareSafety, SourceMiddlewarePhase, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl::{flow, sink, source};
use obzenflow_runtime::pipeline::config::StageConfig;
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
struct NoopMiddleware;
impl Middleware for NoopMiddleware {
    fn label(&self) -> &'static str {
        "noop"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        MiddlewareAction::Continue
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

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(self.slot)
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(NoopMiddleware))
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
