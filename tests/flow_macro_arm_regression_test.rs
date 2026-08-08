// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115r compile coverage for every surviving flow-macro arm.

#[cfg(feature = "test-support")]
use obzenflow_dsl::test_flow;
use obzenflow_dsl::{flow, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::EffectPortRegistry;

fn deferred_flow<F>(factory: F) -> FlowDefinition
where
    F: FnOnce() -> FlowDefinition + Send + 'static,
{
    FlowDefinition::materialize(move |_| Ok(factory()))
}

#[test]
fn flow_surviving_arm_matrix_compiles() {
    {
        let definition_0 = deferred_flow(move || {
            flow! {
                name: "flow_named_without_backpressure_omitted",
                journals: memory_journals(),
                stages: {},
                topology: {}
            }
        });
        drop(definition_0);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let definition_1 = deferred_flow(move || {
            flow! {
                name: "flow_named_without_backpressure_shorthand",
                journals: memory_journals(),
                effect_ports,
                stages: {},
                topology: {}
            }
        });
        drop(definition_1);
    }
    {
        let definition_2 = deferred_flow(move || {
            flow! {
                name: "flow_named_without_backpressure_expression",
                journals: memory_journals(),
                effect_ports: EffectPortRegistry::new(),
                stages: {},
                topology: {}
            }
        });
        drop(definition_2);
    }
    {
        let definition_3 = deferred_flow(move || {
            flow! {
                name: "flow_named_with_backpressure_omitted",
                journals: memory_journals(),
                backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
                stages: {},
                topology: {}
            }
        });
        drop(definition_3);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let definition_4 = deferred_flow(move || {
            flow! {
                name: "flow_named_with_backpressure_shorthand",
                journals: memory_journals(),
                backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
                effect_ports,
                stages: {},
                topology: {}
            }
        });
        drop(definition_4);
    }
    {
        let definition_5 = deferred_flow(move || {
            flow! {
                name: "flow_named_with_backpressure_expression",
                journals: memory_journals(),
                backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
                effect_ports: EffectPortRegistry::new(),
                stages: {},
                topology: {}
            }
        });
        drop(definition_5);
    }
    {
        let definition_6 = deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                stages: {},
                topology: {}
            }
        });
        drop(definition_6);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let definition_7 = deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                effect_ports,
                stages: {},
                topology: {}
            }
        });
        drop(definition_7);
    }
    {
        let definition_8 = deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                effect_ports: EffectPortRegistry::new(),
                stages: {},
                topology: {}
            }
        });
        drop(definition_8);
    }
    {
        let definition_9 = deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
                stages: {},
                topology: {}
            }
        });
        drop(definition_9);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let definition_10 = deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
                effect_ports,
                stages: {},
                topology: {}
            }
        });
        drop(definition_10);
    }
    {
        let definition_11 = deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
                effect_ports: EffectPortRegistry::new(),
                stages: {},
                topology: {}
            }
        });
        drop(definition_11);
    }
}

#[cfg(feature = "test-support")]
#[test]
fn test_flow_surviving_arm_matrix_compiles() {
    {
        let future_0 = test_flow! {
            name: "test_flow_named_without_backpressure_omitted",
            journals: memory_journals(),
            stages: {},
            topology: {}
        };
        drop(future_0);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let future_1 = test_flow! {
            name: "test_flow_named_without_backpressure_shorthand",
            journals: memory_journals(),
            effect_ports,
            stages: {},
            topology: {}
        };
        drop(future_1);
    }
    {
        let future_2 = test_flow! {
            name: "test_flow_named_without_backpressure_expression",
            journals: memory_journals(),
            effect_ports: EffectPortRegistry::new(),
            stages: {},
            topology: {}
        };
        drop(future_2);
    }
    {
        let future_3 = test_flow! {
            name: "test_flow_named_with_backpressure_omitted",
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
            stages: {},
            topology: {}
        };
        drop(future_3);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let future_4 = test_flow! {
            name: "test_flow_named_with_backpressure_shorthand",
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
            effect_ports,
            stages: {},
            topology: {}
        };
        drop(future_4);
    }
    {
        let future_5 = test_flow! {
            name: "test_flow_named_with_backpressure_expression",
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
            effect_ports: EffectPortRegistry::new(),
            stages: {},
            topology: {}
        };
        drop(future_5);
    }
    {
        let future_6 = test_flow! {
            journals: memory_journals(),
            stages: {},
            topology: {}
        };
        drop(future_6);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let future_7 = test_flow! {
            journals: memory_journals(),
            effect_ports,
            stages: {},
            topology: {}
        };
        drop(future_7);
    }
    {
        let future_8 = test_flow! {
            journals: memory_journals(),
            effect_ports: EffectPortRegistry::new(),
            stages: {},
            topology: {}
        };
        drop(future_8);
    }
    {
        let future_9 = test_flow! {
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
            stages: {},
            topology: {}
        };
        drop(future_9);
    }
    {
        let effect_ports = EffectPortRegistry::new();
        let future_10 = test_flow! {
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
            effect_ports,
            stages: {},
            topology: {}
        };
        drop(future_10);
    }
    {
        let future_11 = test_flow! {
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1),
            effect_ports: EffectPortRegistry::new(),
            stages: {},
            topology: {}
        };
        drop(future_11);
    }
}
