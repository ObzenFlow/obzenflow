// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

/// FLOWIP-120c H2 kind agreement: a factory and the instance it creates
/// must report the same middleware kind, because build-time guards read
/// the factory while chain runners enforce on the instance.
#[test]
fn factory_and_instance_kinds_agree() {
    use crate::middleware::control::rate_limiter::RateLimiterBuilder;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{MiddlewareFactory, MiddlewareKind};
    use obzenflow_runtime::pipeline::config::StageConfig;

    let config = StageConfig {
        stage_id: StageId::new(),
        name: "kind_agreement".to_string(),
        flow_name: "kind_agreement_flow".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        resolved_policies: Default::default(),
    };

    let factories: Vec<(Box<dyn MiddlewareFactory>, MiddlewareKind, bool)> = vec![
        (
            crate::middleware::control::circuit_breaker::circuit_breaker(3),
            MiddlewareKind::Policy,
            true,
        ),
        (
            RateLimiterBuilder::new(5.0).build(),
            MiddlewareKind::Policy,
            false,
        ),
    ];

    for (factory, expected, supports_generic_create) in factories {
        assert_eq!(
            factory.kind(),
            expected,
            "factory '{}' kind mismatch",
            factory.label()
        );
        if !supports_generic_create {
            assert!(
                factory
                    .create(&config, Arc::new(ControlMiddlewareAggregator::new()))
                    .is_err(),
                "factory '{}' should fail closed on generic create",
                factory.label()
            );
            continue;
        }
        let instance = factory
            .create(&config, Arc::new(ControlMiddlewareAggregator::new()))
            .expect("factory should materialize");
        assert_eq!(
            instance.kind(),
            factory.kind(),
            "instance kind for '{}' must agree with its factory",
            factory.label()
        );
    }
}
