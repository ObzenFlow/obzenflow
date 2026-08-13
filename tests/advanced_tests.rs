// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// tests/advanced_tests.rs
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler, TypedTransformHandler,
};
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

/// File-local payload for the advanced tests. The JSON shape matches
/// what `EventGenerator` / `Doubler` emit; the type fingerprints the
/// stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AdvancedTestEvent {
    value: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    doubled: Option<u64>,
}

impl TypedPayload for AdvancedTestEvent {
    const EVENT_TYPE: &'static str = "advanced_tests.event";
}
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Test using the DSL macros with EventStore
#[tokio::test]
async fn test_dsl_pipeline() -> Result<()> {
    // Define pipeline stages
    #[derive(Clone, Debug)]
    struct EventGenerator {
        events: Vec<AdvancedTestEvent>,
        emitted: usize,
    }

    impl EventGenerator {
        fn new() -> Self {
            let events = vec![10, 20, 30]
                .into_iter()
                .map(|value| AdvancedTestEvent {
                    value,
                    doubled: None,
                })
                .collect();
            Self { events, emitted: 0 }
        }
    }

    impl TypedFiniteSourceHandler for EventGenerator {
        type Output = AdvancedTestEvent;

        fn next(
            &mut self,
        ) -> Result<
            Option<Vec<Self::Output>>,
            obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
        > {
            if self.emitted < self.events.len() {
                let event = self.events[self.emitted].clone();
                self.emitted += 1;
                Ok(Some(vec![event]))
            } else {
                Ok(None)
            }
        }
    }

    #[derive(Clone, Debug)]
    struct Doubler;

    impl Doubler {
        fn new() -> Self {
            Self
        }
    }

    impl TypedTransformHandler for Doubler {
        type Input = AdvancedTestEvent;
        type Output = AdvancedTestEvent;

        fn process(
            &self,
            input: AdvancedTestEvent,
        ) -> std::result::Result<AdvancedTestEvent, HandlerError> {
            Ok(AdvancedTestEvent {
                value: input.value,
                doubled: Some(input.value * 2),
            })
        }
    }

    #[derive(Clone, Debug)]
    struct Summer {
        total: Arc<AtomicU64>,
    }

    impl Summer {
        fn new(total: Arc<AtomicU64>) -> Self {
            Self { total }
        }
    }

    #[async_trait]
    impl InlineSink for Summer {
        type Input = AdvancedTestEvent;

        fn describe(&self) -> SinkDescription {
            SinkDescription::unspecified()
        }

        async fn write(
            &mut self,
            event: AdvancedTestEvent,
            _context: SinkWriteContext,
        ) -> std::result::Result<SinkWriteReport, HandlerError> {
            if let Some(doubled) = event.doubled {
                self.total.fetch_add(doubled, Ordering::Relaxed);
            }
            Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
                DeliveryMethod::Custom("Sum".to_string()),
                None,
            )))
        }
    }

    // Create shared state
    let total = Arc::new(AtomicU64::new(0));
    let summer_total = total.clone();

    // Run pipeline with DSL
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let generator_handler = EventGenerator::new();
        let doubler_handler = Doubler::new();
        let summer_handler = Summer::new(summer_total);

        Ok(flow! {
            name: "dsl_transformation_test",
            journals: disk_journals(PathBuf::from("target/advanced_tests")),

            stages: {
                generator = source!(AdvancedTestEvent => generator_handler);
                doubler = transform!(AdvancedTestEvent -> AdvancedTestEvent => doubler_handler);
                summer = sink!(AdvancedTestEvent => summer_handler);
            },

            topology: {
                generator |> doubler;
                doubler |> summer;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Run the pipeline
    handle.run().await?;

    // Verify sum: (10 + 20 + 30) * 2 = 120
    assert_eq!(total.load(Ordering::Relaxed), 120);

    // Clean up
    // Cleanup handled by tempdir
    Ok(())
}

// Additional tests for more complex scenarios will be added as the DSL evolves
// to support features like multi-sink fanout, complex event filtering, etc.
