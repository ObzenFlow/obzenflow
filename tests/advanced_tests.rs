// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// tests/advanced_tests.rs
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Test using the DSL macros with EventStore
#[tokio::test]
async fn test_dsl_pipeline() -> Result<()> {
    // Define pipeline stages
    #[derive(Clone, Debug)]
    struct EventGenerator {
        events: Vec<(String, serde_json::Value)>,
        emitted: usize,
        writer_id: WriterId,
    }

    impl EventGenerator {
        fn new() -> Self {
            let events = vec![
                ("Input".to_string(), json!({ "value": 10 })),
                ("Input".to_string(), json!({ "value": 20 })),
                ("Input".to_string(), json!({ "value": 30 })),
            ];
            Self {
                events,
                emitted: 0,
                writer_id: WriterId::from(StageId::new()),
            }
        }
    }

    impl FiniteSourceHandler for EventGenerator {
        fn next(
            &mut self,
        ) -> Result<
            Option<Vec<ChainEvent>>,
            obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
        > {
            if self.emitted < self.events.len() {
                let (event_type, payload) = &self.events[self.emitted];
                self.emitted += 1;
                Ok(Some(vec![ChainEventFactory::data_event(
                    self.writer_id,
                    event_type.clone(),
                    payload.clone(),
                )]))
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

    #[async_trait]
    impl TransformHandler for Doubler {
        fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
            if let Some(value) = event.payload().get("value").and_then(|v| v.as_u64()) {
                Ok(vec![ChainEventFactory::data_event(
                    event.writer_id,
                    "Doubled",
                    json!({
                        "value": value,
                        "doubled": value * 2,
                    }),
                )])
            } else {
                Ok(vec![event])
            }
        }

        async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
            Ok(())
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
    impl SinkHandler for Summer {
        async fn consume(
            &mut self,
            event: ChainEvent,
        ) -> std::result::Result<DeliveryPayload, HandlerError> {
            if let Some(doubled) = event.payload().get("doubled").and_then(|v| v.as_u64()) {
                self.total.fetch_add(doubled, Ordering::Relaxed);
            }
            Ok(DeliveryPayload::success(
                "summer",
                DeliveryMethod::Custom("Sum".to_string()),
                None,
            ))
        }
    }

    // Create shared state
    let total = Arc::new(AtomicU64::new(0));
    let summer = Summer::new(total.clone());

    // Run pipeline with DSL
    let handle = flow! {
        name: "dsl_transformation_test",
        journals: disk_journals(PathBuf::from("target/advanced_tests")),
        middleware: [],

        stages: {
            gen = source!("generator" => EventGenerator::new());
            dbl = transform!("doubler" => Doubler::new());
            sum = sink!("summer" => summer);
        },

        topology: {
            gen |> dbl;
            dbl |> sum;
        }
    }
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
