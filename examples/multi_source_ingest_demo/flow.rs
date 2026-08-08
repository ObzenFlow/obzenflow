// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow definition for the multi_source_ingest_demo example.
//!
//! This file is the canonical teaching surface for FLOWIP-114c. Three
//! sources of three different concrete types feed three per-branch
//! alignment transforms, which all emit the common `IngestedEvent`
//! envelope. The aggregator fan-in is homogeneous on `IngestedEvent`.
//! Every stage is typed; every edge is typed.

use anyhow::Result;
use obzenflow_core::id::StageId;
use obzenflow_core::WriterId;
use obzenflow_dsl::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;

use crate::domain::{FileLine, IngestSummary, IngestedEvent, KafkaRawEvent, WebhookEnvelope};
use crate::handlers::{
    align_file_fn, align_kafka_fn, align_webhook_fn, FileSource, IngestAggregator, KafkaSource,
    SummaryConsole, WebhookSource,
};

/// Build and run the multi_source_ingest_demo flow.
pub async fn run() -> Result<()> {
    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let kafka_handler = KafkaSource {
            writer_id: WriterId::from(StageId::new()),
            remaining: 3,
        };
        let webhook_handler = WebhookSource {
            writer_id: WriterId::from(StageId::new()),
            remaining: 3,
        };
        let file_handler = FileSource {
            writer_id: WriterId::from(StageId::new()),
            remaining: 3,
        };
        let align_kafka_handler = align_kafka_fn();
        let align_webhook_handler = align_webhook_fn();
        let align_file_handler = align_file_fn();
        let aggregator_handler = IngestAggregator;
        let summary_sink_handler = SummaryConsole;

        Ok(flow! {
            name: "multi_source_ingest_demo",
            journals: disk_journals(std::path::PathBuf::from("target/multi_source_ingest_demo")),

            stages: {
                // Three sources, three different concrete types.
                kafka_source   = source!(KafkaRawEvent   => kafka_handler);
                webhook_source = source!(WebhookEnvelope => webhook_handler);
                file_source    = source!(FileLine        => file_handler);

                // Each alignment transform emits the common fan-in type.
                align_kafka   = transform!(KafkaRawEvent   -> IngestedEvent => align_kafka_handler);
                align_webhook = transform!(WebhookEnvelope -> IngestedEvent => align_webhook_handler);
                align_file    = transform!(FileLine        -> IngestedEvent => align_file_handler);

                aggregator   = stateful!(IngestedEvent -> IngestSummary => aggregator_handler);
                summary_sink = sink!(IngestSummary => summary_sink_handler);
            },

            topology: {
                kafka_source   |> align_kafka;
                webhook_source |> align_webhook;
                file_source    |> align_file;

                align_kafka   |> aggregator;
                align_webhook |> aggregator;
                align_file    |> aggregator;

                aggregator |> summary_sink;
            }
        })
    });

    FlowApplication::run(definition).await?;
    Ok(())
}
