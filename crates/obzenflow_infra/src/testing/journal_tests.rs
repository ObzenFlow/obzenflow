// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::DiskJournal;
use obzenflow_core::event::observability::{
    CaptureReason, CaptureScope, CaptureSeq, CaptureStamp, ObservabilityContext,
};
use obzenflow_core::event::{MetricsCoordinationEvent, SystemEventFactory, SystemPayload};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, RunManifestMetrics, JOURNAL_SCHEMA_VERSION, OBSERVABILITY_CAPTURE_CAPABILITY,
    RUN_MANIFEST_FILENAME,
};
use obzenflow_core::{FlowId, Journal, JournalOwner, SystemId};

fn observation(flow: FlowId, observer: SystemId) -> ObservabilityContext {
    let mut packet = ObservabilityContext::new(CaptureStamp {
        capture_scope: CaptureScope {
            flow_id: flow,
            resume_generation: Default::default(),
        },
        observer: observer.into(),
        capture_seq: CaptureSeq(1),
        capture_reason: CaptureReason::Record,
        observed_at_ms: 1,
    });
    packet.processing_time = Some(obzenflow_core::time::MetricsDuration::from_millis(1));
    packet
}

fn read(path: &Path) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut file = BufReader::new(std::fs::File::open(path)?);
    let mut decoder = Decoder::cold(path);
    let mut bytes = Vec::new();
    let mut offset = 0;
    let mut records = Vec::new();
    while let Some((consumed, termination)) = read_frame_sync(&mut file, &mut bytes)? {
        match dispose(
            classify_frame::<SystemEvent>(&bytes, &mut decoder, offset),
            termination,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: false,
            },
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    records.push(serde_json::to_value(record)?);
                }
            }
            Disposition::Corrupt(problem) => return Err(problem.to_string().into()),
            _ => return Err("incomplete regression fixture".into()),
        }
        offset += consumed as u64;
    }
    Ok(records)
}

fn protected(mut record: serde_json::Value) -> serde_json::Value {
    record["envelope"]
        .as_object_mut()
        .unwrap()
        .remove("observability");
    record
}

#[tokio::test]
async fn archive_observation_rewrite_preserves_both_metrics_consumers() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let flow = FlowId::new();
    let pipeline = SystemId::new();
    let metrics = SystemId::new();
    let names = [
        "system.log",
        "metrics-coordination.log",
        "metrics-export.log",
    ];
    let paths = names.map(|name| root.join(name));
    let system = DiskJournal::<SystemEvent>::with_owner_in_run(
        paths[0].clone(),
        JournalOwner::system(pipeline),
        flow,
    )
    .unwrap();
    let coordination = DiskJournal::<SystemEvent>::with_owner_in_run(
        paths[1].clone(),
        JournalOwner::system(metrics),
        flow,
    )
    .unwrap();
    let export = DiskJournal::<SystemEvent>::with_owner_in_run(
        paths[2].clone(),
        JournalOwner::system(metrics),
        flow,
    )
    .unwrap();
    let factory = SystemEventFactory::new(pipeline);
    // Control the physical dependency: the metrics writer's first definition
    // is in the SECOND carrier, in optional evidence. Omitting observations
    // moves that carrier and removes the definition. Both subsequent metrics
    // consumers reuse it through the real shared definition store.
    let mut first = factory.pipeline_starting();
    first.envelope.observability = Some(observation(flow, pipeline));
    system.append(first, Default::default()).await.unwrap();
    let mut second = factory.pipeline_running();
    second.envelope.observability = Some(observation(flow, metrics));
    system.append(second, Default::default()).await.unwrap();
    coordination
        .append(
            SystemEventFactory::new(metrics).metrics_ready(),
            Default::default(),
        )
        .await
        .unwrap();
    export
        .append(
            SystemEvent::new(
                metrics.into(),
                SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Exported {
                    watermark: Default::default(),
                }),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    drop((system, coordination, export));
    let manifest = RunManifest {
        journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
        obzenflow_version: env!("CARGO_PKG_VERSION").into(),
        flow_id: flow.to_string(),
        pipeline_writer_id: pipeline.into(),
        flow_name: "rewrite_regression".into(),
        created_at: chrono::Utc::now(),
        replay: None,
        resume: None,
        stages: Default::default(),
        system_journal_file: names[0].into(),
        metrics_journals: Some(RunManifestMetrics {
            writer_id: metrics.into(),
            coordination_journal_file: names[1].into(),
            export_journal_file: names[2].into(),
        }),
        effective_config: None,
        capabilities: [(OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1)].into(),
        bounded_direct_fact_admission: vec![],
    };
    std::fs::write(
        root.join(RUN_MANIFEST_FILENAME),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let before = paths.each_ref().map(|path| {
        read(path)
            .unwrap()
            .into_iter()
            .map(protected)
            .collect::<Vec<_>>()
    });
    assert_eq!(before.each_ref().map(Vec::len), [2, 1, 1]);
    let original = paths.each_ref().map(|path| std::fs::read(path).unwrap());
    eprintln!("archive rewrite regression: phase=rewrite; records=4, consumers=2");
    assert_eq!(omit_observations(root, |_| false).unwrap(), 2);
    for (path, expected) in paths.iter().zip(&before) {
        assert_eq!(&read(path).unwrap(), expected);
    }
    // Recreate each omitted consumer independently. This is a deterministic
    // invalid-reference control, not a race over a live flow's export schedule.
    for index in [1, 2] {
        eprintln!(
            "archive rewrite regression: phase=stale-consumer-control; file={}",
            names[index]
        );
        let corrected = std::fs::read(&paths[index]).unwrap();
        std::fs::write(&paths[index], &original[index]).unwrap();
        let stale = read(&paths[index]);
        std::fs::write(&paths[index], corrected).unwrap();
        assert!(
            stale.is_err(),
            "stale {} must reference the removed carrier definition",
            names[index]
        );
        assert_eq!(read(&paths[index]).unwrap(), before[index]);
    }
}
