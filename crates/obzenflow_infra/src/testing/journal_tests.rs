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

fn read<T: JournalEvent>(
    path: &Path,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut file = BufReader::new(std::fs::File::open(path)?);
    let mut decoder = Decoder::cold(path);
    let mut bytes = Vec::new();
    let mut offset = 0;
    let mut records = Vec::new();
    while let Some((consumed, termination)) = read_frame_sync(&mut file, &mut bytes)? {
        match dispose(
            classify_frame::<T>(&bytes, &mut decoder, offset),
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
        read::<SystemEvent>(path)
            .unwrap()
            .into_iter()
            .map(protected)
            .collect::<Vec<_>>()
    });
    assert_eq!(before.each_ref().map(Vec::len), [2, 1, 1]);
    let original = paths.each_ref().map(|path| std::fs::read(path).unwrap());
    for export_verified in [false, true] {
        for (path, bytes) in paths.iter().zip(&original) {
            std::fs::write(path, bytes).unwrap();
        }
        eprintln!("archive rewrite regression: phase=rewrite; records=4, consumers=2, export_verified={export_verified}");
        let output = root.join("verified.jsonl");
        let removed = if export_verified {
            let expected: Vec<_> = before.iter().flatten().cloned().collect();
            omit_observations_and_export_verified(root, &BTreeSet::new(), &expected, &output)
                .unwrap()
        } else {
            omit_observations(root, |_| false).unwrap()
        };
        assert_eq!(removed, 2);
        for (path, expected) in paths.iter().zip(&before) {
            assert_eq!(&read::<SystemEvent>(path).unwrap(), expected);
        }
        // Recreate each omitted consumer independently. Both a cold decoder
        // and the public export must reject the stale physical reference.
        for index in [1, 2] {
            eprintln!(
                "archive rewrite regression: phase=stale-consumer-control; file={}",
                names[index]
            );
            let corrected = std::fs::read(&paths[index]).unwrap();
            std::fs::write(&paths[index], &original[index]).unwrap();
            let stale = read::<SystemEvent>(&paths[index]);
            let stale_export = export_jsonl(root, Some(&output));
            std::fs::write(&paths[index], corrected).unwrap();
            assert!(
                stale.is_err() && stale_export.is_err(),
                "stale {} must reference the removed carrier definition",
                names[index]
            );
            assert_eq!(read::<SystemEvent>(&paths[index]).unwrap(), before[index]);
        }
    }
}

struct ObservationArchive {
    directory: tempfile::TempDir,
    rows: Vec<serde_json::Value>,
    observed: [JournalCommitRef; 3],
}

// The two grouped source placements and their forwarded placement share one
// event id. Physical filename order deliberately differs from public export
// order. A receipt refers to the second source placement, not just its event id.
async fn observation_archive() -> ObservationArchive {
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::delivery_payload::{
        DeliveryMethod, DeliveryOutcome, DeliveryPayload, DeliverySubject,
    };
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::journal::archive::manifest::RunManifestStage;
    use obzenflow_core::StageId;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let flow = FlowId::new();
    let pipeline = SystemId::new();
    let stages = [StageId::new(), StageId::new()];
    let system = DiskJournal::<SystemEvent>::with_owner_in_run(
        root.join("system.log"),
        JournalOwner::system(pipeline),
        flow,
    )
    .unwrap();
    system
        .append(
            SystemEventFactory::new(pipeline).pipeline_running(),
            Default::default(),
        )
        .await
        .unwrap();
    let source = DiskJournal::<ChainEvent>::with_owner_in_run(
        root.join("z-source.log"),
        JournalOwner::stage(stages[0]),
        flow,
    )
    .unwrap();
    let sink = DiskJournal::<ChainEvent>::with_owner_in_run(
        root.join("a-sink.log"),
        JournalOwner::stage(stages[1]),
        flow,
    )
    .unwrap();
    let mut event = ChainEventFactory::data_event(
        stages[0].into(),
        "fixture.input",
        std::num::NonZeroU32::MIN,
        serde_json::json!({"id": 7}),
    );
    event.envelope.observability = Some(observation(flow, pipeline));
    let group = source
        .append_group(
            "two-placements",
            vec![event.clone(), event.clone()],
            Default::default(),
        )
        .await
        .unwrap();
    let forwarded = sink.append(event, Default::default()).await.unwrap();
    let receipt = ChainEventFactory::delivery_event(
        stages[1].into(),
        DeliveryPayload {
            subject: DeliverySubject::from_record(&group[1]),
            outcome: DeliveryOutcome::success(DeliveryMethod::Noop, None),
        },
    );
    sink.append(receipt, Default::default()).await.unwrap();
    let observed = [
        group[0].commitment(),
        group[1].commitment(),
        forwarded.commitment(),
    ];
    drop((source, sink, system));

    let manifest = RunManifest {
        journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
        obzenflow_version: env!("CARGO_PKG_VERSION").into(),
        flow_id: flow.to_string(),
        pipeline_writer_id: pipeline.into(),
        flow_name: "observation_preservation".into(),
        created_at: chrono::Utc::now(),
        replay: None,
        resume: None,
        stages: [
            (
                "a_source",
                stages[0],
                StageType::FiniteSource,
                "z-source.log",
            ),
            ("b_sink", stages[1], StageType::Sink, "a-sink.log"),
        ]
        .into_iter()
        .map(|(name, stage, stage_type, file)| {
            (
                name.into(),
                RunManifestStage {
                    dsl_var: name.into(),
                    stage_type,
                    is_effectful: false,
                    stage_id: stage.to_string(),
                    stage_logic_version: "1".into(),
                    data_journal_file: file.into(),
                    error_journal_file: format!("{name}-error.log"),
                    inbound: vec![],
                    ordered_delivery: true,
                },
            )
        })
        .collect(),
        system_journal_file: "system.log".into(),
        metrics_journals: None,
        effective_config: None,
        capabilities: [(OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1)].into(),
        bounded_direct_fact_admission: vec![],
    };
    std::fs::write(
        root.join(RUN_MANIFEST_FILENAME),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let export = root.join("original.jsonl");
    export_jsonl(root, Some(&export)).unwrap();
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(export)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 5);
    ObservationArchive {
        directory,
        rows,
        observed,
    }
}

#[tokio::test]
async fn archive_export_verification_preserves_grouped_and_forwarded_commitments() {
    let fixture = observation_archive().await;
    let root = fixture.directory.path();
    assert_eq!(fixture.observed[0].event_id, fixture.observed[1].event_id);
    assert_eq!(fixture.observed[0].event_id, fixture.observed[2].event_id);
    assert_ne!(fixture.observed[0].sequence, fixture.observed[1].sequence);
    assert_ne!(
        fixture.observed[0].journal_writer_id,
        fixture.observed[2].journal_writer_id
    );
    // Known fixture positions are an independent oracle, not the rewriter's
    // commitment selector. Keep all, only source member 1, then no observations.
    for (retained, omitted_rows, removed) in [
        (fixture.observed.into_iter().collect(), vec![], 0),
        (BTreeSet::from([fixture.observed[0]]), vec![2, 3], 2),
        (BTreeSet::new(), vec![1, 2, 3], 1),
    ] {
        let mut expected = fixture.rows.clone();
        for index in omitted_rows {
            expected[index] = protected(expected[index].clone());
        }
        assert_eq!(
            omit_observations_and_export_verified(
                root,
                &retained,
                &expected,
                &root.join("checked.jsonl")
            )
            .unwrap(),
            removed
        );
        assert_eq!(
            read::<ChainEvent>(&root.join("z-source.log")).unwrap(),
            expected[1..3]
        );
        assert_eq!(
            read::<ChainEvent>(&root.join("a-sink.log")).unwrap(),
            expected[3..5]
        );
    }
}

#[tokio::test]
async fn archive_export_verification_rejects_incomplete_or_changed_oracles() {
    let fixture = observation_archive().await;
    let root = fixture.directory.path();
    let retained = BTreeSet::from([fixture.observed[0]]);
    let mut expected = fixture.rows.clone();
    for index in [2, 3] {
        expected[index] = protected(expected[index].clone());
    }
    // Exercise the public operation, so a rewrite-only early return cannot
    // accidentally pass. Every mismatch is in otherwise well-formed JSON.
    for defect in [
        "retained-observation",
        "missing-retained-observation",
        "payload",
        "descriptor",
        "receipt-subject",
        "group",
        "member",
        "missing-row",
        "extra-row",
        "duplicate-row",
        "reordered-rows",
    ] {
        let mut wrong = expected.clone();
        match defect {
            "retained-observation" => {
                wrong[1]["envelope"]["observability"]["capture"]["observed_at_ms"] = 99.into()
            }
            "missing-retained-observation" => wrong[1] = protected(wrong[1].clone()),
            "payload" => wrong[1]["payload"]["id"] = 8.into(),
            "descriptor" => {
                wrong[1]["envelope"]["provenance"]["event"]["payload_schema_version"] = 2.into()
            }
            "receipt-subject" => wrong[4]["payload"]["subject"]["input"]["sequence"] = 1.into(),
            "group" => {
                wrong[1]["envelope"]["provenance"]["journal"]["journal_group_id"] = "changed".into()
            }
            "member" => {
                wrong[1]["envelope"]["provenance"]["journal"]["journal_group_member"]["index"] =
                    1.into()
            }
            "missing-row" => {
                wrong.pop();
            }
            "extra-row" => wrong.push(wrong[4].clone()),
            "duplicate-row" => wrong[3] = wrong[2].clone(),
            "reordered-rows" => wrong.swap(1, 2),
            _ => unreachable!(),
        }
        let error = omit_observations_and_export_verified(
            root,
            &retained,
            &wrong,
            &root.join("rejected.jsonl"),
        )
        .expect_err(defect);
        assert!(
            error.to_string().contains("archive export"),
            "{defect}: {error}"
        );
    }
}

#[tokio::test]
async fn archive_export_verification_rejects_corruption_before_replacement() {
    let fixture = observation_archive().await;
    let root = fixture.directory.path();
    let path = root.join("z-source.log");
    let mut corrupt = std::fs::read(&path).unwrap();
    corrupt[codec::frame::HEADER_LEN] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    // This earlier-sorted journal must not be replaced before the later source
    // has been decoded successfully, even though it may already be staged.
    let sink = std::fs::read(root.join("a-sink.log")).unwrap();
    let output = root.join("must-not-exist.jsonl");
    assert!(omit_observations_and_export_verified(root, &BTreeSet::new(), &[], &output).is_err());
    assert!(!output.exists());
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
    assert_eq!(std::fs::read(root.join("a-sink.log")).unwrap(), sink);
}
