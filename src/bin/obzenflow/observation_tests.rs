// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::archive::manifest::{
    RunManifest, OBSERVABILITY_CAPTURE_CAPABILITY, RUN_MANIFEST_FILENAME,
};
use obzenflow_core::{FlowId, JournalOwner, SystemId};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::runtime_config::JournalSync;

#[tokio::test]
async fn pending_tail_reaches_wait_decision_and_detaches_without_inventing_settlement() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = SystemId::new();
    let flow_id = FlowId::new();
    let _system = DiskJournal::<SystemEvent>::with_owner_in_run(
        dir.path().join("system.log"),
        JournalOwner::system(pipeline),
        flow_id,
        JournalSync::BeforeAck,
    )
    .unwrap();
    let manifest = RunManifest {
        metrics_journals: None,
        journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
        obzenflow_version: env!("CARGO_PKG_VERSION").into(),
        flow_id: flow_id.to_string(),
        recorded_flow_id: None,
        pipeline_writer_id: pipeline.into(),
        flow_name: "pending_viewer".into(),
        created_at: "2026-09-30T00:00:00Z".parse().unwrap(),
        replay: None,
        resume: None,
        interrupted_appends: Vec::new(),
        effective_config: None,
        system_journal_file: "system.log".into(),
        stages: Default::default(),
        capabilities: [(OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1)].into(),
        bounded_direct_fact_admission: vec![],
    };
    std::fs::write(
        dir.path().join(RUN_MANIFEST_FILENAME),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let snapshot = open_disk_run(dir.path()).await.unwrap();
    let cli = Cli::parse_from(["obzenflow", "show", "unused", "--follow", "--jsonl"]);
    let Command::Show(args) = cli.command else {
        unreachable!()
    };
    let mut renderer = Renderer::new(&args.view, false, true, snapshot.journals());
    let (at_wait, reached) = tokio::sync::oneshot::channel();
    let (detach, detached) = tokio::sync::oneshot::channel();
    let mut at_wait = Some(at_wait);
    let mut output = Vec::new();
    let mut diagnostics = Vec::new();
    {
        let observe = follow_records(
            snapshot.into_tail(),
            &args.view,
            &mut renderer,
            (&mut output, &mut diagnostics),
            async {
                detached.await.unwrap();
                Ok(())
            },
            || {
                at_wait
                    .take()
                    .expect("first pending decision")
                    .send(())
                    .unwrap();
                std::future::pending()
            },
        );
        tokio::pin!(observe);
        tokio::select! {
            result = &mut observe => panic!("pending archive ended before its pending decision: {result:?}"),
            _ = reached => {}
        }
        // The operation has read Pending, checked the actual progress, and entered
        // its wait. Only now may this test deliver the user's detach request.
        detach.send(()).unwrap();
        assert_eq!(observe.await.unwrap(), 0);
    }
    let summary: serde_json::Value = serde_json::from_slice(&diagnostics).unwrap();
    assert_eq!(summary["event"], "run_observation_summary");
    assert_eq!(
        summary["reason"],
        "detached; application execution is independent"
    );
    assert!(summary["progress"]["settled_prefix"].is_null());
    assert!(output.is_empty());
}

#[tokio::test]
async fn parent_lineage_names_own_tail() {
    use obzenflow_core::event::{PipelineLifecycleEvent, SystemPayload};
    use obzenflow_core::Journal;
    let dir = tempfile::tempdir().unwrap();
    let pipeline = SystemId::new();
    let flow_id = FlowId::new();
    let system = DiskJournal::<SystemEvent>::with_owner_in_run(
        dir.path().join("system.log"),
        JournalOwner::system(pipeline),
        flow_id,
        JournalSync::BeforeAck,
    )
    .unwrap();
    system
        .append(
            SystemEvent::new(
                pipeline.into(),
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
                    stage_count: Some(0),
                }),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    drop(system);
    let manifest = RunManifest {
        metrics_journals: None,
        journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
        obzenflow_version: env!("CARGO_PKG_VERSION").into(),
        flow_id: flow_id.to_string(),
        recorded_flow_id: None,
        pipeline_writer_id: pipeline.into(),
        flow_name: "interrupted".into(),
        created_at: "2026-11-07T00:00:00Z".parse().unwrap(),
        replay: None,
        resume: None,
        interrupted_appends: Vec::new(),
        effective_config: None,
        system_journal_file: "system.log".into(),
        stages: Default::default(),
        capabilities: [(OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1)].into(),
        bounded_direct_fact_admission: vec![],
    };
    std::fs::write(
        dir.path().join(RUN_MANIFEST_FILENAME),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let system_log = dir.path().join("system.log");
    let committed_end = std::fs::metadata(&system_log).unwrap().len();
    std::io::Write::write_all(
        &mut std::fs::OpenOptions::new()
            .append(true)
            .open(&system_log)
            .unwrap(),
        &[0; 4096],
    )
    .unwrap();

    let mut snapshot = open_disk_run(dir.path()).await.unwrap();
    let cli = Cli::parse_from(["obzenflow", "show", "unused", "--color", "never"]);
    let Command::Show(args) = cli.command else {
        unreachable!()
    };
    let mut renderer = Renderer::new(&args.view, false, true, snapshot.journals());
    let mut output = Vec::new();
    while let Some(record) = snapshot.next().await.unwrap() {
        renderer.record(&mut output, Listed::from(record)).unwrap();
    }
    let tail = snapshot.into_tail();
    renderer
        .finish(
            &mut output,
            &mut Vec::new(),
            Observed::of(&tail),
            ObservationEnd::Snapshot,
        )
        .unwrap();
    let shown: String = String::from_utf8(output)
        .unwrap()
        .split_whitespace()
        .collect();
    let expected: String = format!(
        "LINEAGE Tail system.log ends at offset {committed_end}; 4096 bytes after it are \
         left in place (zero-filled)"
    )
    .split_whitespace()
    .collect();
    assert!(shown.contains(&expected), "{shown}");
}
