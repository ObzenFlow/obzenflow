// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Busy controls must not starve owner results or lifecycle progress.
use super::support::*;
use crate::bootstrap::{
    bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig, StartupMode,
};
use crate::pipeline::fsm::PipelineFsmEvent as E;
use crate::pipeline::PipelineState as S;
use crate::supervised_base::ChannelBuilder;
use obzenflow_core::event::context::StageType;
use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::SystemId;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn pipeline_supervisor_has_no_inline_fsm_or_child_journal_reader() {
    let source = include_str!("../supervisor.rs");
    assert!(!source.contains("fsm!"));
    for reader in [
        "report_reader",
        "reader_from",
        "committed_position",
        "read_event",
        "read_all_unordered",
    ] {
        assert!(
            !source.contains(reader),
            "parent coordination contains {reader}"
        );
    }
}

pub async fn persistent_controls_cannot_starve_acknowledgements_or_child_termination(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..Default::default()
    });
    let mut journals = make_journals();
    let system_id = SystemId::new();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal);
    let source_handle = owned_test_stage(source, StageType::FiniteSource, None);
    let sink_handle = owned_test_stage(sink, StageType::Sink, None);
    let source_results = source_handle.signals.clone();
    let sink_results = sink_handle.signals.clone();
    ctx.source_supervisors
        .insert(source, Arc::new(source_handle));
    ctx.stage_supervisors.insert(sink, Arc::new(sink_handle));
    let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
    let mut states = watcher.subscribe();
    let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
    let controls = tokio::spawn(async move {
        while sender.send(E::Start).await.is_ok() {
            tokio::task::yield_now().await;
        }
    });
    wait_for_state(&mut states, "Running", |s| matches!(s, S::Running)).await;
    source_results.complete();
    sink_results.complete();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    controls.await.unwrap();
    assert_eq!(*states.borrow(), S::Drained);
}
