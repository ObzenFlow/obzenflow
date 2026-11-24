// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::replay_testkit;

use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Alpha {
    source: String,
    value: u64,
}

impl TypedPayload for Alpha {
    const EVENT_TYPE: &'static str = "flowip_134g.alpha";
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory")
        .map(|entry| entry.expect("flow directory entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("flow produced a replay archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest readable"),
    )
    .expect("manifest parses")
}

async fn read_stage_appended(run_dir: &Path, stage_name: &str) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest data journal for {stage_name}"));
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(journal_file)).await
}

fn data_signature(events: &[JournalRecord<ChainPayload>]) -> Vec<(String, serde_json::Value)> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Fact(_) | ChainPayload::CompositeData(_) => {
                Some((envelope.event_type().to_string(), envelope.payload()))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn lazy_iterator_replay_never_consumes_live_input() {
    fn definition(base: PathBuf, polls: Arc<AtomicUsize>) -> FlowDefinition {
        let counter = polls.clone();
        let input = obzenflow::stages::sources::ValuesSource::new(std::iter::from_fn(move || {
            let index = counter.fetch_add(1, Ordering::SeqCst);
            (index < 2).then(|| Alpha {
                source: "iterator".into(),
                value: index as u64,
            })
        }));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "source construction is cold"
        );
        FlowDefinition::materialize(move |_| {
            let output = replay_testkit::Discard::<Alpha>::default();
            Ok(flow! {
                name: "lazy_iterator_replay",
                journals: disk_journals(base),
                stages: {
                    input = source!(Alpha => input);
                    output = sink!(Alpha => output);
                },
                topology: { input |> output; }
            })
        })
    }

    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().join("journals");
    let live_polls = Arc::new(AtomicUsize::new(0));
    replay_testkit::unsynced_application()
        .with_cli_args(["lazy-iterator-live"])
        .run_async(definition(base.clone(), live_polls.clone()))
        .await
        .unwrap();
    assert_eq!(live_polls.load(Ordering::SeqCst), 3);
    let live = latest_run_dir(&base);
    let expected = data_signature(&read_stage_appended(&live, "input").await);
    assert_eq!(expected.len(), 2);

    let replay_polls = Arc::new(AtomicUsize::new(0));
    replay_testkit::unsynced_application()
        .with_cli_args([
            OsString::from("lazy-iterator-replay"),
            OsString::from("--replay-from"),
            live.as_os_str().to_os_string(),
        ])
        .run_async(definition(base.clone(), replay_polls.clone()))
        .await
        .unwrap();
    assert_eq!(replay_polls.load(Ordering::SeqCst), 0);
    let replay = latest_run_dir(&base);
    assert_ne!(live, replay);
    assert_eq!(
        data_signature(&read_stage_appended(&replay, "input").await),
        expected
    );
    assert_eq!(
        verify_run_dirs(&live, &replay, &VerifyOptions::default())
            .unwrap()
            .exit_code(),
        0
    );
}
