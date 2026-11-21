// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::replay_testkit;

use async_trait::async_trait;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use obzenflow_core::{outcome, StageId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::stages::source::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, AtMostOnce, FiniteSourceConnector,
    InfiniteSourceConnector, Reattach, Refuse, SourceReaderInitContext,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Alpha {
    source: String,
    value: u64,
}

impl TypedPayload for Alpha {
    const EVENT_TYPE: &'static str = "flowip_134g.alpha";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Beta {
    source: String,
    value: u64,
}

impl TypedPayload for Beta {
    const EVENT_TYPE: &'static str = "flowip_134g.beta";
}

#[derive(Debug, Default)]
struct Counters {
    opens: [AtomicUsize; 4],
    sync_finite_calls: AtomicUsize,
    async_finite_calls: AtomicUsize,
    sync_infinite_calls: AtomicUsize,
    async_infinite_calls: AtomicUsize,
    async_finite_cleanup: AtomicUsize,
    async_infinite_cleanup: AtomicUsize,
}

fn pair(source: &'static str, value: u64) -> Vec<outcome![Alpha | Beta]> {
    vec![
        outcome(Alpha {
            source: source.to_string(),
            value,
        }),
        outcome(Beta {
            source: source.to_string(),
            value,
        }),
    ]
}

struct SyncFinite {
    emitted: bool,
    counters: Arc<Counters>,
}

impl TypedFiniteSourceHandler for SyncFinite {
    type Output = outcome![Alpha | Beta];

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.counters
            .sync_finite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            Ok(None)
        } else {
            Ok(Some(pair("sync_finite", 1)))
        }
    }
}

struct AsyncFinite {
    emitted: bool,
    counters: Arc<Counters>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for AsyncFinite {
    type Output = outcome![Alpha | Beta];

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.counters
            .async_finite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            Ok(None)
        } else {
            Ok(Some(pair("async_finite", 2)))
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.counters
            .async_finite_cleanup
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct SyncInfinite {
    emitted: bool,
    counters: Arc<Counters>,
}

impl TypedInfiniteSourceHandler for SyncInfinite {
    type Output = outcome![Alpha | Beta];

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        self.counters
            .sync_infinite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            Ok(Vec::new())
        } else {
            Ok(pair("sync_infinite", 3))
        }
    }
}

struct AsyncInfinite {
    emitted: bool,
    counters: Arc<Counters>,
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for AsyncInfinite {
    type Output = outcome![Alpha | Beta];

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        self.counters
            .async_infinite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(Vec::new())
        } else {
            Ok(pair("async_infinite", 4))
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.counters
            .async_infinite_cleanup
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
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
            payload if payload.consumes_data_credit() => {
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
        let input = obzenflow::effects::Port::owned(obzenflow::stages::sources::ValuesSource::new(
            std::iter::from_fn(move || {
                let index = counter.fetch_add(1, Ordering::SeqCst);
                (index < 2).then(|| Alpha {
                    source: "iterator".into(),
                    value: index as u64,
                })
            }),
        ));
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

// An external test crate implements the production contract through the stable
// runtime source module, with no erased adapter or lifecycle hook access.
#[derive(Clone)]
struct SyncFiniteConnector(Arc<Counters>);

impl FiniteSourceConnector for SyncFiniteConnector {
    type Output = outcome![Alpha | Beta];
    type Reader = SyncFinite;
    type Continuation = Refuse;
    fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        assert_eq!(context.flow_name, "typed_source_journal_parity");
        assert!(!context.stage_name.is_empty());
        self.0.opens[0].fetch_add(1, Ordering::SeqCst);
        Ok(SyncFinite {
            emitted: false,
            counters: self.0.clone(),
        })
    }
}

// An external test crate implements the production contract through the stable
// runtime source module, with no erased adapter or lifecycle hook access.
#[derive(Clone)]
struct AsyncFiniteConnector(Arc<Counters>);

#[async_trait]
impl AsyncFiniteSourceConnector for AsyncFiniteConnector {
    type Output = outcome![Alpha | Beta];
    type Reader = AsyncFinite;
    type Continuation = Refuse;
    async fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        assert_eq!(context.flow_name, "typed_source_journal_parity");
        assert!(!context.stage_name.is_empty());
        self.0.opens[1].fetch_add(1, Ordering::SeqCst);
        Ok(AsyncFinite {
            emitted: false,
            counters: self.0.clone(),
        })
    }
}

// An external test crate implements the production contract through the stable
// runtime source module, with no erased adapter or lifecycle hook access.
#[derive(Clone)]
struct SyncInfiniteConnector(Arc<Counters>);

impl InfiniteSourceConnector for SyncInfiniteConnector {
    type Output = outcome![Alpha | Beta];
    type Reader = SyncInfinite;
    type Continuation = Reattach<AtMostOnce>;
    fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        assert_eq!(context.flow_name, "typed_source_journal_parity");
        assert!(!context.stage_name.is_empty());
        self.0.opens[2].fetch_add(1, Ordering::SeqCst);
        Ok(SyncInfinite {
            emitted: false,
            counters: self.0.clone(),
        })
    }
}

// An external test crate implements the production contract through the stable
// runtime source module, with no erased adapter or lifecycle hook access.
#[derive(Clone)]
struct AsyncInfiniteConnector(Arc<Counters>);

#[async_trait]
impl AsyncInfiniteSourceConnector for AsyncInfiniteConnector {
    type Output = outcome![Alpha | Beta];
    type Reader = AsyncInfinite;
    type Continuation = Reattach<AtMostOnce>;
    async fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        assert_eq!(context.flow_name, "typed_source_journal_parity");
        assert!(!context.stage_name.is_empty());
        self.0.opens[3].fetch_add(1, Ordering::SeqCst);
        Ok(AsyncInfinite {
            emitted: false,
            counters: self.0.clone(),
        })
    }
}

#[tokio::test]
async fn four_connector_families_open_independent_readers() {
    let counters = Arc::new(Counters::default());
    let context = || SourceReaderInitContext {
        stage_id: StageId::new(),
        stage_name: "independent".into(),
        flow_name: "typed_source_journal_parity".into(),
    };
    let connector = SyncFiniteConnector(counters.clone());
    let mut a = connector.open(context()).unwrap();
    let mut b = connector.clone().open(context()).unwrap();
    let rows = a.next().unwrap();
    assert!(a.next().unwrap().is_none());
    assert_eq!(b.next().unwrap(), rows);
    let connector = SyncInfiniteConnector(counters.clone());
    let mut a = connector.open(context()).unwrap();
    let mut b = connector.clone().open(context()).unwrap();
    let rows = a.next().unwrap();
    assert!(a.next().unwrap().is_empty());
    assert_eq!(b.next().unwrap(), rows);
    let connector = AsyncFiniteConnector(counters.clone());
    let mut a = connector.open(context()).await.unwrap();
    let mut b = connector.clone().open(context()).await.unwrap();
    let rows = a.next().await.unwrap();
    a.drain().await.unwrap();
    assert_eq!(b.next().await.unwrap(), rows);
    b.drain().await.unwrap();
    let connector = AsyncInfiniteConnector(counters.clone());
    let mut a = connector.open(context()).await.unwrap();
    let mut b = connector.clone().open(context()).await.unwrap();
    let rows = a.next().await.unwrap();
    a.drain().await.unwrap();
    assert_eq!(b.next().await.unwrap(), rows);
    b.drain().await.unwrap();
    for count in &counters.opens {
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
