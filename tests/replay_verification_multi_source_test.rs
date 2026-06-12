// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j over the FLOWIP-095d shape: two order channels merging below
//! the canonical deterministic merge (the fan-in has an effectful descendant,
//! so the build marks it ordered), verified as a user-runnable command.
//!
//! Two independent live runs compare equal under the positional projection,
//! which is 095d's arrival-timing-independence claim surfaced as a verb, and
//! a replay verifies against its source with effect-lane identity asserted.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{EffectfulTransformHandler, FiniteSourceHandler};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: u64,
}

impl TypedPayload for OrderPlaced {
    const EVENT_TYPE: &'static str = "replay_verification_multi.order_placed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Charged {
    order_id: u64,
    receipt: u64,
}

impl TypedPayload for Charged {
    const EVENT_TYPE: &'static str = "replay_verification_multi.charged";
}

#[derive(Clone, Debug)]
struct Channel {
    ids: Vec<u64>,
    cursor: usize,
    writer_id: WriterId,
}

impl Channel {
    fn new(ids: Vec<u64>) -> Self {
        Self {
            ids,
            cursor: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for Channel {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let Some(order_id) = self.ids.get(self.cursor).copied() else {
            return Ok(None);
        };
        self.cursor += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            OrderPlaced::EVENT_TYPE,
            json!(OrderPlaced { order_id }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct ChargeEffect {
    order_id: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for ChargeEffect {
    const EFFECT_TYPE: &'static str = "replay_verification_multi.charge";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = Charged;

    fn label(&self) -> &str {
        "charge"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "order_id": self.order_id })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Charged {
            order_id: self.order_id,
            receipt: self.order_id + 5000,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("charge:{}", self.order_id)))
    }
}

/// The effectful merge: charging sits directly at the fan-in, so the build
/// marks the merge itself as a deterministic orderer (canonical merge).
#[derive(Clone, Debug)]
struct ChargeOrders {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for ChargeOrders {
    type Input = OrderPlaced;

    async fn process(&self, order: OrderPlaced, fx: &mut Effects) -> Result<(), HandlerError> {
        fx.perform(ChargeEffect {
            order_id: order.order_id,
            calls: self.calls.clone(),
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "replay-verification-charge-v1"
    }
}

fn discard<T>(
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| std::future::ready(())
}

fn build_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    flow! {
        name: "replay_verification_multi_source",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            web_orders = source!(OrderPlaced => Channel::new(vec![1, 3, 5]));
            store_orders = source!(OrderPlaced => Channel::new(vec![2, 4, 6]));
            charge = effectful_transform!(
                OrderPlaced -> { Charged } => ChargeOrders { calls },
                effects: [ChargeEffect],
                middleware: []
            );
            receipts = sink!(Charged => SinkTyped::with_delivery(discard::<Charged>()));
        },

        topology: {
            web_orders |> charge;
            store_orders |> charge;
            charge |> receipts;
        }
    }
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("run should have produced an archive")
}

async fn run_flow(journal_base: &Path, replay_from: Option<&Path>) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf(), calls.clone()))
        .await
        .expect("flow should complete");
    calls
}

#[tokio::test(flavor = "multi_thread")]
async fn ordered_fan_in_verifies_across_live_runs_and_replay() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Two independent live runs: the canonical merge makes the fan-in
    // delivery order a function of stream content, never arrival timing, so
    // they compare equal under the positional projection.
    let first_calls = run_flow(&journal_base, None).await;
    assert_eq!(first_calls.load(Ordering::SeqCst), 6);
    let first = latest_run_dir(&journal_base);

    let second_calls = run_flow(&journal_base, None).await;
    assert_eq!(second_calls.load(Ordering::SeqCst), 6);
    let second = latest_run_dir(&journal_base);

    let outcome = verify_run_dirs(&first, &second, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    assert_eq!(report.identity_mode, "positional");
    assert!(
        report.stages["charge"].order_certified,
        "the effectful fan-in runs the canonical merge and certifies"
    );
    assert_eq!(
        outcome.exit_code(),
        0,
        "arrival-timing independence (FLOWIP-095d), surfaced as a verb: {}",
        obzenflow_infra::verify::render_verdict(&outcome)
    );

    // Replay of the first run: zero charges, identity asserted.
    let replay_calls = run_flow(&journal_base, Some(&first)).await;
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    let candidate = latest_run_dir(&journal_base);

    let outcome = verify_run_dirs(&first, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    assert_eq!(report.identity_mode, "lineage");
    assert_eq!(outcome.exit_code(), 0);
}
