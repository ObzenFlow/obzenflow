// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120i: replay provenance reaches every sink delivery.
//!
//! The labelling surface (`DeliveryContext`, decision 6) derives a delivery's
//! provenance from `event.replay_context`, which the replay driver stamps on
//! re-injected source events and every derived event inherits. This suite is
//! the evidence for that derivation on a gateway-shaped topology: a source, a
//! multi-type validation stage, an effectful authorization stage whose effect
//! succeeds, declines, or fails per order, and three sinks, one of which
//! converges cancellations from two producers, the shape the
//! `payment_gateway_resilience` tutorial labels in front of users.
//!
//! Under strict replay every delivery at every sink must carry replay
//! provenance, including the converged fan-in path and outputs derived from
//! recorded effect failures. Live runs must carry none.

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::ChainEventContent,
    id::StageId,
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{EffectfulTransformHandler, FiniteSourceHandler};
use obzenflow_runtime::stages::sink::{DeliveryProvenance, SinkTyped};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Domain facts, gateway-shaped
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: u64,
    amount: u64,
}

impl TypedPayload for OrderPlaced {
    const EVENT_TYPE: &'static str = "replay_provenance.order_placed";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ValidatedOrder {
    order_id: u64,
    amount: u64,
}

impl TypedPayload for ValidatedOrder {
    const EVENT_TYPE: &'static str = "replay_provenance.validated";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AuthGrant {
    order_id: u64,
    auth_code: u64,
}

impl TypedPayload for AuthGrant {
    const EVENT_TYPE: &'static str = "replay_provenance.auth_grant";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OrderAuthorized {
    order_id: u64,
    auth_code: u64,
}

impl TypedPayload for OrderAuthorized {
    const EVENT_TYPE: &'static str = "replay_provenance.authorized";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AuthorizationUnavailable {
    order_id: u64,
    reason: String,
}

impl TypedPayload for AuthorizationUnavailable {
    const EVENT_TYPE: &'static str = "replay_provenance.unavailable";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OrderCancelled {
    order_id: u64,
    origin: String,
}

impl TypedPayload for OrderCancelled {
    const EVENT_TYPE: &'static str = "replay_provenance.cancelled";
}

// ---------------------------------------------------------------------------
// Source: six orders covering every path
// ---------------------------------------------------------------------------

/// Orders 1..=6. Order 3 and 6 are invalid (cancelled at validation). Of the
/// valid orders, even ids hit the simulated gateway outage (unavailable),
/// amounts above 500 are declined (cancelled at authorization), and the rest
/// authorize.
#[derive(Clone, Debug)]
struct OrderSource {
    next: u64,
    writer_id: WriterId,
}

impl OrderSource {
    fn new() -> Self {
        Self {
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for OrderSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 6 {
            return Ok(None);
        }
        let order_id = self.next;
        self.next += 1;
        let amount = if order_id == 5 { 700 } else { 100 * order_id };
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            OrderPlaced::EVENT_TYPE,
            json!(OrderPlaced { order_id, amount }),
        )]))
    }
}

// ---------------------------------------------------------------------------
// Validation: multi-type output, no effects
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct ValidateOrder;

#[async_trait]
impl EffectfulTransformHandler for ValidateOrder {
    type Input = OrderPlaced;

    async fn process(&self, order: OrderPlaced, fx: &mut Effects) -> Result<(), HandlerError> {
        if order.order_id.is_multiple_of(3) {
            fx.emit(OrderCancelled {
                order_id: order.order_id,
                origin: "validation".to_string(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
            return Ok(());
        }
        fx.emit(ValidatedOrder {
            order_id: order.order_id,
            amount: order.amount,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "replay-provenance-validate-v1"
    }
}

// ---------------------------------------------------------------------------
// Authorization: effect succeeds, declines, or times out per order
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct AuthorizeEffect {
    order_id: u64,
    amount: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for AuthorizeEffect {
    const EFFECT_TYPE: &'static str = "replay_provenance.authorize";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = AuthGrant;

    fn label(&self) -> &str {
        "authorize"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "order_id": self.order_id, "amount": self.amount })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.order_id.is_multiple_of(2) {
            return Err(EffectError::Execution(
                "gateway_timeout_simulated".to_string(),
            ));
        }
        Ok(AuthGrant {
            order_id: self.order_id,
            auth_code: self.order_id + 9000,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("authorize:{}", self.order_id)))
    }
}

#[derive(Clone, Debug)]
struct AuthorizePayment {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for AuthorizePayment {
    type Input = ValidatedOrder;

    async fn process(&self, order: ValidatedOrder, fx: &mut Effects) -> Result<(), HandlerError> {
        let outcome = fx
            .perform(AuthorizeEffect {
                order_id: order.order_id,
                amount: order.amount,
                calls: self.calls.clone(),
            })
            .await;

        match outcome {
            Ok(grant) if order.amount > 500 => {
                let _ = grant;
                fx.emit(OrderCancelled {
                    order_id: order.order_id,
                    origin: "declined".to_string(),
                })
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            }
            Ok(grant) => {
                fx.emit(OrderAuthorized {
                    order_id: order.order_id,
                    auth_code: grant.auth_code,
                })
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            }
            Err(err) => {
                // FLOWIP-120i: the semantic projection is identical for the
                // live failure and its replayed RecordedFailure, so this fact
                // is byte-identical across live and replay runs.
                fx.emit(AuthorizationUnavailable {
                    order_id: order.order_id,
                    reason: err.semantic_reason().into_owned(),
                })
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            }
        }
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "replay-provenance-authorize-v1"
    }
}

// ---------------------------------------------------------------------------
// Probe sinks: the public FLOWIP-120i surface, `SinkTyped::with_delivery`
// through the canonical `sink!(In => handler)` arm, recording each
// delivery's payload and provenance
// ---------------------------------------------------------------------------

type Probe<T> = Arc<Mutex<Vec<(T, DeliveryProvenance)>>>;

fn probe<T>(
    deliveries: &Probe<T>,
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    let deliveries = deliveries.clone();
    move |payload: T, delivery| {
        deliveries
            .lock()
            .expect("probe lock poisoned")
            .push((payload, delivery.provenance()));
        std::future::ready(())
    }
}

// ---------------------------------------------------------------------------
// Flow
// ---------------------------------------------------------------------------

struct Probes {
    paid: Probe<OrderAuthorized>,
    cancelled: Probe<OrderCancelled>,
    review: Probe<AuthorizationUnavailable>,
}

impl Probes {
    fn new() -> Self {
        Self {
            paid: Arc::new(Mutex::new(Vec::new())),
            cancelled: Arc::new(Mutex::new(Vec::new())),
            review: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

fn build_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>, probes: &Probes) -> FlowDefinition {
    let paid = probe(&probes.paid);
    let cancelled = probe(&probes.cancelled);
    let review = probe(&probes.review);
    flow! {
        name: "replay_provenance_labelling",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            orders = source!(OrderPlaced => OrderSource::new());
            validate = effectful_transform!(
                OrderPlaced -> { ValidatedOrder, OrderCancelled } => ValidateOrder,
                effects: [],
                middleware: []
            );
            authorize = effectful_transform!(
                ValidatedOrder -> { OrderAuthorized, AuthorizationUnavailable, OrderCancelled, AuthGrant } => AuthorizePayment { calls },
                effects: [AuthorizeEffect],
                middleware: []
            );
            paid_orders = sink!(OrderAuthorized => SinkTyped::with_delivery(paid));
            cancelled_orders = sink!(OrderCancelled => SinkTyped::with_delivery(cancelled));
            manual_review = sink!(AuthorizationUnavailable => SinkTyped::with_delivery(review));
        },

        topology: {
            orders |> validate;
            validate |> authorize;
            authorize |> paid_orders;
            authorize |> manual_review;
            validate |> cancelled_orders;
            authorize |> cancelled_orders;
        }
    }
}

// ---------------------------------------------------------------------------
// Journal helpers (local, keeping this suite off the test-support gate)
// ---------------------------------------------------------------------------

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

async fn read_stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run_manifest.json should be readable"),
    )
    .expect("run_manifest.json should parse");
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain data journal for '{stage_key}'"));
    let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(stage_journal),
            JournalOwner::stage(StageId::new()),
        )
        .expect("stage journal should open");

    journal
        .read_causally_ordered()
        .await
        .expect("stage journal should read")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

/// Every `Data` row in the stage journal must carry `replay_context`.
/// Returns the event types of rows that do not, for a useful failure message.
async fn data_rows_missing_replay_context(run_dir: &Path, stage_key: &str) -> Vec<String> {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| matches!(event.content, ChainEventContent::Data { .. }))
        .filter(|event| event.replay_context.is_none())
        .map(|event| event.event_type().to_string())
        .collect()
}

fn assert_probe_provenance<T: Clone + std::fmt::Debug>(
    probe: &Probe<T>,
    expected: DeliveryProvenance,
    sink_label: &str,
) -> Vec<T> {
    let deliveries = probe.lock().expect("probe lock poisoned").clone();
    assert!(
        !deliveries.is_empty(),
        "{sink_label}: expected at least one delivery"
    );
    for (payload, provenance) in &deliveries {
        assert_eq!(
            *provenance, expected,
            "{sink_label}: delivery {payload:?} has provenance {provenance:?}, expected {expected:?}"
        );
    }
    deliveries.into_iter().map(|(payload, _)| payload).collect()
}

/// FLOWIP-120i: labels are stdout presentation only. No journal row in a
/// labelled run may contain the label literal.
fn assert_journals_carry_no_label(run_dir: &Path) {
    for entry in walkdir(run_dir) {
        let Ok(contents) = std::fs::read_to_string(&entry) else {
            continue;
        };
        assert!(
            !contents.contains("[replay]"),
            "journal artifact {} contains the stdout label literal",
            entry.display()
        );
    }
}

fn walkdir(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walkdir(&path));
        } else {
            files.push(path);
        }
    }
    files
}

// ---------------------------------------------------------------------------
// The demonstrating test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_sink_delivery_carries_replay_provenance_under_strict_replay() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Live run: no delivery carries replay provenance.
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_probes = Probes::new();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            &live_probes,
        ))
        .await
        .expect("live flow should complete");

    // Orders: 1 authorized, 2 unavailable, 3 cancelled(validation),
    // 4 unavailable, 5 cancelled(declined, amount 700), 6 cancelled(validation).
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        4,
        "orders 1, 2, 4, 5 reach the gateway"
    );
    let live_paid = assert_probe_provenance(
        &live_probes.paid,
        DeliveryProvenance::Live,
        "paid_orders(live)",
    );
    let live_cancelled = assert_probe_provenance(
        &live_probes.cancelled,
        DeliveryProvenance::Live,
        "cancelled_orders(live)",
    );
    let live_review = assert_probe_provenance(
        &live_probes.review,
        DeliveryProvenance::Live,
        "manual_review(live)",
    );
    assert_eq!(live_paid.len(), 1);
    assert_eq!(live_cancelled.len(), 3);
    assert_eq!(live_review.len(), 2);
    for unavailable in &live_review {
        assert_eq!(
            unavailable.reason, "gateway_timeout_simulated",
            "the recorded failure reason must be the semantic message, never a Display wrapper"
        );
    }

    let archive_dir = latest_run_dir(&journal_base);

    // Strict replay: zero effect executions, and every delivery at every sink
    // carries replay provenance, including the converged cancellation path.
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_probes = Probes::new();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            replay_calls.clone(),
            &replay_probes,
        ))
        .await
        .expect("replay flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not execute the gateway effect"
    );

    let replay_paid = assert_probe_provenance(
        &replay_probes.paid,
        DeliveryProvenance::Replayed,
        "paid_orders(replay)",
    );
    let replay_cancelled = assert_probe_provenance(
        &replay_probes.cancelled,
        DeliveryProvenance::Replayed,
        "cancelled_orders(replay)",
    );
    let replay_review = assert_probe_provenance(
        &replay_probes.review,
        DeliveryProvenance::Replayed,
        "manual_review(replay)",
    );

    // Business payloads match the live run byte for byte, including the
    // failure reasons mapped from recorded effect failures (FLOWIP-120i
    // semantic projection): no normalisation anywhere in user code.
    assert_eq!(replay_paid, live_paid, "paid payloads must match live");
    assert_eq!(
        replay_cancelled, live_cancelled,
        "cancelled payloads must match live"
    );
    assert_eq!(
        replay_review, live_review,
        "unavailable payloads, including failure reasons, must match live"
    );

    // Producer journals in the replay run: every Data row carries
    // replay_context, the substrate the DeliveryContext label reads.
    let replay_run_dir = latest_run_dir(&journal_base);
    assert_ne!(
        replay_run_dir, archive_dir,
        "replay should write its own run directory"
    );
    for stage in ["orders", "validate", "authorize"] {
        let missing = data_rows_missing_replay_context(&replay_run_dir, stage).await;
        assert!(
            missing.is_empty(),
            "stage '{stage}' has Data rows without replay_context in the replay run: {missing:?}"
        );
    }

    // Labels never leak into recorded facts: neither run's journal artifacts
    // contain the stdout label literal.
    assert_journals_carry_no_label(&archive_dir);
    assert_journals_carry_no_label(&replay_run_dir);
}
