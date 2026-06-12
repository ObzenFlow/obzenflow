// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j golden path: record a gateway-shaped run, strict-replay it,
//! and let the verifier certify the reconstruction from the journals.
//!
//! The fixture is the tutorial shape: a source, a multi-type validation
//! stage, an effectful authorization stage whose effect succeeds, declines,
//! or fails per order, and three sinks, one of which converges cancellations
//! from two producers. That converging delivery sink is deliberately outside
//! the order-certified region (FLOWIP-095d leaves it availability-driven),
//! and its projection carries no positionally compared rows, so the vacuous
//! certification rule must let the headline verdict through.
//!
//! The suite also proves replay closure as the user sees it: a replay of the
//! replay verifies against both its parent and, after the parent directory is
//! deleted, against generation zero, because lineage is proven from the
//! effect-lane namespace in the journals rather than from manifest chains.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, RefusalReason, VerifyOptions, VerifyOutcome};
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

// ---------------------------------------------------------------------------
// Domain facts, gateway-shaped
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: u64,
    amount: u64,
}

impl TypedPayload for OrderPlaced {
    const EVENT_TYPE: &'static str = "replay_verification.order_placed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ValidatedOrder {
    order_id: u64,
    amount: u64,
}

impl TypedPayload for ValidatedOrder {
    const EVENT_TYPE: &'static str = "replay_verification.validated";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthGrant {
    order_id: u64,
    auth_code: u64,
}

impl TypedPayload for AuthGrant {
    const EVENT_TYPE: &'static str = "replay_verification.auth_grant";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderAuthorized {
    order_id: u64,
    auth_code: u64,
}

impl TypedPayload for OrderAuthorized {
    const EVENT_TYPE: &'static str = "replay_verification.authorized";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthorizationUnavailable {
    order_id: u64,
    reason: String,
}

impl TypedPayload for AuthorizationUnavailable {
    const EVENT_TYPE: &'static str = "replay_verification.unavailable";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderCancelled {
    order_id: u64,
    origin: String,
}

impl TypedPayload for OrderCancelled {
    const EVENT_TYPE: &'static str = "replay_verification.cancelled";
}

// ---------------------------------------------------------------------------
// Fixture flow: six orders covering authorized, unavailable, and cancelled
// ---------------------------------------------------------------------------

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
        "replay-verification-validate-v1"
    }
}

#[derive(Clone, Debug)]
struct AuthorizeEffect {
    order_id: u64,
    amount: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for AuthorizeEffect {
    const EFFECT_TYPE: &'static str = "replay_verification.authorize";
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
        "replay-verification-authorize-v1"
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
        name: "replay_verification_golden_path",
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
            paid_orders = sink!(OrderAuthorized => SinkTyped::with_delivery(discard::<OrderAuthorized>()));
            cancelled_orders = sink!(OrderCancelled => SinkTyped::with_delivery(discard::<OrderCancelled>()));
            manual_review = sink!(AuthorizationUnavailable => SinkTyped::with_delivery(discard::<AuthorizationUnavailable>()));
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
// Helpers
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

fn assert_certified_match(outcome: &VerifyOutcome) -> &obzenflow_infra::verify::VerificationReport {
    assert_eq!(
        outcome.exit_code(),
        0,
        "expected a fully certified match: {}",
        obzenflow_infra::verify::render_verdict(outcome)
    );
    let VerifyOutcome::Completed { report, .. } = outcome else {
        panic!("expected a completed verification");
    };
    report
}

// ---------------------------------------------------------------------------
// The golden path
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn replay_verification_certifies_the_reconstruction_and_replay_closure() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Record the baseline live.
    let live_calls = run_flow(&journal_base, None).await;
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        4,
        "orders 1, 2, 4, 5 reach the gateway"
    );
    let baseline = latest_run_dir(&journal_base);

    // Strict replay: zero effect executions.
    let replay_calls = run_flow(&journal_base, Some(&baseline)).await;
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not execute the gateway effect"
    );
    let candidate = latest_run_dir(&journal_base);
    assert_ne!(baseline, candidate);

    // The verdict, from the journals.
    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let report = assert_certified_match(&outcome);

    assert_eq!(report.report_version, 1);
    assert_eq!(report.mode, "whole_run");
    assert_eq!(
        report.identity_mode, "lineage",
        "a replay candidate asserts its lineage"
    );
    assert_eq!(report.totals.divergences, 0);

    // The certified region really compared rows (projection discipline: the
    // candidate's rows all carry replay_context, the baseline's none, and the
    // effect lane carries identity, yet nothing diverges).
    assert!(report.stages["validate"].order_certified);
    assert!(report.stages["validate"].positional_rows_baseline > 0);
    assert!(report.stages["authorize"].order_certified);
    assert!(report.stages["authorize"].positional_rows_baseline > 0);

    // The converging delivery sink sits outside the certified region and is
    // vacuously certified: it must not block the headline verdict.
    let sink = &report.stages["cancelled_orders"];
    assert!(!sink.order_certified);
    assert!(
        sink.vacuous,
        "delivery-only sink fan-in is vacuously certified"
    );
    assert_eq!(sink.status, "matched");

    // The sidecar report is the receipt, inside the candidate run directory,
    // and the verifier wrote nothing into either run's journals.
    let report_path = candidate
        .join("verification")
        .join(format!("{}.json", report.baseline.flow_id));
    assert!(report_path.exists(), "report sidecar should exist");

    // Replay closure: replay the replay, verify against the parent.
    let gen2_calls = run_flow(&journal_base, Some(&candidate)).await;
    assert_eq!(gen2_calls.load(Ordering::SeqCst), 0);
    let gen2 = latest_run_dir(&journal_base);
    assert_ne!(gen2, candidate);

    let outcome = verify_run_dirs(&candidate, &gen2, &VerifyOptions::default())
        .expect("replay-of-replay verification should run");
    assert_certified_match(&outcome);

    // Deleted-generation lineage: remove the intermediate replay and verify
    // generation two against generation zero. Lineage is proven from the
    // effect-lane namespace in the journals, never from manifest chains.
    std::fs::remove_dir_all(&candidate).expect("delete the intermediate generation");
    let outcome = verify_run_dirs(&baseline, &gen2, &VerifyOptions::default())
        .expect("deleted-generation verification should run");
    let report = assert_certified_match(&outcome);
    assert_eq!(report.identity_mode, "lineage");
}

#[tokio::test]
async fn verification_refuses_an_unavailable_archive() {
    let temp = tempfile::tempdir().expect("tempdir");
    let missing = temp.path().join("no-such-run");
    let outcome = verify_run_dirs(&missing, &missing, &VerifyOptions::default())
        .expect("refusals are outcomes, not errors");
    assert_eq!(outcome.exit_code(), 3);
    assert!(matches!(
        outcome,
        VerifyOutcome::Refused(RefusalReason::ArchiveUnavailable { .. })
    ));
}
