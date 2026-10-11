// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134i B4: the durable execution contract test.
//!
//! A flow with a real external effect ends naturally, stops on SIGTERM or dies
//! on SIGKILL, and the supported verb reproduces the uninterrupted outcome
//! without repeating a committed effect. The test has Jepsen's shape: a
//! workload (two YAML order channels into `AuthorizePayment`), an
//! independently recorded external history (the gateway's ledger), a fault
//! injector (signals sent only once a journal predicate holds) and a checker
//! (I1 to I10, over every archive and the lineage it belongs to). Each
//! scenario keeps its archives, gateway ledger and receipts under
//! `target/durable-execution-contract/<scenario>`.

#![cfg(unix)]

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod gateway_demo;

use async_trait::async_trait;
use gateway_demo::domain::{
    CustomerOrderPlaced, PaymentAuthorized, PaymentDeclineReason, PaymentDeclined,
    PaymentMethodState,
};
use gateway_demo::gateway::AuthorizePaymentOutcome;
use obzenflow::application::FlowApplication;
use obzenflow::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, StageCompletion,
};
use obzenflow::error::HandlerError;
use obzenflow::flow::backpressure::enforced;
use obzenflow::flow::{effectful_transform, flow, sink, source, stateful, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::journal::read::{
    open_disk_run, ChainPayload, ExecutionPayload, PipelineLifecycleEvent, RunJournalKind,
    RunOutcome, RunRecord, RunRecordData, SystemPayload,
};
use obzenflow::journal::JournalRecord;
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::{
    DeliveryMethod, DeliveryResult, DiscardSink, InlineSink, SinkDescription, SinkOperationError,
    SinkRedeliverySafety, SinkWriteFailure, SinkWritePhase,
};
use obzenflow::stages::sources::{
    SourcePollErrorKind, YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection, YamlSource,
};
use obzenflow::stages::stateful::{Accumulator, OnEOF, StatefulWithEmission};
use obzenflow::stages::transforms::EffectfulTransformHandler;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::EventId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};
use sysinfo::{Pid, PidExt, ProcessExt, Signal, System, SystemExt};

const ROOT: &str = "target/durable-execution-contract";
const CHILD: &str = "durable_execution_contract_child";
const PLAN: &str = "OBZENFLOW_134I_CONTRACT_PLAN";
/// The store channel's third record fails to decode, before the first cut.
const INVALID_ORDER: &str = "S-BAD";
const FIRST_AFTER_INVALID: &str = "S-03";
/// Held after its charge at the first cut, and again when S5b re-executes it.
const FIRST_CUT: &str = "S-12";
/// Held inside S4's resumed run once its reconstruction completes (S5).
const SECOND_CUT: &str = "S-18";
const DECLINES: [(&str, &str); 3] = [
    ("S-05", "InsufficientFunds"),
    ("S-14", "AddressMismatch"),
    ("S-21", "InsufficientFunds"),
];
const AUTHORIZED: &str = "authorized";
const DECLINED: &str = "declined";

// ---------------------------------------------------------------------------
// Workload: 6 web orders that finish early, 23 store orders and one invalid
// store record.

fn web_ids() -> Vec<String> {
    (1..=6).map(|n| format!("W-{n:02}")).collect()
}

fn store_ids() -> Vec<String> {
    (1..=23).map(|n| format!("S-{n:02}")).collect()
}

fn valid_ids() -> BTreeSet<String> {
    web_ids().into_iter().chain(store_ids()).collect()
}

fn decision(order_id: &str) -> &'static str {
    if DECLINES.iter().any(|(declined, _)| *declined == order_id) {
        DECLINED
    } else {
        AUTHORIZED
    }
}

fn order_yaml(order_id: &str, channel: &str, amount_cents: &str, state: &str) -> String {
    format!(
        "  - order_id: {order_id}\n    customer_id: C-{order_id}\n    channel: {channel}\n    \
         amount_cents: {amount_cents}\n    payment_method_state: {state}\n    phase: Warmup\n"
    )
}

fn web_input() -> String {
    let orders: String = web_ids()
        .iter()
        .enumerate()
        .map(|(index, id)| order_yaml(id, "Web", &(1_000 + 10 * index).to_string(), "Valid"))
        .collect();
    format!("web_orders:\n{orders}")
}

fn store_input() -> String {
    let mut orders = String::new();
    for (index, id) in store_ids().iter().enumerate() {
        if index == 2 {
            orders.push_str(&order_yaml(INVALID_ORDER, "Store", "not-a-number", "Valid"));
        }
        let state = DECLINES
            .iter()
            .find(|(declined, _)| declined == id)
            .map_or("Valid", |(_, state)| state);
        orders.push_str(&order_yaml(
            id,
            "Store",
            &(2_000 + 10 * index).to_string(),
            state,
        ));
    }
    format!("store_orders:\n{orders}")
}

#[derive(Clone, Debug)]
struct OrderYaml;

impl YamlDecoder for OrderYaml {
    type Output = CustomerOrderPlaced;

    fn decode(&self, record: YamlRecord<'_>) -> Result<CustomerOrderPlaced, YamlDecodeError> {
        record.deserialize()
    }
}

// ---------------------------------------------------------------------------
// External history: the test-double gateway.

/// One physical gateway call, as the gateway records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct GatewayCall {
    key: String,
    order_id: String,
    call: Call,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Call {
    Charged,
    Declined,
    /// A key the gateway already decided: no second charge.
    Replayed,
}

fn idempotency_key(order_id: &str) -> String {
    format!("payment-authorize:{order_id}")
}

/// Appends every physical call to the ledger with one unbuffered write, which
/// survives SIGKILL. Holds `hold` after recording its call until `release`
/// exists.
#[derive(Clone, Debug)]
struct Gateway {
    ledger: PathBuf,
    hold: Option<String>,
    release: PathBuf,
}

impl Gateway {
    async fn authorize(
        &self,
        order: &CustomerOrderPlaced,
    ) -> Result<AuthorizePaymentOutcome, EffectError> {
        let key = idempotency_key(&order.order_id);
        let decided = read_lines::<GatewayCall>(&self.ledger)
            .iter()
            .any(|call| call.key == key);
        let outcome = match order.payment_method_state {
            PaymentMethodState::InsufficientFunds => {
                declined(order, PaymentDeclineReason::InsufficientFunds)
            }
            PaymentMethodState::AddressMismatch => {
                declined(order, PaymentDeclineReason::AddressMismatch)
            }
            PaymentMethodState::Valid | PaymentMethodState::InvalidNumber => {
                AuthorizePaymentOutcome::Authorized(PaymentAuthorized {
                    order_id: order.order_id.clone(),
                    customer_id: order.customer_id.clone(),
                    amount_cents: order.amount_cents,
                    phase: order.phase.clone(),
                    authorization_id: format!("AUTH-{}", order.order_id),
                })
            }
        };
        let call = match (decided, &outcome) {
            (true, _) => Call::Replayed,
            (false, AuthorizePaymentOutcome::Authorized(_)) => Call::Charged,
            (false, AuthorizePaymentOutcome::Declined(_)) => Call::Declined,
        };
        append_line(
            &self.ledger,
            &GatewayCall {
                key,
                order_id: order.order_id.clone(),
                call,
            },
        )
        .map_err(|error| EffectError::Transport(error.to_string()))?;
        if self.hold.as_deref() == Some(order.order_id.as_str()) {
            while !self.release.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(outcome)
    }
}

fn declined(order: &CustomerOrderPlaced, reason: PaymentDeclineReason) -> AuthorizePaymentOutcome {
    AuthorizePaymentOutcome::Declined(PaymentDeclined {
        order_id: order.order_id.clone(),
        customer_id: order.customer_id.clone(),
        amount_cents: order.amount_cents,
        phase: order.phase.clone(),
        reason,
    })
}

fn append_line(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(&line)
}

fn read_lines<T: DeserializeOwned>(path: &Path) -> Vec<T> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).expect("a complete JSON line"))
        .collect()
}

/// Authorizes one order at the gateway, keyed by its order id.
#[derive(Clone, Debug)]
struct AuthorizePayment {
    order: CustomerOrderPlaced,
    gateway: Gateway,
}

#[async_trait]
impl Effect for AuthorizePayment {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = obzenflow::effects::Portless;
    type Outcome = AuthorizePaymentOutcome;
    type OutcomeSemantics = obzenflow::effects::DomainFacts;

    fn label(&self) -> &str {
        "authorize_payment"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({
            "order_id": self.order.order_id,
            "customer_id": self.order.customer_id,
            "amount_cents": self.order.amount_cents,
            "payment_method_state": self.order.payment_method_state,
        })
    }

    async fn execute(&self, _: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.gateway.authorize(&self.order).await
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(idempotency_key(&self.order.order_id)))
    }
}

#[derive(Clone, Debug)]
struct Authorize {
    gateway: Gateway,
}

type Authorization = obzenflow::schema::stage_fact_set![PaymentAuthorized, PaymentDeclined];
type AuthorizeEffects = obzenflow::effects::effect_set![AuthorizePayment];

#[async_trait]
impl EffectfulTransformHandler for Authorize {
    type Input = CustomerOrderPlaced;
    type Output = Authorization;
    type AllowedEffects = AuthorizeEffects;

    async fn process(
        &self,
        order: CustomerOrderPlaced,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        // `perform` records whichever payment fact happened.
        fx.perform(AuthorizePayment {
            order,
            gateway: self.gateway.clone(),
        })
        .await
        .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "durable-execution-contract-v1"
    }
}

/// The settlement ledger's final snapshot, emitted at end of input only.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct SettlementLedger {
    authorized: u64,
    amount_cents: u64,
}

impl TypedPayload for SettlementLedger {
    const EVENT_TYPE: &'static str = "settlement.ledger";
}

#[derive(Clone, Debug)]
struct Settle;

impl Accumulator for Settle {
    type State = SettlementLedger;
    type Input = PaymentAuthorized;
    type Output = SettlementLedger;

    fn initial_state(&self) -> SettlementLedger {
        SettlementLedger::default()
    }

    fn accumulate(&self, ledger: &mut SettlementLedger, payment: PaymentAuthorized) {
        ledger.authorized += 1;
        ledger.amount_cents += payment.amount_cents;
    }

    fn outputs(&self, ledger: &SettlementLedger) -> Vec<SettlementLedger> {
        vec![ledger.clone()]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    authorization_id: String,
}

/// Appends one receipt per delivery; repeating a recorded write is safe.
#[derive(Clone, Debug)]
struct ReceiptFile {
    path: PathBuf,
}

#[async_trait]
impl InlineSink for ReceiptFile {
    type Input = PaymentAuthorized;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::FileWrite {
            path: self.path.clone(),
        })
        .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, payment: PaymentAuthorized) -> Result<(), SinkWriteFailure> {
        append_line(
            &self.path,
            &Receipt {
                order_id: payment.order_id,
                authorization_id: payment.authorization_id,
            },
        )
        .map_err(|error| {
            SinkWriteFailure::current_only(
                SinkWritePhase::Execute,
                SinkOperationError::remote(error.to_string()),
            )
        })
    }
}

fn build_flow(scenario: &Scenario, hold: Option<String>) -> FlowDefinition {
    let orders = |path: PathBuf, pointer: &str| {
        YamlSource::builder(OrderYaml)
            .path(path)
            .selection(YamlSelection::SequenceAt(pointer.into()))
            .batch_size(1)
            .build()
            .expect("the order channel is configured")
    };
    let web_orders_input = orders(scenario.web_input(), "/web_orders");
    let store_orders_input = orders(scenario.store_input(), "/store_orders");
    let authorize = Authorize {
        gateway: Gateway {
            ledger: scenario.ledger(),
            hold,
            release: scenario.release(),
        },
    };
    let receipt_file = ReceiptFile {
        path: scenario.receipts(),
    };
    let journals = scenario.journals();
    FlowDefinition::materialize(move |_runtime_config| {
        let ledger = StatefulWithEmission::new(Settle, OnEOF);
        let settled_ledger = DiscardSink::<SettlementLedger>::new();
        Ok(flow! {
            name: "durable_execution_contract",
            journals: disk_journals(journals),
            backpressure: enforced(4).stall_timeout_ms(120_000),

            stages: {
                web_orders = source!(CustomerOrderPlaced => web_orders_input);
                store_orders = source!(CustomerOrderPlaced => store_orders_input);
                authorize_payment = effectful_transform!(
                    CustomerOrderPlaced -> { PaymentAuthorized, PaymentDeclined }
                        uses AuthorizePayment => authorize
                );
                settlement = stateful!(PaymentAuthorized -> SettlementLedger => ledger);
                settled = sink!(SettlementLedger => settled_ledger);
                receipts = sink!(PaymentAuthorized => receipt_file);
            },

            topology: {
                web_orders |> authorize_payment;
                store_orders |> authorize_payment;
                authorize_payment |> settlement;
                settlement |> settled;
                authorize_payment |> receipts;
            }
        })
    })
}

// ---------------------------------------------------------------------------
// Fault injector: each run is this test binary re-executing itself.

#[derive(Debug, Serialize, Deserialize)]
struct Plan {
    scenario: Scenario,
    args: Vec<String>,
    hold: Option<String>,
}

#[test]
#[ignore = "subprocess fixture; run by durable_execution_contract"]
fn durable_execution_contract_child() {
    let plan: Plan = serde_json::from_str(&std::env::var(PLAN).expect("the parent passes a plan"))
        .expect("the plan parses");
    let args = std::iter::once("obzenflow".to_string()).chain(plan.args);
    if let Err(error) = FlowApplication::builder()
        .with_cli_args(args)
        .run_blocking(build_flow(&plan.scenario, plan.hold))
    {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

/// A scenario's directory: journals, gateway ledger and receipts, and the
/// directory its channels read.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    root: PathBuf,
    inputs: PathBuf,
}

impl Scenario {
    fn directory(name: &str) -> PathBuf {
        let root = Path::new(ROOT).join(name);
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("clear the scenario");
        }
        std::fs::create_dir_all(&root).expect("create the scenario");
        std::fs::canonicalize(root).expect("resolve the scenario")
    }

    fn fresh(name: &str) -> Self {
        let root = Self::directory(name);
        let inputs = root.join("inputs");
        std::fs::create_dir(&inputs).expect("create the inputs");
        let scenario = Self { root, inputs };
        std::fs::write(scenario.web_input(), web_input()).expect("write web orders");
        std::fs::write(scenario.store_input(), store_input()).expect("write store orders");
        scenario
    }

    /// A separate history: a copy of `archive` with the gateway ledger and
    /// receipts as they stood at its cut. It reads the same input files, whose
    /// versions the archive pinned (B10).
    fn copied(name: &str, from: &Scenario, archive: &Path) -> (Self, PathBuf) {
        let scenario = Self {
            root: Self::directory(name),
            inputs: from.inputs.clone(),
        };
        let copy = scenario
            .journals()
            .join("flows")
            .join(archive.file_name().expect("a run directory"));
        std::fs::create_dir_all(&copy).expect("create the copy");
        for entry in std::fs::read_dir(archive).expect("read the archive") {
            let entry = entry.expect("an archive file");
            std::fs::copy(entry.path(), copy.join(entry.file_name())).expect("copy a file");
        }
        for (file, to) in [
            (from.ledger(), scenario.ledger()),
            (from.receipts(), scenario.receipts()),
        ] {
            std::fs::copy(file, to).expect("copy the external state");
        }
        let copy = std::fs::canonicalize(copy).expect("resolve the copy");
        (scenario, copy)
    }

    fn journals(&self) -> PathBuf {
        self.root.join("journals")
    }

    fn web_input(&self) -> PathBuf {
        self.inputs.join("web_orders.yaml")
    }

    fn store_input(&self) -> PathBuf {
        self.inputs.join("store_orders.yaml")
    }

    fn ledger(&self) -> PathBuf {
        self.root.join("gateway-ledger.jsonl")
    }

    fn receipts(&self) -> PathBuf {
        self.root.join("receipts.jsonl")
    }

    fn release(&self) -> PathBuf {
        self.root.join("release")
    }

    fn runs(&self) -> BTreeSet<PathBuf> {
        let Ok(entries) = std::fs::read_dir(self.journals().join("flows")) else {
            return BTreeSet::new();
        };
        entries
            .map(|entry| entry.expect("a run directory").path())
            .filter(|run| run.join("run_manifest.json").exists())
            .map(|run| std::fs::canonicalize(run).expect("resolve a run"))
            .collect()
    }

    fn calls(&self) -> Vec<GatewayCall> {
        read_lines(&self.ledger())
    }

    /// Only the store channel's input is present for a resume.
    fn without_web_input(&self) {
        remove_input(&self.web_input());
    }

    /// A strict replay needs no input.
    fn without_inputs(&self) {
        remove_input(&self.web_input());
        remove_input(&self.store_input());
    }
}

/// Copied histories share their inputs, so an input may already be gone.
fn remove_input(input: &Path) {
    if input.exists() {
        std::fs::remove_file(input).expect("remove an input");
    }
}

fn resume(archive: &Path) -> Vec<String> {
    vec![
        "--resume-from".into(),
        archive.display().to_string(),
        "--allow-incomplete-archive".into(),
    ]
}

fn replay(archive: &Path) -> Vec<String> {
    vec!["--replay-from".into(), archive.display().to_string()]
}

struct Child {
    process: tokio::process::Child,
    scenario: Scenario,
    before: BTreeSet<PathBuf>,
    log: PathBuf,
}

static CHILDREN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl Child {
    fn spawn(scenario: &Scenario, args: Vec<String>, hold: Option<&str>) -> Self {
        let before = scenario.runs();
        let number = CHILDREN.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let log_path = scenario.root.join(format!("child-{number}.log"));
        let log = std::fs::File::create(&log_path).expect("create the child log");
        let plan = Plan {
            scenario: scenario.clone(),
            args,
            hold: hold.map(str::to_owned),
        };
        let process =
            tokio::process::Command::new(std::env::current_exe().expect("the test executable"))
                .args([
                    "--ignored",
                    "--exact",
                    CHILD,
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env(
                    PLAN,
                    serde_json::to_string(&plan).expect("the plan serializes"),
                )
                .stdout(log.try_clone().expect("share the child log"))
                .stderr(log)
                .kill_on_drop(true)
                .spawn()
                .expect("the child starts");
        Self {
            process,
            scenario: scenario.clone(),
            before,
            log: log_path,
        }
    }

    /// The run this child is writing.
    async fn run(&self) -> PathBuf {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(run) = self.scenario.runs().difference(&self.before).next() {
                return run.clone();
            }
            assert!(Instant::now() < deadline, "the child never created its run");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn signal(&self, signal: Signal) {
        let pid = Pid::from_u32(self.process.id().expect("the child is running"));
        let mut system = System::new();
        assert!(system.refresh_process(pid));
        assert_eq!(system.process(pid).unwrap().kill_with(signal), Some(true));
    }

    async fn exit(mut self) -> ExitStatus {
        tokio::time::timeout(Duration::from_secs(60), self.process.wait())
            .await
            .expect("the child exits")
            .expect("the child is reaped")
    }

    /// Run to the end and return the run it wrote.
    async fn completed(self) -> PathBuf {
        let run = self.run().await;
        let status = self.exit().await;
        assert!(status.success(), "{} failed: {status}", run.display());
        run
    }
}

/// Wait until `holds` is true of the running archive. The deadline only
/// fails a fixture whose cut never comes; it never decides correctness.
async fn cut(run: &Path, name: &str, holds: impl Fn(&Archive) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(archive) = Archive::try_read(run).await {
            assert!(
                archive.eof("store_orders").is_none(),
                "{name}: store_orders reached its end before the cut"
            );
            if holds(&archive) {
                return;
            }
        }
        assert!(Instant::now() < deadline, "{name}: the cut never held");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Checker: projections read through the supported journal reader.

struct Archive {
    dir: PathBuf,
    records: Vec<RunRecord>,
    outcome: Option<RunOutcome>,
}

impl Archive {
    async fn try_read(dir: &Path) -> Option<Self> {
        let mut snapshot = open_disk_run(dir).await.ok()?;
        let mut records = Vec::new();
        while let Some(record) = snapshot.next().await.ok()? {
            records.push(record);
        }
        let tail = snapshot.into_tail();
        let outcome = tail
            .progress()
            .outcome
            .as_ref()
            .map(|recorded| recorded.outcome.clone());
        Some(Self {
            dir: dir.to_path_buf(),
            records,
            outcome,
        })
    }

    async fn read(dir: &Path) -> Self {
        Self::try_read(dir)
            .await
            .unwrap_or_else(|| panic!("{} is readable", dir.display()))
    }

    fn rows<'a>(
        &'a self,
        stage: &'a str,
        kind: RunJournalKind,
    ) -> impl Iterator<Item = &'a JournalRecord<ChainPayload>> + 'a {
        self.records
            .iter()
            .filter(move |record| {
                record.journal.kind == kind
                    && record
                        .journal
                        .stage
                        .as_ref()
                        .is_some_and(|s| s.key == stage)
            })
            .filter_map(|record| match &record.record {
                RunRecordData::Chain(row) => Some(row.as_ref()),
                RunRecordData::System(_) => None,
            })
    }

    fn facts<'a, T: TypedPayload + DeserializeOwned + 'a>(
        &'a self,
        stage: &'a str,
    ) -> impl Iterator<Item = (&'a JournalRecord<ChainPayload>, T)> + 'a {
        self.rows(stage, RunJournalKind::Data)
            .filter(|row| row.event_type() == T::event_type_name())
            .map(|row| {
                (
                    row,
                    serde_json::from_value(row.payload()).expect("a typed fact"),
                )
            })
    }

    /// Admitted orders with their recorded identities, in journal order.
    fn admitted(&self) -> Vec<(String, EventId)> {
        ["web_orders", "store_orders"]
            .into_iter()
            .flat_map(|source| self.facts::<CustomerOrderPlaced>(source))
            .map(|(row, order)| (order.order_id, *row.id()))
            .collect()
    }

    fn eof(&self, source: &str) -> Option<EofKind> {
        self.rows(source, RunJournalKind::Data)
            .find_map(|row| match &row.payload {
                ChainPayload::FlowControl(control) => control.eof_kind(),
                _ => None,
            })
    }

    /// Committed payment facts per order.
    fn outcomes(&self) -> BTreeMap<String, Vec<&'static str>> {
        let mut outcomes: BTreeMap<String, Vec<&'static str>> = BTreeMap::new();
        for (_, payment) in self.facts::<PaymentAuthorized>("authorize_payment") {
            outcomes
                .entry(payment.order_id)
                .or_default()
                .push(AUTHORIZED);
        }
        for (_, payment) in self.facts::<PaymentDeclined>("authorize_payment") {
            outcomes.entry(payment.order_id).or_default().push(DECLINED);
        }
        outcomes
    }

    fn snapshot(&self) -> Option<SettlementLedger> {
        self.facts::<SettlementLedger>("settlement")
            .map(|(_, ledger)| ledger)
            .last()
    }

    /// Committed successful deliveries per order, through the consumed fact.
    fn delivered(&self) -> BTreeMap<String, usize> {
        let orders: BTreeMap<EventId, String> = self
            .facts::<PaymentAuthorized>("authorize_payment")
            .map(|(row, payment)| (*row.id(), payment.order_id))
            .collect();
        let mut delivered = BTreeMap::new();
        for row in self.rows("receipts", RunJournalKind::Data) {
            if let ChainPayload::Delivery(delivery) = &row.payload {
                if matches!(delivery.result, DeliveryResult::Success { .. }) {
                    let order = orders
                        .get(&delivery.subject.input.event_id)
                        .expect("a receipt names a payment of its run");
                    *delivered.entry(order.clone()).or_default() += 1;
                }
            }
        }
        delivered
    }

    /// The store channel's record rejections.
    fn rejections(&self) -> usize {
        ["store_orders"]
            .into_iter()
            .flat_map(|source| {
                [RunJournalKind::Data, RunJournalKind::Error]
                    .into_iter()
                    .flat_map(move |kind| self.rows(source, kind))
            })
            .filter(|row| {
                matches!(
                    &row.payload,
                    ChainPayload::Execution(ExecutionPayload::SourcePollError(error))
                        if error.error_type == SourcePollErrorKind::Validation
                )
            })
            .count()
    }

    fn reconstruction_completed(&self) -> bool {
        self.records.iter().any(|record| {
            matches!(
                &record.record,
                RunRecordData::System(row) if matches!(
                    row.payload,
                    SystemPayload::PipelineLifecycle(
                        PipelineLifecycleEvent::ReconstructionCompleted { .. }
                    )
                )
            )
        })
    }

    /// How `store_orders` began in this run, up to its first order.
    fn store_opening(&self) -> Vec<String> {
        self.rows("store_orders", RunJournalKind::Data)
            .take_while(|row| row.event_type() != CustomerOrderPlaced::event_type_name())
            .map(|row| row.event_type())
            .collect()
    }

    fn manifest(&self) -> serde_json::Value {
        serde_json::from_str(
            &std::fs::read_to_string(self.dir.join("run_manifest.json"))
                .expect("the manifest is readable"),
        )
        .expect("the manifest parses")
    }

    /// The run this one resumed or replays.
    fn link(&self) -> Option<PathBuf> {
        let manifest = self.manifest();
        manifest["resume"]["resumed_from"]
            .as_str()
            .or(manifest["replay"]["replay_from"].as_str())
            .map(PathBuf::from)
    }

    fn status(&self) -> String {
        match &self.outcome {
            Some(RunOutcome::Completed) => "completed".into(),
            Some(RunOutcome::Failed { reason }) => format!("failed ({reason})"),
            Some(RunOutcome::Cancelled { reason }) => format!("cancelled ({reason})"),
            Some(RunOutcome::NotStarted) => "not started".into(),
            None => "no terminal outcome".into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ended {
    Natural,
    Interrupted { killed: bool },
    Replay,
}

/// One scenario's history: its archives and what ended each one.
struct History {
    name: &'static str,
    scenario: Scenario,
    runs: Vec<(Archive, Ended)>,
    /// Orders whose gateway call was in flight at a cut.
    in_flight: BTreeSet<String>,
}

impl History {
    fn new(name: &'static str, scenario: &Scenario) -> Self {
        Self {
            name,
            scenario: scenario.clone(),
            runs: Vec::new(),
            in_flight: BTreeSet::new(),
        }
    }

    async fn record(&mut self, run: &Path, ended: Ended) -> &Archive {
        self.runs.push((Archive::read(run).await, ended));
        &self.runs[self.runs.len() - 1].0
    }

    fn archive(&self, run: &Path) -> &Archive {
        &self
            .runs
            .iter()
            .find(|(archive, _)| archive.dir == run)
            .expect("a recorded archive")
            .0
    }

    /// I1 to I8 over every archive of this history, against the control.
    fn check(&self, control: &Archive) {
        let name = self.name;
        let valid = valid_ids();
        let mut identities: BTreeMap<String, BTreeSet<EventId>> = BTreeMap::new();
        for (archive, ended) in &self.runs {
            let dir = archive.dir.display();
            let admitted = archive.admitted();
            // I1: one admission per order across the run and its resumes.
            let mut seen = BTreeSet::new();
            for (order, id) in &admitted {
                assert!(
                    seen.insert(order.clone()),
                    "{name} {dir}: {order} admitted twice"
                );
                identities.entry(order.clone()).or_default().insert(*id);
            }
            // I2: the invalid record is never admitted.
            assert!(
                !seen.contains(INVALID_ORDER),
                "{name} {dir}: the invalid order was admitted"
            );
            // I3: one committed outcome per order.
            let outcomes = archive.outcomes();
            for (order, committed) in &outcomes {
                assert_eq!(
                    committed.len(),
                    1,
                    "{name} {dir}: {order} has {committed:?}"
                );
                assert_eq!(committed[0], decision(order), "{name} {dir}: {order}");
            }
            match ended {
                Ended::Natural | Ended::Replay => {
                    assert_eq!(seen, valid, "{name} {dir}: every valid order is admitted");
                    // I5: the business result equals the control's.
                    assert_eq!(outcomes, control.outcomes(), "{name} {dir}: outcomes");
                    assert_eq!(
                        archive.snapshot(),
                        control.snapshot(),
                        "{name} {dir}: ledger"
                    );
                    assert!(archive.snapshot().is_some(), "{name} {dir}: a final ledger");
                }
                // I6: an interrupted archive is honest about completion.
                Ended::Interrupted { .. } => {
                    assert!(
                        !matches!(
                            archive.eof("store_orders"),
                            Some(EofKind::Natural | EofKind::Poison)
                        ),
                        "{name} {dir}: store_orders stopped before its end"
                    );
                    assert_eq!(archive.snapshot(), None, "{name} {dir}: no final ledger");
                }
            }
            // I2: its rejection lives in the run that read it, found by links.
            // A lineage that never read past the invalid record holds none.
            let read_past = seen.contains(FIRST_AFTER_INVALID);
            let mut lineage = vec![archive.dir.clone()];
            while let Some(link) = self
                .runs
                .iter()
                .find(|(run, _)| run.dir == lineage[lineage.len() - 1])
                .and_then(|(run, _)| run.link())
            {
                lineage.push(link);
            }
            let rejections: usize = lineage
                .iter()
                .filter_map(|run| self.runs.iter().find(|(archive, _)| &archive.dir == run))
                .map(|(archive, _)| archive.rejections())
                .sum();
            assert_eq!(
                rejections,
                usize::from(read_past),
                "{name} {dir}: rejections along {lineage:?}"
            );
        }
        for (order, ids) in &identities {
            assert_eq!(
                ids.len(),
                1,
                "{name}: {order} has several identities {ids:?}"
            );
        }

        // I4: one decision per order at the gateway; replays only in flight.
        let calls = self.scenario.calls();
        let completed = self.runs.iter().any(|(_, ended)| *ended == Ended::Natural);
        for order in &valid {
            let decided: Vec<Call> = calls
                .iter()
                .filter(|call| &call.order_id == order && call.call != Call::Replayed)
                .map(|call| call.call)
                .collect();
            if completed {
                let expected = if decision(order) == AUTHORIZED {
                    Call::Charged
                } else {
                    Call::Declined
                };
                assert_eq!(
                    decided,
                    [expected],
                    "{name}: the gateway decided {order} once"
                );
            } else {
                assert!(decided.len() <= 1, "{name}: {order} decided {decided:?}");
            }
        }
        for call in calls.iter().filter(|call| call.call == Call::Replayed) {
            assert!(
                self.in_flight.contains(&call.order_id),
                "{name}: {} replayed without being in flight at a cut",
                call.order_id
            );
        }

        // I8: every receipt delivered at least once; a physical receipt
        // without its committed delivery only where a kill cut it.
        let mut lines: BTreeMap<String, usize> = BTreeMap::new();
        for receipt in read_lines::<Receipt>(&self.scenario.receipts()) {
            *lines.entry(receipt.order_id).or_default() += 1;
        }
        let mut delivered: BTreeMap<String, usize> = BTreeMap::new();
        for (archive, _) in &self.runs {
            for (order, count) in archive.delivered() {
                *delivered.entry(order).or_default() += count;
            }
        }
        let kills = self
            .runs
            .iter()
            .filter(|(_, ended)| matches!(ended, Ended::Interrupted { killed: true }))
            .count();
        let mut uncommitted = 0;
        for (order, written) in &lines {
            let committed = delivered.get(order).copied().unwrap_or(0);
            assert!(
                *written == committed || *written == committed + 1,
                "{name}: {order} written {written}, delivered {committed}"
            );
            uncommitted += written - committed;
        }
        assert!(
            uncommitted <= kills,
            "{name}: {uncommitted} uncommitted receipts"
        );
        if completed {
            for order in valid.iter().filter(|order| decision(order) == AUTHORIZED) {
                assert!(lines.contains_key(order), "{name}: {order} has no receipt");
            }
        }
    }

    /// Every archive's `obzenflow show --jsonl` equals the checker's records.
    fn report(&self) {
        for (archive, _) in &self.runs {
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
                .arg("show")
                .arg(&archive.dir)
                .args(["--jsonl", "--include-runtime"])
                .output()
                .expect("obzenflow show runs");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let shown: Vec<serde_json::Value> = String::from_utf8(output.stdout)
                .expect("show prints UTF-8")
                .lines()
                .map(|line| serde_json::from_str(line).expect("one record per line"))
                .collect();
            let read: Vec<serde_json::Value> = archive
                .records
                .iter()
                .map(|record| serde_json::to_value(record).expect("a record serializes"))
                .collect();
            assert_eq!(
                shown,
                read,
                "{}: show differs from the checker",
                archive.dir.display()
            );
            println!(
                "{}: {} · obzenflow show {}",
                self.name,
                archive.status(),
                archive.dir.display()
            );
        }
    }
}

/// I7: a strict replay calls no gateway and `obzenflow verify` certifies it.
async fn strict_replay(history: &mut History, archive: &Path) -> PathBuf {
    let scenario = history.scenario.clone();
    scenario.without_inputs();
    let calls = scenario.calls();
    let replayed = Child::spawn(&scenario, replay(archive), None)
        .completed()
        .await;
    assert_eq!(
        scenario.calls(),
        calls,
        "{}: a replay calls no gateway",
        history.name
    );
    let verdict = std::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(archive)
        .arg("--candidate")
        .arg(&replayed)
        .output()
        .expect("obzenflow verify runs");
    assert_eq!(
        verdict.status.code(),
        Some(0),
        "{}: {}",
        history.name,
        String::from_utf8_lossy(&verdict.stdout)
    );
    history.record(&replayed, Ended::Replay).await;
    replayed
}

/// The first cut: web orders finished, store orders stalled behind the held
/// order, which the gateway charged without a committed outcome.
async fn first_cut(scenario: &Scenario, child: &Child, name: &str) -> PathBuf {
    let run = child.run().await;
    cut(&run, name, |archive| {
        archive.eof("web_orders") == Some(EofKind::Natural)
            && archive.facts::<CustomerOrderPlaced>("store_orders").count() >= 8
            && scenario
                .calls()
                .iter()
                .any(|call| call.order_id == FIRST_CUT)
            && !archive.outcomes().contains_key(FIRST_CUT)
    })
    .await;
    run
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_execution_contract() {
    // S0: natural completion, the control. S1: its strict replay.
    let s0 = Scenario::fresh("s0");
    let mut control = History::new("S0", &s0);
    let run = Child::spawn(&s0, Vec::new(), None).completed().await;
    control.record(&run, Ended::Natural).await;
    strict_replay(&mut control, &run).await;
    let reference = Archive::read(&run).await;
    control.check(&reference);
    control.report();

    // S2: SIGTERM at the cut, the hold then released; resume. S3: replay.
    let s2 = Scenario::fresh("s2");
    let mut stopped = History::new("S2", &s2);
    stopped.in_flight.insert(FIRST_CUT.into());
    let child = Child::spawn(&s2, Vec::new(), Some(FIRST_CUT));
    let run = first_cut(&s2, &child, "S2").await;
    child.signal(Signal::Term);
    std::fs::write(s2.release(), b"").expect("release the held order");
    assert!(
        child.exit().await.success(),
        "S2: a stopped run exits cleanly"
    );
    stopped
        .record(&run, Ended::Interrupted { killed: false })
        .await;
    s2.without_web_input();
    let resumed = Child::spawn(&s2, resume(&run), None).completed().await;
    stopped.record(&resumed, Ended::Natural).await;
    strict_replay(&mut stopped, &resumed).await;
    stopped.check(&reference);
    stopped.report();

    // S4: SIGKILL at the cut with the held order uncommitted. Its archive
    // starts two histories over the same inputs: S5's copy and S5b's original.
    let s4 = Scenario::fresh("s4");
    let child = Child::spawn(&s4, Vec::new(), Some(FIRST_CUT));
    let killed = first_cut(&s4, &child, "S4").await;
    child.signal(Signal::Kill);
    assert!(!child.exit().await.success());
    let (s5, killed_copy) = Scenario::copied("s5", &s4, &killed);
    s4.without_web_input();

    // S5: a second SIGKILL inside S4's resumed run, after its reconstruction
    // completes; resume of the resume.
    let mut after_rebuild = History::new("S5", &s5);
    after_rebuild
        .in_flight
        .extend([FIRST_CUT.into(), SECOND_CUT.into()]);
    after_rebuild
        .record(&killed_copy, Ended::Interrupted { killed: true })
        .await;
    let child = Child::spawn(&s5, resume(&killed_copy), Some(SECOND_CUT));
    let torn = child.run().await;
    cut(&torn, "S5", |archive| {
        archive.reconstruction_completed()
            && s5.calls().iter().any(|call| call.order_id == SECOND_CUT)
            && !archive.outcomes().contains_key(SECOND_CUT)
    })
    .await;
    child.signal(Signal::Kill);
    assert!(!child.exit().await.success());
    after_rebuild
        .record(&torn, Ended::Interrupted { killed: true })
        .await;
    let rebuilt_head = Child::spawn(&s5, resume(&torn), None).completed().await;
    after_rebuild.record(&rebuilt_head, Ended::Natural).await;

    // S5b: SIGKILL inside S4's resumed run before its reconstruction
    // completes, while the gateway holds the re-executed in-doubt order;
    // resume of the resume with S4's archive present.
    let mut before_rebuild = History::new("S5b", &s4);
    before_rebuild.in_flight.insert(FIRST_CUT.into());
    before_rebuild
        .record(&killed, Ended::Interrupted { killed: true })
        .await;
    let child = Child::spawn(&s4, resume(&killed), Some(FIRST_CUT));
    let torn = child.run().await;
    cut(&torn, "S5b", |archive| {
        !archive.reconstruction_completed()
            && s4
                .calls()
                .iter()
                .filter(|call| call.order_id == FIRST_CUT)
                .count()
                == 2
            && !archive.outcomes().contains_key(FIRST_CUT)
    })
    .await;
    child.signal(Signal::Kill);
    assert!(!child.exit().await.success());
    before_rebuild
        .record(&torn, Ended::Interrupted { killed: true })
        .await;
    let head = Child::spawn(&s4, resume(&torn), None).completed().await;
    // I9: the merged resume records S4's archive among its ancestors.
    let merged: Vec<PathBuf> = serde_json::from_value(
        before_rebuild
            .record(&head, Ended::Natural)
            .await
            .manifest()["resume"]["merged_from"]
            .clone(),
    )
    .expect("merged ancestors");
    assert_eq!(
        merged,
        std::slice::from_ref(&killed),
        "S5b: the torn resume merged S4's archive"
    );

    // S7: S4's archive again refuses before any run starts, naming the head.
    let calls = s4.calls();
    let runs = s4.runs();
    let refused = Child::spawn(&s4, resume(&killed), None);
    let log = refused.log.clone();
    assert_eq!(
        refused.exit().await.code(),
        Some(2),
        "S7: the resume refuses"
    );
    assert_eq!(s4.runs(), runs, "S7: no run starts");
    assert_eq!(s4.calls(), calls, "S7: no gateway call");
    let refusal = std::fs::read_to_string(log).expect("the refusal is logged");
    assert!(
        refusal.contains(&format!(
            "cannot resume {}: it was already resumed by {}, the head of this history.",
            killed.display(),
            head.display()
        )),
        "{refusal}"
    );

    // S6, S6b: strict replays, once neither history needs the shared inputs.
    strict_replay(&mut after_rebuild, &rebuilt_head).await;
    after_rebuild.check(&reference);
    after_rebuild.report();
    strict_replay(&mut before_rebuild, &head).await;
    before_rebuild.check(&reference);
    before_rebuild.report();

    // S8, S8b: store_orders' open is held on a FIFO until every web outcome
    // commits; the open then fails (S8) or the process dies (S8b), so the
    // source never pins. Each resume starts it as a fresh run would (B15).
    for (name, killed) in [("S8", false), ("S8b", true)] {
        let scenario = Scenario::fresh(&name.to_lowercase());
        let mut never_pinned = History::new(name, &scenario);
        std::fs::remove_file(scenario.store_input()).expect("replace the store input");
        assert!(std::process::Command::new("mkfifo")
            .arg(scenario.store_input())
            .status()
            .expect("mkfifo runs")
            .success());
        let child = Child::spawn(&scenario, Vec::new(), None);
        let run = child.run().await;
        let web: BTreeSet<String> = web_ids().into_iter().collect();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(archive) = Archive::try_read(&run).await {
                let outcomes: BTreeSet<String> = archive.outcomes().into_keys().collect();
                let delivered: BTreeSet<String> = archive.delivered().into_keys().collect();
                if archive.eof("web_orders") == Some(EofKind::Natural)
                    && outcomes == web
                    && delivered == web
                {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{name}: web outcomes never committed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if killed {
            child.signal(Signal::Kill);
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .open(scenario.store_input())
                .and_then(|mut fifo| fifo.write_all(b"store_orders: [\n"))
                .expect("feed malformed bytes to the held open");
        }
        assert!(
            !child.exit().await.success(),
            "{name}: the run does not complete"
        );
        never_pinned
            .record(&run, Ended::Interrupted { killed })
            .await;
        std::fs::remove_file(scenario.store_input()).expect("remove the FIFO");
        std::fs::write(scenario.store_input(), store_input()).expect("restore store orders");
        scenario.without_web_input();
        let web_calls = scenario
            .calls()
            .into_iter()
            .filter(|call| call.order_id.starts_with("W-"))
            .count();
        let resumed = Child::spawn(&scenario, resume(&run), None)
            .completed()
            .await;
        // I10: the source pins its input and starts before its first order,
        // and the journal-only web channel calls no gateway.
        let opening = never_pinned
            .record(&resumed, Ended::Natural)
            .await
            .store_opening();
        let started = |event: &str| opening.iter().position(|row| row.contains(event));
        let (pinned, started) = (
            started("runtime.source.input_pinned"),
            started("runtime.source.started_on_resume"),
        );
        assert!(pinned.is_some() && pinned < started, "{name}: {opening:?}");
        let web_calls_after = scenario
            .calls()
            .into_iter()
            .filter(|call| call.order_id.starts_with("W-"))
            .count();
        assert_eq!(
            web_calls, web_calls_after,
            "{name}: web orders call no gateway"
        );
        never_pinned.check(&reference);
        assert_eq!(
            never_pinned.archive(&run).rejections(),
            0,
            "{name}: nothing was read"
        );
        never_pinned.report();
    }
}
