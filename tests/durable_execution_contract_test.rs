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
//!
//! FLOWIP-084l adds HTTP rows: the web channel becomes hosted ingress, the
//! parent keeps a sender ledger that survives the server, and I11 checks
//! every answer against the journal. FLOWIP-122i's I12 checks that only hosted
//! ingress reattaches, recording that its sender redelivers.

#![cfg(unix)]

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod gateway_demo;

use async_trait::async_trait;
use gateway_demo::domain::{
    CustomerOrderPlaced, PaymentAuthorized, PaymentDeclineReason, PaymentDeclined,
    PaymentMethodState,
};
use gateway_demo::gateway::AuthorizePaymentOutcome;
use obzenflow::application::ingress::{IngestionConfig, IngressRefusalReason, IngressTiming};
use obzenflow::application::FlowApplication;
use obzenflow::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, StageCompletion,
};
use obzenflow::error::HandlerError;
use obzenflow::flow::backpressure::enforced;
use obzenflow::flow::{
    async_infinite_source, effectful_transform, flow, sink, source, stateful, FlowDefinition,
};
use obzenflow::journal::disk_journals;
use obzenflow::journal::read::{
    open_disk_run, ChainPayload, ExecutionPayload, FlowControlPayload, PipelineLifecycleEvent,
    RunJournalKind, RunOutcome, RunRecord, RunRecordData, SystemPayload,
};
use obzenflow::journal::JournalRecord;
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::{
    DeliveryMethod, DeliveryResult, DiscardSink, InlineSink, SinkDescription, SinkOperationError,
    SinkRedeliverySafety, SinkWriteFailure, SinkWritePhase,
};
use obzenflow::stages::sources::{
    HostedIngressSource, IngressDecoder, SourcePollErrorKind, YamlDecodeError, YamlDecoder,
    YamlRecord, YamlSelection, YamlSource,
};
use obzenflow::stages::stateful::{Accumulator, OnEOF, StatefulWithEmission};
use obzenflow::stages::transforms::EffectfulTransformHandler;
use obzenflow_core::event::payloads::execution_payload::{ReattachGuarantee, SourceReattachedFact};
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
const HTTP_BASE: &str = "/api/orders";
/// Refused at admission in the run that receives it (I2).
const INVALID_HTTP_ORDER: &str = "W-BAD";
/// How long a sender probing for a stall waits before it gives up. Every
/// other send waits for the server's answer, which a resumed run may delay
/// until its catch-up frees the web edge's window.
const PATIENCE: Duration = Duration::from_secs(1);
/// The server answers by its acknowledgement deadline; this only bounds a
/// server that never answers.
const ANSWER: Duration = Duration::from_secs(IngressTiming::DEFAULT_ACK_DEADLINE.as_secs() + 5);

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

/// Enough HTTP orders to fill the web edge's window twice before the last run.
fn http_ids() -> Vec<String> {
    (1..=14).map(|n| format!("W-{n:02}")).collect()
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

fn orders(path: PathBuf, pointer: &str) -> YamlSource<OrderYaml> {
    YamlSource::builder(OrderYaml)
        .path(path)
        .selection(YamlSelection::SequenceAt(pointer.into()))
        .batch_size(1)
        .build()
        .expect("the order channel is configured")
}

#[derive(Clone, Debug)]
struct HttpOrders;

impl IngressDecoder for HttpOrders {
    type Output = CustomerOrderPlaced;
}

/// One posted order; the invalid one carries an amount that cannot decode.
fn http_order(order_id: &str) -> serde_json::Value {
    let amount_cents = if order_id == INVALID_HTTP_ORDER {
        json!("not-a-number")
    } else {
        json!(1_000)
    };
    json!({
        "event_type": CustomerOrderPlaced::event_type_name(),
        "data": {
            "order_id": order_id,
            "customer_id": format!("C-{order_id}"),
            "channel": "Web",
            "amount_cents": amount_cents,
            "payment_method_state": "Valid",
            "phase": "Warmup",
        },
    })
}

/// The web channel: a YAML file in 134i's histories, hosted HTTP ingress in
/// 084l's.
enum WebOrders {
    Yaml(YamlSource<OrderYaml>),
    Http(HostedIngressSource<HttpOrders>),
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

fn build_flow(scenario: &Scenario, hold: Option<String>, web: WebOrders) -> FlowDefinition {
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
        // The graphs differ only in how `web_orders` is hosted.
        Ok(match web {
            WebOrders::Yaml(web_orders_input) => flow! {
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
            },
            WebOrders::Http(web_orders_input) => flow! {
                name: "durable_execution_contract",
                journals: disk_journals(journals),
                backpressure: enforced(4).stall_timeout_ms(120_000),

                stages: {
                    web_orders = async_infinite_source!(CustomerOrderPlaced => web_orders_input);
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
            },
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
    let mut app = FlowApplication::builder().with_cli_args(args);
    if plan.scenario.then_live {
        if let Err(error) = app.run_blocking(then_live::build_flow(&plan.scenario)) {
            eprintln!("{error}");
            std::process::exit(2);
        }
        return;
    }
    let web = match plan.scenario.http {
        Some(_) => WebOrders::Http(app.http_ingress(
            HttpOrders,
            IngestionConfig {
                base_path: HTTP_BASE.into(),
                ..IngestionConfig::default()
            },
        )),
        None => WebOrders::Yaml(orders(plan.scenario.web_input(), "/web_orders")),
    };
    if let Err(error) = app.run_blocking(build_flow(&plan.scenario, plan.hold, web)) {
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
    /// The port of an HTTP history's server; every run of it serves there.
    http: Option<u16>,
    /// Runs the FLOWIP-084q then-live flow in place of the payment flow.
    #[serde(default)]
    then_live: bool,
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
        let scenario = Self::create(name, None);
        std::fs::write(scenario.web_input(), web_input()).expect("write web orders");
        scenario
    }

    /// Web orders arrive over HTTP; only the store channel reads a file.
    fn over_http(name: &str) -> Self {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("an ephemeral port")
            .port();
        Self::create(name, Some(port))
    }

    fn create(name: &str, http: Option<u16>) -> Self {
        let root = Self::directory(name);
        let inputs = root.join("inputs");
        std::fs::create_dir(&inputs).expect("create the inputs");
        let scenario = Self {
            root,
            inputs,
            http,
            then_live: false,
        };
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
            http: from.http,
            then_live: from.then_live,
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

    fn sender_ledger(&self) -> PathBuf {
        self.root.join("sender-ledger.jsonl")
    }

    fn answers(&self) -> Vec<Sent> {
        read_lines(&self.sender_ledger())
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
    fn spawn(scenario: &Scenario, mut args: Vec<String>, hold: Option<&str>) -> Self {
        if let Some(port) = scenario.http {
            args.extend(["--server".into(), "--server-port".into(), port.to_string()]);
        }
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

/// Wait until `holds` is true of the running archive, with `store_orders`
/// still short of its end.
async fn cut(run: &Path, name: &str, holds: impl Fn(&Archive) -> bool) {
    until(run, name, |archive| {
        assert!(
            archive.eof("store_orders").is_none(),
            "{name}: store_orders reached its end before the cut"
        );
        holds(archive)
    })
    .await;
}

/// Wait until `holds` is true of the running archive. The deadline only
/// fails a fixture whose cut never comes; it never decides correctness.
async fn until(run: &Path, name: &str, holds: impl Fn(&Archive) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if Archive::try_read(run)
            .await
            .is_some_and(|archive| holds(&archive))
        {
            return;
        }
        assert!(Instant::now() < deadline, "{name}: the cut never held");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Sender: the test parent's HTTP client. Its ledger survives the server.

fn request_key(order_id: &str) -> String {
    format!("order:{order_id}")
}

/// One answer, as the sender recorded it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
enum Answer {
    Committed {
        event_id: EventId,
    },
    Duplicate {
        event_id: EventId,
    },
    Rejected {
        code: String,
    },
    /// `outcome_unknown`, a refusal before admission, or no answer at all.
    Unknown,
}

impl Answer {
    fn read(status: u16, body: &serde_json::Value) -> Self {
        let result = &body["results"][0];
        let event_id =
            || serde_json::from_value(result["event_id"].clone()).expect("an answered event id");
        match (status, result["outcome"].as_str()) {
            (202, Some("committed")) => Self::Committed {
                event_id: event_id(),
            },
            (202, Some("duplicate")) => Self::Duplicate {
                event_id: event_id(),
            },
            (400 | 422, Some("rejected")) => Self::Rejected {
                code: result["code"].as_str().unwrap_or_default().into(),
            },
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Sent {
    order_id: String,
    /// The run that received the request.
    run: PathBuf,
    #[serde(flatten)]
    answer: Answer,
}

struct Sender {
    client: reqwest::Client,
    scenario: Scenario,
}

impl Sender {
    fn new(scenario: &Scenario) -> Self {
        Self {
            client: reqwest::Client::new(),
            scenario: scenario.clone(),
        }
    }

    fn url(&self, endpoint: &str) -> String {
        let port = self.scenario.http.expect("an HTTP scenario");
        format!("http://127.0.0.1:{port}{HTTP_BASE}/{endpoint}")
    }

    /// Wait until the run's hosted ingress admits, past any catch-up.
    async fn live(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let health = self.client.get(self.url("health")).send().await;
            if health.is_ok_and(|response| response.status() == reqwest::StatusCode::OK) {
                return;
            }
            assert!(Instant::now() < deadline, "the ingress never went live");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// POST one order under its stable key, wait for the server's answer and
    /// record it.
    async fn send(&self, run: &Path, order_id: &str) -> Answer {
        self.post(run, order_id, ANSWER).await
    }

    /// POST one order, giving up after `wait`, and record the answer.
    async fn post(&self, run: &Path, order_id: &str, wait: Duration) -> Answer {
        let response = self
            .client
            .post(self.url("events"))
            .header("Idempotency-Key", format!("\"{}\"", request_key(order_id)))
            .json(&http_order(order_id))
            .timeout(wait)
            .send()
            .await;
        let answer = match response {
            Ok(response) => {
                let status = response.status().as_u16();
                Answer::read(status, &response.json().await.unwrap_or_default())
            }
            Err(_) => Answer::Unknown,
        };
        append_line(
            &self.scenario.sender_ledger(),
            &Sent {
                order_id: order_id.into(),
                run: run.into(),
                answer: answer.clone(),
            },
        )
        .expect("record the answer");
        answer
    }

    /// Resend every order whose last answer is unknown, returning the answers.
    async fn resend_unknown(&self, run: &Path) -> Vec<(String, Answer)> {
        let mut last: Vec<(String, Answer)> = Vec::new();
        for sent in self.scenario.answers() {
            match last.iter_mut().find(|(order, _)| *order == sent.order_id) {
                Some(entry) => entry.1 = sent.answer,
                None => last.push((sent.order_id, sent.answer)),
            }
        }
        let mut resent = Vec::new();
        for (order, _) in last
            .into_iter()
            .filter(|(_, answer)| *answer == Answer::Unknown)
        {
            let answer = self.send(run, &order).await;
            resent.push((order, answer));
        }
        resent
    }

    /// Send orders until one stalls behind backpressure: its sender gives up
    /// and the journal holds no row for it.
    async fn until_stalled(&self, run: &Path, pool: &mut impl Iterator<Item = String>) -> String {
        for order in pool.by_ref() {
            match self.post(run, &order, PATIENCE).await {
                Answer::Committed { .. } => {}
                Answer::Unknown => {
                    let admitted = Archive::read(run).await.admitted();
                    assert!(
                        admitted.iter().all(|(admitted, _)| *admitted != order),
                        "{order} committed after its sender gave up"
                    );
                    return order;
                }
                other => panic!("{order}: {other:?}"),
            }
        }
        panic!("the web channel never stalled");
    }
}

// ---------------------------------------------------------------------------
// Checker: projections read through the supported journal reader.

fn decision_row(row: &JournalRecord<ChainPayload>) -> Option<(String, u64, u64)> {
    match &row.payload {
        ChainPayload::FlowControl(FlowControlPayload::InputSelection {
            input,
            delivered_count,
            input_delivered_count,
        }) => Some((
            input.stage_key.to_string(),
            delivered_count.0,
            input_delivered_count.0,
        )),
        _ => None,
    }
}

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

    /// Each committed successful delivery's order, through the consumed fact,
    /// or `None` when this read does not hold that payment.
    fn receipt_orders(&self) -> Vec<Option<String>> {
        let orders: BTreeMap<EventId, String> = self
            .facts::<PaymentAuthorized>("authorize_payment")
            .map(|(row, payment)| (*row.id(), payment.order_id))
            .collect();
        self.rows("receipts", RunJournalKind::Data)
            .filter_map(|row| match &row.payload {
                ChainPayload::Delivery(delivery)
                    if matches!(delivery.result, DeliveryResult::Success { .. }) =>
                {
                    Some(orders.get(&delivery.subject.input.event_id).cloned())
                }
                _ => None,
            })
            .collect()
    }

    /// Committed successful deliveries per order, for a run that has ended.
    fn delivered(&self) -> BTreeMap<String, usize> {
        let mut delivered = BTreeMap::new();
        for order in self.receipt_orders() {
            let order = order.expect("a receipt names a payment of its run");
            *delivered.entry(order).or_default() += 1;
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

    /// Each hosted admission's request ID and row.
    fn requests(&self) -> Vec<(String, EventId)> {
        self.facts::<CustomerOrderPlaced>("web_orders")
            .filter_map(|(row, _)| {
                let event = row.authored();
                let request = event.ingress_context.as_ref()?.request.as_ref()?;
                Some((request.id.as_str().to_string(), *row.id()))
            })
            .collect()
    }

    fn system(&self) -> impl Iterator<Item = &SystemPayload> {
        self.records
            .iter()
            .filter_map(|record| match &record.record {
                RunRecordData::System(row) => Some(&row.payload),
                RunRecordData::Chain(_) => None,
            })
    }

    /// Hosted ingress refusals of invalid content.
    fn http_rejections(&self) -> usize {
        self.system()
            .filter(|payload| {
                matches!(
                    payload,
                    SystemPayload::IngressRefusal {
                        reason: IngressRefusalReason::Validation,
                        ..
                    }
                )
            })
            .count()
    }

    /// Recognized duplicates per request ID, each naming its original rows.
    fn duplicates(&self) -> BTreeMap<String, Vec<Vec<EventId>>> {
        let mut duplicates: BTreeMap<String, Vec<Vec<EventId>>> = BTreeMap::new();
        for payload in self.system() {
            if let SystemPayload::IngressDuplicateRecognized {
                request_id,
                originals,
                ..
            } = payload
            {
                duplicates
                    .entry(request_id.as_str().to_string())
                    .or_default()
                    .push(originals.iter().map(|original| original.event_id).collect());
            }
        }
        duplicates
    }

    /// FLOWIP-122i: every reattachment this run recorded, by source.
    fn reattachments(&self) -> Vec<(String, SourceReattachedFact)> {
        self.records
            .iter()
            .filter_map(|record| {
                let RunRecordData::Chain(row) = &record.record else {
                    return None;
                };
                let ChainPayload::Execution(ExecutionPayload::SourceReattached(fact)) =
                    &row.payload
                else {
                    return None;
                };
                Some((record.journal.stage.as_ref()?.key.clone(), fact.clone()))
            })
            .collect()
    }

    /// The web channel's rows around its live handoff, in journal order.
    fn web_handoff(&self) -> Vec<&'static str> {
        self.rows("web_orders", RunJournalKind::Data)
            .filter_map(|row| match &row.payload {
                ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete { .. }) => {
                    Some("catch_up")
                }
                ChainPayload::Execution(ExecutionPayload::SourceReattached(_)) => {
                    Some("reattached")
                }
                _ if row.event_type() == CustomerOrderPlaced::event_type_name() => Some("order"),
                _ => None,
            })
            .collect()
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

    fn decision_rows(&self, stage: &str) -> Vec<(String, u64, u64)> {
        self.rows(stage, RunJournalKind::Data)
            .filter_map(decision_row)
            .collect()
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
    Interrupted {
        killed: bool,
    },
    /// Stopped gracefully once every outcome committed: an HTTP history's end.
    Stopped,
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

    fn valid(&self) -> BTreeSet<String> {
        match self.scenario.http {
            Some(_) => http_ids().into_iter().chain(store_ids()).collect(),
            None => valid_ids(),
        }
    }

    /// Orders the gateway called without an outcome in `run` were in flight
    /// when it was cut.
    fn in_flight_at(&mut self, run: &Path) {
        let outcomes = self.archive(run).outcomes();
        let calls = self.scenario.calls();
        self.in_flight.extend(
            calls
                .into_iter()
                .map(|call| call.order_id)
                .filter(|order| !outcomes.contains_key(order)),
        );
    }

    /// I1 to I8 over every archive of this history. `control` is the natural
    /// run whose business result I5 compares; an HTTP history has none.
    fn check(&self, control: Option<&Archive>) {
        let name = self.name;
        let valid = self.valid();
        let answers = self.scenario.answers();
        let http_refused_in = answers
            .iter()
            .find(|sent| sent.order_id == INVALID_HTTP_ORDER)
            .map(|sent| sent.run.clone());
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
            // I2: the invalid records are never admitted.
            for invalid in [INVALID_ORDER, INVALID_HTTP_ORDER] {
                assert!(
                    !seen.contains(invalid),
                    "{name} {dir}: {invalid} was admitted"
                );
            }
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
            match (ended, control) {
                (Ended::Natural | Ended::Replay, Some(control)) => {
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
                // An HTTP history never reaches natural EOF, so I5 is dropped:
                // I3 checks every outcome and a stop never finalizes the ledger.
                (Ended::Stopped | Ended::Replay, None) => {
                    assert_eq!(seen, valid, "{name} {dir}: every valid order is admitted");
                    assert!(
                        outcomes.keys().eq(valid.iter()),
                        "{name} {dir}: every valid order has its outcome"
                    );
                    assert_eq!(archive.snapshot(), None, "{name} {dir}: no final ledger");
                }
                // I6: an interrupted archive is honest about completion.
                (Ended::Interrupted { .. }, _) => {
                    let channel = match self.scenario.http {
                        Some(_) => "web_orders",
                        None => "store_orders",
                    };
                    assert!(
                        !matches!(
                            archive.eof(channel),
                            Some(EofKind::Natural | EofKind::Poison)
                        ),
                        "{name} {dir}: {channel} stopped before its end"
                    );
                    assert_eq!(archive.snapshot(), None, "{name} {dir}: no final ledger");
                }
                (ended, _) => panic!("{name} {dir}: {ended:?} does not end this history"),
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
            let along = || {
                lineage
                    .iter()
                    .filter_map(|run| self.runs.iter().find(|(archive, _)| &archive.dir == run))
                    .map(|(archive, _)| archive)
            };
            let rejections: usize = along().map(Archive::rejections).sum();
            assert_eq!(
                rejections,
                usize::from(read_past),
                "{name} {dir}: rejections along {lineage:?}"
            );
            // The invalid HTTP order's refusal lives in the run that received it.
            let refused = http_refused_in
                .as_ref()
                .is_some_and(|received| lineage.contains(received));
            let refusals: usize = along().map(Archive::http_rejections).sum();
            assert_eq!(
                refusals,
                usize::from(refused),
                "{name} {dir}: HTTP refusals along {lineage:?}"
            );
            // I12 (FLOWIP-122i): only hosted ingress reattaches, and each
            // resumed run records that its sender redelivers, after the web
            // channel's watermark and before its first live order.
            let resumed = archive.manifest()["resume"]["resume_generation"].as_u64();
            let expected = match (self.scenario.http, resumed, ended) {
                (Some(_), Some(generation), Ended::Interrupted { .. } | Ended::Stopped) => {
                    vec![(
                        "web_orders".to_string(),
                        SourceReattachedFact {
                            generation,
                            guarantee: ReattachGuarantee::SenderRedelivers,
                        },
                    )]
                }
                _ => Vec::new(),
            };
            assert_eq!(
                archive.reattachments(),
                expected,
                "{name} {dir}: reattachments"
            );
            if !expected.is_empty() {
                let handoff = archive.web_handoff();
                let at = handoff
                    .iter()
                    .position(|row| *row == "reattached")
                    .expect("the reattachment is on the web channel");
                assert!(
                    handoff[..at].last() == Some(&"catch_up")
                        && handoff[at + 1..].iter().all(|row| *row == "order"),
                    "{name} {dir}: {handoff:?}"
                );
            }
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
        let completed = self
            .runs
            .iter()
            .any(|(_, ended)| matches!(ended, Ended::Natural | Ended::Stopped));
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
        if self.scenario.http.is_some() {
            self.check_answers(&answers);
        }
        self.check_recorded_order();
    }

    fn check_recorded_order(&self) {
        let name = self.name;
        let canonical = |path: &Path| std::fs::canonicalize(path).unwrap_or(path.to_path_buf());
        let mut recorded_archives = 0;
        let mut rows_seen = 0;
        for (archive, _) in &self.runs {
            let dir = archive.dir.display();
            if archive.manifest()["stages"]["authorize_payment"]["delivery_order"] != "recorded" {
                continue;
            }
            recorded_archives += 1;
            rows_seen += archive.decision_rows("authorize_payment").len();
            let input_of: BTreeMap<EventId, &str> = ["web_orders", "store_orders"]
                .into_iter()
                .flat_map(|source| {
                    archive
                        .rows(source, RunJournalKind::Data)
                        .map(move |row| (*row.id(), source))
                })
                .collect();
            let mut governing: Option<String> = None;
            for row in archive.rows("authorize_payment", RunJournalKind::Data) {
                if let Some(selection) = decision_row(row) {
                    governing = Some(selection.0);
                    continue;
                }
                let parent = row.envelope.provenance.event.causality.parent_ids.last();
                if let Some(input) = parent.and_then(|parent| input_of.get(parent)) {
                    assert_eq!(
                        governing.as_deref(),
                        Some(*input),
                        "{name} {dir}: {} lies under no decision row naming {input}",
                        row.event_type()
                    );
                }
            }
            if !archive.manifest()["resume"].is_object() {
                continue;
            }
            let Some(parent) = archive.link().map(|link| canonical(&link)) else {
                continue;
            };
            let Some((parent, _)) = self
                .runs
                .iter()
                .find(|(other, _)| canonical(&other.dir) == parent)
            else {
                continue;
            };
            let (rows, parent_rows) = (
                archive.decision_rows("authorize_payment"),
                parent.decision_rows("authorize_payment"),
            );
            assert!(
                rows.len() >= parent_rows.len() && rows[..parent_rows.len()] == parent_rows[..],
                "{name} {dir}: the resume's decision rows do not extend its parent's"
            );
        }
        assert!(
            recorded_archives == 0 || rows_seen > 0,
            "{name}: a recorded fan-in wrote no decision rows"
        );
    }

    /// I11: every answer the sender recorded agrees with the journal.
    fn check_answers(&self, answers: &[Sent]) {
        let name = self.name;
        let mut rows: BTreeMap<String, BTreeSet<EventId>> = BTreeMap::new();
        for (archive, _) in &self.runs {
            for (key, id) in archive.requests() {
                rows.entry(key).or_default().insert(id);
            }
        }
        let mut last = BTreeMap::new();
        for sent in answers {
            let order = &sent.order_id;
            let named = rows.get(&request_key(order));
            match &sent.answer {
                // No 202 lacks a row, and each names the key's one row.
                Answer::Committed { event_id } | Answer::Duplicate { event_id } => assert_eq!(
                    named,
                    Some(&BTreeSet::from([*event_id])),
                    "{name}: {order} answered {:?}",
                    sent.answer
                ),
                Answer::Rejected { code } => {
                    assert_eq!(order, INVALID_HTTP_ORDER, "{name}: {order} rejected");
                    assert_eq!(code, "validation_failed", "{name}: {order}");
                    assert_eq!(named, None, "{name}: a rejected order has no row");
                }
                Answer::Unknown => {}
            }
            last.insert(order, &sent.answer);
        }
        // Every unknown is resent until it resolves.
        for (order, answer) in last {
            assert_ne!(answer, &Answer::Unknown, "{name}: {order} never resolved");
        }
        // Each duplicate has one duplicate fact, naming that row, in its run.
        for (archive, _) in &self.runs {
            let mut answered: BTreeMap<String, Vec<Vec<EventId>>> = BTreeMap::new();
            for sent in answers.iter().filter(|sent| sent.run == archive.dir) {
                if let Answer::Duplicate { event_id } = sent.answer {
                    answered
                        .entry(request_key(&sent.order_id))
                        .or_default()
                        .push(vec![event_id]);
                }
            }
            assert_eq!(
                archive.duplicates(),
                answered,
                "{name} {}: duplicate facts",
                archive.dir.display()
            );
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
    control.check(Some(&reference));
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
    stopped.check(Some(&reference));
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
    after_rebuild.check(Some(&reference));
    after_rebuild.report();
    strict_replay(&mut before_rebuild, &head).await;
    before_rebuild.check(Some(&reference));
    before_rebuild.report();

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
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(archive) = Archive::try_read(&run).await {
                if archive
                    .admitted()
                    .iter()
                    .any(|(order, _)| order.starts_with("W-"))
                {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{name}: web orders were never recorded"
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
        let web_calls = scenario
            .calls()
            .into_iter()
            .filter(|call| call.order_id.starts_with("W-"))
            .count();
        assert_eq!(web_calls, 0, "{name}: nothing is charged before the cut");
        let resumed = Child::spawn(&scenario, resume(&run), None)
            .completed()
            .await;
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
        let web_calls_after: Vec<String> = scenario
            .calls()
            .into_iter()
            .filter(|call| call.order_id.starts_with("W-"))
            .map(|call| call.order_id)
            .collect();
        let distinct: BTreeSet<&String> = web_calls_after.iter().collect();
        assert_eq!(
            distinct.len(),
            web_calls_after.len(),
            "{name}: the resume charges each web order once"
        );
        never_pinned.check(Some(&reference));
        assert_eq!(
            never_pinned.archive(&run).rejections(),
            0,
            "{name}: nothing was read"
        );
        never_pinned.report();
    }
}

/// FLOWIP-084l B5: the HTTP rows. One lineage over the mixed store and HTTP
/// graph takes each cut in its own run, then stops gracefully; I7 strictly
/// replays that stopped archive.
#[tokio::test(flavor = "multi_thread")]
async fn durable_execution_contract_over_http() {
    let scenario = Scenario::over_http("http");
    let sender = Sender::new(&scenario);
    let mut history = History::new("HTTP", &scenario);
    let mut pool = http_ids().into_iter();

    // H1, before commit: the held store order and backpressure stall the web
    // channel, so a request sits uncommitted until the server is killed.
    let child = Child::spawn(&scenario, Vec::new(), Some(FIRST_CUT));
    let first = child.run().await;
    sender.live().await;
    cut(&first, "H1", |archive| {
        scenario
            .calls()
            .iter()
            .any(|call| call.order_id == FIRST_CUT)
            && !archive.outcomes().contains_key(FIRST_CUT)
    })
    .await;
    let uncommitted = sender.until_stalled(&first, &mut pool).await;
    child.signal(Signal::Kill);
    assert!(!child.exit().await.success());
    history
        .record(&first, Ended::Interrupted { killed: true })
        .await;
    history.in_flight_at(&first);

    // H2, lost response after commit: a held web order stalls the channel, a
    // sender gives up, then the hold is released and the server is killed
    // once that request's row commits.
    let held = pool.next().expect("an order to hold");
    let child = Child::spawn(&scenario, resume(&first), Some(&held));
    let second = child.run().await;
    sender.live().await;
    assert!(matches!(
        sender.resend_unknown(&second).await.as_slice(),
        [(order, Answer::Committed { .. })] if *order == uncommitted
    ));
    assert!(matches!(
        sender.send(&second, &held).await,
        Answer::Committed { .. }
    ));
    until(&second, "H2", |archive| {
        scenario.calls().iter().any(|call| call.order_id == held)
            && !archive.outcomes().contains_key(&held)
    })
    .await;
    let lost = sender.until_stalled(&second, &mut pool).await;
    std::fs::write(scenario.release(), b"").expect("release the held order");
    until(&second, "H2", |archive| {
        archive.admitted().iter().any(|(order, _)| *order == lost)
    })
    .await;
    child.signal(Signal::Kill);
    assert!(!child.exit().await.success());
    history
        .record(&second, Ended::Interrupted { killed: true })
        .await;
    history.in_flight_at(&second);

    // H3, after acknowledgement: the lost response resolves to its row, then
    // a new order's 202 arrives and the server is killed.
    let child = Child::spawn(&scenario, resume(&second), None);
    let third = child.run().await;
    sender.live().await;
    assert!(matches!(
        sender.resend_unknown(&third).await.as_slice(),
        [(order, Answer::Duplicate { .. })] if *order == lost
    ));
    let acknowledged = pool.next().expect("an order to acknowledge");
    assert!(matches!(
        sender.send(&third, &acknowledged).await,
        Answer::Committed { .. }
    ));
    child.signal(Signal::Kill);
    assert!(!child.exit().await.success());
    history
        .record(&third, Ended::Interrupted { killed: true })
        .await;
    history.in_flight_at(&third);

    // H4: the last resume refuses an invalid order, sends the rest and stops
    // once every outcome and receipt has committed.
    let child = Child::spawn(&scenario, resume(&third), None);
    let last = child.run().await;
    sender.live().await;
    assert!(sender.resend_unknown(&last).await.is_empty());
    assert_eq!(
        sender.send(&last, INVALID_HTTP_ORDER).await,
        Answer::Rejected {
            code: "validation_failed".into()
        }
    );
    for order in pool {
        let answer = sender.send(&last, &order).await;
        assert!(
            matches!(answer, Answer::Committed { .. }),
            "{order}: {answer:?}"
        );
    }
    let valid = history.valid();
    until(&last, "H4", |archive| {
        let receipts: BTreeSet<String> = read_lines::<Receipt>(&scenario.receipts())
            .into_iter()
            .map(|receipt| receipt.order_id)
            .collect();
        archive.outcomes().keys().eq(valid.iter())
            && valid
                .iter()
                .filter(|order| decision(order) == AUTHORIZED)
                .all(|order| receipts.contains(order))
    })
    .await;
    child.signal(Signal::Term);
    assert!(
        child.exit().await.success(),
        "H4: a stopped run exits cleanly"
    );
    history.record(&last, Ended::Stopped).await;
    strict_replay(&mut history, &last).await;
    history.check(None);
    history.report();
}

// ---------------------------------------------------------------------------
// FLOWIP-084q: a then-live join behind two `then_live` merges, killed in each
// phase that holds inputs and resumed. Each merge's batch phase is cut by the
// hydrating and enriching rows. The batch order is canonical, so each row
// checks the resumed lineage against the inputs' own order.

mod then_live {
    use super::*;
    use obzenflow::flow::{join, merge};
    use obzenflow::middleware::rate_limit;
    use obzenflow::stages::sources::ChannelSource;
    use obzenflow::stages::{joins, merges};
    use obzenflow_core::event::payloads::execution_payload::JoinPhaseFact;
    use obzenflow_core::StageKey;

    const ACCOUNTS: [&str; 4] = ["a-1", "a-2", "a-3", "a-4"];
    const ENTRIES: [(&str, &str); 5] = [
        ("a-1", "e-1"),
        ("a-2", "e-2"),
        ("a-3", "e-3"),
        ("a-4", "e-4"),
        ("a-1", "e-5"),
    ];

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(super) struct Account {
        account_id: String,
    }
    impl TypedPayload for Account {
        const EVENT_TYPE: &'static str = "durable_contract.then_live.account";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(super) struct Entry {
        account_id: String,
        note: String,
    }
    impl TypedPayload for Entry {
        const EVENT_TYPE: &'static str = "durable_contract.then_live.entry";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(super) struct Posted {
        account_id: String,
        note: String,
    }
    impl TypedPayload for Posted {
        const EVENT_TYPE: &'static str = "durable_contract.then_live.posted";
    }

    #[derive(Clone, Debug)]
    struct AccountYaml;
    impl YamlDecoder for AccountYaml {
        type Output = Account;
        fn decode(&self, record: YamlRecord<'_>) -> Result<Account, YamlDecodeError> {
            record.deserialize()
        }
    }

    #[derive(Clone, Debug)]
    struct EntryYaml;
    impl YamlDecoder for EntryYaml {
        type Output = Entry;
        fn decode(&self, record: YamlRecord<'_>) -> Result<Entry, YamlDecodeError> {
            record.deserialize()
        }
    }

    fn accounts_input(scenario: &Scenario) -> PathBuf {
        scenario.inputs.join("accounts.yaml")
    }

    fn entries_input(scenario: &Scenario) -> PathBuf {
        scenario.inputs.join("entries.yaml")
    }

    fn fresh(name: &str) -> Scenario {
        let root = Scenario::directory(name);
        let inputs = root.join("inputs");
        std::fs::create_dir(&inputs).expect("create the inputs");
        let scenario = Scenario {
            root,
            inputs,
            http: None,
            then_live: true,
        };
        let accounts: String = ACCOUNTS
            .iter()
            .map(|account| format!("  - account_id: \"{account}\"\n"))
            .collect();
        std::fs::write(accounts_input(&scenario), format!("accounts:\n{accounts}"))
            .expect("write accounts");
        let entries: String = ENTRIES
            .iter()
            .map(|(account, note)| format!("  - account_id: \"{account}\"\n    note: \"{note}\"\n"))
            .collect();
        std::fs::write(entries_input(&scenario), format!("entries:\n{entries}"))
            .expect("write entries");
        scenario
    }

    fn yaml<D: YamlDecoder>(decoder: D, path: PathBuf, pointer: &str) -> YamlSource<D> {
        YamlSource::builder(decoder)
            .path(path)
            .selection(YamlSelection::SequenceAt(pointer.into()))
            .batch_size(1)
            .build()
            .expect("the batch channel is configured")
    }

    pub(super) fn build_flow(scenario: &Scenario) -> FlowDefinition {
        let batch_accounts_input = yaml(AccountYaml, accounts_input(scenario), "/accounts");
        let batch_tx_input = yaml(EntryYaml, entries_input(scenario), "/entries");
        let (account_sender, live_accounts_input) = tokio::sync::mpsc::channel::<Account>(1);
        let (entry_sender, live_tx_input) = tokio::sync::mpsc::channel::<Entry>(1);
        // The senders outlive the run, so the live inputs stay open until a stop.
        Box::leak(Box::new((account_sender, entry_sender)));
        let live_accounts_input = ChannelSource::new(live_accounts_input);
        let live_tx_input = ChannelSource::new(live_tx_input);
        let journals = scenario.journals();
        FlowDefinition::materialize(move |_runtime_config| {
            let post_entry = joins::inner_then_live(
                |account: &Account| account.account_id.clone(),
                |entry: &Entry| entry.account_id.clone(),
                |_account: Account, entry: Entry| Posted {
                    account_id: entry.account_id,
                    note: entry.note,
                },
            );
            let ledger = DiscardSink::<Posted>::new();
            Ok(flow! {
                name: "durable_execution_contract_then_live",
                journals: disk_journals(journals),

                stages: {
                    batch_accounts = source!(Account => batch_accounts_input with rate_limit(2.0));
                    batch_tx = source!(Entry => batch_tx_input with rate_limit(2.0));
                    live_accounts = async_infinite_source!(Account => live_accounts_input);
                    live_tx = async_infinite_source!(Entry => live_tx_input);
                    accounts = merge!(Account => merges::then_live);
                    transactions = merge!(Entry => merges::then_live);
                    posted = join!(catalog accounts: Account, Entry -> Posted => post_entry);
                    ledger = sink!(Posted => ledger);
                },

                topology: {
                    batch_accounts |> accounts;
                    live_accounts |> accounts;
                    batch_tx |> transactions;
                    live_tx |> transactions;
                    (accounts, transactions) |> posted;
                    posted |> ledger;
                }
            })
        })
    }

    fn postings(archive: &Archive) -> Vec<String> {
        archive
            .facts::<Posted>("posted")
            .map(|(_, posted)| posted.note)
            .collect()
    }

    fn phases(archive: &Archive) -> Vec<JoinPhaseFact> {
        archive
            .rows("posted", RunJournalKind::Data)
            .filter_map(|row| match &row.payload {
                ChainPayload::Execution(ExecutionPayload::JoinPhase(fact)) => Some(fact.clone()),
                _ => None,
            })
            .collect()
    }

    #[derive(Clone, Copy, Debug)]
    enum Cut {
        Hydrating,
        Enriching,
        Live,
    }

    impl Cut {
        fn holds(self, archive: &Archive) -> bool {
            let phases = phases(archive);
            match self {
                Self::Hydrating => {
                    archive.facts::<Account>("accounts").count() >= 1
                        && phases.is_empty()
                        && archive.eof("batch_accounts").is_none()
                }
                Self::Enriching => {
                    phases.len() == 1
                        && !postings(archive).is_empty()
                        && archive.eof("batch_tx").is_none()
                }
                Self::Live => phases.len() == 2,
            }
        }
    }

    /// Kill the run at `cut`, resume it to the end of the batch, check the
    /// resumed lineage and strict-replay it.
    async fn row(name: &str, cut: Cut, generations: (u64, u64)) {
        let scenario = fresh(name);
        let child = Child::spawn(&scenario, Vec::new(), None);
        let killed = child.run().await;
        until(&killed, name, |archive| cut.holds(archive)).await;
        child.signal(Signal::Kill);
        assert!(!child.exit().await.success(), "{name}: the kill lands");
        let at_kill = Archive::read(&killed).await;
        assert!(cut.holds(&at_kill), "{name}: the kill landed in {cut:?}");

        let child = Child::spawn(&scenario, resume(&killed), None);
        let resumed = child.run().await;
        until(&resumed, name, |archive| {
            postings(archive).len() == ENTRIES.len() && phases(archive).len() == 2
        })
        .await;
        child.signal(Signal::Term);
        assert!(
            child.exit().await.success(),
            "{name}: the resumed run stops"
        );

        let archive = Archive::read(&resumed).await;
        let notes: Vec<String> = ENTRIES.iter().map(|(_, note)| (*note).into()).collect();
        assert_eq!(
            postings(&archive),
            notes,
            "{name}: each batch entry posts once"
        );
        assert_eq!(
            archive
                .facts::<Entry>("transactions")
                .map(|(_, entry)| entry.note)
                .collect::<Vec<_>>(),
            notes,
            "{name}: the merged entries"
        );
        assert_eq!(
            archive
                .facts::<Account>("accounts")
                .map(|(_, account)| account.account_id)
                .collect::<Vec<_>>(),
            ACCOUNTS,
            "{name}: the merged accounts"
        );
        assert_eq!(
            phases(&archive),
            [
                JoinPhaseFact::EnrichingEntered {
                    generation: generations.0,
                    ended_by: vec![StageKey::from("batch_accounts")],
                },
                JoinPhaseFact::LiveEntered {
                    generation: generations.1,
                    ended_by: vec![StageKey::from("batch_tx")],
                },
            ],
            "{name}: one fact per transition, in the generation that made it"
        );

        remove_input(&accounts_input(&scenario));
        remove_input(&entries_input(&scenario));
        let replayed = Child::spawn(&scenario, replay(&resumed), None)
            .completed()
            .await;
        let verdict = std::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
            .args(["verify", "--baseline"])
            .arg(&resumed)
            .arg("--candidate")
            .arg(&replayed)
            .output()
            .expect("obzenflow verify runs");
        assert_eq!(
            verdict.status.code(),
            Some(0),
            "{name}: {}",
            String::from_utf8_lossy(&verdict.stdout)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_kill_while_hydrating_resumes_into_the_batch() {
        row("then_live_hydrating", Cut::Hydrating, (1, 1)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_kill_while_enriching_resumes_the_batch_once() {
        row("then_live_enriching", Cut::Enriching, (0, 1)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_kill_once_live_reconstructs_both_phases() {
        row("then_live_live", Cut::Live, (0, 0)).await;
    }
}
