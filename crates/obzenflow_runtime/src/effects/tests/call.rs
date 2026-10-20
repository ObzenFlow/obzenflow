// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::effects::{AnswersEffect, StageEffectsError, System, SystemError};
use obzenflow_core::SystemBindingEvidence;

#[derive(Debug, thiserror::Error)]
#[error("the ledger refused the write")]
struct LedgerRefused;

#[async_trait]
trait LedgerPort: Send + Sync {
    async fn write(&self, value: u64) -> Result<u64, LedgerRefused>;
}

struct CountingLedger {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LedgerPort for CountingLedger {
    async fn write(&self, value: u64) -> Result<u64, LedgerRefused> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(value * 10)
    }
}

struct RefusingLedger;

#[async_trait]
impl LedgerPort for RefusingLedger {
    async fn write(&self, _: u64) -> Result<u64, LedgerRefused> {
        Err(LedgerRefused)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct LedgerWritten {
    value: u64,
}

impl TypedPayload for LedgerWritten {
    const EVENT_TYPE: &'static str = "test.ledger_written";
}

#[derive(Clone, Debug, serde::Serialize)]
struct LedgerWrite {
    value: u64,
}

#[async_trait]
impl CallRequest for LedgerWrite {
    const EFFECT_TYPE: &'static str = "test.ledger_write";
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type Port = dyn LedgerPort;
    type Outcome = LedgerWritten;

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("ledger:{}", self.value)))
    }

    async fn call(
        &self,
        ledger: &dyn LedgerPort,
        _: &mut EffectContext,
    ) -> Result<LedgerWritten, EffectError> {
        let value = ledger
            .write(self.value)
            .await
            .map_err(|error| EffectError::unavailable("ledger", "refused", error))?;
        Ok(LedgerWritten { value })
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct LedgerCheck {
    value: u64,
}

#[async_trait]
impl CallRequest for LedgerCheck {
    const EFFECT_TYPE: &'static str = "test.ledger_check";
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type Port = dyn LedgerPort;
    type Outcome = LedgerWritten;

    async fn call(
        &self,
        ledger: &dyn LedgerPort,
        _: &mut EffectContext,
    ) -> Result<LedgerWritten, EffectError> {
        let value = ledger
            .write(self.value)
            .await
            .map_err(|error| EffectError::unavailable("ledger", "refused", error))?;
        Ok(LedgerWritten { value })
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct LedgerCommit;

#[async_trait]
impl CallRequest for LedgerCommit {
    const EFFECT_TYPE: &'static str = "test.ledger_commit";
    const SAFETY: EffectSafety = EffectSafety::Transactional;
    type Port = dyn LedgerPort;
    type Outcome = LedgerWritten;

    async fn call(
        &self,
        _: &dyn LedgerPort,
        _: &mut EffectContext,
    ) -> Result<LedgerWritten, EffectError> {
        Ok(LedgerWritten { value: 0 })
    }
}

const LEDGER_PORT: EffectPortSlot<dyn LedgerPort> = EffectPortSlot::new("port");

#[derive(Clone, Debug)]
struct HandWrittenLedgerWrite {
    request: LedgerWrite,
    binding: EffectBindingUse<Self>,
}

#[async_trait]
impl Effect for HandWrittenLedgerWrite {
    const EFFECT_TYPE: &'static str = "test.ledger_write";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = Named<SystemBindingEvidence>;
    type Outcome = LedgerWritten;
    type OutcomeSemantics = DomainFacts;

    fn label(&self) -> &str {
        "test.ledger_write"
    }

    fn canonical_input(&self) -> Value {
        json!(self.request)
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let ledger = ctx.port(LEDGER_PORT)?;
        let value = ledger
            .write(self.request.value)
            .await
            .map_err(|error| EffectError::unavailable("ledger", "refused", error))?;
        Ok(LedgerWritten { value })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        self.request.idempotency_key()
    }
}

impl NamedEffect for HandWrittenLedgerWrite {
    type BindingEvidence = SystemBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(LEDGER_PORT)
    }
}

fn counting_ledger(calls: &Arc<AtomicUsize>) -> Arc<dyn LedgerPort> {
    Arc::new(CountingLedger {
        calls: calls.clone(),
    })
}

fn declaration_for<E, S>(system: &S) -> EffectDeclaration
where
    E: Effect,
    S: AnswersEffect<E>,
{
    system
        .declaration(None)
        .expect("the system answers the effect")
}

fn ledger_context(
    journal: Arc<MemoryJournal<ChainEvent>>,
    stage_id: StageId,
    effect_ports: EffectPortRegistry,
    declarations: Vec<EffectDeclaration>,
    effect_history: Option<Arc<EffectHistory>>,
) -> EffectInvocationContext {
    let mode = if effect_history.is_some() {
        EffectRuntimeMode::ReplayStrict
    } else {
        EffectRuntimeMode::Live
    };
    let mut ctx = invocation_context_with_mode(
        journal,
        parent_envelope(WriterId::from(stage_id)),
        effect_history,
        mode,
        effect_ports,
    );
    ctx.effect_declarations = declarations;
    ctx.output_contract = output_contract_for::<LedgerWritten>();
    ctx
}

fn committed_facts(journal: &MemoryJournal<ChainEvent>) -> Vec<Value> {
    journal
        .events()
        .into_iter()
        .map(|envelope| envelope.authored())
        .filter(|event| event.event_type() == LedgerWritten::EVENT_TYPE)
        .map(|event| event.payload())
        .collect()
}

#[tokio::test]
async fn a_request_records_what_an_equivalent_hand_written_effect_records() {
    let calls = Arc::new(AtomicUsize::new(0));
    let request_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let request_stage = StageId::new();
    let system: System<dyn LedgerPort> =
        System::eager("simulated-ledger", counting_ledger(&calls)).expect("valid system");
    let declaration = declaration_for::<Call<LedgerWrite>, _>(&system);
    let registry = EffectPortRegistry::collect_from_declarations([&declaration])
        .expect("the system's registration collects")
        .into_run_registry();
    let mut fx =
        Effects::<LedgerWritten, crate::effect_set![Call<LedgerWrite>]>::new(ledger_context(
            request_journal.clone(),
            request_stage,
            registry,
            vec![declaration.runtime_projection()],
            None,
        ));
    let called = fx
        .call(LedgerWrite { value: 4 })
        .await
        .expect("the request succeeds");

    let hand_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let hand_stage = StageId::new();
    let binding = EffectRegistrationBuilder::<HandWrittenLedgerWrite>::new(
        LogicalEffectBindingName::new("simulated-ledger").unwrap(),
        SystemBindingEvidence::named("simulated-ledger").unwrap(),
    )
    .bind_eager(LEDGER_PORT, counting_ledger(&calls))
    .unwrap()
    .finish()
    .unwrap();
    let hand_declaration = EffectDeclaration::named(&binding);
    let hand_registry = EffectPortRegistry::collect_from_declarations([&hand_declaration])
        .unwrap()
        .into_run_registry();
    let mut hand =
        Effects::<LedgerWritten, crate::effect_set![HandWrittenLedgerWrite]>::new(ledger_context(
            hand_journal.clone(),
            hand_stage,
            hand_registry,
            vec![hand_declaration.runtime_projection()],
            None,
        ));
    let performed = hand
        .perform(HandWrittenLedgerWrite {
            request: LedgerWrite { value: 4 },
            binding: binding.invocation(),
        })
        .await
        .expect("the hand-written effect succeeds");

    assert_eq!(called, performed);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let requested = &effect_records(&request_journal)[0].descriptor;
    let hand_written = &effect_records(&hand_journal)[0].descriptor;
    assert_eq!(requested.effect_type, hand_written.effect_type);
    assert_eq!(requested.schema_version, hand_written.schema_version);
    assert_eq!(
        requested.canonical_input_hash,
        hand_written.canonical_input_hash
    );
    assert_eq!(requested.binding, hand_written.binding);
    assert_eq!(
        committed_facts(&request_journal),
        committed_facts(&hand_journal)
    );
}

#[tokio::test]
async fn one_deferred_system_resolves_its_port_once_per_run_for_every_effect_it_answers() {
    let resolutions = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let system: System<dyn LedgerPort> = System::deferred("ledger", {
        let resolutions = resolutions.clone();
        let calls = calls.clone();
        move || {
            resolutions.fetch_add(1, Ordering::SeqCst);
            Ok(counting_ledger(&calls))
        }
    })
    .expect("valid system");
    let write = declaration_for::<Call<LedgerWrite>, _>(&system);
    let check = declaration_for::<Call<LedgerCheck>, _>(&system);
    let another_stage_write = declaration_for::<Call<LedgerWrite>, _>(&system);
    let registry =
        EffectPortRegistry::collect_from_declarations([&write, &check, &another_stage_write])
            .expect("stages that name one system share its registration");

    let first_stage = vec![write.runtime_projection(), check.runtime_projection()];
    let second_stage = vec![another_stage_write.runtime_projection()];
    for expected_resolutions in [1, 2] {
        let run = registry.clone().into_run_registry();
        for (declarations, check_instead) in [
            (&first_stage, false),
            (&first_stage, true),
            (&second_stage, false),
        ] {
            let stage = StageId::new();
            let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage)));
            let mut fx = Effects::<
                LedgerWritten,
                crate::effect_set![Call<LedgerWrite>, Call<LedgerCheck>],
            >::new(ledger_context(
                journal,
                stage,
                run.clone(),
                declarations.clone(),
                None,
            ));
            if check_instead {
                fx.call(LedgerCheck { value: 2 })
                    .await
                    .expect("the check succeeds");
            } else {
                fx.call(LedgerWrite { value: 1 })
                    .await
                    .expect("the write succeeds");
            }
        }
        assert_eq!(resolutions.load(Ordering::SeqCst), expected_resolutions);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
}

#[test]
fn independently_built_systems_with_one_name_fail_collection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let first: System<dyn LedgerPort> =
        System::eager("ledger", counting_ledger(&calls)).expect("valid system");
    let second: System<dyn LedgerPort> =
        System::eager("ledger", counting_ledger(&calls)).expect("valid system");
    let error = match EffectPortRegistry::collect_from_declarations([
        &declaration_for::<Call<LedgerWrite>, _>(&first),
        &declaration_for::<Call<LedgerWrite>, _>(&second),
    ]) {
        Ok(_) => panic!("two systems named alike must not both answer one effect"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        EffectRegistrationCollectionError::DistinctBindings {
            effect_type: "test.ledger_write",
            ..
        }
    ));
}

#[test]
fn system_constructors_refuse_names_that_could_carry_a_location_or_credential() {
    let calls = Arc::new(AtomicUsize::new(0));
    for name in [
        "",
        "simulated risk engine",
        "-leading-hyphen",
        "https://user:secret@vendor.example",
        &"s".repeat(65),
    ] {
        let eager = System::<dyn LedgerPort>::eager(name, counting_ledger(&calls));
        assert_eq!(
            eager.expect_err("an invalid name is refused"),
            SystemError::InvalidName {
                system: name.to_string()
            }
        );
        let deferred = System::<dyn LedgerPort>::deferred(name, || {
            Err(EffectPortResolutionError::CredentialUnavailable)
        });
        assert!(matches!(
            deferred,
            Err(SystemError::InvalidName { system }) if system == name
        ));
    }
    for name in [
        "simulated-risk-engine",
        "risk_engine",
        "ledger.v2",
        "0ledger",
    ] {
        let system = System::<dyn LedgerPort>::eager(name, counting_ledger(&calls))
            .expect("a valid name is accepted");
        assert_eq!(system.name(), name);
    }
    assert_eq!(
        SystemError::InvalidName {
            system: "simulated risk engine".to_string()
        }
        .to_string(),
        "system names must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}"
    );
}

#[test]
fn a_stage_refuses_a_transactional_request() {
    let calls = Arc::new(AtomicUsize::new(0));
    let system: System<dyn LedgerPort> =
        System::eager("ledger", counting_ledger(&calls)).expect("valid system");
    let error = AnswersEffect::<Call<LedgerCommit>>::declaration(&system, None)
        .expect_err("transactional requests have no executor slot");
    assert_eq!(
        error,
        StageEffectsError::TransactionalRequest {
            effect_type: "test.ledger_commit"
        }
    );
    assert_eq!(
        error.for_stage("ledger"),
        "stage `ledger`: `test.ledger_commit` is transactional and cannot be performed as a request"
    );
}

#[derive(Clone, PartialEq, Eq)]
struct VendorEvidence {
    canonical: BoundedBindingEvidence,
}

impl EffectBindingEvidence for VendorEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        self.canonical.clone()
    }
}

#[derive(Clone, Debug)]
struct VendorScore {
    binding: EffectBindingUse<Self>,
}

#[async_trait]
impl Effect for VendorScore {
    const EFFECT_TYPE: &'static str = "aml.score_transaction";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = Named<VendorEvidence>;
    type Outcome = LedgerWritten;
    type OutcomeSemantics = DomainFacts;

    fn label(&self) -> &str {
        "score_transaction"
    }

    fn canonical_input(&self) -> Value {
        Value::Null
    }

    async fn execute(&self, _: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(LedgerWritten { value: 0 })
    }
}

impl NamedEffect for VendorScore {
    type BindingEvidence = VendorEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(LEDGER_PORT)
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct ScoreLike;

#[async_trait]
impl CallRequest for ScoreLike {
    const EFFECT_TYPE: &'static str = "aml.score_transaction";
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type Port = dyn LedgerPort;
    type Outcome = LedgerWritten;

    async fn call(
        &self,
        _: &dyn LedgerPort,
        _: &mut EffectContext,
    ) -> Result<LedgerWritten, EffectError> {
        Ok(LedgerWritten { value: 0 })
    }
}

#[test]
fn system_evidence_keeps_the_binding_digest_aml_records_today() {
    let vendor = VendorEvidence {
        canonical: BoundedBindingEvidence::try_new(
            json!({ "system": "simulated-risk-engine" })
                .to_string()
                .into_bytes(),
        )
        .unwrap(),
    };
    assert_eq!(
        super::super::binding::binding_identity::<Call<ScoreLike>>(
            &SystemBindingEvidence::named("simulated-risk-engine").unwrap()
        ),
        super::super::binding::binding_identity::<VendorScore>(&vendor)
    );
}

#[tokio::test]
async fn unavailable_is_a_retryable_dependency_failure_live_and_recorded() {
    let live = EffectError::unavailable("risk_engine", "unavailable", "the risk engine is down");
    assert!(matches!(
        live,
        EffectError::DependencyFailed {
            retry: RetryDisposition::Retryable,
            ..
        }
    ));
    assert_eq!(
        live.failure_cause(),
        Some(EffectFailureCause {
            source: EffectFailureSource::new("risk_engine"),
            code: EffectFailureCode::new("unavailable"),
        })
    );
    assert_eq!(live.semantic_reason(), "the risk engine is down");

    let stage = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage)));
    let system: System<dyn LedgerPort> = System::eager(
        "ledger-outage",
        Arc::new(RefusingLedger) as Arc<dyn LedgerPort>,
    )
    .expect("valid system");
    let declaration = declaration_for::<Call<LedgerWrite>, _>(&system);
    let registry = EffectPortRegistry::collect_from_declarations([&declaration])
        .unwrap()
        .into_run_registry();
    let mut fx =
        Effects::<LedgerWritten, crate::effect_set![Call<LedgerWrite>]>::new(ledger_context(
            journal.clone(),
            stage,
            registry,
            vec![declaration.runtime_projection()],
            None,
        ));
    let refused = fx
        .call(LedgerWrite { value: 3 })
        .await
        .expect_err("the outage refuses");
    assert_eq!(
        refused.failure_cause(),
        Some(EffectFailureCause {
            source: EffectFailureSource::new("ledger"),
            code: EffectFailureCode::new("refused"),
        })
    );

    let records = effect_records(&journal);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("the live history indexes"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut replay =
        Effects::<LedgerWritten, crate::effect_set![Call<LedgerWrite>]>::new(ledger_context(
            replay_journal,
            stage,
            EffectPortRegistry::new(),
            vec![declaration.runtime_projection()],
            Some(history),
        ));
    let recorded = replay
        .call(LedgerWrite { value: 3 })
        .await
        .expect_err("the recorded refusal replays");
    assert!(matches!(
        recorded,
        EffectError::RecordedFailure {
            retry: RetryDisposition::Retryable,
            ..
        }
    ));
    assert_eq!(recorded.failure_cause(), refused.failure_cause());
    assert_eq!(recorded.semantic_reason(), refused.semantic_reason());
}
