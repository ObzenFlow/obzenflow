// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::case_system::{
    CaseRequest, CaseSystemError, CaseSystemPort, SimulatedCaseSystem, CASE_SYSTEM,
};
use super::domain::{AlertRaised, CaseOpened, TransactionCleared};
use super::ledger::LedgerError;
use super::risk_engine::{
    RiskEngineError, RiskEnginePort, RiskVerdict, ScoreRequest, SimulatedRiskEngine, RISK_ENGINE,
};
use async_trait::async_trait;
use obzenflow::effects::{
    BindingEvidenceError, BoundedBindingEvidence, DomainFacts, Effect, EffectBinding,
    EffectBindingBuildError, EffectBindingEvidence, EffectBindingUse, EffectContext, EffectError,
    EffectFailureCode, EffectFailureSource, EffectPortResolutionError, EffectPortResolver,
    EffectPortSlotSet, EffectRegistrationBuilder, EffectSafety, IdempotencyKey,
    LogicalEffectBindingName, Named, NamedEffect, RetryDisposition,
};
use obzenflow::schema::EffectOutcomeFacts;
use serde_json::json;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub const RISK_ENGINE_SOURCE: &str = "risk_engine";
pub const CASE_SYSTEM_SOURCE: &str = "case_system";

#[derive(Clone, PartialEq, Eq)]
pub struct VendorEvidence {
    canonical: BoundedBindingEvidence,
}

impl VendorEvidence {
    fn named(system: &str) -> Result<Self, BindingEvidenceError> {
        Ok(Self {
            canonical: BoundedBindingEvidence::try_new(
                json!({ "system": system }).to_string().into_bytes(),
            )?,
        })
    }
}

impl EffectBindingEvidence for VendorEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        self.canonical.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ScoreTransaction {
    request: ScoreRequest,
    binding: EffectBindingUse<Self>,
}

impl ScoreTransaction {
    pub fn new(request: ScoreRequest, binding: EffectBindingUse<Self>) -> Self {
        Self { request, binding }
    }
}

#[derive(Clone, Debug, EffectOutcomeFacts)]
#[effect_outcome(schema = obzenflow::schema)]
pub enum ScoreOutcome {
    Cleared(TransactionCleared),
    Flagged(AlertRaised),
}

#[async_trait]
impl Effect for ScoreTransaction {
    const EFFECT_TYPE: &'static str = "aml.score_transaction";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = Named<VendorEvidence>;
    type Outcome = ScoreOutcome;
    type OutcomeSemantics = DomainFacts;

    fn label(&self) -> &str {
        "score_transaction"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!(self.request)
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let engine = ctx.port(RISK_ENGINE)?;
        let verdict = engine
            .score(&score_key(&self.request), ctx.flow_id(), &self.request)
            .await
            .map_err(engine_failed)?;
        Ok(match verdict {
            RiskVerdict::Cleared { rules_version } => ScoreOutcome::Cleared(TransactionCleared {
                txn_id: self.request.txn_id,
                rules_version,
            }),
            RiskVerdict::Flagged {
                reasons,
                rules_version,
            } => ScoreOutcome::Flagged(AlertRaised {
                txn_id: self.request.txn_id,
                account: self.request.payer.clone(),
                reasons,
                rules_version,
            }),
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(score_key(&self.request))
    }
}

impl NamedEffect for ScoreTransaction {
    type BindingEvidence = VendorEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(RISK_ENGINE)
    }
}

#[derive(Clone, Debug)]
pub struct OpenCase {
    request: CaseRequest,
    binding: EffectBindingUse<Self>,
}

impl OpenCase {
    pub fn new(request: CaseRequest, binding: EffectBindingUse<Self>) -> Self {
        Self { request, binding }
    }
}

#[async_trait]
impl Effect for OpenCase {
    const EFFECT_TYPE: &'static str = "aml.open_case";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = Named<VendorEvidence>;
    type Outcome = CaseOpened;
    type OutcomeSemantics = DomainFacts;

    fn label(&self) -> &str {
        "open_case"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!(self.request)
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let cases = ctx.port(CASE_SYSTEM)?;
        let case_number = cases
            .open(&case_key(&self.request), ctx.flow_id(), &self.request)
            .await
            .map_err(case_system_failed)?;
        Ok(CaseOpened {
            case_number,
            txn_id: self.request.txn_id,
            account: self.request.account.clone(),
            reasons: self.request.reasons.clone(),
            rules_version: self.request.rules_version.clone(),
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(case_key(&self.request))
    }
}

impl NamedEffect for OpenCase {
    type BindingEvidence = VendorEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(CASE_SYSTEM)
    }
}

fn score_key(request: &ScoreRequest) -> IdempotencyKey {
    IdempotencyKey(format!("aml-score:{}", request.txn_id))
}

fn case_key(request: &CaseRequest) -> IdempotencyKey {
    IdempotencyKey(format!("aml-case:{}", request.txn_id))
}

fn engine_failed(error: RiskEngineError) -> EffectError {
    EffectError::DependencyFailed {
        failure_source: EffectFailureSource::new(RISK_ENGINE_SOURCE),
        code: EffectFailureCode::new("ledger_write_failed"),
        message: error.to_string(),
        retry: RetryDisposition::Retryable,
    }
}

fn case_system_failed(error: CaseSystemError) -> EffectError {
    EffectError::DependencyFailed {
        failure_source: EffectFailureSource::new(CASE_SYSTEM_SOURCE),
        code: EffectFailureCode::new("ledger_write_failed"),
        message: error.to_string(),
        retry: RetryDisposition::Retryable,
    }
}

#[derive(Debug, Default)]
pub struct VendorStats {
    engines_built: AtomicUsize,
    case_systems_built: AtomicUsize,
}

#[cfg(test)]
impl VendorStats {
    pub fn engines_built(&self) -> usize {
        self.engines_built.load(Ordering::SeqCst)
    }

    pub fn case_systems_built(&self) -> usize {
        self.case_systems_built.load(Ordering::SeqCst)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VendorBindingsError {
    #[error("vendor binding evidence is invalid: {0}")]
    Evidence(#[source] BindingEvidenceError),
    #[error("risk engine binding is invalid: {0}")]
    RiskEngine(#[source] EffectBindingBuildError),
    #[error("case system binding is invalid: {0}")]
    CaseSystem(#[source] EffectBindingBuildError),
}

pub struct VendorBindings {
    pub risk_engine: EffectBinding<ScoreTransaction>,
    pub case_system: EffectBinding<OpenCase>,
}

impl VendorBindings {
    pub fn simulated(
        risk_ledger: &Path,
        case_ledger: &Path,
        stats: Arc<VendorStats>,
    ) -> Result<Self, VendorBindingsError> {
        let engine_resolver: EffectPortResolver<dyn RiskEnginePort> = {
            let ledger = risk_ledger.to_path_buf();
            let stats = stats.clone();
            Arc::new(move || {
                stats.engines_built.fetch_add(1, Ordering::SeqCst);
                let engine = SimulatedRiskEngine::open(&ledger).map_err(construction_failed)?;
                Ok(Arc::new(engine) as Arc<dyn RiskEnginePort>)
            })
        };
        let case_resolver: EffectPortResolver<dyn CaseSystemPort> = {
            let ledger = case_ledger.to_path_buf();
            Arc::new(move || {
                stats.case_systems_built.fetch_add(1, Ordering::SeqCst);
                let cases = SimulatedCaseSystem::open(&ledger).map_err(construction_failed)?;
                Ok(Arc::new(cases) as Arc<dyn CaseSystemPort>)
            })
        };

        let risk_engine = EffectRegistrationBuilder::<ScoreTransaction>::new(
            LogicalEffectBindingName::new("risk_engine").expect("static binding name is valid"),
            VendorEvidence::named("simulated-risk-engine")
                .map_err(VendorBindingsError::Evidence)?,
        )
        .bind_deferred(RISK_ENGINE, engine_resolver)
        .map_err(VendorBindingsError::RiskEngine)?
        .finish()
        .map_err(VendorBindingsError::RiskEngine)?;

        let case_system = EffectRegistrationBuilder::<OpenCase>::new(
            LogicalEffectBindingName::new("case_system").expect("static binding name is valid"),
            VendorEvidence::named("simulated-case-system")
                .map_err(VendorBindingsError::Evidence)?,
        )
        .bind_deferred(CASE_SYSTEM, case_resolver)
        .map_err(VendorBindingsError::CaseSystem)?
        .finish()
        .map_err(VendorBindingsError::CaseSystem)?;

        Ok(Self {
            risk_engine,
            case_system,
        })
    }
}

fn construction_failed(error: LedgerError) -> EffectPortResolutionError {
    tracing::error!(%error, "a simulated system could not open its ledger");
    EffectPortResolutionError::ClientConstructionFailed
}

#[cfg(test)]
mod tests {
    use super::super::domain::{
        AccountKey, PaymentFormat, ReasonCode, Reasons, RulesVersion, TxnId,
    };
    use super::*;

    #[test]
    fn keys_are_business_keys_named_by_the_transaction() {
        let score = ScoreRequest {
            txn_id: TxnId(2817341),
            timestamp: "2022/09/05 10:15".to_owned(),
            payer: AccountKey {
                bank: "70".to_owned(),
                account: "8000A1B2C".to_owned(),
            },
            payee: AccountKey {
                bank: "12".to_owned(),
                account: "8000D3E4F".to_owned(),
            },
            amount_paid: 120.0,
            payment_currency: "US Dollar".to_owned(),
            amount_received: 120.0,
            receiving_currency: "US Dollar".to_owned(),
            payment_format: PaymentFormat::Cheque,
            history: None,
        };
        let case = CaseRequest {
            txn_id: TxnId(2817341),
            account: score.payer.clone(),
            reasons: Reasons {
                first: ReasonCode("format_switch".to_owned()),
                rest: Vec::new(),
            },
            rules_version: RulesVersion("sim-2026.10".to_owned()),
        };
        assert_eq!(score_key(&score).0, "aml-score:2817341");
        assert_eq!(case_key(&case).0, "aml-case:2817341");
    }
}
