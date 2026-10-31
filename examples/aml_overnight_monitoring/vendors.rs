// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::case_system::{CaseRequest, CaseSystem, CaseSystemPort, SimulatedCaseSystem};
use super::domain::{AlertRaised, AlertSubject, TransactionCleared};
use super::risk_engine::{RiskEnginePort, RiskVerdict, ScoreRequest, SimulatedRiskEngine};
use obzenflow::effects::simulated::CallLogError;
use obzenflow::effects::{EffectPortResolutionError, IdempotencyKey, Keyed, System, SystemError};
use obzenflow::schema::EffectOutcomeFacts;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub const RISK_ENGINE_SOURCE: &str = "risk_engine";
pub const CASE_SYSTEM_SOURCE: &str = "case_system";

#[derive(Clone, Debug, EffectOutcomeFacts)]
#[effect_outcome(schema = obzenflow::schema)]
pub enum ScoreOutcome {
    Cleared(TransactionCleared),
    Flagged(AlertRaised),
}

impl ScoreOutcome {
    pub fn from_verdict(request: &ScoreRequest, verdict: RiskVerdict) -> Self {
        match verdict {
            RiskVerdict::Cleared { rules_version } => Self::Cleared(TransactionCleared {
                txn_id: request.txn_id,
                rules_version,
            }),
            RiskVerdict::Flagged {
                reasons,
                rules_version,
            } => Self::Flagged(AlertRaised::new(
                AlertSubject::Transaction(request.txn_id),
                request.payer.clone(),
                reasons,
                rules_version,
            )),
        }
    }
}

impl Keyed for ScoreRequest {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("aml-score:{}", self.txn_id))
    }
}

impl Keyed for CaseRequest {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("aml-case:{}", self.alert_id))
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

pub fn simulated_risk_engine(
    ledger: &Path,
    stats: &Arc<VendorStats>,
) -> Result<System<dyn RiskEnginePort>, SystemError> {
    let ledger = ledger.to_path_buf();
    let stats = stats.clone();
    System::deferred("simulated-risk-engine", move || {
        stats.engines_built.fetch_add(1, Ordering::SeqCst);
        let engine = SimulatedRiskEngine::open(&ledger).map_err(construction_failed)?;
        Ok(Arc::new(engine))
    })
}

pub fn simulated_case_system(
    ledger: &Path,
    case_system: CaseSystem,
    stats: &Arc<VendorStats>,
) -> Result<System<dyn CaseSystemPort>, SystemError> {
    let ledger = ledger.to_path_buf();
    let stats = stats.clone();
    let name = match case_system {
        CaseSystem::Simulated => "simulated-case-system",
        CaseSystem::UnreadableReplies => "case-system-unreadable-replies",
    };
    System::deferred(name, move || {
        stats.case_systems_built.fetch_add(1, Ordering::SeqCst);
        let cases = SimulatedCaseSystem::open(&ledger, case_system).map_err(construction_failed)?;
        Ok(Arc::new(cases))
    })
}

fn construction_failed(error: CallLogError) -> EffectPortResolutionError {
    tracing::error!(%error, "a simulated system could not open its call log");
    EffectPortResolutionError::ClientConstructionFailed
}

#[cfg(test)]
mod tests {
    use super::super::domain::{
        AccountKey, AlertId, PaymentFormat, ReasonCode, Reasons, RulesVersion, TxnId,
    };
    use super::super::money::{Currency, Money};
    use super::*;

    #[test]
    fn keys_are_business_keys_named_by_the_transaction_or_the_alert() {
        let amount = Money::parse(Currency::UsDollar, "120.00").expect("a booked amount");
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
            paid: amount,
            received: amount,
            payment_format: PaymentFormat::Cheque,
            history: None,
        };
        let case = CaseRequest {
            alert_id: AlertId("txn:2817341".to_owned()),
            account: score.payer.clone(),
            reasons: Reasons {
                first: ReasonCode("format_switch".to_owned()),
                rest: Vec::new(),
            },
            rules_version: RulesVersion("sim-2026.10".to_owned()),
        };
        let day_case = CaseRequest {
            alert_id: AlertId("day:2022/09/05:70/8000A1B2C".to_owned()),
            ..case.clone()
        };
        assert_eq!(score.idempotency_key().0, "aml-score:2817341");
        assert_eq!(case.idempotency_key().0, "aml-case:txn:2817341");
        assert_eq!(
            day_case.idempotency_key().0,
            "aml-case:day:2022/09/05:70/8000A1B2C"
        );
    }
}
