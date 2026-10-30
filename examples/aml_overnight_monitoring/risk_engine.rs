// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{
    AccountHistory, AccountKey, PaymentFormat, ProfiledTransaction, ReasonCode, Reasons,
    RulesVersion, TxnId, AMLWORLD_TIME,
};
use super::money::{Currency, Money, UsdCents, UsdPerUnit};
use super::vendors::{ScoreOutcome, RISK_ENGINE_SOURCE};
use async_trait::async_trait;
use obzenflow::effects::simulated::{CallLog, CallLogError};
use obzenflow::effects::{EffectContext, EffectError, IdempotencyKey};
use obzenflow::schema::FlowId;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use std::path::Path;

pub const RULES_VERSION: &str = "sim-2026.10";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScoreRequest {
    pub txn_id: TxnId,
    pub timestamp: String,
    pub payer: AccountKey,
    pub payee: AccountKey,
    pub paid: Money,
    pub received: Money,
    pub payment_format: PaymentFormat,
    pub history: Option<AccountHistory>,
}

impl ScoreRequest {
    pub fn for_transaction(profiled: &ProfiledTransaction) -> Self {
        let transaction = &profiled.transaction;
        Self {
            txn_id: transaction.txn_id,
            timestamp: transaction.timestamp.format(AMLWORLD_TIME).to_string(),
            payer: transaction.payer(),
            payee: transaction.payee(),
            paid: transaction.paid,
            received: transaction.received,
            payment_format: transaction.payment_format,
            history: profiled.history.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum RiskVerdict {
    Cleared {
        rules_version: RulesVersion,
    },
    Flagged {
        reasons: Reasons,
        rules_version: RulesVersion,
    },
}

#[obzenflow::port]
#[async_trait]
pub trait RiskEnginePort: Send + Sync {
    #[effect(type = "aml.score_transaction", exactly_once)]
    async fn score(
        &self,
        request: &ScoreRequest,
        key: &IdempotencyKey,
        ctx: &EffectContext,
    ) -> Result<ScoreOutcome, EffectError>;
}

pub struct RiskEngineOutage;

#[async_trait]
impl RiskEnginePort for RiskEngineOutage {
    async fn score(
        &self,
        _: &ScoreRequest,
        _: &IdempotencyKey,
        _: &EffectContext,
    ) -> Result<ScoreOutcome, EffectError> {
        Err(EffectError::unavailable(
            RISK_ENGINE_SOURCE,
            "unavailable",
            "the risk engine is unavailable",
        ))
    }
}

pub struct SimulatedRiskEngine {
    calls: CallLog<RiskVerdict>,
}

impl SimulatedRiskEngine {
    pub fn open(path: &Path) -> Result<Self, CallLogError> {
        Ok(Self {
            calls: CallLog::open(path)?,
        })
    }

    fn answer(
        &self,
        request: &ScoreRequest,
        key: &IdempotencyKey,
        flow_id: FlowId,
    ) -> Result<RiskVerdict, CallLogError> {
        self.calls.answer(key, flow_id, |_| decide(request))
    }
}

#[async_trait]
impl RiskEnginePort for SimulatedRiskEngine {
    async fn score(
        &self,
        request: &ScoreRequest,
        key: &IdempotencyKey,
        ctx: &EffectContext,
    ) -> Result<ScoreOutcome, EffectError> {
        let verdict = self.answer(request, key, ctx.flow_id()).map_err(|error| {
            EffectError::unavailable(RISK_ENGINE_SOURCE, "ledger_write_failed", error)
        })?;
        Ok(ScoreOutcome::from_verdict(request, verdict))
    }
}

struct Rule {
    code: &'static str,
    fires: fn(&ScoreRequest, Option<UsdCents>) -> bool,
}

const TEN_THOUSAND: UsdCents = UsdCents::whole_dollars(10_000);
const TWENTY_THOUSAND: UsdCents = UsdCents::whole_dollars(20_000);
const FIFTY_THOUSAND: UsdCents = UsdCents::whole_dollars(50_000);

const RULES: [Rule; 4] = [
    Rule {
        code: "first_payment_large_cash",
        fires: first_payment_large_cash,
    },
    Rule {
        code: "format_switch",
        fires: format_switch,
    },
    Rule {
        code: "fan_out_large_payment",
        fires: fan_out_large_payment,
    },
    Rule {
        code: "thin_history_large_transfer",
        fires: thin_history_large_transfer,
    },
];

pub fn decide(request: &ScoreRequest) -> RiskVerdict {
    let usd = request
        .paid
        .to_usd_half_up(engine_rate(request.paid.currency()))
        .ok();
    let fired = RULES
        .iter()
        .filter(|rule| (rule.fires)(request, usd))
        .map(|rule| ReasonCode(rule.code.to_owned()))
        .collect();
    let rules_version = RulesVersion(RULES_VERSION.to_owned());
    match Reasons::from_fired(fired) {
        Some(reasons) => RiskVerdict::Flagged {
            reasons,
            rules_version,
        },
        None => RiskVerdict::Cleared { rules_version },
    }
}

const fn quoted(mantissa: u64, decimals: u8) -> UsdPerUnit {
    match NonZeroU64::new(mantissa) {
        Some(mantissa) => UsdPerUnit::quoted(mantissa, decimals),
        None => panic!("the engine quotes every rate above zero"),
    }
}

fn engine_rate(currency: Currency) -> UsdPerUnit {
    match currency {
        Currency::UsDollar | Currency::Euro => const { quoted(1, 0) },
        Currency::Yuan => const { quoted(14, 2) },
        Currency::Yen => const { quoted(71, 4) },
        Currency::Rupee => const { quoted(12, 3) },
        Currency::Bitcoin => const { quoted(19_500, 0) },
        Currency::SwissFranc => const { quoted(102, 2) },
        Currency::UkPound => const { quoted(115, 2) },
        Currency::Ruble => const { quoted(17, 3) },
        Currency::CanadianDollar => const { quoted(75, 2) },
        Currency::AustralianDollar => const { quoted(67, 2) },
        Currency::Shekel => const { quoted(29, 2) },
        Currency::BrazilReal => const { quoted(19, 2) },
        Currency::MexicanPeso => const { quoted(5, 2) },
        Currency::SaudiRiyal => const { quoted(27, 2) },
    }
}

fn at_least(usd: Option<UsdCents>, threshold: UsdCents) -> bool {
    usd.is_none_or(|usd| usd >= threshold)
}

fn first_payment_large_cash(request: &ScoreRequest, usd: Option<UsdCents>) -> bool {
    request.history.is_none()
        && request.payment_format == PaymentFormat::Cash
        && at_least(usd, TEN_THOUSAND)
}

fn format_switch(request: &ScoreRequest, usd: Option<UsdCents>) -> bool {
    request
        .history
        .as_ref()
        .is_some_and(|history| !history.formats.contains(&request.payment_format))
        && at_least(usd, TWENTY_THOUSAND)
}

fn fan_out_large_payment(request: &ScoreRequest, usd: Option<UsdCents>) -> bool {
    request.history.as_ref().is_some_and(|history| {
        history.counterparties >= 20 && history.counterparties * 10 >= history.payments * 9
    }) && at_least(usd, TEN_THOUSAND)
}

fn thin_history_large_transfer(request: &ScoreRequest, usd: Option<UsdCents>) -> bool {
    request
        .history
        .as_ref()
        .is_some_and(|history| history.payments <= 2)
        && matches!(
            request.payment_format,
            PaymentFormat::Wire | PaymentFormat::Bitcoin
        )
        && at_least(usd, FIFTY_THOUSAND)
}

#[cfg(test)]
mod tests {
    use super::super::domain::Transaction;
    use super::*;
    use obzenflow::effects::Keyed;

    fn transaction(
        txn_id: u64,
        format: PaymentFormat,
        currency: Currency,
        amount: &str,
    ) -> Transaction {
        let amount = Money::parse(currency, amount).expect("a booked amount");
        Transaction {
            txn_id: TxnId(txn_id),
            timestamp: chrono::NaiveDateTime::parse_from_str("2022/09/05 10:15", AMLWORLD_TIME)
                .expect("an AMLworld timestamp"),
            from_bank: "70".to_owned(),
            from_account: "8000A1B2C".to_owned(),
            to_bank: "12".to_owned(),
            to_account: "8000D3E4F".to_owned(),
            received: amount,
            paid: amount,
            payment_format: format,
            is_laundering: true,
        }
    }

    fn history(payments: u64, counterparties: u64, formats: &[PaymentFormat]) -> AccountHistory {
        AccountHistory {
            bank: "70".to_owned(),
            account: "8000A1B2C".to_owned(),
            payments,
            counterparties,
            formats: formats.to_vec(),
        }
    }

    fn request(transaction: Transaction, history: Option<AccountHistory>) -> ScoreRequest {
        ScoreRequest::for_transaction(&ProfiledTransaction {
            transaction,
            history,
        })
    }

    fn reasons(verdict: &RiskVerdict) -> Vec<&str> {
        match verdict {
            RiskVerdict::Flagged { reasons, .. } => {
                reasons.iter().map(|reason| reason.0.as_str()).collect()
            }
            RiskVerdict::Cleared { .. } => Vec::new(),
        }
    }

    fn log_lines(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .expect("the call log is readable")
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is JSON"))
            .collect()
    }

    #[test]
    fn the_request_never_carries_the_laundering_label() {
        let request = request(
            transaction(1, PaymentFormat::Cheque, Currency::UsDollar, "120.00"),
            None,
        );
        let json = serde_json::to_string(&request).expect("request serializes");
        assert!(!json.contains("laundering"), "{json}");
    }

    #[test]
    fn each_rule_and_both_verdicts() {
        let cases = [
            (
                request(
                    transaction(1, PaymentFormat::Cash, Currency::UsDollar, "15000.00"),
                    None,
                ),
                vec!["first_payment_large_cash"],
            ),
            (
                request(
                    transaction(2, PaymentFormat::Cash, Currency::UsDollar, "9999.00"),
                    None,
                ),
                vec![],
            ),
            (
                request(
                    transaction(3, PaymentFormat::Cash, Currency::Yen, "2000000.00"),
                    Some(history(4, 2, &[PaymentFormat::Cash])),
                ),
                vec![],
            ),
            (
                request(
                    transaction(4, PaymentFormat::CreditCard, Currency::UsDollar, "25000.00"),
                    Some(history(30, 6, &[PaymentFormat::Ach, PaymentFormat::Cheque])),
                ),
                vec!["format_switch"],
            ),
            (
                request(
                    transaction(5, PaymentFormat::Ach, Currency::UsDollar, "12000.00"),
                    Some(history(40, 38, &[PaymentFormat::Ach])),
                ),
                vec!["fan_out_large_payment"],
            ),
            (
                request(
                    transaction(6, PaymentFormat::Wire, Currency::UsDollar, "80000.00"),
                    Some(history(2, 2, &[PaymentFormat::Wire])),
                ),
                vec!["thin_history_large_transfer"],
            ),
        ];
        for (request, expected) in cases {
            let verdict = decide(&request);
            assert_eq!(reasons(&verdict), expected, "txn {}", request.txn_id);
            let rules_version = match &verdict {
                RiskVerdict::Cleared { rules_version }
                | RiskVerdict::Flagged { rules_version, .. } => rules_version,
            };
            assert_eq!(rules_version.0, RULES_VERSION);
        }
    }

    #[test]
    fn a_flag_lists_every_rule_that_fired_in_the_engine_order() {
        let verdict = decide(&request(
            transaction(7, PaymentFormat::Wire, Currency::UsDollar, "60000.00"),
            Some(history(1, 1, &[PaymentFormat::Cheque])),
        ));
        assert_eq!(
            reasons(&verdict),
            vec!["format_switch", "thin_history_large_transfer"]
        );
    }

    #[test]
    fn a_repeated_key_is_answered_replayed_with_the_first_answer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("risk.jsonl");
        let flagged = request(
            transaction(8, PaymentFormat::Wire, Currency::UsDollar, "60000.00"),
            Some(history(1, 1, &[PaymentFormat::Cheque])),
        );
        let engine = SimulatedRiskEngine::open(&path).expect("the call log opens");
        let first_run = FlowId::new();
        let key = flagged.idempotency_key();
        let first = engine
            .answer(&flagged, &key, first_run)
            .expect("first call");
        let repeat = engine
            .answer(&flagged, &key, first_run)
            .expect("repeated call");
        assert_eq!(repeat, first);

        assert_eq!(
            log_lines(&path),
            vec![
                serde_json::json!({
                    "result": "decided",
                    "key": "aml-score:8",
                    "flow_id": first_run.to_string(),
                    "response": {
                        "verdict": "flagged",
                        "reasons": ["format_switch", "thin_history_large_transfer"],
                        "rules_version": RULES_VERSION,
                    },
                }),
                serde_json::json!({
                    "result": "replayed",
                    "key": "aml-score:8",
                    "flow_id": first_run.to_string(),
                }),
            ]
        );
    }
}
