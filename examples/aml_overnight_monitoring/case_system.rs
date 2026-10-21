// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{AccountKey, AlertId, AlertRaised, CaseNumber, Reasons, RulesVersion};
use super::ledger::{CallResult, Ledger, LedgerAnswer, LedgerError};
use async_trait::async_trait;
use obzenflow::effects::IdempotencyKey;
use obzenflow::schema::FlowId;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Mutex, PoisonError};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CaseRequest {
    pub alert_id: AlertId,
    pub account: AccountKey,
    pub reasons: Reasons,
    pub rules_version: RulesVersion,
}

impl CaseRequest {
    pub fn for_alert(alert: &AlertRaised) -> Self {
        Self {
            alert_id: alert.alert_id.clone(),
            account: alert.account.clone(),
            reasons: alert.reasons.clone(),
            rules_version: alert.rules_version.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CaseAnswer {
    pub case_number: CaseNumber,
}

impl LedgerAnswer for CaseAnswer {
    const DECIDED: CallResult = CallResult::Opened;
}

#[derive(Debug, thiserror::Error)]
#[error("the case system could not record the call: {0}")]
pub struct CaseSystemError(#[from] std::io::Error);

impl CaseSystemError {
    pub fn code(&self) -> &'static str {
        "ledger_write_failed"
    }
}

#[obzenflow::port]
#[async_trait]
pub trait CaseSystemPort: Send + Sync {
    async fn open(
        &self,
        key: &IdempotencyKey,
        flow_id: FlowId,
        request: &CaseRequest,
    ) -> Result<CaseNumber, CaseSystemError>;
}

struct CaseBook {
    ledger: Ledger<CaseAnswer>,
    last: CaseNumber,
}

pub struct SimulatedCaseSystem {
    book: Mutex<CaseBook>,
}

impl SimulatedCaseSystem {
    pub fn open(ledger: &Path) -> Result<Self, LedgerError> {
        let ledger = Ledger::<CaseAnswer>::open(ledger)?;
        let last = ledger
            .answers()
            .map(|answer| answer.case_number)
            .max()
            .unwrap_or(CaseNumber(0));
        Ok(Self {
            book: Mutex::new(CaseBook { ledger, last }),
        })
    }
}

#[async_trait]
impl CaseSystemPort for SimulatedCaseSystem {
    async fn open(
        &self,
        key: &IdempotencyKey,
        flow_id: FlowId,
        _: &CaseRequest,
    ) -> Result<CaseNumber, CaseSystemError> {
        let mut book = self.book.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(first) = book.ledger.first_answer(&key.0).cloned() {
            book.ledger.record_repeat(&key.0, flow_id)?;
            return Ok(first.case_number);
        }
        let case_number = CaseNumber(book.last.0 + 1);
        book.ledger
            .record_first(&key.0, flow_id, CaseAnswer { case_number })?;
        book.last = case_number;
        Ok(case_number)
    }
}

#[cfg(test)]
mod tests {
    use super::super::domain::ReasonCode;
    use super::*;

    fn request(txn_id: u64) -> CaseRequest {
        CaseRequest {
            alert_id: AlertId(format!("txn:{txn_id}")),
            account: AccountKey {
                bank: "70".to_owned(),
                account: "8000A1B2C".to_owned(),
            },
            reasons: Reasons {
                first: ReasonCode("format_switch".to_owned()),
                rest: Vec::new(),
            },
            rules_version: RulesVersion("sim-2026.10".to_owned()),
        }
    }

    fn key(txn_id: u64) -> IdempotencyKey {
        IdempotencyKey(format!("aml-case:txn:{txn_id}"))
    }

    #[tokio::test]
    async fn cases_are_numbered_in_call_order_and_a_repeat_keeps_its_number() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cases.jsonl");
        let cases = SimulatedCaseSystem::open(&path).expect("ledger opens");
        let run = FlowId::new();
        assert_eq!(
            cases.open(&key(5), run, &request(5)).await.unwrap(),
            CaseNumber(1)
        );
        assert_eq!(
            cases.open(&key(3), run, &request(3)).await.unwrap(),
            CaseNumber(2)
        );
        assert_eq!(
            cases.open(&key(5), run, &request(5)).await.unwrap(),
            CaseNumber(1)
        );

        let lines: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .expect("ledger is readable")
            .lines()
            .map(|line| serde_json::from_str(line).expect("ledger line is JSON"))
            .collect();
        assert_eq!(
            lines,
            vec![
                serde_json::json!({"key": "aml-case:txn:5", "result": "opened", "flow_id": run.to_string(), "case_number": 1}),
                serde_json::json!({"key": "aml-case:txn:3", "result": "opened", "flow_id": run.to_string(), "case_number": 2}),
                serde_json::json!({"key": "aml-case:txn:5", "result": "replayed", "flow_id": run.to_string()}),
            ]
        );
    }

    #[tokio::test]
    async fn a_reopened_case_system_keeps_numbers_and_numbers_on_from_the_highest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cases.jsonl");
        let first_run = FlowId::new();
        {
            let cases = SimulatedCaseSystem::open(&path).expect("ledger opens");
            for txn_id in [11, 12, 13] {
                cases
                    .open(&key(txn_id), first_run, &request(txn_id))
                    .await
                    .expect("case opens");
            }
        }

        let reopened = SimulatedCaseSystem::open(&path).expect("ledger reopens");
        let resumed = FlowId::new();
        assert_eq!(
            reopened
                .open(&key(12), resumed, &request(12))
                .await
                .unwrap(),
            CaseNumber(2)
        );
        assert_eq!(
            reopened
                .open(&key(14), resumed, &request(14))
                .await
                .unwrap(),
            CaseNumber(4)
        );
    }
}
