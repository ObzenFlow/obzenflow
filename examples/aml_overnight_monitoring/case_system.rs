// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{AccountKey, AlertId, AlertRaised, CaseNumber, Reasons, RulesVersion};
use async_trait::async_trait;
use obzenflow::effects::simulated::{CallLog, CallLogError};
use obzenflow::effects::IdempotencyKey;
use obzenflow::schema::FlowId;
use serde::{Deserialize, Serialize};
use std::path::Path;

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

#[derive(Debug, thiserror::Error)]
#[error("the case system could not record the call: {0}")]
pub struct CaseSystemError(#[from] CallLogError);

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

pub struct SimulatedCaseSystem {
    calls: CallLog<CaseAnswer>,
}

impl SimulatedCaseSystem {
    pub fn open(path: &Path) -> Result<Self, CallLogError> {
        Ok(Self {
            calls: CallLog::open(path)?,
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
        let answer = self.calls.answer(key, flow_id, |decided| CaseAnswer {
            case_number: CaseNumber(decided + 1),
        })?;
        Ok(answer.case_number)
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
        let cases = SimulatedCaseSystem::open(&path).expect("the call log opens");
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
            .expect("the call log is readable")
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is JSON"))
            .collect();
        assert_eq!(
            lines,
            vec![
                serde_json::json!({"result": "decided", "key": "aml-case:txn:5", "flow_id": run.to_string(), "response": {"case_number": 1}}),
                serde_json::json!({"result": "decided", "key": "aml-case:txn:3", "flow_id": run.to_string(), "response": {"case_number": 2}}),
                serde_json::json!({"result": "replayed", "key": "aml-case:txn:5", "flow_id": run.to_string()}),
            ]
        );
    }

    #[tokio::test]
    async fn a_reopened_case_system_keeps_numbers_and_numbers_on_from_the_decided_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cases.jsonl");
        let first_run = FlowId::new();
        {
            let cases = SimulatedCaseSystem::open(&path).expect("the call log opens");
            for txn_id in [11, 12, 13] {
                cases
                    .open(&key(txn_id), first_run, &request(txn_id))
                    .await
                    .expect("case opens");
            }
        }

        let reopened = SimulatedCaseSystem::open(&path).expect("the call log reopens");
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
