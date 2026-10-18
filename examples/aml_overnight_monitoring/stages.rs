// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::case_system::CaseRequest;
use super::domain::{
    AccountHistory, AccountKey, AlertRaised, AlertSubject, CaseOpened, CaseUnavailable, Day,
    ProfiledTransaction, ReasonCode, Reasons, RulesVersion, ScoreUnavailable, Transaction,
    TransactionCleared,
};
use super::risk_engine::ScoreRequest;
use super::vendors::{OpenCase, ScoreTransaction, CASE_SYSTEM_SOURCE, RISK_ENGINE_SOURCE};
use async_trait::async_trait;
use obzenflow::effects::{EffectError, Effects, StageCompletion};
use obzenflow::error::HandlerError;
use obzenflow::stages::joins::{JoinReferenceView, TypedJoinHandler};
use obzenflow::stages::stateful::{StatefulEmission, TypedStatefulHandler};
use obzenflow::stages::transforms::EffectfulTransformHandler;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

pub const DAILY_PROFILE_VERSION: &str = "aml-daily-profile-v1";

#[derive(Clone, Debug)]
pub struct AccountProfile;

impl TypedJoinHandler for AccountProfile {
    type State = ();
    type ReferenceKey = AccountKey;
    type Reference = AccountHistory;
    type Stream = Transaction;
    type Output = ProfiledTransaction;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, history: &AccountHistory) -> Result<AccountKey, HandlerError> {
        Ok(history.key())
    }

    fn process_stream(
        &self,
        _: &mut Self::State,
        references: &mut JoinReferenceView<'_, AccountKey, AccountHistory>,
        transaction: Transaction,
    ) -> Result<Vec<ProfiledTransaction>, HandlerError> {
        let history = references.select(&transaction.payer());
        Ok(vec![ProfiledTransaction {
            transaction,
            history,
        }])
    }
}

#[derive(Clone, Debug, Default)]
pub struct Counterparties {
    paid: BTreeSet<AccountKey>,
    paid_by: BTreeSet<AccountKey>,
}

#[derive(Clone, Debug)]
pub struct DailyProfile {
    fan_out_counterparties: NonZeroUsize,
    fan_in_counterparties: NonZeroUsize,
}

impl DailyProfile {
    pub fn new(fan_out_counterparties: NonZeroUsize, fan_in_counterparties: NonZeroUsize) -> Self {
        Self {
            fan_out_counterparties,
            fan_in_counterparties,
        }
    }

    fn alerts(&self, accounts: &BTreeMap<(Day, AccountKey), Counterparties>) -> Vec<AlertRaised> {
        accounts
            .iter()
            .filter_map(|((day, account), counterparties)| {
                let mut fired = Vec::new();
                if counterparties.paid.len() >= self.fan_out_counterparties.get() {
                    fired.push(ReasonCode("fan_out".to_owned()));
                }
                if counterparties.paid_by.len() >= self.fan_in_counterparties.get() {
                    fired.push(ReasonCode("fan_in".to_owned()));
                }
                Reasons::from_fired(fired).map(|reasons| {
                    AlertRaised::new(
                        AlertSubject::Day(day.clone()),
                        account.clone(),
                        reasons,
                        RulesVersion(DAILY_PROFILE_VERSION.to_owned()),
                    )
                })
            })
            .collect()
    }
}

impl TypedStatefulHandler for DailyProfile {
    type State = BTreeMap<(Day, AccountKey), Counterparties>;
    type Input = Transaction;
    type Output = AlertRaised;

    fn initial_state(&self) -> Self::State {
        BTreeMap::new()
    }

    fn accumulate(&self, accounts: &mut Self::State, transaction: Transaction) {
        let day = transaction.day();
        accounts
            .entry((day.clone(), transaction.payer()))
            .or_default()
            .paid
            .insert(transaction.payee());
        accounts
            .entry((day, transaction.payee()))
            .or_default()
            .paid_by
            .insert(transaction.payer());
    }

    fn emit(
        &self,
        accounts: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: accounts.clone(),
            outputs: self.alerts(accounts),
        })
    }
}

pub type ScoreOutput =
    obzenflow::schema::stage_fact_set![TransactionCleared, AlertRaised, ScoreUnavailable];
pub type ScoreEffects = obzenflow::effects::effect_set![ScoreTransaction];

#[derive(Clone, Debug)]
pub struct ScoreHandler;

#[async_trait]
impl EffectfulTransformHandler for ScoreHandler {
    type Input = ProfiledTransaction;
    type Output = ScoreOutput;
    type AllowedEffects = ScoreEffects;

    async fn process(
        &self,
        profiled: ProfiledTransaction,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let binding = fx.project_named_effect::<ScoreTransaction, _>()?;
        let request = ScoreRequest::for_transaction(&profiled);
        if let Err(error) = fx.perform(ScoreTransaction::new(request, binding)).await {
            let Some(reason) = unavailable_reason(&error, RISK_ENGINE_SOURCE) else {
                return Err(error.into());
            };
            fx.emit(ScoreUnavailable {
                txn_id: profiled.transaction.txn_id,
                account: profiled.transaction.payer(),
                reason,
            })
            .await?;
        }
        Ok(fx.complete()?)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "aml-score-v1"
    }
}

pub type OpenCaseOutput = obzenflow::schema::stage_fact_set![CaseOpened, CaseUnavailable];
pub type OpenCaseEffects = obzenflow::effects::effect_set![OpenCase];

#[derive(Clone, Debug)]
pub struct OpenCaseHandler;

#[async_trait]
impl EffectfulTransformHandler for OpenCaseHandler {
    type Input = AlertRaised;
    type Output = OpenCaseOutput;
    type AllowedEffects = OpenCaseEffects;

    async fn process(
        &self,
        alert: AlertRaised,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let binding = fx.project_named_effect::<OpenCase, _>()?;
        if let Err(error) = fx
            .perform(OpenCase::new(CaseRequest::for_alert(&alert), binding))
            .await
        {
            let Some(reason) = unavailable_reason(&error, CASE_SYSTEM_SOURCE) else {
                return Err(error.into());
            };
            fx.emit(CaseUnavailable {
                alert_id: alert.alert_id,
                account: alert.account,
                reason,
            })
            .await?;
        }
        Ok(fx.complete()?)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "aml-open-case-v1"
    }
}

fn unavailable_reason(error: &EffectError, system: &str) -> Option<String> {
    error
        .failure_cause()
        .filter(|cause| cause.source.as_str() == system)
        .map(|_| error.semantic_reason().into_owned())
}

#[cfg(test)]
mod tests {
    use super::super::domain::{AlertId, PaymentFormat, TxnId};
    use super::*;
    use obzenflow::effects::{EffectFailureCode, EffectFailureSource, RetryDisposition};

    fn payment(txn_id: u64, timestamp: &str, from: &str, to: &str) -> Transaction {
        Transaction {
            txn_id: TxnId(txn_id),
            timestamp: timestamp.to_owned(),
            from_bank: "48".to_owned(),
            from_account: from.to_owned(),
            to_bank: "48".to_owned(),
            to_account: to.to_owned(),
            amount_received: 250.0,
            receiving_currency: "US Dollar".to_owned(),
            amount_paid: 250.0,
            payment_currency: "US Dollar".to_owned(),
            payment_format: PaymentFormat::Ach,
            is_laundering: false,
        }
    }

    fn three() -> NonZeroUsize {
        NonZeroUsize::new(3).expect("three is not zero")
    }

    fn alerted(alerts: &[AlertRaised]) -> Vec<(&str, Vec<&str>)> {
        alerts
            .iter()
            .map(|alert| {
                (
                    alert.alert_id.0.as_str(),
                    alert
                        .reasons
                        .iter()
                        .map(|reason| reason.0.as_str())
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn whole_day_typologies_fire_at_and_above_their_thresholds_only() {
        let profile = DailyProfile::new(three(), three());
        let mut accounts = profile.initial_state();
        let day = "2022/09/05 12:00";
        let payments = [
            ("out-at", "p-1"),
            ("out-at", "p-2"),
            ("out-at", "p-3"),
            ("out-below", "p-4"),
            ("out-below", "p-5"),
            ("out-below", "p-5"),
            ("q-1", "in-above"),
            ("q-2", "in-above"),
            ("q-3", "in-above"),
            ("q-4", "in-above"),
            ("q-5", "in-below"),
            ("q-6", "in-below"),
            ("both", "p-6"),
            ("both", "p-7"),
            ("both", "p-8"),
            ("q-7", "both"),
            ("q-8", "both"),
            ("q-9", "both"),
        ];
        for (n, (from, to)) in payments.into_iter().enumerate() {
            profile.accumulate(&mut accounts, payment(n as u64, day, from, to));
        }

        let alerts = profile.drain(&accounts).expect("the profile drains");
        assert_eq!(
            alerted(&alerts),
            [
                ("day:2022/09/05:48/both", vec!["fan_out", "fan_in"]),
                ("day:2022/09/05:48/in-above", vec!["fan_in"]),
                ("day:2022/09/05:48/out-at", vec!["fan_out"]),
            ]
        );
        for alert in &alerts {
            assert_eq!(
                alert.subject,
                AlertSubject::Day(Day("2022/09/05".to_owned()))
            );
            assert_eq!(alert.rules_version.0, DAILY_PROFILE_VERSION);
        }
    }

    #[test]
    fn counterparties_are_counted_per_day() {
        let profile = DailyProfile::new(three(), three());
        let mut accounts = profile.initial_state();
        for (n, (timestamp, to)) in [
            ("2022/09/04 23:59", "p-1"),
            ("2022/09/05 00:01", "p-2"),
            ("2022/09/05 00:02", "p-3"),
        ]
        .into_iter()
        .enumerate()
        {
            profile.accumulate(&mut accounts, payment(n as u64, timestamp, "split", to));
        }
        assert!(profile
            .drain(&accounts)
            .expect("the profile drains")
            .is_empty());

        profile.accumulate(
            &mut accounts,
            payment(9, "2022/09/05 00:03", "split", "p-4"),
        );
        let alerts = profile.drain(&accounts).expect("the profile drains");
        assert_eq!(
            alerts
                .iter()
                .map(|alert| &alert.alert_id)
                .collect::<Vec<_>>(),
            [&AlertId("day:2022/09/05:48/split".to_owned())]
        );
    }

    fn refused(system: &str) -> EffectError {
        EffectError::DependencyFailed {
            failure_source: EffectFailureSource::new(system),
            code: EffectFailureCode::new("ledger_write_failed"),
            message: format!("the {system} could not record the call"),
            retry: RetryDisposition::Retryable,
        }
    }

    #[test]
    fn each_system_refusal_maps_to_its_unavailable_fact_live_and_recorded() {
        for system in [RISK_ENGINE_SOURCE, CASE_SYSTEM_SOURCE] {
            let live = refused(system);
            let live_reason = unavailable_reason(&live, system).expect("refusal is unavailable");
            let recorded = EffectError::RecordedFailure {
                error_type: "dependency_failed".into(),
                error_message: live_reason.clone(),
                retry: RetryDisposition::Retryable,
                cause: live.failure_cause(),
                detail: None,
            };
            assert_eq!(unavailable_reason(&recorded, system), Some(live_reason));
        }
    }

    #[test]
    fn other_failures_are_not_unavailable_outcomes() {
        assert_eq!(
            unavailable_reason(&refused(CASE_SYSTEM_SOURCE), RISK_ENGINE_SOURCE),
            None
        );
        assert_eq!(
            unavailable_reason(
                &EffectError::Journal("disk full".to_owned()),
                RISK_ENGINE_SOURCE
            ),
            None
        );
    }
}
