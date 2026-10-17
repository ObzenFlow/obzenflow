// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::case_system::CaseRequest;
use super::domain::{
    AccountHistory, AccountKey, AlertRaised, CaseOpened, CaseUnavailable, ProfiledTransaction,
    ScoreUnavailable, Transaction, TransactionCleared,
};
use super::risk_engine::ScoreRequest;
use super::vendors::{OpenCase, ScoreTransaction, CASE_SYSTEM_SOURCE, RISK_ENGINE_SOURCE};
use async_trait::async_trait;
use obzenflow::effects::{EffectError, Effects, StageCompletion};
use obzenflow::error::HandlerError;
use obzenflow::stages::joins::{JoinReferenceView, TypedJoinHandler};
use obzenflow::stages::transforms::EffectfulTransformHandler;

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
                txn_id: alert.txn_id,
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
    use super::*;
    use obzenflow::effects::{EffectFailureCode, EffectFailureSource, RetryDisposition};

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
