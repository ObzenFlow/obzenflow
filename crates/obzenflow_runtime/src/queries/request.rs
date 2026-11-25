// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::QueryContext;
use crate::effects::SystemPortEvidence;
use crate::effects::{
    BoundEffect, Effect, EffectBindingUse, EffectContext, EffectError, EffectPortSlot,
    EffectPortSlotSet, EffectSafety, Named, RecordedReply,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::source::QueryError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::Duration;

/// `FiniteRead` is the recorded reply to one finite read. It holds records and the
/// request of the next read, or the end of the input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FiniteRead<Record, Next> {
    Batch { records: Vec<Record>, next: Next },
    EndOfInput,
}

/// A request that a port's `#[query]` method takes. The port attribute implements
/// it for the request type of each `#[query]` method.
#[doc(hidden)]
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "`query` takes the first request of a port's `#[query]` method; `{Self}` is the request of no `#[query]` method",
    label = "this value is not a query request"
)]
pub trait QueryRequest: Serialize + Clone + Send + Sync + 'static {
    const QUERY_TYPE: &'static str;
    type Port: ?Sized + Send + Sync + 'static;
    type Answer: Clone + Serialize + DeserializeOwned + Send + Sync + 'static;
    async fn query(
        &self,
        port: &Self::Port,
        ctx: &QueryContext,
    ) -> Result<Self::Answer, QueryError>;
}

#[doc(hidden)]
pub trait QueryResult {
    type Answer;
}

impl<T> QueryResult for Result<T, QueryError> {
    type Answer = T;
}

impl From<QueryError> for EffectError {
    fn from(error: QueryError) -> Self {
        let message = error.to_string();
        let cause = match error.retry_after {
            Some(retry_after) => EffectError::RateLimited {
                message,
                retry_after,
            },
            None => EffectError::Execution(message),
        };
        EffectError::SourceRead {
            error: Box::new(cause),
            failure: error.failure,
        }
    }
}

/// The effect that asks one query through the port of its source. The effect
/// call path records it as an idempotent call with a recorded reply, named
/// `query.answered` or `query.failed`.
pub(crate) struct QueryCall<R: QueryRequest> {
    request: R,
    deadline: Option<Duration>,
    projection: EffectBindingUse<QueryCall<R>>,
}

impl<R: QueryRequest> QueryCall<R> {
    pub(crate) fn new(
        request: R,
        deadline: Option<Duration>,
        projection: EffectBindingUse<QueryCall<R>>,
    ) -> Self {
        Self {
            request,
            deadline,
            projection,
        }
    }
}

pub(crate) const fn query_port_slot<R: QueryRequest>() -> EffectPortSlot<R::Port> {
    EffectPortSlot::new("port")
}

impl<R: QueryRequest> Clone for QueryCall<R> {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            deadline: self.deadline,
            projection: self.projection.clone(),
        }
    }
}

impl<R: QueryRequest> std::fmt::Debug for QueryCall<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryCall")
            .field("query_type", &R::QUERY_TYPE)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<R: QueryRequest> Effect for QueryCall<R> {
    const EFFECT_TYPE: &'static str = R::QUERY_TYPE;
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<SystemPortEvidence>;
    type Outcome = R::Answer;
    type OutcomeSemantics = RecordedReply;

    fn label(&self) -> &str {
        R::QUERY_TYPE
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::json!(self.request)
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let port = ctx.port(query_port_slot::<R>())?;
        let context = ctx.query_context();
        let asked = self.request.query(&port, &context);
        let answer = match self.deadline {
            Some(deadline) => match tokio::time::timeout(deadline, asked).await {
                Ok(answer) => answer,
                Err(_) => Err(QueryError::recoverable(
                    SourcePollErrorKind::Timeout,
                    SourceDiagnosticReason::TimedOut,
                )),
            },
            None => asked.await,
        };
        answer.map_err(EffectError::from)
    }

    fn recorded_request(&self) -> Option<serde_json::Value> {
        Some(self.canonical_input())
    }
}

impl<P, R> crate::commands::AnswersEffect<QueryCall<R>> for crate::systems::Port<P>
where
    P: ?Sized + Send + Sync + 'static,
    R: QueryRequest<Port = P>,
{
    fn declaration(
        &self,
    ) -> Result<crate::effects::EffectDeclaration, crate::commands::StageEffectsError> {
        crate::effects::binding::validate_effect_type(R::QUERY_TYPE)
            .map_err(|_| crate::effects::EffectBindingBuildError::InvalidEffectType)?;
        let binding = crate::effects::EffectRegistrationBuilder::<QueryCall<R>>::new(
            self.binding_name(),
            SystemPortEvidence,
        )
        .bind_system(query_port_slot::<R>(), &self.port)?
        .finish()?;
        Ok(
            crate::effects::EffectDeclaration::for_binding::<QueryCall<R>>(
                binding.declaration_binding(),
            ),
        )
    }
}

impl<R: QueryRequest> BoundEffect for QueryCall<R> {
    type BindingEvidence = SystemPortEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.projection
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(query_port_slot::<R>())
    }
}
