// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::{checked_system_name, BuiltSystem, EffectError, LocatedSystem, PortImpl, ServesPort, SourceSystem, SystemName};
use obzenflow_core::effect_binding::{SystemBinding, SystemLocation};
use obzenflow_core::event::observability::{ObservationRecord, ObservationRecorder};
pub use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
pub use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::{BindingEvidenceError, SystemBindingEvidence};
pub use obzenflow_core::query::{QueryBookmark, QueryError};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

/// The read position and measurement destination for a live query.
pub struct QueryContext {
    pub(crate) stage_key: String,
    pub(crate) number: u64,
    pub(crate) recorder: Option<Arc<dyn ObservationRecorder>>,
}

impl QueryContext {
    pub fn stage_key(&self) -> &str { &self.stage_key }
    pub fn read_number(&self) -> u64 { self.number }
    pub fn observe(&self, record: ObservationRecord) {
        if let Some(recorder) = &self.recorder { recorder.observe(record); }
    }
    pub fn for_test(stage_key: &str, number: u64) -> Self {
        Self { stage_key: stage_key.to_owned(), number, recorder: None }
    }
}

/// A configuration that can describe its supplier without opening it.
pub trait QueryBinding: Send + Sync + 'static {
    type System: SystemName + Send + Sync + 'static;
    type Evidence: Serialize;
    fn evidence(&self) -> Self::Evidence;
    fn resolve(&self) -> Result<Self::System, QueryError>;
    #[doc(hidden)]
    fn location(&self) -> Result<SystemLocation, BindingEvidenceError> {
        let configuration = serde_json::to_value(self.evidence()).map_err(|_| BindingEvidenceError::InvalidLocation)?;
        Ok(SystemLocation::Application { configuration })
    }
}

impl<B: LocatedSystem> QueryBinding for B {
    type System = B::System;
    type Evidence = SystemLocation;
    fn evidence(&self) -> SystemLocation { LocatedSystem::location(self) }
    fn location(&self) -> Result<SystemLocation, BindingEvidenceError> { Ok(self.evidence()) }
    fn resolve(&self) -> Result<Self::System, QueryError> {
        self.build().map_err(|_| QueryError::terminal(SourcePollErrorKind::Transport, SourceDiagnosticReason::InputUnavailable))
    }
}

/// The selected supplier of a typed query port.
pub struct QueryPort<P: ?Sized + Send + Sync + 'static> {
    pub(crate) system: SourceSystem,
    marker: PhantomData<fn() -> Arc<P>>,
}

impl<P: ?Sized + Send + Sync + 'static> Clone for QueryPort<P> {
    fn clone(&self) -> Self { Self { system: self.system.clone(), marker: PhantomData } }
}

impl<P: ?Sized + Send + Sync + 'static> QueryPort<P> {
    pub fn to<S: BuiltSystem>(system: S) -> Self
    where S::System: Send + Sync + 'static, PortImpl<S::System>: ServesPort<P, S::System> {
        let system = <PortImpl<S::System> as ServesPort<P,S::System>>::coerce(system.shared());
        let evidence = SystemBindingEvidence::named(checked_system_name::<S::System>()).expect("valid system name");
        Self::bind(evidence, move || Ok(system.clone()))
    }

    pub fn deferred<S, F>(resolve: F) -> Self
    where S: SystemName + Send + Sync + 'static, PortImpl<S>: ServesPort<P, S>,
          F: Fn() -> Result<S,QueryError> + Send + Sync + 'static {
        let evidence = SystemBindingEvidence::named(checked_system_name::<S>()).expect("valid system name");
        Self::bind(evidence, move || resolve().map(|s| PortImpl::<S>::coerce(Arc::new(s))))
    }

    pub fn located<B: QueryBinding>(config: B) -> Result<Self, BindingEvidenceError>
    where PortImpl<B::System>: ServesPort<P, B::System> {
        let evidence = SystemBindingEvidence::located(checked_system_name::<B::System>(), config.location()?)?;
        Ok(Self::bind(evidence, move || config.resolve().map(|s| PortImpl::<B::System>::coerce(Arc::new(s)))))
    }

    fn bind(evidence: SystemBindingEvidence, resolve: impl Fn() -> Result<Arc<P>,QueryError> + Send + Sync + 'static) -> Self {
        Self { system: SourceSystem::query(evidence,resolve), marker: PhantomData }
    }

    pub fn binding(&self) -> &SystemBinding { self.system.binding() }
    pub fn system_name(&self) -> &str { self.system.name() }
}

#[doc(hidden)]
#[async_trait::async_trait]
pub trait QueryRequest: Serialize + Clone + Send + Sync + 'static {
    const QUERY_TYPE: &'static str;
    type Port: ?Sized + Send + Sync + 'static;
    type Answer: Serialize + DeserializeOwned + Send + Sync + 'static;
    async fn query(&self, port: &Self::Port, ctx: &QueryContext) -> Result<Self::Answer, QueryError>;
}

#[doc(hidden)]
pub trait QueryResult { type Answer; }
impl<T> QueryResult for Result<T, QueryError> { type Answer=T; }

impl From<QueryError> for EffectError {
    fn from(error: QueryError) -> Self {
        let message = error.to_string();
        let cause = match error.retry_after {
            Some(retry_after) => EffectError::RateLimited { message, retry_after },
            None => EffectError::Execution(message),
        };
        EffectError::SourceRead { error: Box::new(cause), failure: Box::new(error.failure) }
    }
}

pub(crate) async fn ask<R: QueryRequest>(
    stage: &mut crate::effects::EffectsCore, port: &QueryPort<R::Port>, request: &R,
) -> Result<R::Answer, EffectError> {
    use obzenflow_core::query::SourceInput;
    let argument = serde_json::to_value(request).map_err(|e| EffectError::Serialization(e.to_string()))?;
    let registry = stage.source_registry().clone();
    let context = stage.query_context();
    let record = stage.source_input(|input| match input {
        SourceInput::QueryAnswered { query_type, request, .. } | SourceInput::QueryFailed { query_type, request, .. } =>
            query_type == R::QUERY_TYPE && request == &argument,
        SourceInput::IngressMember { .. } => false,
    }, || async {
        let result = match port.system.resolve_query::<R::Port>(&registry)? {
            Ok(system) => request.query(&system, &context).await,
            Err(error) => Err(error),
        };
        let query_type = R::QUERY_TYPE.to_owned();
        let request_value = argument.clone();
        Ok(match result {
            Ok(answer) => SourceInput::QueryAnswered { query_type, request: request_value,
                answer: serde_json::to_value(answer).map_err(|e| EffectError::Serialization(e.to_string()))? },
            Err(error) => SourceInput::QueryFailed { query_type, request: request_value, error },
        })
    }).await?;
    match record {
        SourceInput::QueryAnswered { answer, .. } => serde_json::from_value(answer).map_err(|e| EffectError::Serialization(e.to_string())),
        SourceInput::QueryFailed { error, .. } => Err(error.into()),
        SourceInput::IngressMember { .. } => unreachable!("query received ingress input"),
    }
}

impl From<crate::effects::EffectPortResolutionError> for QueryError {
    fn from(_:crate::effects::EffectPortResolutionError)->Self { Self::terminal(SourcePollErrorKind::Transport,SourceDiagnosticReason::InputUnavailable) }
}
