// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite,
    UnifiedAsyncFiniteSourceHandler,
};
use super::prepared::AdmittedSource;
use super::typed::{configuration_fatal, effect_fatal};
use super::SourceContinuation;
use crate::commands::AnswersEffect;
use crate::effects::{EffectInvocationContext, EffectsCore};
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::queries::{FiniteRead, QueryCall, QueryContext, QueryError};
use crate::systems::Port;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait PlaceholderInput: Send + Sync {
    #[query(type = "obzenflow.placeholder.read")]
    async fn read(
        &self,
        read: &PlaceholderRead,
        ctx: &QueryContext,
    ) -> Result<FiniteRead<serde_json::Value, PlaceholderRead>, QueryError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaceholderRead;

struct Placeholder;

#[async_trait]
impl PlaceholderInput for Placeholder {
    async fn read(
        &self,
        _read: &PlaceholderRead,
        _ctx: &QueryContext,
    ) -> Result<FiniteRead<serde_json::Value, PlaceholderRead>, QueryError> {
        Ok(FiniteRead::EndOfInput)
    }
}

#[doc(hidden)]
pub struct PlaceholderRows {
    message: Option<&'static str>,
    context: Option<EffectInvocationContext>,
}

impl AdmittedSource<PlaceholderRows> {
    pub fn finite_placeholder(message: Option<&'static str>) -> Self {
        let port: Port<dyn PlaceholderInput> = Port::to(Placeholder);
        Self {
            declaration: <Port<dyn PlaceholderInput> as AnswersEffect<
                QueryCall<PlaceholderRead>,
            >>::declaration(&port)
            .map(Some),
            handler: PlaceholderRows {
                message,
                context: None,
            },
            continuation: SourceContinuation::Refuse,
        }
    }
}

impl SealAsyncFinite for PlaceholderRows {}

#[async_trait]
impl UnifiedAsyncFiniteSourceHandler for PlaceholderRows {
    fn install_read_context(&mut self, context: EffectInvocationContext) {
        self.context = Some(context);
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(mut context) = self.context.clone() else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "placeholder read before the runtime installed its stage context",
            ));
        };
        match self.message {
            Some(message) => tracing::warn!("the source placeholder ran: {message}"),
            None => tracing::warn!("the source placeholder ran"),
        }
        context.input_seq = StageInputPosition(1);
        let mut effects = EffectsCore::new(context);
        match effects.query(PlaceholderRead, None).await {
            Ok(_) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof),
            Err(error) => ErasedSourceInvocation::fatal(effect_fatal(&effects, &error)),
        }
    }
}
