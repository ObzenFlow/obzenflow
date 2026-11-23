// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite,
    UnifiedAsyncFiniteSourceHandler,
};
use super::prepared::AdmittedSource;
use super::read::FiniteRead;
use super::typed::{configuration_fatal, protocol_fatal};
use super::SourceContinuation;
use crate::effects::{
    AnswersEffect, Call, EffectContext, EffectDeclaration, EffectError, EffectInvocationContext,
    EffectsCore, Port, StageEffectsError, SystemName,
};
use crate::messaging::upstream_subscription::StageInputPosition;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait PlaceholderInput: Send + Sync {
    #[effect(type = "obzenflow.placeholder.read", exactly_once, records = reply)]
    async fn read(
        &self,
        read: &PlaceholderRead,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<serde_json::Value, PlaceholderRead>, EffectError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaceholderRead;

struct Placeholder;

impl SystemName for Placeholder {
    const NAME: &'static str = "placeholder";
}

#[async_trait]
impl PlaceholderInput for Placeholder {
    async fn read(
        &self,
        _read: &PlaceholderRead,
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<serde_json::Value, PlaceholderRead>, EffectError> {
        Ok(FiniteRead::EndOfInput)
    }
}

#[doc(hidden)]
pub struct PlaceholderRows {
    message: Option<&'static str>,
    port: Port<dyn PlaceholderInput>,
    context: Option<EffectInvocationContext>,
}

impl AdmittedSource<PlaceholderRows> {
    pub fn finite_placeholder(message: Option<&'static str>) -> Self {
        let port = Port::to(Placeholder).own_instance();
        Self {
            system: port.source_system(),
            handler: PlaceholderRows {
                message,
                port,
                context: None,
            },
            continuation: SourceContinuation::Refuse,
        }
    }
}

impl SealAsyncFinite for PlaceholderRows {}

#[async_trait]
impl UnifiedAsyncFiniteSourceHandler for PlaceholderRows {
    fn read_declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        <Port<dyn PlaceholderInput> as AnswersEffect<Call<PlaceholderRead>>>::declaration(
            &self.port,
        )
    }

    fn install_read_context(&mut self, context: EffectInvocationContext) {
        self.context = Some(context);
    }

    fn poll_timeout(&self) -> Option<Duration> {
        None
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
        match effects.perform_call(PlaceholderRead).await {
            Ok(_) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof),
            Err(error) => ErasedSourceInvocation::fatal(
                effects
                    .authoring_halt_fatal()
                    .unwrap_or_else(|| protocol_fatal(error.to_string())),
            ),
        }
    }
}
