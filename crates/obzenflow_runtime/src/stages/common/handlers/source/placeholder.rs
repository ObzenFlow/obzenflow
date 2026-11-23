// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite,
    UnifiedAsyncFiniteSourceHandler,
};
use super::prepared::AdmittedSource;
use super::read::FiniteRead;
use super::traits::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use super::typed::{configuration_fatal, protocol_fatal};
use super::{RefusedBy, SourceContinuation, SourceError};
use crate::effects::{
    AnswersEffect, Call, EffectContext, EffectDeclaration, EffectError, EffectInvocationContext,
    EffectsCore, Port, SourceSystem, StageEffectsError, SystemName,
};
use crate::messaging::upstream_subscription::StageInputPosition;
use async_trait::async_trait;
use obzenflow_core::{ChainEvent, WriterId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// `PlaceholderInput` is the port of the built-in `placeholder` system. Each
/// read ends the input.
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

/// The argument of the placeholder's one read.
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

/// The finite placeholder handler. It reads its input once from the
/// `placeholder` system, which ends the input.
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
            continuation: SourceContinuation::Refuse {
                by: RefusedBy::Handler,
            },
        }
    }
}

impl SealAsyncFinite for PlaceholderRows {}

#[async_trait]
impl UnifiedAsyncFiniteSourceHandler for PlaceholderRows {
    fn read_declaration(&self) -> Option<Result<EffectDeclaration, StageEffectsError>> {
        Some(<Port<dyn PlaceholderInput> as AnswersEffect<
            Call<PlaceholderRead>,
        >>::declaration(&self.port))
    }

    fn install_read_context(&mut self, context: EffectInvocationContext) {
        self.context = Some(context);
    }

    fn install_writer_id(&mut self, _writer_id: WriterId) {}

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
            Ok(_) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof, Vec::new()),
            Err(error) => ErasedSourceInvocation::fatal(
                effects
                    .authoring_halt_fatal()
                    .unwrap_or_else(|| protocol_fatal(error.to_string())),
            ),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[doc(hidden)]
pub struct PlaceholderSource {
    message: Option<&'static str>,
    warned: bool,
}

impl PlaceholderSource {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            message,
            warned: false,
        }
    }

    fn warn_once(&mut self, kind: &str) {
        if !self.warned {
            self.warned = true;
            match self.message {
                Some(message) => tracing::warn!("the {kind} placeholder ran: {message}"),
                None => tracing::warn!("the {kind} placeholder ran"),
            }
        }
    }
}

impl AdmittedSource<PlaceholderSource> {
    pub fn placeholder(message: Option<&'static str>) -> Self {
        Self {
            handler: PlaceholderSource::new(message),
            continuation: SourceContinuation::Refuse {
                by: RefusedBy::Handler,
            },
            system: SourceSystem::placeholder(),
        }
    }
}

impl FiniteSourceHandler for PlaceholderSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.warn_once("finite source");
        Ok(None)
    }
}

impl InfiniteSourceHandler for PlaceholderSource {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        self.warn_once("infinite source");
        Ok(Vec::new())
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for PlaceholderSource {
    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.warn_once("async source");
        Ok(None)
    }
}

#[async_trait]
impl AsyncInfiniteSourceHandler for PlaceholderSource {
    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        self.warn_once("async infinite source");
        Ok(Vec::new())
    }
}
