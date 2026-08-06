// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::EffectInvocationContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::UnifiedTransformHandler;

struct DirectUnifiedTransform;

#[async_trait]
impl UnifiedTransformHandler for DirectUnifiedTransform {
    async fn process(
        &self,
        _event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        _scope: MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {}
