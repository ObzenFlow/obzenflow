// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{EffectContext, EffectError, Port};
use obzenflow_runtime::stages::common::handlers::source::{ContinuousRead, SourceRead};
use obzenflow_runtime::stages::common::handlers::TypedInfiniteSourceHandler;
use obzenflow_runtime::stages::SourceError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NextPreloaded;

#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait PreloadedFeed {
    #[effect(type = "replay_testkit.preloaded.take", at_most_once, records = reply)]
    async fn take(
        &self,
        from: &NextPreloaded,
        ctx: &EffectContext,
    ) -> Result<ContinuousRead<Value, NextPreloaded>, EffectError>;
}

type Queue = Arc<Mutex<VecDeque<Value>>>;

struct Preloaded {
    values: Queue,
}

#[obzenflow_runtime::effects::system("preloaded")]
#[async_trait]
impl PreloadedFeed for Preloaded {
    async fn take(
        &self,
        _from: &NextPreloaded,
        _ctx: &EffectContext,
    ) -> Result<ContinuousRead<Value, NextPreloaded>, EffectError> {
        let value = self.values.lock().expect("preloaded values").pop_front();
        Ok(ContinuousRead {
            records: value.into_iter().collect(),
            next: NextPreloaded,
        })
    }
}

pub struct PreloadedInput<T> {
    port: Port<dyn PreloadedFeed>,
    values: Queue,
    output: PhantomData<fn() -> T>,
}

pub fn preloaded_input<T>(values: impl IntoIterator<Item = T>) -> PreloadedInput<T>
where
    T: Serialize,
{
    let values: Queue = Arc::new(Mutex::new(
        values
            .into_iter()
            .map(|value| serde_json::to_value(value).expect("a preloaded value serializes"))
            .collect(),
    ));
    PreloadedInput {
        port: Port::to(Preloaded {
            values: values.clone(),
        })
        .own_instance(),
        values,
        output: PhantomData,
    }
}

impl<T> TypedInfiniteSourceHandler for PreloadedInput<T>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
{
    type Output = T;

    fn read(&self) -> SourceRead<T> {
        let values = self.values.clone();
        SourceRead::continuous(
            self.port.clone(),
            NextPreloaded,
            move || {
                let ready = !values.lock().expect("preloaded values").is_empty();
                Box::pin(async move {
                    if !ready {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            },
            ReattachGuarantee::AtMostOnce,
            |value| {
                serde_json::from_value(value).map_err(|_| {
                    SourceError::Deserialization(SourceDiagnosticReason::MalformedInput.into())
                })
            },
        )
    }
}
