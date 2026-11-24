// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handlers::source::{
    Continuous, ContinuousRead, InputReadiness, SourceRead,
};
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
pub trait PreloadedFeed: InputReadiness {
    #[query(type = "replay_testkit.preloaded.take")]
    async fn take(
        &self,
        from: &NextPreloaded,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Value, NextPreloaded>, obzenflow_runtime::queries::QueryError>;
}

type Queue = Arc<Mutex<VecDeque<Value>>>;

struct Preloaded {
    values: Queue,
}

#[async_trait]
impl InputReadiness for Preloaded {
    async fn ready(&self) -> Result<(), SourceError> {
        if self.values.lock().expect("preloaded values").is_empty() {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

#[obzenflow_runtime::effects::system("preloaded")]
#[async_trait]
impl PreloadedFeed for Preloaded {
    async fn take(
        &self,
        _from: &NextPreloaded,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Value, NextPreloaded>, obzenflow_runtime::queries::QueryError> {
        let value = self.values.lock().expect("preloaded values").pop_front();
        Ok(ContinuousRead {
            records: value.into_iter().collect(),
            next: NextPreloaded,
        })
    }
}

pub struct PreloadedInput<T> {
    port: obzenflow_runtime::queries::QueryPort<dyn PreloadedFeed>,
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
        port: obzenflow_runtime::queries::QueryPort::to(Preloaded { values }),
        output: PhantomData,
    }
}

impl<T> TypedInfiniteSourceHandler for PreloadedInput<T>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
{
    type Output = T;

    fn read(&self) -> SourceRead<T, Continuous> {
        SourceRead::continuous(self.port.clone(), NextPreloaded, |value| {
            serde_json::from_value(value).map_err(|_| {
                SourceError::Deserialization(SourceDiagnosticReason::MalformedInput.into())
            })
        })
    }
}
