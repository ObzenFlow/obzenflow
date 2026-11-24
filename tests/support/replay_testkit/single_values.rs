// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::queries::{QueryContext, QueryError, QueryPort};
use obzenflow_runtime::stages::common::handlers::source::{FiniteRead, SourceRead};
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::SourceError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::marker::PhantomData;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NextSingleValue(u64);

#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait SingleValueFeed {
    #[query(type = "replay_testkit.single_values.read")]
    async fn read(
        &self,
        from: &NextSingleValue,
        ctx: &QueryContext,
    ) -> Result<FiniteRead<Value, NextSingleValue>, QueryError>;
}

struct SingleValueList {
    values: Vec<Value>,
}

#[obzenflow_runtime::effects::system("single-values")]
#[async_trait]
impl SingleValueFeed for SingleValueList {
    async fn read(
        &self,
        from: &NextSingleValue,
        _ctx: &QueryContext,
    ) -> Result<FiniteRead<Value, NextSingleValue>, QueryError> {
        let value = usize::try_from(from.0)
            .ok()
            .and_then(|at| self.values.get(at));
        Ok(match value {
            Some(value) => FiniteRead::Batch {
                records: vec![value.clone()],
                next: NextSingleValue(from.0 + 1),
            },
            None => FiniteRead::EndOfInput,
        })
    }
}

pub struct SingleValues<T> {
    port: QueryPort<dyn SingleValueFeed>,
    output: PhantomData<fn() -> T>,
}

/// `single_values` makes a finite source that answers each read with one value.
pub fn single_values<T>(values: impl IntoIterator<Item = T>) -> SingleValues<T>
where
    T: Serialize,
{
    let values = values
        .into_iter()
        .map(|value| serde_json::to_value(value).expect("a single value serializes"))
        .collect();
    SingleValues {
        port: QueryPort::to(SingleValueList { values }),
        output: PhantomData,
    }
}

impl<T> TypedFiniteSourceHandler for SingleValues<T>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
{
    type Output = T;

    fn read(&self) -> SourceRead<T> {
        SourceRead::finite(self.port.clone(), NextSingleValue(0), |value| {
            serde_json::from_value(value).map_err(|_| {
                SourceError::Deserialization(SourceDiagnosticReason::MalformedInput.into())
            })
        })
        .in_process()
    }
}
