// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::{ChainEvent, StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::common::handlers::source::traits::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use obzenflow_runtime::stages::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct First;

impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.typed_source.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Second;

impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.typed_source.second";
}

#[derive(Clone, Debug, StageOutputFacts)]
pub enum FirstOrSecond {
    First(First),
    Second(Second),
}

#[derive(Clone, Debug)]
pub struct RawFinite;

impl FiniteSourceHandler for RawFinite {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct RawAsyncFinite;

#[async_trait]
impl AsyncFiniteSourceHandler for RawAsyncFinite {
    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct RawInfinite;

impl InfiniteSourceHandler for RawInfinite {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
pub struct RawAsyncInfinite;

#[async_trait]
impl AsyncInfiniteSourceHandler for RawAsyncInfinite {
    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
pub struct FiniteFirst;

impl TypedFiniteSourceHandler for FiniteFirst {
    type Output = First;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct FiniteBoth;

impl TypedFiniteSourceHandler for FiniteBoth {
    type Output = FirstOrSecond;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct AsyncFiniteFirst;

#[async_trait]
impl TypedAsyncFiniteSourceHandler for AsyncFiniteFirst {
    type Output = First;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct AsyncFiniteBoth;

#[async_trait]
impl TypedAsyncFiniteSourceHandler for AsyncFiniteBoth {
    type Output = FirstOrSecond;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct InfiniteFirst;

impl TypedInfiniteSourceHandler for InfiniteFirst {
    type Output = First;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
pub struct InfiniteBoth;

impl TypedInfiniteSourceHandler for InfiniteBoth {
    type Output = FirstOrSecond;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
pub struct AsyncInfiniteFirst;

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for AsyncInfiniteFirst {
    type Output = First;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
pub struct AsyncInfiniteBoth;

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for AsyncInfiniteBoth {
    type Output = FirstOrSecond;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(Vec::new())
    }
}
