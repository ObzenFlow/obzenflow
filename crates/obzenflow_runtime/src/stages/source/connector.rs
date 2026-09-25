// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cold source configuration and supervised reader acquisition (FLOWIP-122e).
//!
//! A connector contains reusable configuration. Construction, description and
//! cloning must perform no external I/O. Each `open` creates an independent reader
//! with its own cursor, buffers and cleanup state. The runtime alone opens and
//! polls it; strict replay never calls `open`.
//!
//! An asynchronous connector owns partial acquisition until it returns a reader.
//! On error it must roll back partial acquisition, and dropping its opening future
//! must release owned resources without leaving detached work. Once returned, the
//! runtime owns the reader and attempts `drain` once on every orderly exit, even
//! before the first poll. `drain` releases resources only: it must not read more
//! facts, advance a cursor or emit completion. Forced task abort relies on `Drop`;
//! awaited cleanup is not guaranteed after abort or process death.

use super::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use async_trait::async_trait;
use obzenflow_core::{OneFactStageOutput, StageId};

/// Stable source identity, without execution mode, journals or runtime controls.
#[derive(Clone, Debug)]
pub struct SourceReaderInitContext {
    pub stage_id: StageId,
    pub stage_name: String,
    pub flow_name: String,
}

/// Reusable configuration for a synchronous source that eventually exhausts.
pub trait FiniteSourceConnector: Send + Sync + Sized + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    type Reader: TypedFiniteSourceHandler<Output = Self::Output> + 'static;

    fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError>;
}

/// Reusable configuration for an asynchronous source that eventually exhausts.
#[async_trait]
pub trait AsyncFiniteSourceConnector: Send + Sync + Sized + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    type Reader: TypedAsyncFiniteSourceHandler<Output = Self::Output> + 'static;

    async fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError>;
}

/// Reusable configuration for a synchronous source stopped by runtime control.
pub trait InfiniteSourceConnector: Send + Sync + Sized + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    type Reader: TypedInfiniteSourceHandler<Output = Self::Output> + 'static;

    fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError>;
}

/// Reusable configuration for an asynchronous source stopped by runtime control.
#[async_trait]
pub trait AsyncInfiniteSourceConnector: Send + Sync + Sized + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;
    type Reader: TypedAsyncInfiniteSourceHandler<Output = Self::Output> + 'static;

    async fn open(&self, context: SourceReaderInitContext) -> Result<Self::Reader, SourceError>;
}
