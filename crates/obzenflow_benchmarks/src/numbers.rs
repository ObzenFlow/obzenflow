// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A numbered input that benchmark sources read through a port.

use async_trait::async_trait;
use obzenflow_runtime::effects::{EffectContext, EffectError, Port};
use obzenflow_runtime::stages::common::handlers::source::FiniteRead;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The first number of the next read.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct NextNumber(pub u64);

#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait NumberFeed {
    #[effect(type = "benchmarks.numbers.read", exactly_once, records = reply)]
    async fn read(
        &self,
        from: &NextNumber,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<u64, NextNumber>, EffectError>;
}

/// Supplies the numbers from zero up to a total, up to `batch` numbers in
/// each read.
pub struct Numbers {
    total: Option<u64>,
    batch: u64,
    pace: Duration,
}

impl Numbers {
    /// `total` numbers, up to `batch` in each read.
    pub fn counting(total: u64, batch: u64) -> Self {
        Self {
            total: Some(total),
            batch: batch.max(1),
            pace: Duration::ZERO,
        }
    }

    /// `total` numbers, one in each read after a wait of `pace`.
    pub fn paced(total: u64, pace: Duration) -> Self {
        Self {
            pace,
            ..Self::counting(total, 1)
        }
    }

    /// An input that never answers its first read.
    pub fn idle() -> Self {
        Self {
            total: None,
            batch: 1,
            pace: Duration::ZERO,
        }
    }

    pub fn port(self) -> Port<dyn NumberFeed> {
        Port::to(self)
    }
}

#[obzenflow_runtime::effects::system("numbers")]
#[async_trait]
impl NumberFeed for Numbers {
    async fn read(
        &self,
        from: &NextNumber,
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<u64, NextNumber>, EffectError> {
        let Some(total) = self.total else {
            return std::future::pending().await;
        };
        if from.0 >= total {
            return Ok(FiniteRead::EndOfInput);
        }
        if !self.pace.is_zero() {
            tokio::time::sleep(self.pace).await;
        }
        let end = (from.0 + self.batch).min(total);
        Ok(FiniteRead::Batch {
            records: (from.0..end).collect(),
            next: NextNumber(end),
        })
    }
}
