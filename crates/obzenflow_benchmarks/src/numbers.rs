// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_runtime::queries::FiniteRead;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct NextNumber(pub u64);

#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait NumberFeed {
    #[query(type = "benchmarks.numbers.read")]
    async fn read(
        &self,
        from: &NextNumber,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<u64, NextNumber>, obzenflow_runtime::queries::QueryError>;
}

pub struct Numbers {
    total: Option<u64>,
    batch: u64,
    pace: Duration,
}

impl Numbers {
    pub fn counting(total: u64, batch: u64) -> Self {
        Self {
            total: Some(total),
            batch: batch.max(1),
            pace: Duration::ZERO,
        }
    }

    pub fn paced(total: u64, pace: Duration) -> Self {
        Self {
            pace,
            ..Self::counting(total, 1)
        }
    }

    pub fn idle() -> Self {
        Self {
            total: None,
            batch: 1,
            pace: Duration::ZERO,
        }
    }
}

#[async_trait]
impl NumberFeed for Numbers {
    async fn read(
        &self,
        from: &NextNumber,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<u64, NextNumber>, obzenflow_runtime::queries::QueryError> {
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
