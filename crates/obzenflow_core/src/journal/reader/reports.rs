// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Select before materialising. Progress is delivered-prefix coverage, not
//! evidence admission or proof that a parent has applied the preceding reports.
use super::JournalError;
use crate::event::{JournalEvent, JournalRecord};
use crate::JournalPayload;
use async_trait::async_trait;

#[derive(Clone, Copy, Debug)]
pub struct ReportScanBudget {
    pub records: usize,
    pub bytes: usize,
}

impl Default for ReportScanBudget {
    fn default() -> Self {
        Self {
            records: 64,
            bytes: 512 * 1024,
        }
    }
}

pub enum ReportScanItem<P: JournalPayload> {
    Record(Box<JournalRecord<P>>),
    /// Every candidate up to this reader position has already been returned.
    Progress,
    /// A temporary tail. False distinguishes an incomplete frame from clean EOF.
    Tail {
        committed_end: bool,
    },
}

pub struct ReportScan<P: JournalPayload> {
    pub item: ReportScanItem<P>,
    /// Encoded primary bytes traversed by this call, not reconstructed JSON size.
    /// Memory providers have no encoded storage and report zero.
    pub scanned_bytes: usize,
}

/// A bounded append-order selection cursor. One indivisible, hard-bounded frame
/// may exceed a requested quantum. Skipped payload semantics are not validated.
/// Whole groups and all selected members must pass validation before delivery.
#[async_trait]
pub trait JournalReportReader<T: JournalEvent>: Send + Sync {
    async fn next_report(
        &mut self,
        budget: ReportScanBudget,
    ) -> Result<ReportScan<T::Payload>, JournalError>;
    fn position(&self) -> u64;
    fn initial_prefix_complete(&self) -> Result<bool, JournalError>;
    fn is_at_end(&self) -> bool;
}

/// Provider trust boundary: validate committed framing, routing continuity and
/// selected records. Progress must never skip an undelivered candidate. Retain
/// in-flight work across cancellation; errors cannot advance past a corrupt gap.
#[async_trait]
pub trait JournalReportStorageReader<T: JournalEvent>: Send + Sync {
    async fn storage_next_report(
        &mut self,
        budget: ReportScanBudget,
    ) -> Result<ReportScan<T::Payload>, JournalError>;
    fn storage_position(&self) -> u64;
    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError>;
    fn storage_is_at_end(&self) -> bool;
}

#[async_trait]
impl<T: JournalEvent, R: JournalReportStorageReader<T> + ?Sized> JournalReportReader<T> for R {
    async fn next_report(
        &mut self,
        budget: ReportScanBudget,
    ) -> Result<ReportScan<T::Payload>, JournalError> {
        let scan = self.storage_next_report(budget).await?;
        Ok(ReportScan {
            item: match scan.item {
                ReportScanItem::Record(mut record) => {
                    record.admit_in_place()?;
                    ReportScanItem::Record(record)
                }
                other => other,
            },
            scanned_bytes: scan.scanned_bytes,
        })
    }
    fn position(&self) -> u64 {
        self.storage_position()
    }
    fn initial_prefix_complete(&self) -> Result<bool, JournalError> {
        self.storage_initial_prefix_complete()
    }
    fn is_at_end(&self) -> bool {
        self.storage_is_at_end()
    }
}
