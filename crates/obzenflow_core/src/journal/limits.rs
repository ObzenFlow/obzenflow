// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Physical frame and atomic-member admission limits. Logical records are not
//! serialized a second time merely to impose a canonical-JSON size policy.

pub const MAX_FRAME_BODY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_GROUP_RECORDS: usize = 4096;

pub fn validate_group_size(count: usize) -> Result<(), super::JournalError> {
    if count > MAX_GROUP_RECORDS {
        return Err(super::JournalError::Implementation {
            message: "Too many records in atomic group".into(),
            source: "journal admission budget exceeded".into(),
        });
    }
    Ok(())
}

/// Measure canonical JSON for diagnostics and benchmarks, without admission policy.
pub fn record_bytes(value: &impl serde::Serialize) -> Result<usize, serde_json::Error> {
    let mut writer = Counter(0);
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}

struct Counter(usize);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
