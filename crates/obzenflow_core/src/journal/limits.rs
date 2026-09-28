// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Current-schema admission limits, shared by writers and readers. Oversize
//! records/groups are rejected before commitment, never silently truncated.

pub const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_GROUP_BYTES: usize = 64 * 1024 * 1024;
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

pub fn record_bytes(value: &impl serde::Serialize) -> Result<usize, serde_json::Error> {
    let mut writer = Counter(0);
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}

pub fn validate_group<P: crate::event::payloads::JournalPayload>(
    records: &[crate::JournalRecord<P>],
) -> Result<(), super::JournalError> {
    validate_group_size(records.len())?;
    let mut budget = GroupBudget::default();
    for record in records {
        budget.admit(record)?;
    }
    Ok(())
}

/// Incremental bound while preparing a group, before retaining its next member.
#[derive(Default)]
pub struct GroupBudget {
    bytes: usize,
    records: usize,
}

impl GroupBudget {
    pub fn admit(&mut self, record: &impl serde::Serialize) -> Result<(), super::JournalError> {
        let error = |message: &str| super::JournalError::Implementation {
            message: message.into(),
            source: "journal admission budget exceeded".into(),
        };
        self.records += 1;
        validate_group_size(self.records)?;
        self.bytes +=
            record_bytes(record).map_err(|_| error("Journal record exceeds byte budget"))?;
        if self.bytes > MAX_GROUP_BYTES {
            return Err(error("Journal group exceeds byte budget"));
        }
        Ok(())
    }
}

struct Counter(usize);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        if self.0 > MAX_RECORD_BYTES {
            return Err(std::io::Error::other("journal record exceeds byte budget"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
