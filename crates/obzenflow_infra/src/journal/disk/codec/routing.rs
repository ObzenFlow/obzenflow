// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ordinary frame provenance and member extents, also used by definition carriers.
//! The caller verifies the complete frame CRC first.
use super::primitives::{bytes, text, unsigned, Cursor};
use super::{invalid, Result};
use obzenflow_core::event::{JournalCommitRef, JournalRecord};
use obzenflow_core::{EventId, FlowId, JournalId, JournalPayload};

pub(super) struct Member<'a> {
    pub id: EventId,
    pub body: &'a [u8],
}

pub(crate) struct RouteSummary {
    pub group: Option<String>,
    pub first: JournalCommitRef,
    pub previous: Option<JournalCommitRef>,
    pub count: usize,
}

pub(super) struct Envelope<'a> {
    pub summary: RouteSummary,
    pub definitions: &'a [u8],
    pub members: Vec<Member<'a>>,
}

fn id(input: &mut Cursor<'_>) -> Result<ulid::Ulid> {
    Ok(ulid::Ulid::from_bytes(input.take(16)?.try_into().unwrap()))
}

impl<'a> Envelope<'a> {
    pub fn parse(body: &'a [u8]) -> Result<Self> {
        let mut body = Cursor::new(body);
        let mut route = Cursor::new(body.bytes()?);
        let group = match route.byte()? {
            0 => None,
            1 => {
                let name = route.text()?;
                if name.is_empty() {
                    return Err(invalid("empty routing group"));
                }
                Some(name)
            }
            _ => return Err(invalid("unknown frame kind")),
        };
        let count = route.length()?;
        if count == 0
            || count > obzenflow_core::journal::limits::MAX_GROUP_RECORDS
            || (group.is_none() && count != 1)
        {
            return Err(invalid("invalid routing member count"));
        }
        let run_id = FlowId::from_ulid(id(&mut route)?);
        let journal_writer_id = JournalId::from_ulid(id(&mut route)?).into();
        let sequence = route.unsigned()?;
        if sequence == 0 || sequence.checked_add(count as u64 - 1).is_none() {
            return Err(invalid("invalid routing sequence"));
        }
        let previous = if sequence == 1 {
            None
        } else {
            Some(JournalCommitRef {
                run_id,
                journal_writer_id,
                sequence: sequence - 1,
                event_id: id(&mut route)?.into(),
            })
        };
        let definitions = body.bytes()?;
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            let id = id(&mut route)?.into();
            let member_body = body.take(route.length()?)?;
            // Structural extents only: no JSON, clock, witness or record creation.
            let mut fields = Cursor::new(member_body);
            if fields.bytes()?.is_empty() {
                return Err(invalid("empty member provenance"));
            }
            match fields.byte()? {
                0 | 1 => {}
                2 => {
                    if fields.bytes()?.is_empty() {
                        return Err(invalid("empty member observation"));
                    }
                }
                _ => return Err(invalid("unknown observation presence tag")),
            }
            let payload = fields.bytes()?;
            if payload.is_empty() {
                return Err(invalid("invalid payload extent"));
            }
            fields.finish()?;
            members.push(Member {
                id,
                body: member_body,
            });
        }
        route.finish()?;
        body.finish()?;
        let reference = |index: usize| JournalCommitRef {
            run_id,
            journal_writer_id,
            sequence: sequence + index as u64,
            event_id: members[index].id,
        };
        Ok(Self {
            summary: RouteSummary {
                group,
                first: reference(0),
                previous,
                count,
            },
            definitions,
            members,
        })
    }
}

pub(super) fn encode<P: JournalPayload>(
    records: &[JournalRecord<P>],
    group: Option<&str>,
    lengths: &[usize],
) -> Result<Vec<u8>> {
    let first = &records[0];
    let journal = &first.envelope.provenance.journal;
    let sequence = first.local_sequence();
    if sequence == 0 {
        return Err(invalid("missing routing sequence"));
    }
    let mut route = Vec::new();
    match group {
        None => route.push(0),
        Some(group) => {
            route.push(1);
            text(group, &mut route);
        }
    }
    unsigned(records.len() as u64, &mut route);
    route.extend_from_slice(&journal.run_id.as_ulid().to_bytes());
    route.extend_from_slice(
        &journal
            .journal_writer_id
            .as_journal_id()
            .as_ulid()
            .to_bytes(),
    );
    unsigned(sequence, &mut route);
    if sequence > 1 {
        let previous = journal
            .previous
            .ok_or_else(|| invalid("missing routing predecessor"))?;
        route.extend_from_slice(&previous.event_id.as_ulid().to_bytes());
    }
    let mut previous = journal.previous;
    for (index, (record, length)) in records.iter().zip(lengths).enumerate() {
        let actual = &record.envelope.provenance.journal;
        let expected_sequence = sequence
            .checked_add(index as u64)
            .ok_or_else(|| invalid("routing sequence overflow"))?;
        if actual.run_id != journal.run_id
            || actual.journal_writer_id != journal.journal_writer_id
            || actual.previous != previous
            || record.local_sequence() != expected_sequence
        {
            return Err(invalid("noncontiguous routing group"));
        }
        previous = Some(JournalCommitRef {
            run_id: actual.run_id,
            journal_writer_id: actual.journal_writer_id,
            sequence: expected_sequence,
            event_id: *record.id(),
        });
        route.extend_from_slice(&record.id().as_ulid().to_bytes());
        unsigned(*length as u64, &mut route);
    }
    let mut output = Vec::new();
    bytes(&route, &mut output);
    Ok(output)
}
