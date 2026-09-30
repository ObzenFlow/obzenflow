// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only probes for Core's causal and record operations.
//! The benchmark owner coordinates activation, aggregation and task completion.
//! These counters are observations, never inputs to execution or admission.

use crate::event::payloads::chain_payload::EventKind;
use crate::event::provenance::ChainEventProvenance;
use crate::event::{ChainPayload, JournalRecord};
use crate::JournalPayload;
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

macro_rules! counters {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        #[derive(Clone, Copy)]
        #[repr(usize)]
        pub enum Counter { $($variant,)+ }
        const NAMES: &[&str] = &[$($name,)+];
    };
}
counters! {
    PayloadDecodes => "payload_json_decodes",
    BusinessPayloadDecodes => "business_payload_decodes",
    RecordsConstructed => "records_constructed",
    BusinessRecordsConstructed => "business_records_constructed",
    RecordSerializations => "record_accounting_serializations",
    BusinessSerializations => "business_record_accounting_serializations",
    StructuralValidations => "structural_validations",
    ValidatedClockEntries => "validated_clock_entries",
    ClockSerializations => "clock_serializations",
    SerializedClockEntries => "serialized_clock_entries",
    ConstructedClockComponents => "constructed_record_clock_components",
}

/// Exposes the real structural validator only in instrumented development builds.
pub fn validate_structure<P: JournalPayload>(
    record: &JournalRecord<P>,
) -> Result<crate::event::JournalCommitRef, crate::event::CausalError> {
    crate::event::JournalClock::validate_record(record)
}

static COUNTS: [AtomicU64; NAMES.len()] = [const { AtomicU64::new(0) }; NAMES.len()];
static ACTIVE: AtomicBool = AtomicBool::new(false);

pub fn add(counter: Counter, amount: u64) {
    if ACTIVE.load(Ordering::Relaxed) {
        COUNTS[counter as usize].fetch_add(amount, Ordering::Relaxed);
    }
}

fn business(payload: &impl Any) -> bool {
    matches!(
        (payload as &dyn Any).downcast_ref::<ChainPayload>(),
        Some(ChainPayload::Fact(_))
    )
}

pub fn payload_decode(provenance: &impl Any) {
    add(Counter::PayloadDecodes, 1);
    if (provenance as &dyn Any)
        .downcast_ref::<ChainEventProvenance>()
        .is_some_and(|p| p.event_kind == EventKind::Fact)
    {
        add(Counter::BusinessPayloadDecodes, 1);
    }
}

pub fn record_constructed<P: JournalPayload>(record: &JournalRecord<P>) {
    add(Counter::RecordsConstructed, 1);
    if business(&record.payload) {
        add(Counter::BusinessRecordsConstructed, 1);
    }
    add(
        Counter::ConstructedClockComponents,
        record.envelope.provenance.journal.vector_clock.clocks.len() as u64,
    );
}

pub fn record_serialized(payload: &impl Any) {
    add(Counter::RecordSerializations, 1);
    if business(payload) {
        add(Counter::BusinessSerializations, 1);
    }
}

/// Reset only while the benchmark owner has stopped contributing work.
pub fn reset() {
    for count in &COUNTS {
        count.store(0, Ordering::Relaxed);
    }
}

pub fn set_enabled(enabled: bool) {
    ACTIVE.store(enabled, Ordering::SeqCst);
}

/// Readout is allocation-free; aggregation belongs to the benchmark owner.
pub fn snapshot() -> impl Iterator<Item = (&'static str, u64)> {
    NAMES
        .iter()
        .zip(&COUNTS)
        .map(|(name, count)| (*name, count.load(Ordering::Relaxed)))
}
