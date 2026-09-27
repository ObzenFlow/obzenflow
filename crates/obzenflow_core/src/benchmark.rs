// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only counts at production operations, across async/blocking workers.
//! The benchmark runs one census at a time and joins its tasks before finishing.
//! These counters are observations, never inputs to execution or admission.

use crate::event::payloads::chain_payload::EventKind;
use crate::event::provenance::ChainEventProvenance;
use crate::event::{ChainPayload, JournalRecord};
use crate::JournalPayload;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

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
    ConstructedClockComponents => "constructed_record_clock_components",
    ConstructedWitnessReferences => "constructed_record_witness_references",
    PrimaryFrameReads => "primary_frame_reads",
    PrimaryFrameBytes => "primary_frame_bytes",
    VerifiedFrames => "verified_frames",
    VerifiedFrameBytes => "verified_frame_bytes",
    DefinitionCarrierReads => "definition_carrier_reads",
    DefinitionCarrierBytes => "definition_carrier_bytes",
    DecodeBlockingJobs => "decode_blocking_jobs",
}

static COUNTS: [AtomicU64; NAMES.len()] = [const { AtomicU64::new(0) }; NAMES.len()];
static ACTIVE: AtomicBool = AtomicBool::new(false);
static EXCLUSIVE: Mutex<()> = Mutex::new(());

pub fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

pub fn add(counter: Counter, amount: u64) {
    if active() {
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
    add(
        Counter::ConstructedWitnessReferences,
        record.envelope.provenance.journal.causal.witnesses.len() as u64,
    );
}

pub fn record_serialized(payload: &impl Any) {
    add(Counter::RecordSerializations, 1);
    if business(payload) {
        add(Counter::BusinessSerializations, 1);
    }
}

pub struct WorkScope {
    _exclusive: MutexGuard<'static, ()>,
}

impl WorkScope {
    pub fn start() -> Self {
        let exclusive = EXCLUSIVE
            .try_lock()
            .expect("overlapping benchmark work scopes");
        for count in &COUNTS {
            count.store(0, Ordering::Relaxed);
        }
        ACTIVE.store(true, Ordering::SeqCst);
        Self {
            _exclusive: exclusive,
        }
    }

    /// All contributing tasks must already have completed or been joined.
    pub fn finish(self) -> BTreeMap<String, u64> {
        ACTIVE.store(false, Ordering::SeqCst);
        NAMES
            .iter()
            .zip(&COUNTS)
            .map(|(name, count)| ((*name).to_string(), count.load(Ordering::Relaxed)))
            .collect()
    }
}

impl Drop for WorkScope {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::SeqCst);
    }
}
