// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only probes for Infra's codec, storage and blocking dispatch.
//! These observations never control execution or admission. The benchmark
//! crate owns exclusive measurement sessions and cross-component aggregation.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub mod studio;

macro_rules! counters {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        #[derive(Clone, Copy)]
        #[repr(usize)]
        pub enum Counter { $($variant,)+ }
        const NAMES: &[&str] = &[$($name,)+];
    };
}
counters! {
    PrimaryFrameReads => "primary_frame_reads",
    PrimaryFrameBytes => "primary_frame_bytes",
    VerifiedFrames => "verified_frames",
    VerifiedFrameBytes => "verified_frame_bytes",
    DefinitionCarrierReads => "definition_carrier_reads",
    DefinitionCarrierBytes => "definition_carrier_bytes",
    DecodeBlockingJobs => "decode_blocking_jobs",
    AppendBlockingJobs => "append_blocking_jobs",
}

static COUNTS: [AtomicU64; NAMES.len()] = [const { AtomicU64::new(0) }; NAMES.len()];
static ENABLED: AtomicBool = AtomicBool::new(false);

pub fn add(counter: Counter, amount: u64) {
    if ENABLED.load(Ordering::Relaxed) {
        COUNTS[counter as usize].fetch_add(amount, Ordering::Relaxed);
    }
}

pub fn reset() {
    for count in &COUNTS {
        count.store(0, Ordering::Relaxed);
    }
}

pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::SeqCst);
}

pub fn snapshot() -> impl Iterator<Item = (&'static str, u64)> {
    NAMES
        .iter()
        .zip(&COUNTS)
        .map(|(name, count)| (*name, count.load(Ordering::Relaxed)))
}
