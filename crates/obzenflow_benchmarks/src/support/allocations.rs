// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Process-wide allocation attribution, including Tokio blocking workers.
//! Counts requested Rust heap bytes, not RSS, allocator overhead or OS cache.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static LIVE: AtomicU64 = AtomicU64::new(0);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

pub struct Allocator;

/// All currently live requested Rust heap bytes in this process, including
/// fixture and diagnostic allocations. Shared Arc allocations are counted once.
pub fn live_requested_bytes() -> u64 {
    LIVE.load(Relaxed)
}

fn allocated(size: usize) {
    let live = LIVE.fetch_add(size as u64, Relaxed) + size as u64;
    if super::work::active() {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(size as u64, Relaxed);
        PEAK.fetch_max(live, Relaxed);
    }
}

// SAFETY: Every allocation/deallocation is forwarded unchanged to System.
// Accounting uses only allocation-free atomics and never changes pointers/layouts.
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as u64, Relaxed);
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, old, size) };
        if !result.is_null() {
            LIVE.fetch_sub(old.size() as u64, Relaxed);
            allocated(size);
        }
        result
    }
}

pub(super) struct Start(u64);
#[derive(serde::Serialize)]
pub struct Work {
    pub allocation_and_reallocation_calls: u64,
    pub requested_allocation_bytes: u64,
    pub peak_heap_bytes_above_start: u64,
    pub retained_heap_bytes_above_start: u64,
}
impl Start {
    /// Call immediately before enabling the exclusive work scope.
    pub(super) fn new() -> Self {
        let live = LIVE.load(Relaxed);
        CALLS.store(0, Relaxed);
        BYTES.store(0, Relaxed);
        PEAK.store(live, Relaxed);
        Self(live)
    }
    pub(super) fn finish(self) -> Work {
        Work {
            allocation_and_reallocation_calls: CALLS.load(Relaxed),
            requested_allocation_bytes: BYTES.load(Relaxed),
            peak_heap_bytes_above_start: PEAK.load(Relaxed).saturating_sub(self.0),
            retained_heap_bytes_above_start: LIVE.load(Relaxed).saturating_sub(self.0),
        }
    }
}
