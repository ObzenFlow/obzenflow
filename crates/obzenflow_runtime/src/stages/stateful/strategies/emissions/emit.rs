// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The emission strategies an accumulator takes in `.emit(...)`.

use super::{EveryInterval, EveryN, TimeWindow};
use std::time::Duration;

/// `emit::always`, which emits after every input.
pub use super::EmitAlways as always;

/// `emit::on_eof`, which emits once, at end of input.
pub use super::OnEOF as on_eof;

/// Emits after every `count` inputs, keeping the accumulated state.
pub fn every_n(count: u64) -> EveryN {
    EveryN::new(count)
}

/// Emits once `interval` has passed since the first input after the previous
/// emission, keeping the accumulated state.
pub fn every(interval: Duration) -> EveryInterval {
    EveryInterval::new(interval)
}

/// Emits when each window of `duration` ends, starting the state over.
pub fn per_window(duration: Duration) -> TimeWindow {
    TimeWindow::new(duration)
}
