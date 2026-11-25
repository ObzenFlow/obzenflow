// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ready-made sinks. Each is a command a `sink!` row takes, sending one
//! request per input through a port the framework declares.

#[doc(hidden)]
pub mod blocking;
pub mod console;
pub mod discard;
#[cfg(feature = "test-support")]
pub mod recording;
pub mod tracing;

pub use console::{
    ConsoleFormatError, ConsoleOutput, ConsolePort, ConsoleSink, ConsoleTablePort, DebugFormatter,
    Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination, SnapshotTableFormatter,
    TableConsoleSink, TableFormatter, TableRow,
};

pub use discard::{DiscardPort, DiscardSink, Discarded};
#[cfg(feature = "test-support")]
pub use recording::{
    CallCounter, Counted, CountingPort, CountingSink, Recorded, RecordedInput, RecordingPort,
    RecordingSink,
};
pub use tracing::{TracingLevel, TracingOutput, TracingPort, TracingSink};

#[doc(hidden)]
pub fn queued_batch_defaults() -> obzenflow_runtime::effects::SinkBatchDefaults {
    obzenflow_runtime::effects::SinkBatchDefaults {
        size: std::num::NonZeroUsize::new(64),
        max_wait: None,
    }
}

#[doc(hidden)]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}
