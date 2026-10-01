// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deliberate negative controls for qualification of the performance gate.
//! The acceptance owner removes this variable from every ordinary measurement.

#[derive(Clone, Copy, PartialEq)]
pub enum Control {
    None,
    SlowReader,
    MissingReaderOutput,
}

pub fn selected() -> Control {
    static CONTROL: std::sync::OnceLock<Control> = std::sync::OnceLock::new();
    *CONTROL.get_or_init(
        || match std::env::var("OBZENFLOW_BENCH_CONTROL").as_deref() {
            Err(std::env::VarError::NotPresent) => Control::None,
            Ok("slow-reader") => Control::SlowReader,
            Ok("missing-reader-output") => Control::MissingReaderOutput,
            other => panic!("unknown benchmark negative control: {other:?}"),
        },
    )
}
