// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Order certification decides which stages may be compared positionally.
//! Core owns it so the flow builder can run it too (FLOWIP-134k B3); the
//! verifier keeps calling it here.

pub use obzenflow_core::journal::archive::certification::{
    certify, Comparison, StageCertification,
};
