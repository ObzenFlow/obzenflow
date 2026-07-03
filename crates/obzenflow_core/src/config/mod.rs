// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration vocabulary (FLOWIP-010).
//!
//! Pure data shared across the workspace: provenance axes (source and
//! winning scope), run-manifest evidence shapes, secret references, and the
//! per-run value structs consumed on the data path. The knob registry,
//! resolver, and resolved model live in `obzenflow_runtime::runtime_config`;
//! this module holds only what core itself must see (the manifest schema and
//! the event factories).

pub mod evidence;
pub mod provenance;
pub mod secret;
pub mod values;

pub use evidence::{EffectiveConfigEvidence, ResolvedValueDoc, EVIDENCE_SCHEMA_VERSION};
pub use provenance::{ConfigScope, ConfigSource, ConfigValueMeta};
pub use secret::{SecretRef, SecretResolveError, SecretString};
pub use values::{LineagePolicy, DEFAULT_MAX_LINEAGE_DEPTH};
