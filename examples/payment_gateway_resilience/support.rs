// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Test-facing aggregator mirroring `product_catalog_enrichment/support.rs`:
//! integration tests include this one file instead of each sibling module,
//! keeping example items on a public path.

#[path = "console.rs"]
pub mod console;

#[path = "deliveries.rs"]
pub mod deliveries;

#[path = "domain.rs"]
pub mod domain;

#[path = "fixtures.rs"]
pub mod fixtures;

#[path = "flow.rs"]
pub mod flow;

#[path = "gateway.rs"]
pub mod gateway;

#[path = "proof.rs"]
pub mod proof;

#[path = "validation.rs"]
pub mod validation;
