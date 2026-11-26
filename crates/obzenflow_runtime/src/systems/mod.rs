// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A system is any type that implements a port. A flow builds each port from its
//! system with [`Port::to`], or from a [`SystemConfig`] with [`Port::deferred`].
//!
//! One system serves two ports from one instance through an `Arc`:
//!
//! ```
//! # use obzenflow_runtime::systems::Port;
//! # use std::sync::Arc;
//! # #[obzenflow_runtime::effects::port]
//! # trait RiskEnginePort: Send + Sync {}
//! # #[obzenflow_runtime::effects::port]
//! # trait CaseSystemPort: Send + Sync {}
//! struct Vendor;
//!
//! impl RiskEnginePort for Vendor {}
//! impl CaseSystemPort for Vendor {}
//!
//! let vendor = Arc::new(Vendor);
//! let risk: Port<dyn RiskEnginePort> = Port::to(Arc::clone(&vendor));
//! let cases: Port<dyn CaseSystemPort> = Port::to(vendor);
//! ```

mod config;
mod port;

pub use config::SystemConfig;
pub use port::Port;
#[doc(hidden)]
pub use port::{EffectKind, PortImpl, PortKind, QueryKind, ServesPort};
