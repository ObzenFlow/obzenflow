// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095l: a handler's declared relationship to the order in which a
//! multi-source fan-in delivers its inputs. The descriptor captures this at flow
//! build and maps it to an order role that drives canonical-merge enablement.

/// A handler's declaration of whether its observable output depends on the order
/// of its inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputOrderSemantics {
    /// The author has not declared. A stateful or join stage in a multi-source
    /// fan-in cone must declare, so the build refuses an undeclared one rather
    /// than silently imposing the merge or silently trusting commutativity.
    Undeclared,
    /// The output depends on input order. The fan-in above it runs the canonical
    /// merge so reconstruction is deterministic.
    OrderSensitive,
    /// The output is invariant under input permutation, carried by a proof token
    /// the `#[trace_invariant]` attribute mints from a passing commutativity
    /// trial. Such a stage is a barrier: it absorbs reordering, so the fan-in
    /// above it pays no merge cost.
    TraceInvariant(TraceInvarianceProof),
}

/// A sealed witness that a handler's order-invariance was checked by the trial
/// the `#[trace_invariant]` attribute emits. It cannot be constructed by hand:
/// the only constructor is hidden and is called solely by generated code, so a
/// `TraceInvariant` declaration always carries a real, exercised obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceInvarianceProof(());

impl TraceInvarianceProof {
    /// Mint the witness. FLOWIP-095l Gap 12: `pub(crate)` so the struct cannot be
    /// fabricated from another crate (its field is also private). The single
    /// cross-crate mint path is [`InputOrderSemantics::__trace_invariant_proven`].
    pub(crate) const fn __minted_by_trace_invariant_attribute() -> Self {
        Self(())
    }
}

impl InputOrderSemantics {
    /// A proven trace-invariant claim, the only cross-crate minter of the witness.
    /// FLOWIP-095l Gap 12: hidden and called by exactly one blessed path, the
    /// `#[trace_invariant]` attribute expansion, which also emits the trial. Hand use
    /// is unsupported; a join descriptor ignores a `TraceInvariant` declaration, so
    /// minting one for a join has no effect.
    #[doc(hidden)]
    pub const fn __trace_invariant_proven() -> Self {
        Self::TraceInvariant(TraceInvarianceProof::__minted_by_trace_invariant_attribute())
    }

    /// Whether the author made an explicit declaration (declared at all).
    pub fn is_declared(&self) -> bool {
        !matches!(self, Self::Undeclared)
    }
}
