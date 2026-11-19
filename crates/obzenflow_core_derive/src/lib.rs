// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compiler-host macros for ObzenFlow contracts.
//!
//! Provides the sorting behind `outcome![...]`, which `obzenflow_core`
//! re-exports for its `outcome!` macro, and `#[command]`, `#[system]` and
//! `#[port]`, which the runtime's effects module re-exports next to its
//! command and port contracts.

mod command;
mod effects_path;
mod outcome_list;
mod port;
mod system;

use proc_macro::TokenStream;

/// Expand `$crate; A | (B, C)` to the nested outcome list type with its
/// outcomes sorted by type name. `obzenflow_core`'s `outcome!` hands it its
/// `$crate`; it is not called directly.
#[doc(hidden)]
#[proc_macro]
pub fn sorted_outcomes(input: TokenStream) -> TokenStream {
    outcome_list::expand(input.into()).into()
}

/// Declare the ports an effectful command works through.
///
/// `#[command(through = PaymentGatewayPort)]` on a unit struct adds one
/// private field for the port and generates `Self::through(port)`. The
/// command may perform every effect the port declares. It goes above the
/// struct's derives, which then see the field.
///
/// `#[command]` alone reads the struct's fields instead. Each field marked
/// `#[port]` holds a `Port` or a typed binding, and the command may perform
/// every effect the field's port declares. A field written
/// `#[port(RequestA, RequestB)]` allows only the requests it lists, each of
/// which its port must declare. A command that works through several ports
/// marks each with a `#[port]` field, and a command that works through no
/// port has no such field.
///
/// Generated code resolves `::obzenflow::effects`; code that depends on the
/// runtime crate directly names its effects module with
/// `#[command(effects = <path>)]`.
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    command::expand(attr.into(), item.into()).into()
}

/// Name a system on its `impl` of a port, as in
/// `#[system("stripe")] impl PaymentGatewayPort for StripeProcessor`.
///
/// The name is recorded in every call the system answers, so a replay against a
/// different system is refused. It is 1 to 64 characters from `[A-Za-z0-9._-]`
/// and starts with a letter or digit. A type that implements two ports names
/// itself once, on either `impl`. A struct that serves as its own port names
/// itself with `#[port(system = "name")]` instead.
///
/// Generated code resolves `::obzenflow::effects`; code that depends on the
/// runtime crate directly writes `#[system("name", effects = <path>)]`.
#[proc_macro_attribute]
pub fn system(attr: TokenStream, item: TokenStream) -> TokenStream {
    system::expand(attr.into(), item.into()).into()
}

/// Mark a port trait, or the inherent `impl` block of a struct that serves as
/// its own port, so a `Port` takes its port type from the system it is built
/// from. Port methods return `Result<Outcome, EffectError>`. By default the
/// outcome is one fact or an `outcome![...]` list. A port trait is `Send` and
/// `Sync`, which the attribute adds when the trait doesn't write them.
///
/// Each method marked `#[effect(type = "...", exactly_once)]`,
/// `#[effect(type = "...", at_least_once)]` or
/// `#[effect(type = "...", at_most_once)]` declares one effect. It takes its
/// request by reference after `&self`, and the request type then names the
/// effect: a command performs it with `fx.call(request)`. `type` is the effect
/// type the journal records the call under, an optional `schema_version = N`
/// versions the request's shape, and `records = reply` makes the method's
/// answer a reply the runtime records and returns, which writes no fact.
///
/// Every sink or effectful row using the method inherits its guarantee.
/// The guarantee describes the call at the other end:
///
/// - `exactly_once`: the call takes effect once even if it runs again. Either
///   the operation is idempotent, or the method takes `key: &IdempotencyKey`
///   after its request and the other end deduplicates by it, or the method
///   takes `tx: EffectCommitHandle<Outcome>` and commits its outcome through
///   it. A keyed request implements `Keyed`, and its key is journalled with the
///   call.
/// - `at_least_once`: the call may take effect more than once after a crash.
///   Retry middleware may repeat a call after a definite refusal.
/// - `at_most_once`: the call takes effect at most once. The runtime records
///   each attempt's start before making it, and records an attempt a crash
///   left in doubt abandoned instead of making it again.
///
/// After its request, an effect method takes nothing, `&EffectContext`,
/// `key: &IdempotencyKey` optionally followed by `&EffectContext`, or a
/// commit handle. In a struct's `impl` block, methods without `#[effect]` are
/// ordinary methods; on a port trait, every method is a port method.
///
/// A struct that serves as its own port is also its own system, and names itself
/// here with `#[port(system = "name")]`.
///
/// Generated code resolves `::obzenflow::effects`; code that depends on the
/// runtime crate directly writes `#[port(effects = <path>)]`.
#[proc_macro_attribute]
pub fn port(attr: TokenStream, item: TokenStream) -> TokenStream {
    port::expand(attr.into(), item.into()).into()
}
