// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compiler-host macros for ObzenFlow contracts.
//!
//! This crate provides the sorting behind `outcome![...]`, which `obzenflow_core`
//! re-exports for its `outcome!` macro. It also provides `#[command]` and
//! `#[port]`, which the runtime's effects module re-exports.

mod command;
mod effects_path;
mod outcome_list;
mod port;

use effects_path::EffectsPath;
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
/// The facade export resolves contracts through the facade's effects module.
/// A renamed dependency can supply its module with `effects = <path>`.
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    command::expand(attr.into(), item.into(), EffectsPath::Facade).into()
}

/// Declares the ports a command works through, using Runtime's effects module.
/// See [`#[command]`](macro@command) for the supported forms.
#[doc(hidden)]
#[proc_macro_attribute]
pub fn runtime_command(attr: TokenStream, item: TokenStream) -> TokenStream {
    command::expand(attr.into(), item.into(), EffectsPath::Runtime).into()
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
/// commit handle. In a struct's `impl` block, a method without `#[effect]` or
/// `#[query]` is an ordinary method. On a port trait, every method is a port
/// method.
///
/// Each method marked `#[query(type = "...")]` declares one query, which a source
/// asks with `query(first).through(port)`. It takes its request by reference after
/// `&self`, then `&QueryContext`, and returns `Result<Answer, QueryError>`. A port
/// cannot declare both effects and queries.
///
/// A struct that serves as its own port is also its own system. A system in an
/// `Arc` also serves its ports, so one system can serve more than one port.
///
/// The facade export resolves contracts through the facade's effects module.
/// A renamed dependency can supply its module with `effects = <path>`.
#[proc_macro_attribute]
pub fn port(attr: TokenStream, item: TokenStream) -> TokenStream {
    port::expand(attr.into(), item.into(), EffectsPath::Facade).into()
}

/// Declares a port and its effects, using Runtime's effects module.
/// See [`#[port]`](macro@port) for the supported forms.
#[doc(hidden)]
#[proc_macro_attribute]
pub fn runtime_port(attr: TokenStream, item: TokenStream) -> TokenStream {
    port::expand(attr.into(), item.into(), EffectsPath::Runtime).into()
}
