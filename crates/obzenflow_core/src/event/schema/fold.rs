// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Folding one outcome of an `outcome![...]` list of single facts, written
//! `fold!(match fact { … })`.

use super::member_set::{FactOr, MemberAfter, MemberFound, NoOtherFact};

/// A value `fold!` takes apart: an outcome whose facts are still to fold.
#[doc(hidden)]
pub trait FoldStart {
    type Remaining;

    fn fold_start(self) -> Self::Remaining;
}

/// The facts of `fact` still to fold.
#[doc(hidden)]
pub fn fold_start<List: FoldStart>(fact: List) -> List::Remaining {
    fact.fold_start()
}

/// Takes fact `T` out of an outcome, or leaves the facts still to fold.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{T}` is not a fact left to fold",
    label = "the stage's `Output` doesn't list `{T}`, or an earlier arm already folds it",
    note = "each arm names a different fact of the stage's `Output`"
)]
pub trait TakeFact<T, Index>: Sized {
    type Remaining;

    fn take(self) -> Result<T, Self::Remaining>;
}

impl<T, Rest> TakeFact<T, MemberFound> for FactOr<T, Rest> {
    type Remaining = Rest;

    fn take(self) -> Result<T, Rest> {
        match self {
            FactOr::Head(fact) => Ok(fact),
            FactOr::Rest(rest) => Err(rest),
        }
    }
}

#[diagnostic::do_not_recommend]
impl<T, Head, Rest, Index> TakeFact<T, MemberAfter<Index>> for FactOr<Head, Rest>
where
    Rest: TakeFact<T, Index>,
{
    type Remaining = FactOr<Head, Rest::Remaining>;

    fn take(self) -> Result<T, Self::Remaining> {
        match self {
            FactOr::Head(head) => Err(FactOr::Head(head)),
            FactOr::Rest(rest) => rest.take().map_err(FactOr::Rest),
        }
    }
}

/// Every listed fact has an arm.
#[doc(hidden)]
pub trait AllFolded {
    fn folded<Value>(self) -> Value;
}

impl AllFolded for NoOtherFact {
    fn folded<Value>(self) -> Value {
        match self {}
    }
}

impl<Head: Folded, Rest> AllFolded for FactOr<Head, Rest> {
    fn folded<Value>(self) -> Value {
        Head::unfolded()
    }
}

/// A listed fact that no arm folds. Nothing implements it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this fold has no arm for `{Self}`, which the stage's `Output` lists",
    label = "add an arm for `{Self}`",
    note = "every fact in `Output` needs an arm; one that leaves state unchanged is written \
            `{Self} {{ .. }} => {{}}`"
)]
pub trait Folded {
    fn unfolded<Value>() -> Value;
}

/// Fold one outcome of an `outcome![...]` list of single facts by each fact's
/// own pattern, inside an effectful stateful handler's `apply`.
///
/// Each arm is an irrefutable pattern of one listed fact, `|` joins
/// alternatives, `name @ Fact { .. }` binds the whole fact, and `_` may end
/// the arms. A listed fact with no arm, and an arm for a fact that is
/// unlisted or already folded, are refused at compile time.
///
/// ```
/// use obzenflow_core::{fold, outcome, TypedPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct StockReserved { pub hold_id: u32 }
/// impl TypedPayload for StockReserved {
///     const EVENT_TYPE: &'static str = "stock.reserved";
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct StockReleased { pub hold_id: u32 }
/// impl TypedPayload for StockReleased {
///     const EVENT_TYPE: &'static str = "stock.released";
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct SoldOut;
/// impl TypedPayload for SoldOut {
///     const EVENT_TYPE: &'static str = "stock.sold_out";
/// }
///
/// type Output = outcome![StockReserved | StockReleased | SoldOut];
///
/// fn apply(holds: &mut Vec<u32>, fact: Output) {
///     fold!(match fact {
///         StockReserved { hold_id } => { holds.push(hold_id) }
///         StockReleased { hold_id } => { holds.retain(|held| *held != hold_id) }
///         SoldOut => {}
///     });
/// }
///
/// let mut holds = Vec::new();
/// apply(&mut holds, outcome(StockReserved { hold_id: 7 }));
/// apply(&mut holds, outcome(SoldOut));
/// assert_eq!(holds, vec![7]);
/// ```
///
/// A listed fact with no arm, and an arm for a fact that is unlisted, are
/// refused:
///
/// ```compile_fail
/// # use obzenflow_core::{fold, outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct StockReserved;
/// # impl TypedPayload for StockReserved { const EVENT_TYPE: &'static str = "doc.reserved"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct CancelIgnored;
/// # impl TypedPayload for CancelIgnored { const EVENT_TYPE: &'static str = "doc.ignored"; }
/// fn apply(fact: outcome![StockReserved | CancelIgnored]) {
///     fold!(match fact {
///         StockReserved => {}
///     });
/// }
/// ```
///
/// ```compile_fail
/// # use obzenflow_core::{fold, outcome, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct StockReserved;
/// # impl TypedPayload for StockReserved { const EVENT_TYPE: &'static str = "doc.reserved"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct SoldOut;
/// # impl TypedPayload for SoldOut { const EVENT_TYPE: &'static str = "doc.sold_out"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Unlisted;
/// # impl TypedPayload for Unlisted { const EVENT_TYPE: &'static str = "doc.unlisted"; }
/// fn apply(fact: outcome![StockReserved | SoldOut]) {
///     fold!(match fact {
///         StockReserved => {}
///         SoldOut => {}
///         Unlisted => {}
///     });
/// }
/// ```
#[macro_export]
macro_rules! fold {
    (match $fact:ident { $($arms:tt)* }) => {{
        let remaining = $crate::event::schema::__private::fold_start($fact);
        $crate::__fold!(@arms remaining ; $($arms)*)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __fold {
    (@arms $rest:ident ;) => {
        $crate::event::schema::__private::AllFolded::folded($rest)
    };
    (@arms $rest:ident ; , $($arms:tt)*) => {
        $crate::__fold!(@arms $rest ; $($arms)*)
    };
    (@arms $rest:ident ; _ => $body:block $(,)?) => {
        match $rest {
            _ => $body,
        }
    };
    (@arms $rest:ident ; $($arms:tt)+) => {
        $crate::__fold!(@pattern $rest ; [] [] $($arms)+)
    };
    (@pattern $rest:ident ; [$($alts:tt)*] [$($current:tt)+] | $($tail:tt)*) => {
        $crate::__fold!(@pattern $rest ; [$($alts)* [$($current)+]] [] $($tail)*)
    };
    (@pattern $rest:ident ; [$($alts:tt)*] [$($current:tt)+] => $body:block $($tail:tt)*) => {
        $crate::__fold!(@take $rest ; $body ; [$($tail)*] $($alts)* [$($current)+])
    };
    (@pattern $rest:ident ; [$($alts:tt)*] [$($current:tt)*] $next:tt $($tail:tt)*) => {
        $crate::__fold!(@pattern $rest ; [$($alts)*] [$($current)* $next] $($tail)*)
    };
    (@take $rest:ident ; $body:block ; [$(,)? _ => $last:block $(,)?] [$($alt:tt)+]) => {
        match $crate::event::schema::__private::TakeFact::<
            $crate::__fold!(@fact $($alt)+),
            _,
        >::take($rest)
        {
            ::core::result::Result::Ok($($alt)+) => $body,
            ::core::result::Result::Err(_) => $last,
        }
    };
    (@take $rest:ident ; $body:block ; [$($arms:tt)*] [$($alt:tt)+]) => {
        match $crate::event::schema::__private::TakeFact::<
            $crate::__fold!(@fact $($alt)+),
            _,
        >::take($rest)
        {
            ::core::result::Result::Ok($($alt)+) => $body,
            ::core::result::Result::Err($rest) => $crate::__fold!(@arms $rest ; $($arms)*),
        }
    };
    (@take $rest:ident ; $body:block ; [$($arms:tt)*] [$($alt:tt)+] $($alts:tt)+) => {
        match $crate::event::schema::__private::TakeFact::<
            $crate::__fold!(@fact $($alt)+),
            _,
        >::take($rest)
        {
            ::core::result::Result::Ok($($alt)+) => $body,
            ::core::result::Result::Err($rest) => {
                $crate::__fold!(@take $rest ; $body ; [$($arms)*] $($alts)+)
            }
        }
    };
    (@fact $binding:ident @ $($segment:ident)::+ $($pattern:tt)*) => {
        $($segment)::+
    };
    (@fact $($segment:ident)::+ $($pattern:tt)*) => {
        $($segment)::+
    };
}
