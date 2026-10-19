// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::dsl::stage_descriptor::StageDescriptor;
use crate::dsl::typing::TypeHint;
use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::join::{
    left, left_as_of, AsOf, AsOfJoin, AsOfReferenceView, JoinCatalogKind, JoinReferenceMode,
    LeftJoinBuilder, TypedAsOfJoinHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FxRate {
    currency: String,
    effective_at: u32,
    rate: f64,
}

impl TypedPayload for FxRate {
    const EVENT_TYPE: &'static str = "test.as_of.fx_rate";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Payment {
    currency: String,
    booked_at: u32,
}

impl TypedPayload for Payment {
    const EVENT_TYPE: &'static str = "test.as_of.payment";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Priced {
    rate: f64,
}

impl TypedPayload for Priced {
    const EVENT_TYPE: &'static str = "test.as_of.priced";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RateMissing {
    currency: String,
}

impl TypedPayload for RateMissing {
    const EVENT_TYPE: &'static str = "test.as_of.rate_missing";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CurrencyNotQuoted {
    currency: String,
}

impl TypedPayload for CurrencyNotQuoted {
    const EVENT_TYPE: &'static str = "test.as_of.currency_not_quoted";
}

#[derive(Clone, Debug, StageOutputFacts)]
enum PricingOutcome {
    Priced(Priced),
    RateMissing(RateMissing),
    CurrencyNotQuoted(CurrencyNotQuoted),
}

fn price_payment(rate: AsOf<FxRate>, payment: Payment) -> PricingOutcome {
    match rate {
        AsOf::Found(rate) => PricingOutcome::Priced(Priced { rate: rate.rate }),
        AsOf::BeforeFirstVersion => PricingOutcome::RateMissing(RateMissing {
            currency: payment.currency,
        }),
        AsOf::NoVersions => PricingOutcome::CurrencyNotQuoted(CurrencyNotQuoted {
            currency: payment.currency,
        }),
    }
}

#[derive(Clone, Debug)]
struct PriceHandler;

impl TypedAsOfJoinHandler for PriceHandler {
    type State = ();
    type ReferenceKey = String;
    type EffectiveTime = u32;
    type Reference = FxRate;
    type Stream = Payment;
    type Output = PricingOutcome;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(
        &self,
        rate: &Self::Reference,
    ) -> Result<(Self::ReferenceKey, Self::EffectiveTime), HandlerError> {
        Ok((rate.currency.clone(), rate.effective_at))
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut AsOfReferenceView<
            '_,
            Self::ReferenceKey,
            Self::EffectiveTime,
            Self::Reference,
        >,
        payment: Self::Stream,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        let rate = references.select_as_of(&payment.currency, &payment.booked_at);
        Ok(vec![price_payment(rate, payment)])
    }
}

fn exact<T: TypedPayload + 'static>() -> TypeHint {
    TypeHint::exact_payload::<T>()
}

fn assert_point_in_time(join: &dyn StageDescriptor) {
    let metadata = join.typing_metadata().expect("a typed join");
    assert_eq!(metadata.reference_type, exact::<FxRate>());
    assert_eq!(metadata.stream_type, exact::<Payment>());
    assert_eq!(
        metadata.output_contract,
        vec![
            exact::<Priced>(),
            exact::<RateMissing>(),
            exact::<CurrencyNotQuoted>()
        ]
    );
    let shape = join.join_shape().expect("a join");
    assert_eq!(shape.mode, JoinReferenceMode::FiniteEof);
    assert_eq!(shape.catalog, JoinCatalogKind::Versioned);
}

#[test]
fn left_as_of_takes_todays_join_row_with_a_per_fact_output() {
    let price = left_as_of(
        |rate: &FxRate| rate.currency.clone(),
        |rate: &FxRate| rate.effective_at,
        |payment: &Payment| payment.currency.clone(),
        |payment: &Payment| payment.booked_at,
        price_payment,
    );
    let join = crate::join!(
        catalog fx_rates: FxRate,
        Payment -> { Priced, RateMissing, CurrencyNotQuoted } => price
    );
    assert_point_in_time(join.as_ref());
}

#[test]
fn the_builder_takes_the_same_four_accessors() {
    let price = LeftJoinBuilder::<FxRate, Payment, PricingOutcome>::new()
        .catalog_key(|rate: &FxRate| rate.currency.clone())
        .effective_at(|rate: &FxRate| rate.effective_at)
        .stream_key(|payment: &Payment| payment.currency.clone())
        .as_of(|payment: &Payment| payment.booked_at)
        .build(price_payment);
    let join = crate::join!(
        catalog fx_rates: FxRate,
        Payment -> { Priced, RateMissing, CurrencyNotQuoted } => price
    );
    assert_point_in_time(join.as_ref());
}

#[test]
fn an_author_written_handler_bound_with_as_of_join_takes_the_same_row() {
    let price = AsOfJoin::new(PriceHandler);
    let join = crate::join!(
        catalog fx_rates: FxRate,
        Payment -> { Priced, RateMissing, CurrencyNotQuoted } => price
    );
    assert_point_in_time(join.as_ref());
}

#[test]
fn a_latest_value_join_keeps_a_latest_value_catalog() {
    let enrich = left(
        |rate: &FxRate| rate.currency.clone(),
        |payment: &Payment| payment.currency.clone(),
        |rate: Option<FxRate>, _payment: Payment| Priced {
            rate: rate.map_or(1.0, |rate| rate.rate),
        },
    );
    let join = crate::join!(catalog fx_rates: FxRate, Payment -> Priced => enrich);
    let shape = join.join_shape().expect("a join");
    assert_eq!(shape.catalog, JoinCatalogKind::LatestValue);
}
