// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use std::fmt;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "&'static str", try_from = "String")]
pub enum Currency {
    UsDollar,
    Euro,
    UkPound,
    Yen,
    Yuan,
    Rupee,
    Ruble,
    CanadianDollar,
    AustralianDollar,
    SwissFranc,
    MexicanPeso,
    BrazilReal,
    Shekel,
    SaudiRiyal,
    Bitcoin,
}

impl Currency {
    const ALL: [Currency; 15] = [
        Self::UsDollar,
        Self::Euro,
        Self::UkPound,
        Self::Yen,
        Self::Yuan,
        Self::Rupee,
        Self::Ruble,
        Self::CanadianDollar,
        Self::AustralianDollar,
        Self::SwissFranc,
        Self::MexicanPeso,
        Self::BrazilReal,
        Self::Shekel,
        Self::SaudiRiyal,
        Self::Bitcoin,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::UsDollar => "US Dollar",
            Self::Euro => "Euro",
            Self::UkPound => "UK Pound",
            Self::Yen => "Yen",
            Self::Yuan => "Yuan",
            Self::Rupee => "Rupee",
            Self::Ruble => "Ruble",
            Self::CanadianDollar => "Canadian Dollar",
            Self::AustralianDollar => "Australian Dollar",
            Self::SwissFranc => "Swiss Franc",
            Self::MexicanPeso => "Mexican Peso",
            Self::BrazilReal => "Brazil Real",
            Self::Shekel => "Shekel",
            Self::SaudiRiyal => "Saudi Riyal",
            Self::Bitcoin => "Bitcoin",
        }
    }

    const fn booked_decimals(self) -> u32 {
        match self {
            Self::UsDollar
            | Self::Euro
            | Self::UkPound
            | Self::Yen
            | Self::Yuan
            | Self::Rupee
            | Self::Ruble
            | Self::CanadianDollar
            | Self::AustralianDollar
            | Self::SwissFranc
            | Self::MexicanPeso
            | Self::BrazilReal
            | Self::Shekel
            | Self::SaudiRiyal => 2,
            Self::Bitcoin => 8,
        }
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl From<Currency> for &'static str {
    fn from(currency: Currency) -> Self {
        currency.label()
    }
}

impl TryFrom<String> for Currency {
    type Error = String;

    fn try_from(label: String) -> Result<Self, Self::Error> {
        Self::ALL
            .into_iter()
            .find(|currency| currency.label() == label)
            .ok_or_else(|| format!("unknown currency '{label}'"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MoneyError {
    NotADecimal,
    TooManyDecimals(u32),
    TooLarge,
    ZeroRate,
}

impl fmt::Display for MoneyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotADecimal => f.write_str("not a plain decimal number"),
            Self::TooManyDecimals(allowed) => write!(f, "more than {allowed} decimal places"),
            Self::TooLarge => f.write_str("too large to hold exactly"),
            Self::ZeroRate => f.write_str("a rate must be greater than zero"),
        }
    }
}

impl std::error::Error for MoneyError {}

fn parse_scaled(text: &str, decimals: u32) -> Result<u128, MoneyError> {
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) if !fraction.is_empty() => (whole, fraction),
        Some(_) => return Err(MoneyError::NotADecimal),
        None => (text, ""),
    };
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return Err(MoneyError::NotADecimal);
    }
    let places = u32::try_from(fraction.len())
        .ok()
        .filter(|places| *places <= decimals)
        .ok_or(MoneyError::TooManyDecimals(decimals))?;
    let units: u128 = format!("{whole}{fraction}")
        .parse()
        .map_err(|_| MoneyError::TooLarge)?;
    10u128
        .checked_pow(decimals - places)
        .and_then(|scale| units.checked_mul(scale))
        .ok_or(MoneyError::TooLarge)
}

fn write_scaled(f: &mut fmt::Formatter<'_>, units: u128, decimals: u32) -> fmt::Result {
    let scale = 10u128.pow(decimals);
    write!(
        f,
        "{}.{:0width$}",
        units / scale,
        units % scale,
        width = decimals as usize
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "MoneyRecord", try_from = "MoneyRecord")]
pub struct Money {
    currency: Currency,
    minor_units: u64,
}

#[derive(Serialize, Deserialize)]
struct MoneyRecord {
    amount: String,
    currency: Currency,
}

impl Money {
    pub fn parse(currency: Currency, amount: &str) -> Result<Self, MoneyError> {
        let minor_units = parse_scaled(amount, currency.booked_decimals())?;
        Ok(Self {
            currency,
            minor_units: u64::try_from(minor_units).map_err(|_| MoneyError::TooLarge)?,
        })
    }

    pub fn currency(self) -> Currency {
        self.currency
    }

    pub fn as_usd(self) -> Option<UsdCents> {
        (self.currency == Currency::UsDollar).then_some(UsdCents(self.minor_units))
    }

    pub fn to_usd_half_up(self, rate: UsdPerUnit) -> Result<UsdCents, MoneyError> {
        let product = u128::from(self.minor_units) * u128::from(rate.mantissa.get());
        let exponent = self.currency.booked_decimals() - 2 + u32::from(rate.decimals);
        let cents = match 10u128.checked_pow(exponent) {
            Some(divisor) if product % divisor * 2 >= divisor => product / divisor + 1,
            Some(divisor) => product / divisor,
            None => 0,
        };
        u64::try_from(cents)
            .map(UsdCents)
            .map_err(|_| MoneyError::TooLarge)
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_scaled(
            f,
            u128::from(self.minor_units),
            self.currency.booked_decimals(),
        )
    }
}

impl From<Money> for MoneyRecord {
    fn from(money: Money) -> Self {
        Self {
            amount: money.to_string(),
            currency: money.currency,
        }
    }
}

impl TryFrom<MoneyRecord> for Money {
    type Error = MoneyError;

    fn try_from(record: MoneyRecord) -> Result<Self, Self::Error> {
        Self::parse(record.currency, &record.amount)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct UsdPerUnit {
    mantissa: NonZeroU64,
    decimals: u8,
}

impl UsdPerUnit {
    pub const fn quoted(mantissa: NonZeroU64, decimals: u8) -> Self {
        Self { mantissa, decimals }
    }

    pub fn parse(rate: &str) -> Result<Self, MoneyError> {
        let places = rate
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.len());
        let decimals =
            u8::try_from(places).map_err(|_| MoneyError::TooManyDecimals(u32::from(u8::MAX)))?;
        let mantissa = parse_scaled(rate, u32::from(decimals))?;
        let mantissa = u64::try_from(mantissa).map_err(|_| MoneyError::TooLarge)?;
        Ok(Self {
            mantissa: NonZeroU64::new(mantissa).ok_or(MoneyError::ZeroRate)?,
            decimals,
        })
    }
}

impl fmt::Display for UsdPerUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.decimals == 0 {
            return write!(f, "{}", self.mantissa);
        }
        write_scaled(f, u128::from(self.mantissa.get()), u32::from(self.decimals))
    }
}

impl From<UsdPerUnit> for String {
    fn from(rate: UsdPerUnit) -> Self {
        rate.to_string()
    }
}

impl TryFrom<String> for UsdPerUnit {
    type Error = MoneyError;

    fn try_from(rate: String) -> Result<Self, Self::Error> {
        Self::parse(&rate)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct UsdCents(u64);

impl UsdCents {
    pub const fn whole_dollars(dollars: u32) -> Self {
        Self(dollars as u64 * 100)
    }
}

impl fmt::Display for UsdCents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_scaled(f, u128::from(self.0), 2)
    }
}

impl From<UsdCents> for String {
    fn from(cents: UsdCents) -> Self {
        cents.to_string()
    }
}

impl TryFrom<String> for UsdCents {
    type Error = MoneyError;

    fn try_from(amount: String) -> Result<Self, Self::Error> {
        u64::try_from(parse_scaled(&amount, 2)?)
            .map(Self)
            .map_err(|_| MoneyError::TooLarge)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct UsdTotal(u128);

impl UsdTotal {
    pub fn plus(self, cents: UsdCents) -> Self {
        Self(self.0 + u128::from(cents.0))
    }
}

impl fmt::Display for UsdTotal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_scaled(f, self.0, 2)
    }
}

impl From<UsdTotal> for String {
    fn from(total: UsdTotal) -> Self {
        total.to_string()
    }
}

impl TryFrom<String> for UsdTotal {
    type Error = MoneyError;

    fn try_from(amount: String) -> Result<Self, Self::Error> {
        parse_scaled(&amount, 2).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn money(currency: Currency, amount: &str) -> Money {
        Money::parse(currency, amount).expect("a booked amount")
    }

    fn rate(rate: &str) -> UsdPerUnit {
        UsdPerUnit::parse(rate).expect("a quoted rate")
    }

    #[test]
    fn every_currency_reads_back_its_label_and_books_at_least_cents() {
        for currency in Currency::ALL {
            assert_eq!(
                Currency::try_from(currency.label().to_owned()),
                Ok(currency)
            );
            assert!(currency.booked_decimals() >= 2, "{currency}");
        }
        assert!(Currency::try_from("Doubloon".to_owned()).is_err());
    }

    #[test]
    fn amounts_parse_exactly_or_not_at_all() {
        assert_eq!(money(Currency::UkPound, "1633.36").to_string(), "1633.36");
        assert_eq!(money(Currency::Yen, "12").to_string(), "12.00");
        assert_eq!(
            money(Currency::Bitcoin, "0.001234").to_string(),
            "0.00123400"
        );
        for (text, refusal) in [
            ("", MoneyError::NotADecimal),
            ("-1.00", MoneyError::NotADecimal),
            ("+1.00", MoneyError::NotADecimal),
            (" 1.00", MoneyError::NotADecimal),
            ("1.", MoneyError::NotADecimal),
            (".5", MoneyError::NotADecimal),
            ("1e3", MoneyError::NotADecimal),
            ("1,000.00", MoneyError::NotADecimal),
            ("1.234", MoneyError::TooManyDecimals(2)),
            ("184467440737095516.16", MoneyError::TooLarge),
        ] {
            assert_eq!(
                Money::parse(Currency::UsDollar, text),
                Err(refusal),
                "{text:?}"
            );
        }
        assert_eq!(
            Money::parse(Currency::Bitcoin, "0.000000001"),
            Err(MoneyError::TooManyDecimals(8))
        );
    }

    #[test]
    fn rates_keep_their_quoted_digits_and_are_never_zero() {
        assert_eq!(rate("0.9904").to_string(), "0.9904");
        assert_eq!(rate("0.01250").to_string(), "0.01250");
        assert_eq!(rate("19827.47").to_string(), "19827.47");
        assert_eq!(rate("1").to_string(), "1");
        assert_eq!(UsdPerUnit::parse("0.000"), Err(MoneyError::ZeroRate));
        assert_eq!(UsdPerUnit::parse("-0.5"), Err(MoneyError::NotADecimal));
        assert_eq!(UsdPerUnit::parse("1e-3"), Err(MoneyError::NotADecimal));
    }

    #[test]
    fn a_conversion_rounds_once_to_the_cent_and_a_tie_goes_up() {
        let euro = money(Currency::Euro, "1.00");
        assert_eq!(
            euro.to_usd_half_up(rate("1.005"))
                .map(|cents| cents.to_string()),
            Ok("1.01".to_owned())
        );
        assert_eq!(
            euro.to_usd_half_up(rate("1.0049"))
                .map(|cents| cents.to_string()),
            Ok("1.00".to_owned())
        );
        let bitcoin = money(Currency::Bitcoin, "0.5");
        assert_eq!(
            bitcoin
                .to_usd_half_up(rate("19827.47"))
                .map(|cents| cents.to_string()),
            Ok("9913.74".to_owned())
        );
        let yen = money(Currency::Yen, "100.00");
        assert_eq!(
            yen.to_usd_half_up(rate("0.007116"))
                .map(|cents| cents.to_string()),
            Ok("0.71".to_owned())
        );
    }

    #[test]
    fn only_us_dollars_are_booked_cents() {
        assert_eq!(
            money(Currency::UsDollar, "250.00").as_usd(),
            Some(UsdCents::whole_dollars(250))
        );
        assert_eq!(money(Currency::Euro, "250.00").as_usd(), None);
    }

    #[test]
    fn a_total_adds_whole_cents() {
        let total = UsdTotal::default()
            .plus(UsdCents::whole_dollars(250))
            .plus(money(Currency::UsDollar, "0.99").as_usd().expect("cents"));
        assert_eq!(total.to_string(), "250.99");
    }

    #[test]
    fn money_reads_back_from_the_journal_and_refuses_floats() {
        let booked = money(Currency::UkPound, "1633.36");
        let json = serde_json::to_value(booked).expect("money serializes");
        assert_eq!(
            json,
            serde_json::json!({"amount": "1633.36", "currency": "UK Pound"})
        );
        let read: Money = serde_json::from_value(json).expect("money reads back");
        assert_eq!(read, booked);
        assert!(serde_json::from_value::<Money>(
            serde_json::json!({"amount": 1633.36, "currency": "UK Pound"})
        )
        .is_err());
        assert!(serde_json::from_value::<UsdPerUnit>(serde_json::json!(0.9904)).is_err());
        assert!(serde_json::from_value::<UsdCents>(serde_json::json!(12.5)).is_err());
    }
}
