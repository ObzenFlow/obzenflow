// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exact US dollar amounts and balances, written and read as decimal text.

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MoneyError {
    NotADecimal,
    TooManyDecimals,
    TooLarge,
}

impl fmt::Display for MoneyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotADecimal => f.write_str("not a plain decimal number"),
            Self::TooManyDecimals => f.write_str("more than two decimal places"),
            Self::TooLarge => f.write_str("too large to hold exactly"),
        }
    }
}

impl std::error::Error for MoneyError {}

fn parse_cents(text: &str) -> Result<u64, MoneyError> {
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) if !fraction.is_empty() => (whole, fraction),
        Some(_) => return Err(MoneyError::NotADecimal),
        None => (text, ""),
    };
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return Err(MoneyError::NotADecimal);
    }
    if fraction.len() > 2 {
        return Err(MoneyError::TooManyDecimals);
    }
    let dollars: u64 = whole.parse().map_err(|_| MoneyError::TooLarge)?;
    let cents: u64 = format!("{fraction:0<2}")
        .parse()
        .map_err(|_| MoneyError::TooLarge)?;
    dollars
        .checked_mul(100)
        .and_then(|dollars| dollars.checked_add(cents))
        .ok_or(MoneyError::TooLarge)
}

fn write_cents(f: &mut fmt::Formatter<'_>, cents: u64) -> fmt::Result {
    write!(f, "{}.{:02}", cents / 100, cents % 100)
}

/// A dollar amount, exact to the cent, written and read as "49.99".
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct UsdCents(u64);

impl UsdCents {
    pub const fn whole_dollars(dollars: u32) -> Self {
        Self(dollars as u64 * 100)
    }

    pub fn parse(amount: &str) -> Result<Self, MoneyError> {
        parse_cents(amount).map(Self)
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn plus(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    /// The amount as a statement prints it, "$49.99".
    pub fn dollars(self) -> String {
        format!("${self}")
    }
}

impl fmt::Display for UsdCents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_cents(f, self.0)
    }
}

impl From<UsdCents> for String {
    fn from(amount: UsdCents) -> Self {
        amount.to_string()
    }
}

impl TryFrom<String> for UsdCents {
    type Error = MoneyError;

    fn try_from(amount: String) -> Result<Self, Self::Error> {
        Self::parse(&amount)
    }
}

/// An account's balance, which a returned payment's fee can take below zero,
/// written and read as "31.20" or "-3.80".
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct Balance(i64);

impl Balance {
    pub fn parse(balance: &str) -> Result<Self, MoneyError> {
        let (negative, magnitude) = match balance.strip_prefix('-') {
            Some(magnitude) => (true, magnitude),
            None => (false, balance),
        };
        let cents = i64::try_from(parse_cents(magnitude)?).map_err(|_| MoneyError::TooLarge)?;
        Ok(Self(if negative { -cents } else { cents }))
    }

    pub fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// Whether the balance pays this amount in full.
    pub fn covers(self, amount: UsdCents) -> bool {
        self.0 >= Self::signed(amount)
    }

    pub fn credited(self, amount: UsdCents) -> Self {
        Self(self.0.saturating_add(Self::signed(amount)))
    }

    pub fn debited(self, amount: UsdCents) -> Self {
        Self(self.0.saturating_sub(Self::signed(amount)))
    }

    fn signed(amount: UsdCents) -> i64 {
        i64::try_from(amount.0).unwrap_or(i64::MAX)
    }

    /// The balance as a statement prints it, "$31.20" or "-$3.80".
    pub fn dollars(self) -> String {
        let sign = if self.0 < 0 { "-" } else { "" };
        format!("{sign}${}", UsdCents(self.0.unsigned_abs()))
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 < 0 {
            f.write_str("-")?;
        }
        write_cents(f, self.0.unsigned_abs())
    }
}

impl From<Balance> for String {
    fn from(balance: Balance) -> Self {
        balance.to_string()
    }
}

impl TryFrom<String> for Balance {
    type Error = MoneyError;

    fn try_from(balance: String) -> Result<Self, Self::Error> {
        Self::parse(&balance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usd_cents_decimal_text_parses_exactly() {
        assert_eq!(UsdCents::parse("49.99"), Ok(UsdCents(4999)));
        assert_eq!(UsdCents::parse("35"), Ok(UsdCents::whole_dollars(35)));
        assert_eq!(UsdCents::parse("0.5"), Ok(UsdCents(50)));
        assert_eq!(UsdCents(4999).to_string(), "49.99");
        assert_eq!(UsdCents(4999).dollars(), "$49.99");
    }

    #[test]
    fn usd_cents_malformed_text_refused() {
        for (text, refusal) in [
            ("", MoneyError::NotADecimal),
            ("-1.00", MoneyError::NotADecimal),
            ("$1.00", MoneyError::NotADecimal),
            ("1.", MoneyError::NotADecimal),
            (".5", MoneyError::NotADecimal),
            ("1e3", MoneyError::NotADecimal),
            ("1,000.00", MoneyError::NotADecimal),
            ("1.234", MoneyError::TooManyDecimals),
            ("184467440737095516.16", MoneyError::TooLarge),
        ] {
            assert_eq!(UsdCents::parse(text), Err(refusal), "{text:?}");
        }
    }

    #[test]
    fn balance_negative_text_round_trips() {
        let overdrawn = Balance::parse("-3.80").expect("a balance");
        assert!(overdrawn.is_negative());
        assert_eq!(overdrawn.to_string(), "-3.80");
        assert_eq!(overdrawn.dollars(), "-$3.80");
        assert_eq!(
            Balance::parse("31.20").map(|b| b.dollars()),
            Ok("$31.20".to_owned())
        );
    }

    #[test]
    fn balance_covers_equal_amount() {
        let balance = Balance::parse("31.20").expect("a balance");
        assert!(balance.covers(UsdCents::parse("31.20").expect("an amount")));
        assert!(!balance.covers(UsdCents::parse("31.21").expect("an amount")));
        assert_eq!(
            balance.debited(UsdCents::whole_dollars(35)),
            Balance::parse("-3.80").expect("a balance")
        );
        assert_eq!(
            Balance::parse("-3.80")
                .expect("a balance")
                .credited(UsdCents::parse("3.80").expect("an amount")),
            Balance::default()
        );
    }

    #[test]
    fn money_json_floats_refused() {
        assert_eq!(
            serde_json::to_value(UsdCents(4999)).expect("an amount serializes"),
            serde_json::json!("49.99")
        );
        assert_eq!(
            serde_json::from_value::<Balance>(serde_json::json!("-3.80")).ok(),
            Balance::parse("-3.80").ok()
        );
        assert!(serde_json::from_value::<UsdCents>(serde_json::json!(49.99)).is_err());
        assert!(serde_json::from_value::<Balance>(serde_json::json!(-3.8)).is_err());
    }
}
