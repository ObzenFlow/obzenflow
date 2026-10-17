// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::TypedPayload;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxnId(pub u64);

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountKey {
    pub bank: String,
    pub account: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Day(pub String);

impl Day {
    pub fn of(timestamp: &str) -> Self {
        Self(
            timestamp
                .split_once(' ')
                .map_or(timestamp, |(day, _)| day)
                .to_owned(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PaymentFormat {
    #[serde(rename = "ACH")]
    Ach,
    Bitcoin,
    Cash,
    Cheque,
    #[serde(rename = "Credit Card")]
    CreditCard,
    Reinvestment,
    Wire,
}

impl FromStr for PaymentFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ACH" => Ok(Self::Ach),
            "Bitcoin" => Ok(Self::Bitcoin),
            "Cash" => Ok(Self::Cash),
            "Cheque" => Ok(Self::Cheque),
            "Credit Card" => Ok(Self::CreditCard),
            "Reinvestment" => Ok(Self::Reinvestment),
            "Wire" => Ok(Self::Wire),
            other => Err(format!("unknown payment format '{other}'")),
        }
    }
}

impl fmt::Display for PaymentFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ach => "ACH",
            Self::Bitcoin => "Bitcoin",
            Self::Cash => "Cash",
            Self::Cheque => "Cheque",
            Self::CreditCard => "Credit Card",
            Self::Reinvestment => "Reinvestment",
            Self::Wire => "Wire",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountHistory {
    pub bank: String,
    pub account: String,
    pub payments: u64,
    pub counterparties: u64,
    #[serde(deserialize_with = "payment_formats")]
    pub formats: Vec<PaymentFormat>,
}

impl AccountHistory {
    pub fn key(&self) -> AccountKey {
        AccountKey {
            bank: self.bank.clone(),
            account: self.account.clone(),
        }
    }
}

impl TypedPayload for AccountHistory {
    const EVENT_TYPE: &'static str = "aml.account_history";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub txn_id: TxnId,
    pub timestamp: String,
    pub from_bank: String,
    pub from_account: String,
    pub to_bank: String,
    pub to_account: String,
    pub amount_received: f64,
    pub receiving_currency: String,
    pub amount_paid: f64,
    pub payment_currency: String,
    pub payment_format: PaymentFormat,
    #[serde(deserialize_with = "laundering_label")]
    pub is_laundering: bool,
}

impl Transaction {
    pub fn payer(&self) -> AccountKey {
        AccountKey {
            bank: self.from_bank.clone(),
            account: self.from_account.clone(),
        }
    }

    pub fn payee(&self) -> AccountKey {
        AccountKey {
            bank: self.to_bank.clone(),
            account: self.to_account.clone(),
        }
    }

    pub fn day(&self) -> Day {
        Day::of(&self.timestamp)
    }
}

impl TypedPayload for Transaction {
    const EVENT_TYPE: &'static str = "aml.transaction";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfiledTransaction {
    pub transaction: Transaction,
    pub history: Option<AccountHistory>,
}

impl TypedPayload for ProfiledTransaction {
    const EVENT_TYPE: &'static str = "aml.transaction_profiled";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReasonCode(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RulesVersion(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reasons {
    pub first: ReasonCode,
    pub rest: Vec<ReasonCode>,
}

impl Reasons {
    pub fn from_fired(fired: Vec<ReasonCode>) -> Option<Self> {
        let mut fired = fired.into_iter();
        let first = fired.next()?;
        Some(Self {
            first,
            rest: fired.collect(),
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &ReasonCode> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }
}

impl Serialize for Reasons {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(1 + self.rest.len()))?;
        for reason in self.iter() {
            seq.serialize_element(reason)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Reasons {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fired = Vec::<ReasonCode>::deserialize(deserializer)?;
        Self::from_fired(fired).ok_or_else(|| de::Error::custom("a flag needs at least one reason"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CaseNumber(pub u64);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionCleared {
    pub txn_id: TxnId,
    pub rules_version: RulesVersion,
}

impl TypedPayload for TransactionCleared {
    const EVENT_TYPE: &'static str = "aml.transaction_cleared";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AlertId(pub String);

impl fmt::Display for AlertId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSubject {
    Transaction(TxnId),
    Day(Day),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertRaised {
    pub alert_id: AlertId,
    pub subject: AlertSubject,
    pub account: AccountKey,
    pub reasons: Reasons,
    pub rules_version: RulesVersion,
}

impl AlertRaised {
    pub fn new(
        subject: AlertSubject,
        account: AccountKey,
        reasons: Reasons,
        rules_version: RulesVersion,
    ) -> Self {
        let alert_id = match &subject {
            AlertSubject::Transaction(txn_id) => AlertId(format!("txn:{txn_id}")),
            AlertSubject::Day(day) => AlertId(format!(
                "day:{}:{}/{}",
                day.0, account.bank, account.account
            )),
        };
        Self {
            alert_id,
            subject,
            account,
            reasons,
            rules_version,
        }
    }
}

impl TypedPayload for AlertRaised {
    const EVENT_TYPE: &'static str = "aml.alert_raised";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoreUnavailable {
    pub txn_id: TxnId,
    pub account: AccountKey,
    pub reason: String,
}

impl TypedPayload for ScoreUnavailable {
    const EVENT_TYPE: &'static str = "aml.score_unavailable";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CaseOpened {
    pub case_number: CaseNumber,
    pub alert_id: AlertId,
    pub account: AccountKey,
    pub reasons: Reasons,
    pub rules_version: RulesVersion,
}

impl TypedPayload for CaseOpened {
    const EVENT_TYPE: &'static str = "aml.case_opened";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CaseUnavailable {
    pub alert_id: AlertId,
    pub account: AccountKey,
    pub reason: String,
}

impl TypedPayload for CaseUnavailable {
    const EVENT_TYPE: &'static str = "aml.case_unavailable";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UsdRates(pub BTreeMap<String, f64>);

impl UsdRates {
    pub fn usd(&self, amount: f64, currency: &str) -> Option<f64> {
        self.0.get(currency).map(|rate| amount * rate)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DayTotals {
    pub transactions: u64,
    pub paid_usd: f64,
    pub unpriced: u64,
    pub by_format: BTreeMap<PaymentFormat, u64>,
    pub labelled_laundering: u64,
}

impl DayTotals {
    pub fn add(&mut self, transaction: &Transaction, usd_rates: &UsdRates) {
        self.transactions += 1;
        match usd_rates.usd(transaction.amount_paid, &transaction.payment_currency) {
            Some(usd) => self.paid_usd += usd,
            None => self.unpriced += 1,
        }
        *self
            .by_format
            .entry(transaction.payment_format)
            .or_default() += 1;
        if transaction.is_laundering {
            self.labelled_laundering += 1;
        }
    }
}

impl TypedPayload for DayTotals {
    const EVENT_TYPE: &'static str = "aml.day_totals";
    const SCHEMA_VERSION: u32 = 1;
}

fn laundering_label<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    struct Label;

    impl<'de> Visitor<'de> for Label {
        type Value = bool;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("0, 1, true or false")
        }

        fn visit_bool<E: de::Error>(self, value: bool) -> Result<bool, E> {
            Ok(value)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<bool, E> {
            match value {
                0 => Ok(false),
                1 => Ok(true),
                other => Err(E::custom(format!("laundering label {other} is not 0 or 1"))),
            }
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<bool, E> {
            u64::try_from(value)
                .map_err(|_| E::custom(format!("laundering label {value} is not 0 or 1")))
                .and_then(|value| self.visit_u64(value))
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<bool, E> {
            match value {
                "0" | "false" => Ok(false),
                "1" | "true" => Ok(true),
                other => Err(E::custom(format!(
                    "laundering label '{other}' is not 0 or 1"
                ))),
            }
        }
    }

    deserializer.deserialize_any(Label)
}

fn payment_formats<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<PaymentFormat>, D::Error> {
    struct Formats;

    impl<'de> Visitor<'de> for Formats {
        type Value = Vec<PaymentFormat>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("payment formats separated by ';' or a list of them")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            value
                .split(';')
                .filter(|format| !format.is_empty())
                .map(|format| format.parse().map_err(E::custom))
                .collect()
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut formats = Vec::new();
            while let Some(format) = seq.next_element()? {
                formats.push(format);
            }
            Ok(formats)
        }
    }

    deserializer.deserialize_any(Formats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(bank: &str, account: &str) -> AccountKey {
        AccountKey {
            bank: bank.to_owned(),
            account: account.to_owned(),
        }
    }

    fn reasons(code: &str) -> Reasons {
        Reasons {
            first: ReasonCode(code.to_owned()),
            rest: Vec::new(),
        }
    }

    fn transaction(
        format: PaymentFormat,
        amount: f64,
        currency: &str,
        laundering: bool,
    ) -> Transaction {
        Transaction {
            txn_id: TxnId(2800001),
            timestamp: "2022/09/05 10:15".to_owned(),
            from_bank: "48".to_owned(),
            from_account: "81F0A0001".to_owned(),
            to_bank: "48".to_owned(),
            to_account: "81F0B0001".to_owned(),
            amount_received: amount,
            receiving_currency: currency.to_owned(),
            amount_paid: amount,
            payment_currency: currency.to_owned(),
            payment_format: format,
            is_laundering: laundering,
        }
    }

    #[test]
    fn alert_ids_derive_from_domain_fields_alone() {
        let payer = account("70", "8000A1B2C");
        let flagged = AlertRaised::new(
            AlertSubject::Transaction(TxnId(2817341)),
            payer.clone(),
            reasons("format_switch"),
            RulesVersion("sim-2026.10".to_owned()),
        );
        assert_eq!(flagged.alert_id, AlertId("txn:2817341".to_owned()));

        let day = AlertRaised::new(
            AlertSubject::Day(Day::of("2022/09/05 23:55")),
            payer,
            reasons("fan_out"),
            RulesVersion("aml-daily-profile-v1".to_owned()),
        );
        assert_eq!(
            day.alert_id,
            AlertId("day:2022/09/05:70/8000A1B2C".to_owned())
        );
    }

    #[test]
    fn day_totals_count_unpriced_payments_instead_of_failing() {
        let rates = UsdRates(BTreeMap::from([
            ("US Dollar".to_owned(), 1.0),
            ("Euro".to_owned(), 1.09),
        ]));
        let mut totals = DayTotals::default();
        totals.add(
            &transaction(PaymentFormat::Ach, 100.0, "US Dollar", false),
            &rates,
        );
        totals.add(
            &transaction(PaymentFormat::Wire, 200.0, "Euro", true),
            &rates,
        );
        totals.add(
            &transaction(PaymentFormat::Ach, 50.0, "Doubloon", false),
            &rates,
        );

        assert_eq!(totals.transactions, 3);
        assert!(
            (totals.paid_usd - 318.0).abs() < 1e-9,
            "{}",
            totals.paid_usd
        );
        assert_eq!(totals.unpriced, 1);
        assert_eq!(
            totals.by_format,
            BTreeMap::from([(PaymentFormat::Ach, 2), (PaymentFormat::Wire, 1)])
        );
        assert_eq!(totals.labelled_laundering, 1);
    }
}
