// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::money::{Currency, Money, MoneyError, UsdCents, UsdPerUnit, UsdTotal};
use chrono::NaiveDateTime;
use obzenflow::schema::TypedPayload;
use obzenflow::stages::joins::AsOf;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

pub const AMLWORLD_TIME: &str = "%Y/%m/%d %H:%M";

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
    pub fn of(timestamp: NaiveDateTime) -> Self {
        Self(timestamp.format("%Y/%m/%d").to_string())
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
    #[serde(with = "amlworld_time")]
    pub timestamp: NaiveDateTime,
    pub from_bank: String,
    pub from_account: String,
    pub to_bank: String,
    pub to_account: String,
    pub received: Money,
    pub paid: Money,
    pub payment_format: PaymentFormat,
    #[serde(deserialize_with = "laundering_label")]
    pub is_laundering: bool,
}

#[derive(Debug, Deserialize)]
pub struct TransactionRow {
    txn_id: TxnId,
    #[serde(with = "amlworld_time")]
    timestamp: NaiveDateTime,
    from_bank: String,
    from_account: String,
    to_bank: String,
    to_account: String,
    amount_received: String,
    receiving_currency: Currency,
    amount_paid: String,
    payment_currency: Currency,
    payment_format: PaymentFormat,
    #[serde(deserialize_with = "laundering_label")]
    is_laundering: bool,
}

impl TryFrom<TransactionRow> for Transaction {
    type Error = &'static str;

    fn try_from(row: TransactionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            txn_id: row.txn_id,
            timestamp: row.timestamp,
            from_bank: row.from_bank,
            from_account: row.from_account,
            to_bank: row.to_bank,
            to_account: row.to_account,
            received: Money::parse(row.receiving_currency, &row.amount_received)
                .map_err(|_| "amount_received")?,
            paid: Money::parse(row.payment_currency, &row.amount_paid)
                .map_err(|_| "amount_paid")?,
            payment_format: row.payment_format,
            is_laundering: row.is_laundering,
        })
    }
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
        Day::of(self.timestamp)
    }
}

impl TypedPayload for Transaction {
    const EVENT_TYPE: &'static str = "aml.transaction";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FxRate {
    pub currency: Currency,
    pub effective_at: NaiveDateTime,
    pub usd_per_unit: UsdPerUnit,
}

impl TypedPayload for FxRate {
    const EVENT_TYPE: &'static str = "aml.fx_rate";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateInForce {
    pub effective_at: NaiveDateTime,
    pub usd_per_unit: UsdPerUnit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsdValue {
    AsBooked(UsdCents),
    Converted { rate: RateInForce, usd: UsdCents },
    Unpriced,
}

impl UsdValue {
    pub fn cents(self) -> Option<UsdCents> {
        match self {
            Self::AsBooked(usd) | Self::Converted { usd, .. } => Some(usd),
            Self::Unpriced => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PricedTransaction {
    pub transaction: Transaction,
    pub value: UsdValue,
}

impl PricedTransaction {
    pub fn at(rate: AsOf<FxRate>, transaction: Transaction) -> Result<Self, MoneyError> {
        let value = match (transaction.paid.as_usd(), rate) {
            (Some(usd), _) => UsdValue::AsBooked(usd),
            (None, AsOf::Found(rate)) => UsdValue::Converted {
                usd: transaction.paid.to_usd_half_up(rate.usd_per_unit)?,
                rate: RateInForce {
                    effective_at: rate.effective_at,
                    usd_per_unit: rate.usd_per_unit,
                },
            },
            (None, AsOf::BeforeFirstVersion | AsOf::NoVersions) => UsdValue::Unpriced,
        };
        Ok(Self { transaction, value })
    }
}

impl TypedPayload for PricedTransaction {
    const EVENT_TYPE: &'static str = "aml.transaction_priced";
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DayTotals {
    pub transactions: u64,
    pub paid_usd: UsdTotal,
    pub unpriced: u64,
    pub by_format: BTreeMap<PaymentFormat, u64>,
    pub labelled_laundering: u64,
}

impl DayTotals {
    pub fn add(&mut self, priced: &PricedTransaction) {
        let transaction = &priced.transaction;
        self.transactions += 1;
        match priced.value.cents() {
            Some(usd) => self.paid_usd = self.paid_usd.plus(usd),
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

mod amlworld_time {
    use super::AMLWORLD_TIME;
    use chrono::NaiveDateTime;
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        timestamp: &NaiveDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&timestamp.format(AMLWORLD_TIME))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<NaiveDateTime, D::Error> {
        let text = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&text, AMLWORLD_TIME)
            .map_err(|_| de::Error::custom(format!("timestamp '{text}' is not YYYY/MM/DD HH:MM")))
    }
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

    fn booked(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, AMLWORLD_TIME).expect("an AMLworld timestamp")
    }

    fn rate(currency: Currency, usd_per_unit: &str) -> FxRate {
        FxRate {
            currency,
            effective_at: booked("2022/09/05 10:00"),
            usd_per_unit: UsdPerUnit::parse(usd_per_unit).expect("a quoted rate"),
        }
    }

    fn transaction(
        format: PaymentFormat,
        currency: Currency,
        amount: &str,
        laundering: bool,
    ) -> Transaction {
        let booked_amount = Money::parse(currency, amount).expect("a booked amount");
        Transaction {
            txn_id: TxnId(2800001),
            timestamp: booked("2022/09/05 10:15"),
            from_bank: "48".to_owned(),
            from_account: "81F0A0001".to_owned(),
            to_bank: "48".to_owned(),
            to_account: "81F0B0001".to_owned(),
            received: booked_amount,
            paid: booked_amount,
            payment_format: format,
            is_laundering: laundering,
        }
    }

    fn row(amount_paid: &str) -> TransactionRow {
        TransactionRow {
            txn_id: TxnId(2800001),
            timestamp: booked("2022/09/05 10:15"),
            from_bank: "48".to_owned(),
            from_account: "81F0A0001".to_owned(),
            to_bank: "12".to_owned(),
            to_account: "80890E60D".to_owned(),
            amount_received: "1633.36".to_owned(),
            receiving_currency: Currency::UkPound,
            amount_paid: amount_paid.to_owned(),
            payment_currency: Currency::UsDollar,
            payment_format: PaymentFormat::Cash,
            is_laundering: false,
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
            AlertSubject::Day(Day::of(booked("2022/09/05 23:55"))),
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
    fn timestamps_and_amounts_keep_their_booked_spelling_in_the_journal() {
        let booked = transaction(PaymentFormat::Ach, Currency::UsDollar, "100.00", false);
        let json = serde_json::to_value(&booked).expect("a transaction serializes");
        assert_eq!(json["timestamp"], "2022/09/05 10:15");
        assert_eq!(
            json["paid"],
            serde_json::json!({"amount": "100.00", "currency": "US Dollar"})
        );
        let read: Transaction = serde_json::from_value(json).expect("and reads back");
        assert_eq!(read, booked);
    }

    #[test]
    fn a_row_becomes_a_transaction_only_when_every_amount_is_exact() {
        let transaction = Transaction::try_from(row("250.00")).expect("a valid row");
        assert_eq!(
            transaction.paid.as_usd(),
            Some(UsdCents::whole_dollars(250))
        );
        assert_eq!(transaction.received.currency(), Currency::UkPound);
        assert_eq!(
            Transaction::try_from(row("250.005")).map(|_| ()),
            Err("amount_paid")
        );
    }

    #[test]
    fn day_totals_add_each_payments_cents_and_count_the_unpriced() {
        let mut totals = DayTotals::default();
        for (rate, transaction) in [
            (
                AsOf::NoVersions,
                transaction(PaymentFormat::Ach, Currency::UsDollar, "100.00", false),
            ),
            (
                AsOf::Found(rate(Currency::Euro, "1.09")),
                transaction(PaymentFormat::Wire, Currency::Euro, "200.00", true),
            ),
            (
                AsOf::BeforeFirstVersion,
                transaction(PaymentFormat::Ach, Currency::Yuan, "50.00", false),
            ),
        ] {
            totals.add(&PricedTransaction::at(rate, transaction).expect("a priced payment"));
        }

        assert_eq!(totals.transactions, 3);
        assert_eq!(totals.paid_usd.to_string(), "318.00");
        assert_eq!(totals.unpriced, 1);
        assert_eq!(
            totals.by_format,
            BTreeMap::from([(PaymentFormat::Ach, 2), (PaymentFormat::Wire, 1)])
        );
        assert_eq!(totals.labelled_laundering, 1);
    }
}
