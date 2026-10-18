// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::DayTotals;

pub fn format_totals(totals: &DayTotals) -> String {
    let by_format = totals
        .by_format
        .iter()
        .map(|(format, count)| format!("{format} {count}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Day totals: {} transactions, {} labelled as laundering. By format, {by_format}.",
        totals.transactions, totals.labelled_laundering
    )
}

#[cfg(test)]
mod tests {
    use super::super::domain::PaymentFormat;
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn totals_read_as_one_line() {
        let totals = DayTotals {
            transactions: 3,
            by_format: BTreeMap::from([(PaymentFormat::Ach, 2), (PaymentFormat::CreditCard, 1)]),
            labelled_laundering: 1,
        };
        assert_eq!(
            format_totals(&totals),
            "Day totals: 3 transactions, 1 labelled as laundering. By format, ACH 2, Credit Card 1."
        );
    }
}
