// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow::queries::{QueryBinding, QueryBookmark, QueryContext, QueryError};
use obzenflow::stages::sources::{
    CsvFile, CsvLine, CsvPage, CsvRead, SourceDiagnosticReason, SourcePollErrorKind,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TicketFixture {
    pub count: u32,
}

pub struct ScriptedTickets {
    count: u32,
}

impl QueryBinding for TicketFixture {
    type System = ScriptedTickets;
    type Evidence = TicketFixture;

    fn evidence(&self) -> TicketFixture {
        self.clone()
    }

    fn resolve(&self) -> Result<ScriptedTickets, QueryError> {
        Ok(ScriptedTickets { count: self.count })
    }
}

#[obzenflow::system("scripted-tickets")]
#[async_trait]
impl CsvFile for ScriptedTickets {
    async fn read(&self, from: &CsvRead, _: &QueryContext) -> Result<CsvPage, QueryError> {
        let columns = [
            "ticket_id",
            "customer_id",
            "created_at",
            "priority",
            "category",
        ];
        let headers = from
            .columns
            .clone()
            .unwrap_or_else(|| columns.iter().map(|name| (*name).to_owned()).collect());
        let mut selected = Vec::with_capacity(headers.len());
        for header in &headers {
            let index = columns
                .iter()
                .position(|name| *name == header.as_str())
                .ok_or_else(|| {
                    QueryError::terminal(
                        SourcePollErrorKind::Validation,
                        SourceDiagnosticReason::UnexpectedShape,
                    )
                })?;
            selected.push(index);
        }

        let first = match &from.after {
            Some(bookmark) => bookmark.decode::<u32>()?,
            None => u32::try_from(from.skip_rows).unwrap_or(u32::MAX),
        };
        if first >= self.count {
            return Ok(CsvPage::EndOfInput);
        }

        let limit = u32::try_from(from.limit.get()).unwrap_or(u32::MAX);
        let end = first.saturating_add(limit).min(self.count);
        let mut records = Vec::new();
        for index in first..end {
            let ticket_id = format!("TKT-{:06}", index + 1);
            let fields = [
                ticket_id.as_str(),
                "CUST-001",
                "2026-01-01T00:00:00Z",
                "P0",
                "billing",
            ];
            let values = selected
                .iter()
                .map(|&column| fields[column].to_owned())
                .collect();
            records.push(CsvLine::Values {
                index: u64::from(index),
                line: u64::from(index) + 2,
                values,
            });
        }
        Ok(CsvPage::Batch {
            headers,
            records,
            next: QueryBookmark::encode(&end)?,
        })
    }
}
