// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Last observed commitments, displayed without merging independent journals.

use super::*;
use obzenflow_core::event::{CausalCoordinate, JournalWriterId};
use obzenflow_core::JournalId;

const DEFAULT_MATRIX_LIMIT: usize = 16;
const DIAGONAL: &str = "1;4;38;5;252";

struct ClockRow<'a> {
    id: JournalId,
    number: usize,
    name: String,
    values: Option<&'a BTreeMap<CausalCoordinate, u64>>,
}

impl Renderer {
    pub(in crate::render) fn clock_summary(
        &self,
        output: &mut impl Write,
        progress: &RunReadProgress,
    ) -> Result<(), Error> {
        let referenced: BTreeSet<_> = self
            .context
            .journals
            .values()
            .filter_map(|journal| journal.last_clock.as_ref())
            .flat_map(|clock| clock.values.keys())
            .map(|coordinate| *coordinate.journal_writer_id.as_journal_id())
            .collect();
        let journals = self.context.journals.values().filter(|journal| {
            journal.journal.kind != RunJournalKind::Error
                || journal.last_clock.is_some()
                || referenced.contains(&journal.journal.id)
        });
        let mut rows: Vec<_> = journals
            .map(|journal| ClockRow {
                id: journal.journal.id,
                number: self.context.journal_number(&journal.journal.id),
                name: safe_text(&journal.name),
                values: journal.last_clock.as_ref().map(|clock| &clock.values),
            })
            .collect();
        // Referenced histories outside this archive still need a column. Their
        // own last records are unobserved, so they receive no invented clock.
        rows.extend(
            referenced
                .iter()
                .filter(|id| !self.context.journals.contains_key(id))
                .map(|id| ClockRow {
                    id: *id,
                    number: self.context.journal_number(id),
                    name: id.to_string(),
                    values: None,
                }),
        );
        rows.sort_by_key(|row| row.number);
        if rows.is_empty() {
            return Ok(());
        }

        writeln!(output)?;
        self.summary_line(
            output,
            HEADING,
            if progress.settled_prefix.is_some() {
                "FINAL JOURNAL CLOCKS"
            } else {
                "LAST OBSERVED JOURNAL CLOCKS"
            },
        )?;
        if rows.len() > DEFAULT_MATRIX_LIMIT && !self.full {
            return self.summary_line(
                output,
                MUTED,
                &format!(
                    "{} journals; use --full to show the complete clock matrix.",
                    rows.len()
                ),
            );
        }
        for line in [
            "Rows: last recorded clocks, including runtime. Columns use the same journal numbers as event clocks.",
            "Cell: latest counter from the column journal included in the row's clock.",
            "Diagonal: own counter. 0: no recorded history. —: no event observed.",
        ] {
            self.summary_line(output, MUTED, line)?;
        }

        let index_width = rows.last().unwrap().number.to_string().len();
        let counter_width = rows
            .iter()
            .filter_map(|row| row.values)
            .flat_map(|values| values.values())
            .map(|value| value.to_string().len())
            .max()
            .unwrap_or(1)
            .max(index_width);
        let name_width = rows
            .iter()
            .map(|row| row.name.chars().count())
            .max()
            .unwrap_or(7)
            .max(7)
            .min(self.width / 3)
            .min(self.width.saturating_sub(index_width + counter_width + 6))
            .max(1);
        let label_width = index_width + name_width + 4;
        let columns_per_panel =
            (self.width.saturating_sub(label_width) / (counter_width + 2)).max(1);

        for columns in (0..rows.len())
            .collect::<Vec<_>>()
            .chunks(columns_per_panel)
        {
            writeln!(output)?;
            let mut header = format!("  {:>index_width$}  {:<name_width$}", "#", "Journal");
            for column in columns {
                header.push_str(&format!("  {:>counter_width$}", rows[*column].number));
            }
            self.summary_write(output, MUTED, &header)?;
            for (index, row) in rows.iter().enumerate() {
                let names = cell_lines(&row.name, name_width);
                let mut line = format!("  {:>index_width$}  {:<name_width$}", row.number, names[0]);
                for column in columns {
                    let coordinate =
                        CausalCoordinate::new(JournalWriterId::from_journal_id(rows[*column].id));
                    let cell = row.values.map_or_else(
                        || "—".into(),
                        |values| values.get(&coordinate).copied().unwrap_or(0).to_string(),
                    );
                    line.push_str(&" ".repeat(counter_width + 2 - cell.chars().count()));
                    if self.color {
                        let shade = if *column == index && row.values.is_some() {
                            DIAGONAL
                        } else if cell == "0" || row.values.is_none() {
                            MUTED
                        } else {
                            BODY
                        };
                        line.push_str(&format!("\x1b[{shade}m{cell}\x1b[0m"));
                    } else {
                        line.push_str(&cell);
                    }
                }
                self.summary_write(output, BODY, &line)?;
                for name in names.iter().skip(1) {
                    self.summary_write(output, BODY, &format!("  {:index_width$}  {name}", ""))?;
                }
            }
        }
        Ok(())
    }
}
