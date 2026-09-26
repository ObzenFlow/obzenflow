// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Output policy and terminal layout over typed views. Event classification and
//! identity resolution belong to view.rs; this module never matches core events.

use super::payload::{abbreviated, compact, pretty, safe_text, wrap_fields};
use super::view::{
    BodyView, Category, ClockView, EventView, HeaderView, InputsView, ProgressView, ReplayNote,
    UpstreamView,
};
use super::Error;
use serde::Serialize;
use std::io::Write;

#[derive(Clone, Copy)]
pub(super) enum OutputMode {
    Jsonl,
    Compact,
    Expanded { full: bool, explain: bool },
}

pub(super) struct RenderOptions {
    pub mode: OutputMode,
    pub width: usize,
    pub color: bool,
}

pub(super) struct TerminalRenderer {
    pub options: RenderOptions,
}

#[derive(Clone, Copy)]
enum Role {
    Normal,
    Reporter,
    Output,
    JournalCounter,
    Muted,
}

struct Span {
    text: String,
    role: Role,
}

impl Span {
    fn new(text: impl AsRef<str>, role: Role) -> Self {
        Self {
            text: safe_text(text.as_ref()),
            role,
        }
    }
}

struct Palette {
    normal: u8,
    secondary: u8,
    primary: u8,
    bold: bool,
}

impl From<&HeaderView<'_>> for Palette {
    fn from(header: &HeaderView<'_>) -> Self {
        let (normal, secondary, primary) = match header.category {
            Category::Fact if header.read_model => (114, 157, 194),
            Category::Fact => (208, 215, 223),
            Category::Effect | Category::Delivery => (217, 224, 231),
            Category::Runtime => (245, 250, 255),
        };
        Self {
            normal,
            secondary,
            primary,
            bold: header.category == Category::Fact,
        }
    }
}

impl TerminalRenderer {
    pub fn render(&self, output: &mut impl Write, view: &EventView<'_>) -> Result<(), Error> {
        if matches!(self.options.mode, OutputMode::Jsonl) {
            // The universal view retains the source. Machine output never
            // needs human aliases, parent resolution, payload projection or proof.
            serde_json::to_writer(&mut *output, view.source())?;
            writeln!(output)?;
            return Ok(());
        }
        let header = view.header();
        let relation = view.relation();
        let palette = Palette::from(&header);
        let inputs = match &relation.inputs {
            InputsView::Source => "()".to_owned(),
            InputsView::None => String::new(),
            InputsView::Recorded(inputs) => {
                let mut labels: Vec<_> = inputs
                    .iter()
                    .take(3)
                    .map(|input| input.unwrap_or("recorded input (unresolved)").to_owned())
                    .collect();
                if labels.is_empty() {
                    labels.push("input not recorded".into());
                }
                if inputs.len() > 3 {
                    labels.push(format!("{} more recorded parents", inputs.len() - 3));
                }
                format!("({})", labels.join(", "))
            }
        };
        let replay = match relation.replay {
            ReplayNote::None => "",
            ReplayNote::Replayed => " [replay]",
            ReplayNote::RecordedEffect => " [read from journal]",
        };
        let error = if relation.processing_error.is_some() {
            " [processing error]"
        } else {
            ""
        };
        let relation_spans = [
            Span::new(relation.output, Role::Output),
            Span::new(
                format!(" ← {}{inputs}{replay}{error}", header.reporter),
                Role::Normal,
            ),
        ];
        if matches!(self.options.mode, OutputMode::Compact) {
            let mut line = format!("{}  {}", header.heading, plain(&relation_spans));
            if let Some(body) = self.compact_body(&view.body())? {
                line.push_str("  ");
                line.push_str(&body);
            }
            writeln!(
                output,
                "{}",
                self.paint(
                    &palette,
                    &abbreviated(&line, self.options.width),
                    Role::Normal
                )
            )?;
            return Ok(());
        }
        let OutputMode::Expanded { full, explain } = self.options.mode else {
            unreachable!()
        };
        self.spans(
            output,
            &palette,
            &[
                Span::new(format!("{} (stage: ", header.heading), Role::Normal),
                Span::new(&header.reporter, Role::Reporter),
                Span::new(format!(", journal: {})", header.journal), Role::Normal),
            ],
        )?;
        self.spans(output, &palette, &relation_spans)?;
        writeln!(output, "{}", self.clock(&view.clock(), &header))?;
        if explain || full {
            let evidence = view.evidence();
            writeln!(output, "causal proof: {}", compact(&evidence.proof)?)?;
            writeln!(
                output,
                "committed witnesses: {}",
                compact(evidence.witnesses)?
            )?;
        }
        if let Some(message) = relation.processing_error {
            for line in wrap_fields(
                &[format!("processing error: {}", safe_text(message))],
                self.options.width,
            ) {
                writeln!(output, "{line}")?;
            }
        }
        if full {
            // One complete source record. Do not repeat a projected payload
            // above the same payload inside its canonical envelope.
            self.json(output, view.source(), false)?;
        } else {
            self.body(output, &view.body(), &header, explain)?;
        }
        if explain {
            self.spans(
                output,
                &palette,
                &[Span::new(view.explanation(), Role::Muted)],
            )?;
        }
        writeln!(output)?;
        Ok(())
    }

    fn compact_body(&self, body: &BodyView<'_>) -> Result<Option<String>, Error> {
        Ok(match body {
            BodyView::Verbatim(payload) => Some(compact(payload)?),
            BodyView::EffectOutcome(payload) => Some(compact(payload)?),
            BodyView::EffectAttempt(payload) => Some(compact(payload)?),
            BodyView::EffectRecovery(payload) => Some(compact(payload)?),
            BodyView::ConsumptionProgress(progress) => Some(format!(
                "Input: {} · {}",
                safe_text(&upstream(&progress.upstream)),
                plain(&progress_fields(progress))
            )),
            BodyView::MetricsExport(_) => None,
        })
    }

    fn body(
        &self,
        output: &mut impl Write,
        body: &BodyView<'_>,
        header: &HeaderView<'_>,
        explain: bool,
    ) -> Result<(), Error> {
        let runtime = header.category == Category::Runtime;
        let palette = Palette::from(header);
        match body {
            BodyView::Verbatim(payload) => self.json(output, payload, runtime)?,
            BodyView::EffectOutcome(payload) => self.json(output, payload, runtime)?,
            BodyView::EffectAttempt(payload) => self.json(output, payload, runtime)?,
            BodyView::EffectRecovery(payload) => self.json(output, payload, runtime)?,
            BodyView::MetricsExport(clock) if explain => {
                self.spans(
                    output,
                    &palette,
                    &[Span::new("Export watermark (recorded):", Role::Muted)],
                )?;
                writeln!(output, "{}", self.clock(clock, header))?;
            }
            BodyView::MetricsExport(_) => {}
            BodyView::ConsumptionProgress(progress) => {
                self.spans(
                    output,
                    &palette,
                    &[
                        Span::new("Input: ", Role::Normal),
                        Span::new(upstream(&progress.upstream), Role::Output),
                    ],
                )?;
                self.spans(output, &palette, &progress_fields(progress))?;
                if explain {
                    if let Some(clock) = &progress.input_clock {
                        self.spans(
                            output,
                            &palette,
                            &[Span::new("Input watermark (recorded):", Role::Muted)],
                        )?;
                        writeln!(output, "{}", self.clock(clock, header))?;
                    }
                    if let Some(clock) = &progress.advertised_clock {
                        if progress
                            .input_clock
                            .as_ref()
                            .is_some_and(|input| input.same_history(clock))
                        {
                            self.spans(
                                output,
                                &palette,
                                &[Span::new(
                                    "Advertised clock: same recorded clock as the input watermark.",
                                    Role::Muted,
                                )],
                            )?;
                        } else {
                            self.spans(
                                output,
                                &palette,
                                &[Span::new("Advertised clock (recorded):", Role::Muted)],
                            )?;
                            writeln!(output, "{}", self.clock(clock, header))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn json(
        &self,
        output: &mut impl Write,
        value: &impl Serialize,
        muted: bool,
    ) -> Result<(), Error> {
        for line in pretty(value)?.lines() {
            if self.options.color && muted {
                writeln!(output, "\x1b[38;5;245m{line}\x1b[0m")?;
            } else {
                writeln!(output, "{line}")?;
            }
        }
        Ok(())
    }

    pub fn clock(&self, clock: &ClockView<'_>, header: &HeaderView<'_>) -> String {
        let palette = Palette::from(header);
        let mut result = self.paint(&palette, "⟨", Role::Muted);
        let mut columns = 1;
        for (index, cell) in clock.cells().iter().enumerate() {
            let label = format!("{}:", cell.journal);
            let digits = cell.counter.to_string();
            let cell_width = label.len() + digits.len();
            if index > 0 {
                result.push_str(&self.paint(&palette, ",", Role::Muted));
                columns += 1;
                if columns + cell_width + 1 > self.options.width {
                    result.push_str("\n  ");
                    columns = 2;
                }
            }
            if cell.reporting {
                result.push_str(&self.paint(&palette, &label, Role::Output));
                result.push_str(&self.paint(&palette, &digits, Role::JournalCounter));
            } else {
                result.push_str(&self.paint(&palette, &format!("{label}{digits}"), Role::Muted));
            }
            columns += cell_width;
        }
        result.push_str(&self.paint(&palette, "⟩", Role::Muted));
        result
    }

    /// Layout escaped text before painting spans. Semantic callers supply
    /// spans, never byte ranges into a hand-built ANSI string.
    fn spans(
        &self,
        output: &mut impl Write,
        palette: &Palette,
        spans: &[Span],
    ) -> Result<(), Error> {
        let text = plain(spans);
        let mut remaining = text.as_str();
        for (index, line) in wrap_fields(std::slice::from_ref(&text), self.options.width)
            .iter()
            .enumerate()
        {
            let line = if index == 0 {
                line.as_str()
            } else {
                remaining = remaining.trim_start();
                write!(output, "  ")?;
                &line[2..]
            };
            let offset = text.len() - remaining.len();
            let mut start = 0;
            for span in spans {
                let end = start + span.text.len();
                let overlap_start = start.max(offset);
                let overlap_end = end.min(offset + line.len());
                if overlap_start < overlap_end {
                    write!(
                        output,
                        "{}",
                        self.paint(palette, &text[overlap_start..overlap_end], span.role)
                    )?;
                }
                start = end;
            }
            writeln!(output)?;
            remaining = &remaining[line.len()..];
        }
        Ok(())
    }

    fn paint(&self, palette: &Palette, text: &str, role: Role) -> String {
        if !self.options.color || text.is_empty() {
            return text.into();
        }
        let shade = match role {
            Role::Normal => palette.normal,
            Role::Reporter => palette.primary,
            Role::Output | Role::JournalCounter => palette.secondary,
            Role::Muted => 245,
        };
        let emphasis = match role {
            Role::JournalCounter => "1;4;",
            Role::Reporter | Role::Output => "1;",
            Role::Normal if palette.bold => "1;",
            _ => "",
        };
        format!("\x1b[{emphasis}38;5;{shade}m{text}\x1b[0m")
    }
}

fn plain(spans: &[Span]) -> String {
    spans.iter().map(|span| span.text.as_str()).collect()
}

fn upstream(input: &UpstreamView<'_>) -> String {
    match input {
        UpstreamView::Known {
            name,
            journal: Some(number),
        } => format!("{name} (journal: {number})"),
        UpstreamView::Known {
            name,
            journal: None,
        } => (*name).into(),
        UpstreamView::Unresolved { path, index } => {
            format!("{path} (unresolved; reader index: {})", index.0)
        }
    }
}

fn progress_fields(progress: &ProgressView<'_>) -> Vec<Span> {
    let mut spans = vec![
        Span::new("Progress: ", Role::Normal),
        Span::new(progress.sequence.0.to_string(), Role::Output),
        Span::new(
            if progress.eof_seen {
                " · EOF: seen"
            } else {
                " · EOF: not seen"
            },
            Role::Normal,
        ),
    ];
    if let Some(advertised) = progress.advertised {
        spans.push(Span::new(" · Advertised: ", Role::Normal));
        spans.push(Span::new(advertised.0.to_string(), Role::Output));
    }
    if let Some(duration) = progress.stalled {
        spans.push(Span::new(" · Stalled: ", Role::Normal));
        spans.push(Span::new(format!("{} ms", duration.0), Role::Output));
    }
    spans
}
