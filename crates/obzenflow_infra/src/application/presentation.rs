// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt::Display;
use std::io::IsTerminal;
use std::path::PathBuf;

const INDENT: &str = "  ";
const NESTED_INDENT: &str = "    ";
const BULLET_CONTINUATION_INDENT: &str = "      ";
const MAX_VISIBLE_COLUMNS: usize = 80;
const MAX_LINES: usize = 25;

pub struct Banner {
    title: String,
    description: Option<String>,
    items: Vec<BannerItem>,
    art: Option<String>,
    ansi_art: Option<String>,
}

enum BannerItem {
    ConfigRow { key: String, value: String },
    Block(String),
    Section { title: String, body: String },
    Bullets { title: String, items: Vec<String> },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BannerItemKind {
    Row,
    Block,
}

impl BannerItem {
    fn kind(&self) -> BannerItemKind {
        match self {
            Self::ConfigRow { .. } => BannerItemKind::Row,
            Self::Block { .. } | Self::Section { .. } | Self::Bullets { .. } => {
                BannerItemKind::Block
            }
        }
    }
}

impl Banner {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            description: None,
            items: Vec::new(),
            art: None,
            ansi_art: None,
        }
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn config(mut self, key: impl Into<String>, value: impl Display) -> Self {
        self.items.push(BannerItem::ConfigRow {
            key: key.into(),
            value: value.to_string(),
        });
        self
    }

    pub fn config_if(self, enabled: bool, key: impl Into<String>, value: impl Display) -> Self {
        if enabled {
            self.config(key, value)
        } else {
            self
        }
    }

    pub fn config_block(mut self, block: impl Display) -> Self {
        self.items.push(BannerItem::Block(block.to_string()));
        self
    }

    pub fn config_block_if(self, enabled: bool, block: impl Display) -> Self {
        if enabled {
            self.config_block(block)
        } else {
            self
        }
    }

    pub fn section(mut self, title: impl Into<String>, body: impl Display) -> Self {
        self.items.push(BannerItem::Section {
            title: title.into(),
            body: body.to_string(),
        });
        self
    }

    pub fn bullets<I, S>(mut self, title: impl Into<String>, items: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.items.push(BannerItem::Bullets {
            title: title.into(),
            items: items.into_iter().map(Into::into).collect(),
        });
        self
    }

    pub fn art(mut self, art: impl Into<String>) -> Self {
        self.art = Some(art.into());
        self
    }

    pub fn ansi_art(mut self, art: impl Into<String>) -> Self {
        self.ansi_art = Some(art.into());
        self
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub(crate) fn render_for_stdout(&self) -> RenderedBanner {
        self.render_with_stdout_is_tty(std::io::stdout().is_terminal())
    }

    fn render_with_stdout_is_tty(&self, stdout_is_tty: bool) -> RenderedBanner {
        let (art_kind, art) = self.select_art(stdout_is_tty);
        let mut warnings = Vec::new();

        if let Some((kind_label, art)) = art_kind.zip(art.as_deref()) {
            warnings.extend(validate_art_size(kind_label, art));
        }

        let mut out = String::new();

        if let Some(art) = art {
            out.push_str(&normalise_art_text(&art));
            out.push('\n');
        } else {
            out.push_str(self.title.trim());
            out.push('\n');
            out.push_str(&"=".repeat(self.title.trim().len()));
            out.push('\n');
        }

        let mut content = String::new();

        if let Some(description) = &self.description {
            if !description.trim().is_empty() {
                content.push_str(description.trim());
                content.push('\n');
            }
        }

        if !content.is_empty() && !self.items.is_empty() {
            content.push('\n');
        }

        let mut previous_kind = None;
        for item in &self.items {
            if let Some(previous_kind) = previous_kind {
                if previous_kind != BannerItemKind::Row || item.kind() != BannerItemKind::Row {
                    content.push('\n');
                }
            }
            render_banner_item(&mut content, item);
            previous_kind = Some(item.kind());
        }

        if !content.is_empty() {
            out.push_str(&content);
        }
        out.push('\n');

        RenderedBanner {
            text: out,
            warnings,
        }
    }

    fn select_art(&self, stdout_is_tty: bool) -> (Option<&'static str>, Option<String>) {
        if stdout_is_tty {
            if let Some(ansi_art) = &self.ansi_art {
                return (Some("ANSI"), Some(ansi_art.clone()));
            }
        }

        if let Some(art) = &self.art {
            return (Some("ASCII"), Some(art.clone()));
        }

        (None, None)
    }
}

pub(crate) struct RenderedBanner {
    pub(crate) text: String,
    pub(crate) warnings: Vec<String>,
}

#[derive(Default)]
pub struct Footer {
    blocks: Vec<FooterBlock>,
}

enum FooterBlock {
    Paragraph(String),
    Section { title: String, body: String },
    Bullets { title: String, items: Vec<String> },
}

impl Footer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn paragraph(mut self, text: impl Display) -> Self {
        self.blocks.push(FooterBlock::Paragraph(text.to_string()));
        self
    }

    pub fn section(mut self, title: impl Into<String>, body: impl Display) -> Self {
        self.blocks.push(FooterBlock::Section {
            title: title.into(),
            body: body.to_string(),
        });
        self
    }

    pub fn bullets<I, S>(mut self, title: impl Into<String>, items: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.blocks.push(FooterBlock::Bullets {
            title: title.into(),
            items: items.into_iter().map(Into::into).collect(),
        });
        self
    }

    pub fn finish(self) -> String {
        let mut out = String::new();
        let mut first = true;

        for block in self.blocks {
            if !first {
                out.push_str("\n\n");
            }
            render_footer_block(&mut out, block);
            first = false;
        }

        while out.ends_with('\n') {
            out.pop();
        }

        out
    }
}

impl From<String> for Footer {
    fn from(value: String) -> Self {
        Footer::new().paragraph(value)
    }
}

impl From<&str> for Footer {
    fn from(value: &str) -> Self {
        Footer::new().paragraph(value)
    }
}

pub struct Presentation {
    banner: Banner,
    footer_banner: Option<Banner>,
    footer: Option<Box<dyn Fn(RunPresentationOutcome) -> Footer + Send + Sync + 'static>>,
}

impl Presentation {
    pub fn new(banner: Banner) -> Self {
        Self {
            banner,
            footer_banner: None,
            footer: None,
        }
    }

    pub fn with_footer_banner(mut self, banner: Banner) -> Self {
        self.footer_banner = Some(banner);
        self
    }

    pub fn with_footer<F, T>(mut self, footer: F) -> Self
    where
        F: Fn(RunPresentationOutcome) -> T + Send + Sync + 'static,
        T: Into<Footer>,
    {
        self.footer = Some(Box::new(move |outcome| footer(outcome).into()));
        self
    }

    pub fn banner(&self) -> &Banner {
        &self.banner
    }

    pub fn footer_banner(&self) -> Option<&Banner> {
        self.footer_banner.as_ref()
    }

    pub(crate) fn render_footer_banner(&self) -> Option<RenderedBanner> {
        self.footer_banner.as_ref().map(Banner::render_for_stdout)
    }

    pub(crate) fn render_footer(&self, outcome: RunPresentationOutcome) -> String {
        match &self.footer {
            Some(custom) => custom(outcome).finish(),
            None => outcome.into_footer().finish(),
        }
    }
}

pub enum RunPresentationOutcome {
    Completed {
        flow_name: String,
        run_dir: Option<PathBuf>,
    },
    Stopped {
        flow_name: String,
        run_dir: Option<PathBuf>,
    },
    Failed {
        flow_name: Option<String>,
        error: String,
        run_dir: Option<PathBuf>,
    },
}

impl RunPresentationOutcome {
    pub fn default_footer(&self) -> String {
        self.to_footer().finish()
    }

    pub fn to_footer(&self) -> Footer {
        match self {
            Self::Completed { flow_name, run_dir } => match run_dir {
                Some(run_dir) => Footer::new().paragraph(format!(
                    "{flow_name} completed. Journal: {}\nTo replay, add: --replay-from {}\n(Source config env vars are ignored during replay)",
                    run_dir.display(),
                    run_dir.display(),
                )),
                None => Footer::new().paragraph(format!("{flow_name} completed.")),
            },
            Self::Stopped { flow_name, run_dir } => match run_dir {
                Some(run_dir) => {
                    Footer::new().paragraph(format!("{flow_name} stopped. Journal: {}", run_dir.display()))
                }
                None => Footer::new().paragraph(format!("{flow_name} stopped.")),
            },
            Self::Failed {
                flow_name,
                error,
                run_dir,
            } => {
                let prefix = flow_name
                    .as_ref()
                    .map(|name| format!("{name} failed"))
                    .unwrap_or_else(|| "Flow failed".to_string());
                match run_dir {
                    Some(run_dir) => {
                        Footer::new().paragraph(format!("{prefix}: {error}. Journal: {}", run_dir.display()))
                    }
                    None => Footer::new().paragraph(format!("{prefix}: {error}")),
                }
            }
        }
    }

    pub fn into_footer(self) -> Footer {
        match self {
            Self::Completed { flow_name, run_dir } => match run_dir {
                Some(run_dir) => Footer::new().paragraph(format!(
                    "{flow_name} completed. Journal: {}\nTo replay, add: --replay-from {}\n(Source config env vars are ignored during replay)",
                    run_dir.display(),
                    run_dir.display(),
                )),
                None => Footer::new().paragraph(format!("{flow_name} completed.")),
            },
            Self::Stopped { flow_name, run_dir } => match run_dir {
                Some(run_dir) => {
                    Footer::new().paragraph(format!("{flow_name} stopped. Journal: {}", run_dir.display()))
                }
                None => Footer::new().paragraph(format!("{flow_name} stopped.")),
            },
            Self::Failed {
                flow_name,
                error,
                run_dir,
            } => {
                let prefix = flow_name
                    .map(|name| format!("{name} failed"))
                    .unwrap_or_else(|| "Flow failed".to_string());
                match run_dir {
                    Some(run_dir) => {
                        Footer::new().paragraph(format!("{prefix}: {error}. Journal: {}", run_dir.display()))
                    }
                    None => Footer::new().paragraph(format!("{prefix}: {error}")),
                }
            }
        }
    }
}

fn render_banner_item(out: &mut String, item: &BannerItem) {
    match item {
        BannerItem::ConfigRow { key, value } => {
            out.push_str(INDENT);
            out.push_str(key);
            out.push_str(": ");
            out.push_str(value);
            out.push('\n');
        }
        BannerItem::Block(block) => push_prefixed_lines(out, block, INDENT),
        BannerItem::Section { title, body } => {
            out.push_str(INDENT);
            out.push_str(title.trim());
            out.push_str(":\n");
            push_prefixed_lines(out, body, NESTED_INDENT);
        }
        BannerItem::Bullets { title, items } => {
            out.push_str(INDENT);
            out.push_str(title.trim());
            out.push_str(":\n");
            for item in items {
                push_bullet_item(out, item, NESTED_INDENT, BULLET_CONTINUATION_INDENT);
            }
        }
    }
}

fn render_footer_block(out: &mut String, block: FooterBlock) {
    match block {
        FooterBlock::Paragraph(text) => push_raw_lines(out, &text),
        FooterBlock::Section { title, body } => {
            out.push_str(title.trim());
            out.push_str(":\n");
            push_raw_lines(out, &body);
        }
        FooterBlock::Bullets { title, items } => {
            out.push_str(title.trim());
            out.push_str(":\n");
            for item in items {
                push_bullet_item(out, &item, "", "  ");
            }
        }
    }
}

fn push_prefixed_lines(out: &mut String, text: &str, prefix: &str) {
    if text.is_empty() {
        out.push_str(prefix);
        out.push('\n');
        return;
    }

    for line in text.lines() {
        out.push_str(prefix);
        out.push_str(line);
        out.push('\n');
    }
}

fn push_raw_lines(out: &mut String, text: &str) {
    if text.is_empty() {
        out.push('\n');
        return;
    }

    for line in text.lines() {
        out.push_str(line);
        out.push('\n');
    }

    if out.ends_with('\n') {
        out.pop();
    }
}

fn push_bullet_item(out: &mut String, item: &str, prefix: &str, continuation_prefix: &str) {
    let mut lines = item.lines();
    match lines.next() {
        Some(first_line) => {
            out.push_str(prefix);
            out.push_str("- ");
            out.push_str(first_line);
            out.push('\n');
            for line in lines {
                out.push_str(continuation_prefix);
                out.push_str(line);
                out.push('\n');
            }
        }
        None => {
            out.push_str(prefix);
            out.push_str("-\n");
        }
    }
}

fn normalise_art_text(raw: &str) -> String {
    let without_first_newline = raw.strip_prefix('\n').unwrap_or(raw);
    let without_last_newline = without_first_newline
        .strip_suffix('\n')
        .unwrap_or(without_first_newline);
    without_last_newline.to_string()
}

fn validate_art_size(kind: &'static str, art: &str) -> Vec<String> {
    let visible = match kind {
        "ANSI" => strip_ansi_escape_codes(art),
        _ => art.to_string(),
    };
    let normalised = normalise_art_text(&visible);

    let mut max_width = 0usize;
    let mut height = 0usize;

    for line in normalised.lines() {
        height += 1;
        max_width = max_width.max(line.len());
    }

    if height == 0 && !normalised.is_empty() {
        height = 1;
        max_width = normalised.len();
    }

    let mut warnings = Vec::new();
    if max_width > MAX_VISIBLE_COLUMNS {
        warnings.push(format!(
            "{kind} banner art is {max_width} visible columns (recommended max {MAX_VISIBLE_COLUMNS})",
        ));
    }
    if height > MAX_LINES {
        warnings.push(format!(
            "{kind} banner art is {height} lines (recommended max {MAX_LINES})",
        ));
    }
    warnings
}

fn strip_ansi_escape_codes(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0usize;

    while idx < bytes.len() {
        if bytes[idx] == 0x1b {
            // ESC + [ ... (CSI)
            if idx + 1 < bytes.len() && bytes[idx + 1] == b'[' {
                idx += 2;
                while idx < bytes.len() {
                    let b = bytes[idx];
                    // CSI terminator byte.
                    if (0x40..=0x7E).contains(&b) {
                        idx += 1;
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            // ESC + ] ... (OSC)
            if idx + 1 < bytes.len() && bytes[idx + 1] == b']' {
                idx += 2;
                while idx < bytes.len() {
                    // BEL
                    if bytes[idx] == 0x07 {
                        idx += 1;
                        break;
                    }
                    // ESC \\
                    if bytes[idx] == 0x1b && idx + 1 < bytes.len() && bytes[idx + 1] == b'\\' {
                        idx += 2;
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            // Other escape sequences: best-effort drop ESC and continue.
            idx += 1;
            continue;
        }

        out.push(bytes[idx]);
        idx += 1;
    }

    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_title_renders_with_separator() {
        let rendered = Banner::new("Hello").render_with_stdout_is_tty(false);
        assert_eq!(rendered.text, "Hello\n=====\n\n");
        assert!(rendered.warnings.is_empty());
    }

    #[test]
    fn description_and_config_are_indented_and_separated() {
        let rendered = Banner::new("Title")
            .description("Desc")
            .config("mode", "test")
            .config_block("line1\nline2")
            .render_with_stdout_is_tty(false);

        assert_eq!(
            rendered.text,
            "Title\n=====\nDesc\n\n  mode: test\n\n  line1\n  line2\n\n"
        );
    }

    #[test]
    fn banner_preserves_item_order_across_rows_sections_and_blocks() {
        let rendered = Banner::new("Title")
            .config("first", "1")
            .section("Second", "two")
            .config("third", "3")
            .render_with_stdout_is_tty(false);

        assert_eq!(
            rendered.text,
            "Title\n=====\n  first: 1\n\n  Second:\n    two\n\n  third: 3\n\n"
        );
    }

    #[test]
    fn banner_sections_and_bullets_render_structured_content() {
        let rendered = Banner::new("Title")
            .section("Notes", "line1\nline2")
            .bullets("Highlights", ["first", "second\ncontinued"])
            .render_with_stdout_is_tty(false);

        assert_eq!(
            rendered.text,
            "Title\n=====\n  Notes:\n    line1\n    line2\n\n  Highlights:\n    - first\n    - second\n      continued\n\n"
        );
    }

    #[test]
    fn ascii_art_replaces_title_and_separator() {
        let rendered = Banner::new("Title")
            .art("ASCII")
            .render_with_stdout_is_tty(false);
        assert_eq!(rendered.text, "ASCII\n\n");
    }

    #[test]
    fn ansi_art_only_renders_when_stdout_is_a_tty() {
        let banner = Banner::new("Title")
            .art("ASCII")
            .ansi_art("\x1b[31mANSI\x1b[0m");

        let tty_rendered = banner.render_with_stdout_is_tty(true);
        assert!(tty_rendered.text.starts_with("\x1b[31mANSI\x1b[0m\n"));

        let non_tty_rendered = banner.render_with_stdout_is_tty(false);
        assert!(non_tty_rendered.text.starts_with("ASCII\n"));
        assert!(!non_tty_rendered.text.contains("\x1b["));
    }

    #[test]
    fn normalise_art_strips_single_leading_and_trailing_newline() {
        assert_eq!(normalise_art_text("\nhello\n"), "hello");
        assert_eq!(normalise_art_text("\n\nhi\n\n"), "\nhi\n");
    }

    #[test]
    fn strip_ansi_escape_codes_strips_csi_and_osc_sequences() {
        assert_eq!(
            strip_ansi_escape_codes("\x1b[31mRED\x1b[0m"),
            "RED".to_string()
        );

        let hyperlink = format!("\x1b]8;;{}\x07Hi\x1b]8;;\x07", "a".repeat(200));
        assert_eq!(strip_ansi_escape_codes(&hyperlink), "Hi".to_string());
    }

    #[test]
    fn art_size_warnings_trigger_for_wide_and_tall_art() {
        let mut lines = Vec::new();
        lines.push("X".repeat(81));
        for _ in 0..25 {
            lines.push("x".to_string());
        }
        let art = lines.join("\n");

        let warnings = validate_art_size("ASCII", &art);
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].contains("ASCII banner art is 81 visible columns"));
        assert!(warnings[1].contains("ASCII banner art is 26 lines"));
    }

    #[test]
    fn ansi_size_warnings_are_based_on_visible_width() {
        let art = format!("\x1b]8;;{}\x07Hi\x1b]8;;\x07", "a".repeat(200));

        let warnings = validate_art_size("ANSI", &art);
        assert!(warnings.is_empty());
    }

    #[test]
    fn default_footer_matches_outcomes() {
        let completed = RunPresentationOutcome::Completed {
            flow_name: "flow".to_string(),
            run_dir: None,
        };
        assert_eq!(completed.default_footer(), "flow completed.");

        let run_dir = PathBuf::from("tmp/run");
        let completed_with_journal = RunPresentationOutcome::Completed {
            flow_name: "flow".to_string(),
            run_dir: Some(run_dir.clone()),
        };
        assert_eq!(
            completed_with_journal.default_footer(),
            "flow completed. Journal: tmp/run\nTo replay, add: --replay-from tmp/run\n(Source config env vars are ignored during replay)"
        );

        let stopped = RunPresentationOutcome::Stopped {
            flow_name: "flow".to_string(),
            run_dir: None,
        };
        assert_eq!(stopped.default_footer(), "flow stopped.");

        let failed = RunPresentationOutcome::Failed {
            flow_name: Some("flow".to_string()),
            error: "err".to_string(),
            run_dir: None,
        };
        assert_eq!(failed.default_footer(), "flow failed: err");

        let failed_without_name = RunPresentationOutcome::Failed {
            flow_name: None,
            error: "err".to_string(),
            run_dir: Some(run_dir),
        };
        assert_eq!(
            failed_without_name.default_footer(),
            "Flow failed: err. Journal: tmp/run"
        );
    }

    #[test]
    fn footer_builder_renders_paragraphs_sections_and_bullets() {
        let footer = Footer::new()
            .paragraph("done")
            .section("Summary", "line1\nline2")
            .bullets("Next", ["first", "second\ncontinued"])
            .finish();

        assert_eq!(
            footer,
            "done\n\nSummary:\nline1\nline2\n\nNext:\n- first\n- second\n  continued"
        );
    }

    #[test]
    fn footer_banner_uses_same_rendering_pipeline() {
        let presentation = Presentation::new(Banner::new("Start"))
            .with_footer_banner(Banner::new("End").art("END ART"));

        let rendered = presentation
            .render_footer_banner()
            .expect("footer banner should be present");

        assert_eq!(rendered.text, "END ART\n\n");
        assert!(rendered.warnings.is_empty());
    }
}
