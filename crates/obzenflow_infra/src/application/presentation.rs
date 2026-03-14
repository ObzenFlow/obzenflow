// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt::Display;
use std::io::IsTerminal;
use std::path::PathBuf;

const INDENT: &str = "  ";
const MAX_VISIBLE_COLUMNS: usize = 80;
const MAX_LINES: usize = 25;

pub struct Banner {
    title: String,
    description: Option<String>,
    config_rows: Vec<(String, String)>,
    config_blocks: Vec<String>,
    art: Option<String>,
    ansi_art: Option<String>,
}

impl Banner {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            description: None,
            config_rows: Vec::new(),
            config_blocks: Vec::new(),
            art: None,
            ansi_art: None,
        }
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn config(mut self, key: impl Into<String>, value: impl Display) -> Self {
        self.config_rows.push((key.into(), value.to_string()));
        self
    }

    pub fn config_block(mut self, block: impl Display) -> Self {
        self.config_blocks.push(block.to_string());
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

        if let Some(description) = &self.description {
            if !description.trim().is_empty() {
                out.push_str(description.trim());
                out.push('\n');
            }
        }

        if self
            .description
            .as_ref()
            .is_some_and(|d| !d.trim().is_empty())
            || !self.config_rows.is_empty()
            || !self.config_blocks.is_empty()
        {
            out.push('\n');
        }

        for (key, value) in &self.config_rows {
            out.push_str(INDENT);
            out.push_str(key);
            out.push_str(": ");
            out.push_str(value);
            out.push('\n');
        }

        for block in &self.config_blocks {
            for line in block.lines() {
                out.push_str(INDENT);
                out.push_str(line);
                out.push('\n');
            }
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

pub struct Presentation {
    banner: Banner,
    footer: Option<Box<dyn Fn(RunPresentationOutcome) -> String + Send + Sync + 'static>>,
}

impl Presentation {
    pub fn new(banner: Banner) -> Self {
        Self {
            banner,
            footer: None,
        }
    }

    pub fn with_footer<F>(mut self, footer: F) -> Self
    where
        F: Fn(RunPresentationOutcome) -> String + Send + Sync + 'static,
    {
        self.footer = Some(Box::new(footer));
        self
    }

    pub fn banner(&self) -> &Banner {
        &self.banner
    }

    pub(crate) fn render_footer(&self, outcome: RunPresentationOutcome) -> String {
        match &self.footer {
            Some(custom) => custom(outcome),
            None => outcome.default_footer(),
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
        match self {
            Self::Completed { flow_name, run_dir } => match run_dir {
                Some(run_dir) => format!(
                    "{flow_name} completed. Journal: {}\nTo replay, add: --replay-from {}\n(Source config env vars are ignored during replay)",
                    run_dir.display(),
                    run_dir.display(),
                ),
                None => format!("{flow_name} completed."),
            },
            Self::Stopped { flow_name, run_dir } => match run_dir {
                Some(run_dir) => format!("{flow_name} stopped. Journal: {}", run_dir.display()),
                None => format!("{flow_name} stopped."),
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
                    Some(run_dir) => format!("{prefix}: {error}. Journal: {}", run_dir.display()),
                    None => format!("{prefix}: {error}"),
                }
            }
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
