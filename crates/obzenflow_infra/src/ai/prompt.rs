// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prompt construction helpers.
//!
//! This module provides a small, provider-agnostic `Prompt` builder for assembling
//! a single user-role prompt from common structural parts: text blocks, bulleted
//! rule lists, labeled sections, fenced input, and titled subsections.

use obzenflow_core::ai::UserPrompt;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Prompt {
    buf: String,
}

impl Prompt {
    /// Create a new empty prompt builder.
    pub fn new() -> Self {
        Self { buf: String::new() }
    }

    /// Add a plain text block followed by a blank line.
    pub fn text(&mut self, text: &str) -> &mut Self {
        let trimmed = text.trim_end();
        if trimmed.trim().is_empty() {
            return self;
        }

        self.buf.push_str(trimmed);
        self.buf.push_str("\n\n");
        self
    }

    /// Add a conditional text block. If `value` is `Some`, call `f` with the inner
    /// value and add the result as a text block. If `value` is `None`, add nothing.
    pub fn text_if<T, F>(&mut self, value: Option<T>, f: F) -> &mut Self
    where
        F: FnOnce(T) -> String,
    {
        let Some(value) = value else {
            return self;
        };
        self.text(&f(value))
    }

    /// Add a bulleted list. Each item is prefixed with "- " and followed by a newline.
    /// A blank line is added after the list.
    pub fn rules<I, S>(&mut self, items: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut wrote_any = false;
        for item in items {
            let rule = item.as_ref().trim();
            if rule.is_empty() {
                continue;
            }

            wrote_any = true;
            self.buf.push_str("- ");
            self.buf.push_str(rule);
            self.buf.push('\n');
        }

        if wrote_any {
            self.buf.push('\n');
        }

        self
    }

    /// Add a titled block: "{title}:\n{body}\n\n".
    ///
    /// Use this for output format specifications, labeled context, or any block
    /// that needs a title line.
    pub fn labeled(&mut self, title: &str, body: &str) -> &mut Self {
        let title = title.trim();
        let body = body.trim_end();

        if title.is_empty() && body.trim().is_empty() {
            return self;
        }

        if !title.is_empty() {
            self.buf.push_str(title);
            self.buf.push_str(":\n");
        }

        if !body.trim().is_empty() {
            self.buf.push_str(body);
        }

        self.buf.push_str("\n\n");
        self
    }

    /// Add items rendered inside a titled code fence.
    /// Renders as: "{title}:\n```text\n{rendered items}\n```\n"
    ///
    /// Fence safety: the fence delimiter is hardcoded as triple backticks. If any
    /// rendered item contains a triple-backtick sequence, the fence will be broken.
    pub fn fenced_items<T, F>(&mut self, title: &str, items: &[T], render: F) -> &mut Self
    where
        F: Fn(&T) -> String,
    {
        let title = title.trim();
        if !title.is_empty() {
            self.buf.push_str(title);
            self.buf.push_str(":\n");
        }

        self.buf.push_str("```text\n");
        for item in items {
            let rendered = render(item);
            self.buf.push_str(rendered.trim_end());
            self.buf.push('\n');
        }
        self.buf.push_str("```\n");

        self
    }

    /// Add titled subsections. Each item is rendered as a heading and body.
    /// Renders as:
    /// "{title}:\n\n### {heading1}\n{body1}\n\n### {heading2}\n..."
    ///
    /// If the render function returns an empty heading, the heading line is skipped
    /// and only the body is rendered.
    pub fn sections<T, F>(&mut self, title: &str, items: &[T], render: F) -> &mut Self
    where
        F: Fn(&T) -> (String, String),
    {
        if items.is_empty() {
            return self;
        }

        let title = title.trim();
        if !title.is_empty() {
            self.buf.push_str(title);
            self.buf.push_str(":\n\n");
        }

        for item in items {
            let (heading, body) = render(item);
            let heading = heading.trim();
            let body = body.trim_end();

            if !heading.is_empty() {
                self.buf.push_str("### ");
                self.buf.push_str(heading);
                self.buf.push('\n');
            }

            if !body.trim().is_empty() {
                self.buf.push_str(body);
            }

            self.buf.push_str("\n\n");
        }

        self
    }

    /// Like `sections`, but the render function also receives a 1-based index for each item.
    pub fn indexed_sections<T, F>(&mut self, title: &str, items: &[T], render: F) -> &mut Self
    where
        F: Fn(usize, &T) -> (String, String),
    {
        if items.is_empty() {
            return self;
        }

        let title = title.trim();
        if !title.is_empty() {
            self.buf.push_str(title);
            self.buf.push_str(":\n\n");
        }

        for (idx, item) in items.iter().enumerate() {
            let (heading, body) = render(idx + 1, item);
            let heading = heading.trim();
            let body = body.trim_end();

            if !heading.is_empty() {
                self.buf.push_str("### ");
                self.buf.push_str(heading);
                self.buf.push('\n');
            }

            if !body.trim().is_empty() {
                self.buf.push_str(body);
            }

            self.buf.push_str("\n\n");
        }

        self
    }

    /// Consume the builder and return the assembled prompt as a `UserPrompt`.
    pub fn finish(self) -> UserPrompt {
        self.buf.into()
    }
}

#[cfg(test)]
mod tests {
    use super::Prompt;

    #[test]
    fn text_adds_blank_line() {
        let mut p = Prompt::new();
        p.text("Hello");
        assert_eq!(p.finish().as_str(), "Hello\n\n");
    }

    #[test]
    fn text_if_none_is_noop() {
        let mut p = Prompt::new();
        p.text_if::<&str, _>(None, |_| "ignored".to_string());
        assert_eq!(p.finish().as_str(), "");
    }

    #[test]
    fn text_if_some_renders_block() {
        let mut p = Prompt::new();
        p.text_if(Some("rust"), |value| format!("My interests: {value}"));
        assert_eq!(p.finish().as_str(), "My interests: rust\n\n");
    }

    #[test]
    fn rules_empty_is_noop() {
        let mut p = Prompt::new();
        let rules: Vec<&str> = Vec::new();
        p.rules(rules);
        assert_eq!(p.finish().as_str(), "");
    }

    #[test]
    fn rules_renders_bullets_and_trailing_blank_line() {
        let mut p = Prompt::new();
        p.rules(["First", "Second"]);
        assert_eq!(p.finish().as_str(), "- First\n- Second\n\n");
    }

    #[test]
    fn labeled_renders_title_and_body() {
        let mut p = Prompt::new();
        p.labeled("Output format", "Themes:\n- A");
        assert_eq!(p.finish().as_str(), "Output format:\nThemes:\n- A\n\n");
    }

    #[test]
    fn fenced_items_empty_slice_renders_empty_fence() {
        let mut p = Prompt::new();
        let items: Vec<usize> = Vec::new();
        p.fenced_items("Input", &items, |_| {
            unreachable!("render should not be called")
        });
        assert_eq!(p.finish().as_str(), "Input:\n```text\n```\n");
    }

    #[test]
    fn fenced_items_renders_each_item_on_its_own_line() {
        let mut p = Prompt::new();
        let items = [1usize, 2usize];
        p.fenced_items("Input", &items, |value| format!("Line {value}"));
        assert_eq!(
            p.finish().as_str(),
            "Input:\n```text\nLine 1\nLine 2\n```\n"
        );
    }

    #[test]
    fn sections_skips_empty_heading() {
        let mut p = Prompt::new();
        let items = [0usize];
        p.sections("Chunk summaries", &items, |_| {
            (String::new(), "Body".to_string())
        });
        assert_eq!(p.finish().as_str(), "Chunk summaries:\n\nBody\n\n");
    }

    #[test]
    fn indexed_sections_passes_1_based_indices() {
        let mut p = Prompt::new();
        let items = ["A", "B"];
        p.indexed_sections("Things", &items, |idx, value| {
            (format!("Item {idx}"), value.to_string())
        });
        assert_eq!(
            p.finish().as_str(),
            "Things:\n\n### Item 1\nA\n\n### Item 2\nB\n\n"
        );
    }

    #[test]
    fn composition_hn_style_map_prompt() {
        #[derive(Debug, Clone)]
        struct Story {
            n: usize,
            title: &'static str,
        }

        fn render_story_line(n: usize, title: &str) -> String {
            format!("{n}. {title}")
        }

        let interests = Some("rust".to_string());
        let stories = [Story { n: 1, title: "One" }, Story { n: 2, title: "Two" }];

        let min_citations = stories.len().min(6);

        let mut rules: Vec<String> = vec![
            "Do not invent facts that are not implied by the titles.".to_string(),
            "Use a neutral, specific tone.".to_string(),
            "IMPORTANT: Do not repeat the input story list.".to_string(),
            "Cite stories only by number, like (12); do not paste URLs.".to_string(),
        ];
        rules.push(format!(
            "Reference at least {min_citations} distinct story numbers across Themes + Notable stories."
        ));

        let mut p = Prompt::new();
        p.text_if(interests.as_deref(), |value| {
            format!("My interests: {value}")
        })
        .text("Summarise these Hacker News stories (titles + URLs are provided as input).")
        .rules(rules)
        .labeled(
            "Output format (follow exactly)",
            "Themes:\n\
- <theme> (n, n, n): 1 sentence\n\
Notable stories:\n\
- (n) Title: 1 sentence",
        )
        .fenced_items(
            "Input stories (numbered; do not repeat)",
            &stories,
            |story| render_story_line(story.n, story.title),
        );

        assert_eq!(
            p.finish().as_str(),
            concat!(
                "My interests: rust\n\n",
                "Summarise these Hacker News stories (titles + URLs are provided as input).\n\n",
                "- Do not invent facts that are not implied by the titles.\n",
                "- Use a neutral, specific tone.\n",
                "- IMPORTANT: Do not repeat the input story list.\n",
                "- Cite stories only by number, like (12); do not paste URLs.\n",
                "- Reference at least 2 distinct story numbers across Themes + Notable stories.\n\n",
                "Output format (follow exactly):\n",
                "Themes:\n",
                "- <theme> (n, n, n): 1 sentence\n",
                "Notable stories:\n",
                "- (n) Title: 1 sentence\n\n",
                "Input stories (numbered; do not repeat):\n",
                "```text\n",
                "1. One\n",
                "2. Two\n",
                "```\n",
            )
        );
    }
}
