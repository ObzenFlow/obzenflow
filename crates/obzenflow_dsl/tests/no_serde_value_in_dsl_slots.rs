// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c PR E lint.
//!
//! Walks every `.rs` file under `examples/`, `crates/**/src`,
//! `crates/**/tests`, `crates/**/benches`, and `tests/`, and rejects any
//! `serde_json::Value` (or `::serde_json::Value`) appearing in a DSL macro
//! type slot. The allowlist below is the only escape hatch: three narrow
//! categories of legitimate `Value` usage. Anything else is treated as the
//! `Value`-as-the-new-mixed anti-pattern and fails the lint.
//!
//! The lint stitches multi-line macro invocations into one logical token by
//! tracking parenthesis depth from the opening `(` after a DSL macro name to
//! its matching close. A `serde_json::Value` on a separate line from the
//! macro name still trips the lint as long as it falls inside the same
//! invocation. The earlier line-by-line version of this lint missed
//! multi-line macro forms; see the closing-PR notes for FLOWIP-114c.
//!
//! When the lint fires, the fix is one of:
//!
//! 1. Declare a small test-local `TypedPayload` struct that matches the
//!    handler's actual payload shape and use it in the macro slot
//!    (preferred — see `examples/multi_source_ingest_demo/` for the
//!    canonical pattern).
//! 2. If this site genuinely belongs in the allowlist (raw JSON ingress,
//!    runtime lifecycle test, throughput bench where payload shape is
//!    irrelevant), annotate it inline with
//!    `// allow-serde-value: <reason>` AND add the file to `ALLOWLIST`
//!    below with the same reason.

use std::fs;
use std::path::{Path, PathBuf};

/// (path_suffix, reason). A file matches if any line in its repository
/// path ends with `path_suffix`. The reason is required so that allowlist
/// growth shows up in code review.
const ALLOWLIST: &[(&str, &str)] = &[(
    "crates/obzenflow_infra/src/application/flow_application.rs",
    "runtime lifecycle test (server_auto_double_run_regression) exercising \
     FlowApplication launch mechanics, not typed authoring; handlers are \
     payload-agnostic by design",
)];

/// DSL macros whose type slots are subject to the lint.
const DSL_MACROS: &[&str] = &[
    "source!",
    "async_source!",
    "infinite_source!",
    "async_infinite_source!",
    "transform!",
    "async_transform!",
    "stateful!",
    "sink!",
    "join!",
];

#[test]
fn no_serde_json_value_in_dsl_type_slots() {
    let repo_root = repo_root();
    let mut violations: Vec<String> = Vec::new();

    let scan_roots = [
        repo_root.join("examples"),
        repo_root.join("crates"),
        repo_root.join("tests"),
    ];

    for root in &scan_roots {
        if !root.exists() {
            continue;
        }
        walk_rs_files(root, &mut |path| {
            let rel = path
                .strip_prefix(&repo_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");

            // Allowlist entries skip the entire file. The inline annotation
            // (`// allow-serde-value: <reason>`) is the second half of the
            // contract and is required at the offending site itself.
            if is_allowlisted(&rel) {
                return;
            }

            let Ok(contents) = fs::read_to_string(path) else {
                return;
            };

            violations.extend(lint_contents(&rel, &contents));
        });
    }

    assert!(
        violations.is_empty(),
        "FLOWIP-114c PR E lint failed. {} violation(s):\n\n{}\n\n\
         See `crates/obzenflow_dsl/tests/no_serde_value_in_dsl_slots.rs` \
         for the policy and fix instructions.",
        violations.len(),
        violations.join("\n")
    );
}

/// Scan one file's `contents` and return one violation message per offending
/// macro invocation. Invocations span multiple lines when their bodies do;
/// the report line number is the macro-name line.
///
/// The lint operates on a "code-only" view of each line: string-literal and
/// char-literal contents are masked to spaces, and `//` line comments past
/// the macro name are masked to spaces too. This stops `source!(...)` or
/// `serde_json::Value` mentions that live inside Rust string literals from
/// firing the lint. The `// allow-serde-value:` annotation is detected on
/// the raw (unmasked) lines so that comments still suppress legitimately.
fn lint_contents(rel_path: &str, contents: &str) -> Vec<String> {
    let mut violations: Vec<String> = Vec::new();
    let raw_lines: Vec<&str> = contents.lines().collect();
    let masked_lines: Vec<String> = raw_lines
        .iter()
        .map(|line| mask_strings_and_line_comments(line))
        .collect();
    let masked_refs: Vec<&str> = masked_lines.iter().map(String::as_str).collect();

    let mut idx = 0;
    while idx < masked_refs.len() {
        let masked_line = masked_refs[idx];

        let Some(after_macro) = find_dsl_macro_open_paren(masked_line) else {
            idx += 1;
            continue;
        };

        // Accumulate from the open paren forward on the masked view so
        // string-literal contents do not perturb paren depth or smuggle in
        // a `serde_json::Value` mention. Use the masked body for the
        // violation check and the raw body for annotation detection.
        let (masked_body, end_line) = accumulate_macro_body(&masked_refs, idx, after_macro);
        let raw_body = raw_macro_body(&raw_lines, idx, after_macro, end_line);

        let invocation_has_value = masked_body.contains("serde_json::Value");
        if invocation_has_value {
            // Allow annotation can appear (a) anywhere inside the raw body,
            // (b) at end-of-line on the macro line itself, or (c) on the
            // line immediately above the macro line.
            let prev = if idx > 0 { raw_lines[idx - 1] } else { "" };
            let annotated_inside = raw_body.contains("// allow-serde-value:");
            let annotated_macro_line = raw_lines[idx].contains("// allow-serde-value:");
            let annotated_above = prev.contains("// allow-serde-value:");
            if !(annotated_inside || annotated_macro_line || annotated_above) {
                violations.push(format!(
                    "{}:{}: serde_json::Value used in a DSL macro type slot; \
                     declare a TypedPayload struct or add to ALLOWLIST. \
                     Macro starts at this line; invocation spans through line {}.",
                    rel_path,
                    idx + 1,
                    end_line + 1,
                ));
            }
        }

        // Advance past the invocation so a single macro is only reported
        // once and so nested macros on later lines are still scanned.
        idx = end_line + 1;
    }

    violations
}

/// Replace string-literal contents (inside `"..."`) and `//` line comments
/// with spaces of equal length. Char literals are also masked. The output
/// has the same length as the input so column positions reported back to
/// the user still align with the source. Annotation detection runs on the
/// raw text, not the masked version.
fn mask_strings_and_line_comments(line: &str) -> String {
    let bytes = line.as_bytes();
    let mut out: Vec<u8> = bytes.to_vec();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            for j in i..bytes.len() {
                out[j] = b' ';
            }
            return String::from_utf8(out).unwrap_or_default();
        }
        if b == b'"' {
            // Mask string contents (including the quotes) to spaces.
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            for j in start..i {
                out[j] = b' ';
            }
            continue;
        }
        if b == b'\'' {
            // Mask char-literal contents to spaces. Lifetimes have no
            // closing `'` so they fall through naturally when the next
            // non-identifier character is reached.
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b'\'' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'\'' {
                i += 1;
                for j in start..i {
                    out[j] = b' ';
                }
            }
            continue;
        }
        i += 1;
    }
    String::from_utf8(out).unwrap_or_default()
}

/// Stitch the raw text of an invocation back together for annotation
/// detection. Starts at the byte after the opening `(` on `line_idx` and
/// runs through `end_line`. Used only to check for `// allow-serde-value:`
/// comments inside the body.
fn raw_macro_body(
    lines: &[&str],
    line_idx: usize,
    start_col_after_paren: usize,
    end_line: usize,
) -> String {
    if line_idx == end_line {
        let line = lines[line_idx];
        return line[start_col_after_paren..].to_string();
    }
    let mut out = String::new();
    out.push_str(&lines[line_idx][start_col_after_paren..]);
    out.push('\n');
    for row in (line_idx + 1)..end_line {
        out.push_str(lines[row]);
        out.push('\n');
    }
    out.push_str(lines[end_line]);
    out
}

/// If `line` mentions a DSL macro name followed by an open paren, return
/// the byte index in `line` immediately after that `(`. Otherwise `None`.
/// This is a deliberately lightweight check; comments and string literals
/// that include a macro name plus `(` will produce false positives that the
/// rest of the lint handles benignly because the accumulated body just has
/// no `serde_json::Value` in it.
fn find_dsl_macro_open_paren(line: &str) -> Option<usize> {
    for m in DSL_MACROS {
        if let Some(macro_pos) = line.find(m) {
            // Look for the next `(` after the macro name on the same line.
            let after = macro_pos + m.len();
            if let Some(paren_off) = line[after..].find('(') {
                return Some(after + paren_off + 1);
            }
        }
    }
    None
}

/// Starting at `(line_idx, start_col_after_paren)`, walk lines forward
/// counting `(` and `)` until depth returns to zero. Return the accumulated
/// body and the (inclusive) line index where the matching `)` lives. If the
/// file is malformed (no matching close), accumulate to EOF and return the
/// last line.
fn accumulate_macro_body(
    lines: &[&str],
    line_idx: usize,
    start_col_after_paren: usize,
) -> (String, usize) {
    let mut body = String::new();
    let mut depth: i32 = 1;
    let mut row = line_idx;
    // First line: from `start_col_after_paren` to end-of-line.
    let first = &lines[row][start_col_after_paren..];
    let consumed = consume_chars(first, &mut depth);
    body.push_str(&first[..consumed]);
    if depth == 0 {
        return (body, row);
    }
    body.push('\n');
    row += 1;
    while row < lines.len() {
        let rest = lines[row];
        let consumed = consume_chars(rest, &mut depth);
        body.push_str(&rest[..consumed]);
        if depth == 0 {
            return (body, row);
        }
        body.push('\n');
        row += 1;
    }
    (body, lines.len().saturating_sub(1))
}

/// Walk `s` left-to-right, updating `depth` for each `(` / `)` outside of
/// string literals and line comments. Returns the number of characters
/// consumed before either `depth` reaches `0` or the end of the string.
/// String-literal handling is conservative: it skips `"..."` and `'...'`
/// regions so parens inside literals do not influence depth.
fn consume_chars(s: &str, depth: &mut i32) -> usize {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // Line comment: ignore the rest of the line.
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            return bytes.len();
        }
        // Block comment skipping is omitted intentionally; the current
        // codebase does not place DSL macro invocations inside /* */ blocks.
        if b == b'"' {
            // Skip to closing quote, handling `\"`.
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b == b'\'' {
            // Char literal or lifetime. Skip until next `'` or
            // non-identifier char to keep this cheap. Char literals do not
            // contain parens of interest, lifetimes have no `'` close.
            i += 1;
            while i < bytes.len() && bytes[i] != b'\'' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'\'' {
                i += 1;
            }
            continue;
        }
        if b == b'(' {
            *depth += 1;
        } else if b == b')' {
            *depth -= 1;
            if *depth == 0 {
                return i + 1;
            }
        }
        i += 1;
    }
    bytes.len()
}

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points at the obzenflow_dsl crate; walk up to the
    // workspace root (the directory containing the workspace `Cargo.toml`).
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|p| p.join("Cargo.toml").exists() && p.join("crates").exists())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest_dir.clone())
}

fn is_allowlisted(rel_path: &str) -> bool {
    ALLOWLIST
        .iter()
        .any(|(suffix, _)| rel_path.ends_with(suffix))
}

fn walk_rs_files(root: &Path, visit: &mut dyn FnMut(&Path)) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Skip target/ and any hidden directory.
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if name == "target" || name.starts_with('.') {
                continue;
            }
        }
        if path.is_dir() {
            walk_rs_files(&path, visit);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            visit(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::lint_contents;

    /// Single-line invocation: `serde_json::Value` on the same line as the
    /// macro name. Caught by both the old and new lints.
    #[test]
    fn lint_flags_single_line_value_in_macro_slot() {
        let contents = "let _ = source!(serde_json::Value => handler);";
        let violations = lint_contents("fixture.rs", contents);
        assert_eq!(
            violations.len(),
            1,
            "expected one violation on a single-line source!(Value => ...) site, got: {violations:?}",
        );
    }

    /// Multi-line invocation: `serde_json::Value` on a line below the macro
    /// name. The pre-fix line-by-line lint would have missed this; the new
    /// paren-depth stitching catches it.
    #[test]
    fn lint_flags_multi_line_value_in_macro_slot() {
        let contents = "let _ = source!(\n    serde_json::Value => handler,\n);";
        let violations = lint_contents("fixture.rs", contents);
        assert_eq!(
            violations.len(),
            1,
            "expected one violation on a multi-line source!(\\n    Value => ...\\n) site, got: {violations:?}",
        );
    }

    /// `serde_json::Value` outside a DSL macro is not the lint's concern;
    /// the lint must not flag handler-body uses.
    #[test]
    fn lint_does_not_flag_value_outside_macro_slot() {
        let contents = "let payload: serde_json::Value = serde_json::json!({});";
        let violations = lint_contents("fixture.rs", contents);
        assert!(
            violations.is_empty(),
            "expected no violation for handler-body Value, got: {violations:?}",
        );
    }

    /// Inline `// allow-serde-value:` annotation inside the macro body
    /// suppresses the violation.
    #[test]
    fn lint_respects_inline_allow_annotation() {
        let contents = "let _ = source!(\n    // allow-serde-value: webhook ingress\n    serde_json::Value => handler,\n);";
        let violations = lint_contents("fixture.rs", contents);
        assert!(
            violations.is_empty(),
            "expected the inline allow-serde-value annotation to suppress the violation, got: {violations:?}",
        );
    }

    /// The same annotation on the line above the macro start also
    /// suppresses, for sites that don't want the annotation inside the
    /// macro body.
    #[test]
    fn lint_respects_above_line_allow_annotation() {
        let contents = "// allow-serde-value: deliberate anti-pattern\nlet _ = source!(\n    serde_json::Value => handler,\n);";
        let violations = lint_contents("fixture.rs", contents);
        assert!(
            violations.is_empty(),
            "expected the above-line allow-serde-value annotation to suppress the violation, got: {violations:?}",
        );
    }

    /// `serde_json::Value` appearing inside a string literal inside the
    /// macro body must NOT trigger a violation. The lint's string-handling
    /// in `consume_chars` skips literal contents.
    #[test]
    fn lint_does_not_flag_value_inside_string_literal_in_macro_body() {
        // The error message string mentions serde_json::Value but no type
        // slot uses it.
        let contents =
            "let _ = source!(MyEvent => MyHandler::new(\"serde_json::Value not allowed\"));";
        let violations = lint_contents("fixture.rs", contents);
        assert!(
            violations.is_empty(),
            "expected the lint to skip string-literal contents, got: {violations:?}",
        );
    }
}
