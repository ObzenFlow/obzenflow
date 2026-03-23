// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub fn truncate_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let mut iter = s.chars();
    for _ in 0..max_chars {
        if iter.next().is_none() {
            return s.to_string();
        }
    }

    if iter.next().is_none() {
        return s.to_string();
    }

    if max_chars <= 3 {
        return "...".to_string();
    }

    let mut out = s.chars().take(max_chars - 3).collect::<String>();
    out.push_str("...");
    out
}
