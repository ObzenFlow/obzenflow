// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared routing helpers for hosted HTTP surfaces.
//!
//! The public managed-surface contract uses `:name` path parameters, while the
//! infrastructure route trie uses `matchit`'s `{name}` syntax internally.

/// Convert a public ObzenFlow route template into matchit's path syntax.
///
/// Public syntax:
/// - exact segments: `/api/v1/count`
/// - named segments: `/:key`, `/range/:start/:end`
pub(crate) fn public_template_to_matchit(template: &str) -> Result<String, String> {
    if !template.starts_with('/') {
        return Err("Route template must start with '/'".to_string());
    }

    if template.contains('{') || template.contains('}') {
        return Err("Route template must not contain '{' or '}' (use ':name' parameters)".to_string());
    }

    if template == "/" {
        return Ok("/".to_string());
    }

    let mut out = String::with_capacity(template.len());
    out.push('/');

    for (idx, segment) in template
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .enumerate()
    {
        if idx > 0 {
            out.push('/');
        }

        if let Some(param_name) = segment.strip_prefix(':') {
            if param_name.is_empty() {
                return Err("Path parameter name cannot be empty".to_string());
            }
            if !param_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                return Err(format!(
                    "Invalid path parameter name '{param_name}': use ASCII alphanumeric or '_'"
                ));
            }

            out.push('{');
            out.push_str(param_name);
            out.push('}');
            continue;
        }

        if segment.contains(':') {
            return Err(format!(
                "Invalid route segment '{segment}': ':' is only supported as a prefix for path parameters"
            ));
        }

        if segment.starts_with('*') || segment.contains('*') {
            return Err(format!(
                "Unsupported route segment '{segment}': wildcard segments are not supported"
            ));
        }

        out.push_str(segment);
    }

    Ok(out)
}

pub(crate) fn matchit_template_to_public(matchit_template: &str) -> String {
    if matchit_template == "/" {
        return "/".to_string();
    }

    let mut out = String::with_capacity(matchit_template.len());
    out.push('/');

    for (idx, segment) in matchit_template
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .enumerate()
    {
        if idx > 0 {
            out.push('/');
        }

        if let Some(stripped) = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
            if stripped.starts_with('*') {
                // Keep matchit syntax for catch-all patterns (not currently supported by 093b).
                out.push_str(segment);
            } else {
                out.push(':');
                out.push_str(stripped);
            }
            continue;
        }

        out.push_str(segment);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_template_to_matchit_keeps_root() {
        assert_eq!(public_template_to_matchit("/").unwrap(), "/");
    }

    #[test]
    fn public_template_to_matchit_translates_named_params() {
        assert_eq!(
            public_template_to_matchit("/items/:id").unwrap(),
            "/items/{id}"
        );
        assert_eq!(
            public_template_to_matchit("/range/:start/:end").unwrap(),
            "/range/{start}/{end}"
        );
    }

    #[test]
    fn public_template_to_matchit_rejects_empty_param_name() {
        let err = public_template_to_matchit("/items/:").unwrap_err();
        assert!(err.contains("cannot be empty"), "unexpected err: {err}");
    }

    #[test]
    fn public_template_to_matchit_rejects_mid_segment_colons() {
        let err = public_template_to_matchit("/items/id:foo").unwrap_err();
        assert!(err.contains("only supported as a prefix"), "unexpected err: {err}");
    }

    #[test]
    fn public_template_to_matchit_rejects_matchit_syntax() {
        let err = public_template_to_matchit("/items/{id}").unwrap_err();
        assert!(err.contains("must not contain"), "unexpected err: {err}");
    }

    #[test]
    fn matchit_template_to_public_translates_named_params() {
        assert_eq!(matchit_template_to_public("/items/{id}"), "/items/:id");
        assert_eq!(
            matchit_template_to_public("/range/{start}/{end}"),
            "/range/:start/:end"
        );
    }
}
