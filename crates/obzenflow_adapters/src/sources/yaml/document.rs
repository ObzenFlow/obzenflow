// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One immutable YAML snapshot built from parser events (FLOWIP-084n B6).
//!
//! Aliases, anchors, tags, merge keys, non-string keys, duplicate keys and
//! additional documents are rejected from the event stream, before any alias
//! could expand. Plain scalars resolve by the YAML 1.2.2 core schema.

use super::YamlSelection;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, TextPosition};
use obzenflow_runtime::stages::source::SourceError;
use saphyr_parser::{Event, Marker, Parser, ScalarStyle};
use std::borrow::Cow;
use std::collections::HashSet;
use std::num::NonZeroU32;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Node {
    pub(super) kind: NodeKind,
    pub(super) start: TextPosition,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum NodeKind {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Seq(Vec<Node>),
    Map(Vec<(String, Node)>),
}

/// saphyr reports one-based lines and zero-based character columns.
fn position(marker: &Marker) -> TextPosition {
    TextPosition {
        line: NonZeroU32::new(u32::try_from(marker.line()).unwrap_or(u32::MAX))
            .unwrap_or(NonZeroU32::MIN),
        column: u32::try_from(marker.col())
            .ok()
            .and_then(|col| col.checked_add(1))
            .and_then(NonZeroU32::new),
    }
}

fn located(reason: SourceDiagnosticReason, at: TextPosition) -> SourceDiagnostic {
    SourceDiagnostic::new(reason).position(at.line, at.column)
}

/// The whole input could not be decoded as one supported document.
fn undecodable(reason: SourceDiagnosticReason, at: TextPosition) -> SourceError {
    SourceError::Deserialization(located(reason, at))
}

/// Parses one bounded UTF-8 document into an owned snapshot.
pub(super) fn parse(bytes: &[u8]) -> Result<Node, SourceError> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        let prefix = String::from_utf8_lossy(&bytes[..error.valid_up_to()]);
        undecodable(SourceDiagnosticReason::MalformedInput, end_of(&prefix))
    })?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);

    let mut tree = TreeBuilder::default();
    let mut documents = 0usize;
    for item in Parser::new_from_str(text) {
        // The scan error's text is parser detail; only its position survives.
        let (event, span) = item.map_err(|error| {
            undecodable(
                SourceDiagnosticReason::MalformedInput,
                position(error.marker()),
            )
        })?;
        let at = position(&span.start);
        if is_decorated(&event) {
            return Err(undecodable(
                SourceDiagnosticReason::UnsupportedConstruct,
                at,
            ));
        }
        match event {
            Event::DocumentStart(_) => {
                documents += 1;
                if documents > 1 {
                    return Err(undecodable(
                        SourceDiagnosticReason::UnsupportedConstruct,
                        at,
                    ));
                }
            }
            Event::Scalar(value, style, ..) => tree.scalar(value, style, at)?,
            Event::SequenceStart(..) => tree.open(Frame::seq(at), at)?,
            Event::MappingStart(..) => tree.open(Frame::map(at), at)?,
            Event::SequenceEnd | Event::MappingEnd => tree.close()?,
            Event::StreamStart | Event::StreamEnd | Event::DocumentEnd | Event::Nothing => {}
            Event::Alias(_) => {
                return Err(undecodable(
                    SourceDiagnosticReason::UnsupportedConstruct,
                    at,
                ));
            }
        }
    }
    tree.finish()
}

fn end_of(prefix: &str) -> TextPosition {
    let line = prefix.matches('\n').count().saturating_add(1);
    let column = prefix
        .rsplit('\n')
        .next()
        .map_or(0, |last| last.chars().count())
        .saturating_add(1);
    TextPosition {
        line: NonZeroU32::new(u32::try_from(line).unwrap_or(u32::MAX)).unwrap_or(NonZeroU32::MIN),
        column: NonZeroU32::new(u32::try_from(column).unwrap_or(u32::MAX)),
    }
}

/// Anchors (nonzero anchor id), explicit tags and aliases are outside the subset.
fn is_decorated(event: &Event<'_>) -> bool {
    match event {
        Event::Alias(_) => true,
        Event::Scalar(_, _, anchor, tag)
        | Event::SequenceStart(anchor, tag)
        | Event::MappingStart(anchor, tag) => *anchor != 0 || tag.is_some(),
        _ => false,
    }
}

enum Frame {
    Seq {
        items: Vec<Node>,
        start: TextPosition,
    },
    Map {
        entries: Vec<(String, Node)>,
        keys: HashSet<String>,
        pending_key: Option<String>,
        start: TextPosition,
    },
}

impl Frame {
    fn seq(start: TextPosition) -> Self {
        Self::Seq {
            items: Vec::new(),
            start,
        }
    }

    fn map(start: TextPosition) -> Self {
        Self::Map {
            entries: Vec::new(),
            keys: HashSet::new(),
            pending_key: None,
            start,
        }
    }

    fn into_node(self) -> Node {
        match self {
            Self::Seq { items, start } => Node {
                kind: NodeKind::Seq(items),
                start,
            },
            Self::Map { entries, start, .. } => Node {
                kind: NodeKind::Map(entries),
                start,
            },
        }
    }
}

#[derive(Default)]
struct TreeBuilder {
    stack: Vec<Frame>,
    root: Option<Node>,
}

impl TreeBuilder {
    fn at_key_position(&self) -> bool {
        matches!(
            self.stack.last(),
            Some(Frame::Map {
                pending_key: None,
                ..
            })
        )
    }

    fn scalar(
        &mut self,
        value: Cow<'_, str>,
        style: ScalarStyle,
        at: TextPosition,
    ) -> Result<(), SourceError> {
        if let Some(Frame::Map {
            keys,
            pending_key: pending @ None,
            ..
        }) = self.stack.last_mut()
        {
            let key = resolve_key(&value, style)
                .ok_or_else(|| undecodable(SourceDiagnosticReason::UnsupportedConstruct, at))?;
            if !keys.insert(key.clone()) {
                return Err(undecodable(SourceDiagnosticReason::DuplicateKey, at));
            }
            *pending = Some(key);
            return Ok(());
        }
        self.push(Node {
            kind: resolve_scalar(&value, style),
            start: at,
        })
    }

    /// A collection in key position is a complex key, outside the subset.
    fn open(&mut self, frame: Frame, at: TextPosition) -> Result<(), SourceError> {
        if self.at_key_position() {
            return Err(undecodable(
                SourceDiagnosticReason::UnsupportedConstruct,
                at,
            ));
        }
        self.stack.push(frame);
        Ok(())
    }

    fn close(&mut self) -> Result<(), SourceError> {
        match self.stack.pop() {
            Some(frame) => self.push(frame.into_node()),
            None => Err(SourceError::Deserialization(
                SourceDiagnosticReason::MalformedInput.into(),
            )),
        }
    }

    fn push(&mut self, node: Node) -> Result<(), SourceError> {
        match self.stack.last_mut() {
            None => {
                self.root = Some(node);
                Ok(())
            }
            Some(Frame::Seq { items, .. }) => {
                items.push(node);
                Ok(())
            }
            Some(Frame::Map {
                entries,
                pending_key,
                ..
            }) => match pending_key.take() {
                Some(key) => {
                    entries.push((key, node));
                    Ok(())
                }
                None => Err(undecodable(
                    SourceDiagnosticReason::UnsupportedConstruct,
                    node.start,
                )),
            },
        }
    }

    /// An empty stream holds no document, which differs from an empty sequence.
    fn finish(self) -> Result<Node, SourceError> {
        self.root.ok_or_else(|| {
            SourceError::Deserialization(SourceDiagnosticReason::UnexpectedShape.into())
        })
    }
}

/// Mapping keys must be strings; a plain `<<` would be a merge key.
fn resolve_key(value: &str, style: ScalarStyle) -> Option<String> {
    if !matches!(style, ScalarStyle::Plain) {
        return Some(value.to_owned());
    }
    match resolve_scalar(value, style) {
        NodeKind::Str(key) if key != "<<" => Some(key),
        _ => None,
    }
}

/// YAML 1.2.2 core schema (§10.3.2). Quoted and block scalars stay strings;
/// timestamps are not part of the core schema, so dates stay strings too.
fn resolve_scalar(value: &str, style: ScalarStyle) -> NodeKind {
    if !matches!(style, ScalarStyle::Plain) {
        return NodeKind::Str(value.to_owned());
    }
    match value {
        "" | "~" | "null" | "Null" | "NULL" => NodeKind::Null,
        "true" | "True" | "TRUE" => NodeKind::Bool(true),
        "false" | "False" | "FALSE" => NodeKind::Bool(false),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => NodeKind::Float(f64::INFINITY),
        "-.inf" | "-.Inf" | "-.INF" => NodeKind::Float(f64::NEG_INFINITY),
        ".nan" | ".NaN" | ".NAN" => NodeKind::Float(f64::NAN),
        _ => resolve_number(value).unwrap_or_else(|| NodeKind::Str(value.to_owned())),
    }
}

fn resolve_number(value: &str) -> Option<NodeKind> {
    let radix = |digits: &str, radix: u32| {
        (!digits.is_empty() && digits.chars().all(|c| c.is_digit(radix)))
            .then(|| i128::from_str_radix(digits, radix).ok())
            .flatten()
            .map(NodeKind::Int)
    };
    if let Some(digits) = value.strip_prefix("0o") {
        return radix(digits, 8);
    }
    if let Some(digits) = value.strip_prefix("0x") {
        return radix(digits, 16);
    }
    if is_core_int(value) {
        // Out-of-range integers stay text, so a numeric field rejects the record.
        return Some(
            value
                .parse::<i128>()
                .map_or_else(|_| NodeKind::Str(value.to_owned()), NodeKind::Int),
        );
    }
    if is_core_float(value) {
        return value.parse::<f64>().ok().map(NodeKind::Float);
    }
    None
}

fn unsigned(value: &str) -> &str {
    value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value)
}

fn digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

/// `[-+]? [0-9]+`
fn is_core_int(value: &str) -> bool {
    digits(unsigned(value))
}

/// `[-+]? ( \. [0-9]+ | [0-9]+ ( \. [0-9]* )? ) ( [eE] [-+]? [0-9]+ )?`
fn is_core_float(value: &str) -> bool {
    let body = unsigned(value);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let mantissa_ok = match mantissa.split_once('.') {
        Some(("", fraction)) => digits(fraction),
        Some((whole, fraction)) => digits(whole) && fraction.bytes().all(|b| b.is_ascii_digit()),
        None => digits(mantissa),
    };
    mantissa_ok && exponent.is_none_or(|exponent| digits(unsigned(exponent)))
}

/// Parses an RFC 6901 pointer into unescaped reference tokens.
pub(super) fn pointer_tokens(pointer: &str) -> Option<Vec<String>> {
    if pointer.is_empty() {
        return Some(Vec::new());
    }
    pointer
        .strip_prefix('/')?
        .split('/')
        .map(|token| {
            let mut unescaped = String::with_capacity(token.len());
            let mut chars = token.chars();
            while let Some(c) = chars.next() {
                match c {
                    '~' => match chars.next() {
                        Some('0') => unescaped.push('~'),
                        Some('1') => unescaped.push('/'),
                        _ => return None,
                    },
                    other => unescaped.push(other),
                }
            }
            Some(unescaped)
        })
        .collect()
}

fn array_index(token: &str) -> Option<usize> {
    let canonical = token == "0" || (digits(token) && !token.starts_with('0'));
    canonical.then(|| token.parse().ok()).flatten()
}

/// Resolves the configured selection into the records it names.
pub(super) fn select(root: Node, selection: &YamlSelection) -> Result<Vec<Node>, SourceError> {
    let not_found = |at: TextPosition| {
        SourceError::Validation(located(SourceDiagnosticReason::SelectionNotFound, at))
    };
    match selection {
        YamlSelection::Document => Ok(vec![root]),
        YamlSelection::Sequence => into_sequence(root),
        YamlSelection::SequenceAt(pointer) => {
            let tokens = pointer_tokens(pointer).ok_or_else(|| not_found(root.start))?;
            let mut node = root;
            for token in tokens {
                let at = node.start;
                node = match node.kind {
                    NodeKind::Map(entries) => entries
                        .into_iter()
                        .find_map(|(key, value)| (key == token).then_some(value)),
                    NodeKind::Seq(items) => {
                        array_index(&token).and_then(|index| items.into_iter().nth(index))
                    }
                    _ => None,
                }
                .ok_or_else(|| not_found(at))?;
            }
            into_sequence(node)
        }
    }
}

fn into_sequence(node: Node) -> Result<Vec<Node>, SourceError> {
    match node.kind {
        NodeKind::Seq(items) => Ok(items),
        _ => Err(SourceError::Validation(located(
            SourceDiagnosticReason::UnexpectedShape,
            node.start,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;

    fn reason(result: Result<Node, SourceError>) -> SourceDiagnosticReason {
        result.expect_err("document rejected").diagnostic().reason()
    }

    fn root(text: &str) -> Node {
        parse(text.as_bytes()).expect("document parses")
    }

    fn line_col(error: &SourceError) -> Option<(u32, Option<u32>)> {
        error
            .diagnostic()
            .location()
            .position()
            .map(|p| (p.line.get(), p.column.map(NonZeroU32::get)))
    }

    #[test]
    fn plain_scalars_follow_the_core_schema() {
        let cases = [
            ("null", NodeKind::Null),
            ("~", NodeKind::Null),
            ("True", NodeKind::Bool(true)),
            ("FALSE", NodeKind::Bool(false)),
            ("007", NodeKind::Int(7)),
            ("-12", NodeKind::Int(-12)),
            ("0o17", NodeKind::Int(15)),
            ("0x1F", NodeKind::Int(31)),
            ("1.5e3", NodeKind::Float(1500.0)),
            (".5", NodeKind::Float(0.5)),
            ("-.inf", NodeKind::Float(f64::NEG_INFINITY)),
            // YAML 1.1 booleans, dates and partial numbers stay strings.
            ("yes", NodeKind::Str("yes".into())),
            ("NO", NodeKind::Str("NO".into())),
            ("2026-10-09", NodeKind::Str("2026-10-09".into())),
            ("inf", NodeKind::Str("inf".into())),
            ("1_000", NodeKind::Str("1_000".into())),
            ("0o9", NodeKind::Str("0o9".into())),
            (
                "999999999999999999999999999999999999999999",
                NodeKind::Str("999999999999999999999999999999999999999999".into()),
            ),
        ];
        for (text, expected) in cases {
            assert_eq!(root(text).kind, expected, "{text}");
        }
        assert_eq!(root("\"true\"").kind, NodeKind::Str("true".into()));
        assert_eq!(root("'007'").kind, NodeKind::Str("007".into()));
        assert!(matches!(root(".nan").kind, NodeKind::Float(f) if f.is_nan()));
    }

    #[test]
    fn unsupported_constructs_are_rejected_before_expansion() {
        let cases = [
            "base: &base {a: 1}\nother: *base\n",
            "value: !custom 1\n",
            "base: {a: 1}\nmerged:\n  <<: {b: 2}\n",
            "1: integer key\n",
            "? [complex]\n: key\n",
            "--- 1\n--- 2\n",
        ];
        for text in cases {
            assert_eq!(
                reason(parse(text.as_bytes())),
                SourceDiagnosticReason::UnsupportedConstruct,
                "{text}"
            );
        }
        // A quoted `<<` and a quoted numeric key are ordinary strings.
        assert!(parse(b"\"<<\": 1\n'1': 2\n").is_ok());
    }

    #[test]
    fn document_failures_carry_reason_and_position_only() {
        let duplicate = parse(b"a: 1\nb: 2\na: 3\n").unwrap_err();
        assert_eq!(
            duplicate.diagnostic().reason(),
            SourceDiagnosticReason::DuplicateKey
        );
        assert_eq!(line_col(&duplicate), Some((3, Some(1))));

        let malformed = parse(b"key: [unterminated SECRET\n").unwrap_err();
        assert_eq!(malformed.kind(), SourcePollErrorKind::Deserialization);
        assert_eq!(
            malformed.diagnostic().reason(),
            SourceDiagnosticReason::MalformedInput
        );
        assert!(!format!("{malformed} {malformed:?}").contains("SECRET"));

        let invalid_utf8 = parse(b"a: 1\nb: \xff\n").unwrap_err();
        assert_eq!(
            invalid_utf8.diagnostic().reason(),
            SourceDiagnosticReason::MalformedInput
        );
        assert_eq!(line_col(&invalid_utf8), Some((2, Some(4))));

        assert_eq!(reason(parse(b"")), SourceDiagnosticReason::UnexpectedShape);
        assert_eq!(
            reason(parse(b"# only a comment\n")),
            SourceDiagnosticReason::UnexpectedShape
        );
    }

    #[test]
    fn positions_are_one_based_with_unicode_scalar_columns() {
        let node = root("a:\n  é: [1, 2]\n");
        let NodeKind::Map(entries) = node.kind else {
            panic!("mapping");
        };
        let NodeKind::Map(inner) = &entries[0].1.kind else {
            panic!("nested mapping");
        };
        let NodeKind::Seq(items) = &inner[0].1.kind else {
            panic!("sequence");
        };
        // `é` is one column although it is two bytes.
        assert_eq!(
            (
                items[1].start.line.get(),
                items[1].start.column.map(NonZeroU32::get)
            ),
            (2, Some(10))
        );
    }

    #[test]
    fn selection_resolves_pointers_and_distinguishes_missing_from_empty() {
        let text = "web_orders:\n  - 1\n  - 2\nempty: []\n\"a/b\":\n  - 3\nscalar: 4\n";
        let pointer = |p: &str| YamlSelection::SequenceAt(p.into());
        assert_eq!(
            select(root(text), &pointer("/web_orders")).unwrap().len(),
            2
        );
        assert!(select(root(text), &pointer("/empty")).unwrap().is_empty());
        assert_eq!(select(root(text), &pointer("/a~1b")).unwrap().len(), 1);
        assert_eq!(
            select(root(text), &pointer("/missing"))
                .unwrap_err()
                .diagnostic()
                .reason(),
            SourceDiagnosticReason::SelectionNotFound
        );
        assert_eq!(
            select(root(text), &pointer("/scalar"))
                .unwrap_err()
                .diagnostic()
                .reason(),
            SourceDiagnosticReason::UnexpectedShape
        );
        assert_eq!(
            select(root(text), &YamlSelection::Sequence)
                .unwrap_err()
                .diagnostic()
                .reason(),
            SourceDiagnosticReason::UnexpectedShape
        );
        assert_eq!(
            select(root(text), &YamlSelection::Document).unwrap().len(),
            1
        );
        assert_eq!(
            select(root("[[1], [2, 3]]"), &pointer("/1")).unwrap().len(),
            2
        );
        assert!(pointer_tokens("no-leading-slash").is_none());
        assert!(pointer_tokens("/bad~2escape").is_none());
        assert_eq!(array_index("01"), None);
    }
}
