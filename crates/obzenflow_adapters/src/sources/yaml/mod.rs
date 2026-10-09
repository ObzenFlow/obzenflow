// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Finite YAML source (FLOWIP-084n).
//!
//! [`YamlSource`] is cold configuration: building and cloning it do no I/O, so
//! strict replay never needs the file. Opening reads at most `max_bytes`,
//! parses one immutable document and resolves the [`YamlSelection`]. Each
//! selected record goes to an application [`YamlDecoder`]; a rejected record
//! is journalled as `Validation` and reading continues with the next one.

mod de;
mod document;

use document::Node;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::OneFactStageOutput;
use obzenflow_runtime::stages::source::{
    FiniteSourceConnector, SourceError, SourceReaderInitContext, TypedFiniteSourceHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_MAX_BYTES: usize = 1024 * 1024;

/// Which part of the document holds the records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum YamlSelection {
    /// Pass the complete root value to the decoder once.
    Document,
    /// Require a root sequence and decode each element.
    Sequence,
    /// Resolve an RFC 6901 JSON Pointer, require a sequence there and decode
    /// each element.
    SequenceAt(String),
}

/// Application-owned mapping from one selected YAML record to one fact.
pub trait YamlDecoder: Clone + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn decode(&self, record: YamlRecord<'_>) -> Result<Self::Output, YamlDecodeError>;
}

/// One selected record. Exposes neither parser internals nor file access.
pub struct YamlRecord<'a> {
    node: &'a Node,
    index: u64,
}

impl YamlRecord<'_> {
    /// Zero-based index of this record within the selection.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Deserialize this record with serde. Errors name schema fields only.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, YamlDecodeError> {
        T::deserialize(de::NodeDeserializer::new(self.node))
            .map_err(|error| YamlDecodeError(error.into_diagnostic()))
    }
}

impl std::fmt::Debug for YamlRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YamlRecord")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

/// Error returned by a [`YamlDecoder`]. It carries a typed diagnostic only;
/// the reader adds the record index and position.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct YamlDecodeError(SourceDiagnostic);

impl YamlDecodeError {
    pub fn missing_field(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::MissingField).within_field(field))
    }

    pub fn invalid_value(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue).within_field(field))
    }

    pub fn invalid_record() -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord))
    }

    /// Nests the path under a schema-declared parent field.
    pub fn within_field(self, name: &'static str) -> Self {
        Self(self.0.within_field(name))
    }

    pub fn code(self, code: SourceErrorCode) -> Self {
        Self(self.0.code(code))
    }

    fn locate(self, index: u64, node: &Node) -> SourceDiagnostic {
        let diagnostic = self.0.record(index);
        if diagnostic.location().position().is_some() {
            return diagnostic;
        }
        diagnostic.position(node.start.line, node.start.column)
    }
}

pub struct YamlSourceBuilder<D> {
    decoder: D,
    path: Option<PathBuf>,
    selection: YamlSelection,
    batch_size: usize,
    max_bytes: usize,
}

impl<D> std::fmt::Debug for YamlSourceBuilder<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YamlSourceBuilder")
            .field("decoder", &std::any::type_name::<D>())
            .field("batch_size", &self.batch_size)
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

impl<D: YamlDecoder> YamlSourceBuilder<D> {
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn selection(mut self, selection: YamlSelection) -> Self {
        self.selection = selection;
        self
    }

    /// Maximum records decoded per poll; must be positive.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Maximum file size read at opening; must be positive.
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Validates local configuration only; the file is not touched.
    pub fn build(self) -> anyhow::Result<YamlSource<D>> {
        let path = self
            .path
            .ok_or_else(|| anyhow::anyhow!("yaml source: path required"))?;
        anyhow::ensure!(
            self.batch_size > 0,
            "yaml source: batch_size must be positive"
        );
        anyhow::ensure!(
            self.max_bytes > 0,
            "yaml source: max_bytes must be positive"
        );
        if let YamlSelection::SequenceAt(pointer) = &self.selection {
            anyhow::ensure!(
                document::pointer_tokens(pointer).is_some(),
                "yaml source: selection must be an RFC 6901 JSON Pointer"
            );
        }
        Ok(YamlSource {
            path,
            decoder: self.decoder,
            selection: self.selection,
            batch_size: self.batch_size,
            max_bytes: self.max_bytes,
        })
    }
}

/// Cold, reusable YAML configuration. File I/O happens only in `open`.
#[derive(Clone)]
pub struct YamlSource<D> {
    path: PathBuf,
    decoder: D,
    selection: YamlSelection,
    batch_size: usize,
    max_bytes: usize,
}

impl<D: YamlDecoder> YamlSource<D> {
    pub fn builder(decoder: D) -> YamlSourceBuilder<D> {
        YamlSourceBuilder {
            decoder,
            path: None,
            selection: YamlSelection::Document,
            batch_size: DEFAULT_BATCH_SIZE,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl<D: YamlDecoder> std::fmt::Debug for YamlSource<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YamlSource")
            .field("decoder", &std::any::type_name::<D>())
            .field("output", &std::any::type_name::<D::Output>())
            .finish_non_exhaustive()
    }
}

impl<D: YamlDecoder> SourceTyping for YamlSource<D> {
    type Output = D::Output;
}

impl<D: YamlDecoder> FiniteSourceConnector for YamlSource<D> {
    type Output = D::Output;
    type Reader = YamlReader<D>;

    fn open(&self, _context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        let bytes = read_bounded(&self.path, self.max_bytes)?;
        let root = document::parse(&bytes)?;
        let records = document::select(root, &self.selection)?;
        Ok(YamlReader {
            records: records.into_iter(),
            next_index: 0,
            batch_size: self.batch_size,
            pending: None,
            decoder: self.decoder.clone(),
        })
    }
}

/// Reads at most `max_bytes + 1`, so oversize input never reaches the parser.
fn read_bounded(path: &Path, max_bytes: usize) -> Result<Vec<u8>, SourceError> {
    let unavailable = || SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into());
    let limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| unavailable())?
        .take(limit)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    if bytes.len() > max_bytes {
        return Err(SourceError::Validation(
            SourceDiagnosticReason::SizeLimitExceeded.into(),
        ));
    }
    Ok(bytes)
}

/// One independently owned cursor over an immutable snapshot.
pub struct YamlReader<D> {
    records: std::vec::IntoIter<Node>,
    next_index: u64,
    batch_size: usize,
    pending: Option<SourceError>,
    decoder: D,
}

impl<D: YamlDecoder> std::fmt::Debug for YamlReader<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YamlReader")
            .field("decoder", &std::any::type_name::<D>())
            .field("next_index", &self.next_index)
            .field("remaining", &self.records.len())
            .finish_non_exhaustive()
    }
}

impl<D: YamlDecoder> TypedFiniteSourceHandler for YamlReader<D> {
    type Output = D::Output;

    /// A rejection behind a valid prefix is reported on the next poll, so batch
    /// size never changes which records are admitted.
    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if let Some(rejection) = self.pending.take() {
            return Err(rejection);
        }
        let mut batch = Vec::with_capacity(self.batch_size.min(self.records.len()));
        while batch.len() < self.batch_size {
            let Some(node) = self.records.next() else {
                break;
            };
            let index = self.next_index;
            self.next_index = self.next_index.saturating_add(1);
            match self.decoder.decode(YamlRecord { node: &node, index }) {
                Ok(item) => batch.push(item),
                Err(error) => {
                    let rejection = SourceError::Validation(error.locate(index, &node));
                    if batch.is_empty() {
                        return Err(rejection);
                    }
                    self.pending = Some(rejection);
                    break;
                }
            }
        }
        Ok((!batch.is_empty()).then_some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
    use obzenflow_core::TypedPayload;
    use serde::{Deserialize, Serialize};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn context() -> SourceReaderInitContext {
        SourceReaderInitContext {
            stage_id: obzenflow_core::StageId::new(),
            stage_name: "orders".into(),
            flow_name: "test".into(),
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Order {
        order_id: String,
        amount_cents: i64,
    }

    impl TypedPayload for Order {
        const EVENT_TYPE: &'static str = "test.yaml_order";
    }

    /// Serde decoding plus one domain rule, as an application decoder would.
    #[derive(Clone)]
    struct OrderYaml;

    impl YamlDecoder for OrderYaml {
        type Output = Order;

        fn decode(&self, record: YamlRecord<'_>) -> Result<Order, YamlDecodeError> {
            let order: Order = record.deserialize()?;
            if order.amount_cents <= 0 {
                return Err(YamlDecodeError::invalid_value("amount_cents"));
            }
            Ok(order)
        }
    }

    fn file(text: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        file
    }

    fn source(
        file: &NamedTempFile,
        selection: YamlSelection,
        batch: usize,
    ) -> YamlSource<OrderYaml> {
        YamlSource::builder(OrderYaml)
            .path(file.path())
            .selection(selection)
            .batch_size(batch)
            .build()
            .unwrap()
    }

    fn drain(reader: &mut YamlReader<OrderYaml>) -> (Vec<String>, Vec<SourceError>) {
        let mut ids = Vec::new();
        let mut errors = Vec::new();
        loop {
            match reader.next() {
                Ok(Some(batch)) => ids.extend(batch.into_iter().map(|order| order.order_id)),
                Ok(None) => return (ids, errors),
                Err(error) => errors.push(error),
            }
        }
    }

    fn order(id: &str, amount: i64) -> String {
        format!("  - {{order_id: \"{id}\", amount_cents: {amount}}}\n")
    }

    #[test]
    fn selections_emit_records_in_document_order() {
        #[derive(Clone)]
        struct Catalogue;

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Companies {
            companies: Vec<String>,
            aggregators: Vec<String>,
        }

        impl TypedPayload for Companies {
            const EVENT_TYPE: &'static str = "test.yaml_catalogue";
        }

        impl YamlDecoder for Catalogue {
            type Output = Companies;

            fn decode(&self, record: YamlRecord<'_>) -> Result<Companies, YamlDecodeError> {
                record.deserialize()
            }
        }

        let catalogue = file("companies: [a, b]\naggregators: [c]\n");
        let mut whole = YamlSource::builder(Catalogue)
            .path(catalogue.path())
            .build()
            .unwrap()
            .open(context())
            .unwrap();
        assert_eq!(
            whole.next().unwrap(),
            Some(vec![Companies {
                companies: vec!["a".into(), "b".into()],
                aggregators: vec!["c".into()],
            }])
        );
        assert_eq!(whole.next().unwrap(), None);

        let root_sequence =
            file(&format!("{}{}", order("a", 1), order("b", 2)).replace("  - ", "- "));
        let (ids, errors) = drain(
            &mut source(&root_sequence, YamlSelection::Sequence, 64)
                .open(context())
                .unwrap(),
        );
        assert_eq!((ids, errors.len()), (vec!["a".into(), "b".into()], 0));

        let nested = file(&format!(
            "web_orders:\n{}{}store_orders: []\n",
            order("w1", 1),
            order("w2", 2)
        ));
        let at = |pointer: &str| YamlSelection::SequenceAt(pointer.into());
        let (ids, _) = drain(
            &mut source(&nested, at("/web_orders"), 1)
                .open(context())
                .unwrap(),
        );
        assert_eq!(ids, ["w1", "w2"]);
        let (ids, errors) = drain(
            &mut source(&nested, at("/store_orders"), 1)
                .open(context())
                .unwrap(),
        );
        assert!(
            ids.is_empty() && errors.is_empty(),
            "empty selection completes"
        );
        let missing = source(&nested, at("/returns"), 1)
            .open(context())
            .unwrap_err();
        assert_eq!(
            missing.diagnostic().reason(),
            SourceDiagnosticReason::SelectionNotFound
        );
    }

    #[test]
    fn construction_and_clone_are_cold_and_opens_are_independent() {
        let configured = YamlSource::builder(OrderYaml)
            .path("/nonexistent/secret-dir/orders.yaml")
            .selection(YamlSelection::SequenceAt("/secret_pointer".into()))
            .build()
            .expect("building does no I/O");
        let copy = configured.clone();
        let missing = copy.open(context()).unwrap_err();
        assert_eq!(missing.kind(), SourcePollErrorKind::Transport);
        assert_eq!(
            missing.diagnostic().reason(),
            SourceDiagnosticReason::InputUnavailable
        );
        let rendered = format!("{missing} {missing:?} {configured:?}");
        assert!(!rendered.contains("secret"), "{rendered}");

        let input = file(&format!("{}{}", order("a", 1), order("b", 2)).replace("  - ", "- "));
        let configured = source(&input, YamlSelection::Sequence, 1);
        let mut first = configured.open(context()).unwrap();
        let mut second = configured.open(context()).unwrap();
        assert_eq!(first.next().unwrap().unwrap()[0].order_id, "a");
        assert_eq!(first.next().unwrap().unwrap()[0].order_id, "b");
        assert_eq!(second.next().unwrap().unwrap()[0].order_id, "a");

        assert!(YamlSource::builder(OrderYaml).build().is_err());
        assert!(YamlSource::builder(OrderYaml)
            .path("x")
            .batch_size(0)
            .build()
            .is_err());
        assert!(YamlSource::builder(OrderYaml)
            .path("x")
            .max_bytes(0)
            .build()
            .is_err());
        assert!(YamlSource::builder(OrderYaml)
            .path("x")
            .selection(YamlSelection::SequenceAt("orders".into()))
            .build()
            .is_err());
    }

    #[test]
    fn rejections_preserve_neighbours_independently_of_batch_size() {
        // Middle, first, last, consecutive and all-rejected inputs.
        let cases = [
            (vec![("a", 1), ("b", 0), ("c", 1)], vec!["a", "c"], vec![1]),
            (vec![("a", 0), ("b", 1)], vec!["b"], vec![0]),
            (vec![("a", 1), ("b", 0)], vec!["a"], vec![1]),
            (
                vec![("a", 1), ("b", 0), ("c", -1), ("d", 1)],
                vec!["a", "d"],
                vec![1, 2],
            ),
            (vec![("a", 0), ("b", -2)], vec![], vec![0, 1]),
        ];
        for (records, expected_ids, expected_rejections) in cases {
            let text: String = std::iter::once("orders:\n".to_string())
                .chain(records.iter().map(|(id, amount)| order(id, *amount)))
                .collect();
            let input = file(&text);
            for batch in [1, 64] {
                let mut reader = source(&input, YamlSelection::SequenceAt("/orders".into()), batch)
                    .open(context())
                    .unwrap();
                let (ids, errors) = drain(&mut reader);
                assert_eq!(ids, expected_ids, "batch {batch}: {text}");
                let rejected: Vec<_> = errors
                    .iter()
                    .map(|error| {
                        assert_eq!(error.kind(), SourcePollErrorKind::Validation);
                        assert!(!error.is_terminal());
                        let diagnostic = error.diagnostic();
                        assert_eq!(diagnostic.reason(), SourceDiagnosticReason::InvalidValue);
                        diagnostic.location().record_index().unwrap()
                    })
                    .collect();
                assert_eq!(rejected, expected_rejections, "batch {batch}: {text}");
            }
        }
    }

    #[test]
    fn document_failures_fail_opening_before_any_record() {
        let cases = [
            (
                "orders:\n  - &a {order_id: x, amount_cents: 1}\n  - *a\n",
                SourceDiagnosticReason::UnsupportedConstruct,
            ),
            (
                "orders: []\norders: []\n",
                SourceDiagnosticReason::DuplicateKey,
            ),
            ("orders: [\n", SourceDiagnosticReason::MalformedInput),
            ("orders: 3\n", SourceDiagnosticReason::UnexpectedShape),
        ];
        for (text, reason) in cases {
            let input = file(text);
            let error = source(&input, YamlSelection::SequenceAt("/orders".into()), 1)
                .open(context())
                .unwrap_err();
            assert_eq!(error.diagnostic().reason(), reason, "{text}");
        }

        let input = file(&format!("orders:\n{}", order("a", 1)));
        let oversize = YamlSource::builder(OrderYaml)
            .path(input.path())
            .max_bytes(8)
            .build()
            .unwrap()
            .open(context())
            .unwrap_err();
        assert_eq!(
            oversize.diagnostic().reason(),
            SourceDiagnosticReason::SizeLimitExceeded
        );
        assert_eq!(
            oversize.diagnostic().to_string(),
            "the input exceeds its configured size limit"
        );
    }

    #[test]
    fn rejections_carry_location_without_rejected_values() {
        let input = file(
            "orders:\n  - {order_id: \"a\", amount_cents: 1}\n  - {order_id: \"SECRET_ID\", amount_cents: SECRET_AMOUNT}\n  - {order_id: \"b\", amount_cents: 1, SECRET_KEY: 1}\n",
        );
        let mut reader = source(&input, YamlSelection::SequenceAt("/orders".into()), 64)
            .open(context())
            .unwrap();
        let (ids, errors) = drain(&mut reader);
        assert_eq!(ids, ["a", "b"], "Order does not deny unknown fields");
        assert_eq!(errors.len(), 1);
        let diagnostic = errors[0].diagnostic();
        assert_eq!(diagnostic.location().record_index(), Some(1));
        let at = diagnostic.location().position().unwrap();
        assert_eq!(at.line.get(), 3);
        let rendered = format!("{} {:?}", errors[0], errors[0]);
        assert!(!rendered.contains("SECRET"), "{rendered}");
        assert_eq!(
            diagnostic.to_string(),
            format!(
                "invalid value in field amount_cents (line 3, column {})",
                at.column.unwrap()
            )
        );
    }
}
