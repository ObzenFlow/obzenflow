// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The `yaml` module holds the finite YAML source.
//!
//! A [`YamlSource`] handler reads through a file system port. Each read
//! takes at most `max_bytes` from the file, parses the document, resolves the
//! [`YamlSelection`] and returns the next selected records. The port records
//! each reply. The handler maps each recorded record with its [`YamlDecoder`].
//! The handler reports each record that the decoder rejects. The read then
//! continues.

mod de;
mod document;

use super::system::{YamlFile, YamlFileSystem};
use super::version::{input_changed, FileVersion};
use document::Node;
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::OneFactOutcome;
use obzenflow_runtime::queries::{FiniteRead, QueryBookmark, QueryError};
use obzenflow_runtime::stages::common::handlers::source::{SourceRead, TypedFiniteSourceHandler};
use obzenflow_runtime::stages::source::SourceError;
use obzenflow_runtime::systems::Port;
use obzenflow_runtime::typing::SourceTyping;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_MAX_BYTES: usize = 1024 * 1024;

/// `YamlSelection` names the part of the document that holds the records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum YamlSelection {
    /// `Document` selects the root value as the only record.
    Document,
    /// `Sequence` selects each element of the root value, which must be a sequence.
    Sequence,
    /// `SequenceAt` selects each element of the value at an RFC 6901 JSON Pointer.
    /// That value must be a sequence.
    SequenceAt(String),
}

/// `YamlDecoder` maps one selected YAML record to one fact.
/// An application passes its decoder to [`YamlSource::builder`].
pub trait YamlDecoder: Clone + Send + Sync + 'static {
    type Output: OneFactOutcome + Send + Sync + 'static;

    fn decode(&self, record: YamlRecord<'_>) -> Result<Self::Output, YamlDecodeError>;
}

/// `YamlRecord` is one selected record. It gives no access to the parser or to the file.
pub struct YamlRecord<'a> {
    node: &'a Node,
    index: u64,
}

impl YamlRecord<'_> {
    /// `index` returns the zero-based index of this record in the selection.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// `deserialize` decodes this record with serde. An error names only the fields
    /// of the schema.
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

/// `YamlDecodeError` is the error that a [`YamlDecoder`] returns. It holds only
/// a typed diagnostic. The source handler adds the record index and the position.
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

    /// `within_field` puts the schema field `name` at the start of the field path of this error.
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
    port: Option<Port<dyn YamlFile>>,
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

    pub fn through(mut self, port: Port<dyn YamlFile>) -> Self {
        self.port = Some(port);
        self
    }

    pub fn selection(mut self, selection: YamlSelection) -> Self {
        self.selection = selection;
        self
    }

    /// `batch_size` sets the maximum number of records in one read. It must be
    /// positive.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// `max_bytes` sets the maximum number of bytes that one read takes from the
    /// file. It must be positive.
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// `build` checks only the configuration of the source. It does not open the file.
    pub fn build(self) -> anyhow::Result<YamlSource<D>> {
        let port = match (self.path, self.port) {
            (Some(path), None) => YamlFileSystem::at(path).port(),
            (None, Some(port)) => port,
            (None, None) => anyhow::bail!("yaml source: the source must have a path or a port"),
            (Some(_), Some(_)) => {
                anyhow::bail!(
                    "yaml source: the source takes a path or a port, and this builder has both"
                )
            }
        };
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
            port,
            decoder: self.decoder,
            selection: self.selection,
            batch_size: self.batch_size,
            max_bytes: self.max_bytes,
        })
    }
}

/// `YamlSource` reads the records of a YAML file through a cold port of the file
/// system. Its decoder maps each recorded record to the output.
#[derive(Clone)]
pub struct YamlSource<D> {
    port: Port<dyn YamlFile>,
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
            port: None,
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

impl<D: YamlDecoder> TypedFiniteSourceHandler for YamlSource<D> {
    type Output = D::Output;

    fn read(&self) -> SourceRead<D::Output> {
        let decoder = self.decoder.clone();
        SourceRead::pages(
            self.port.clone(),
            YamlRead {
                limit: std::num::NonZeroUsize::new(self.batch_size)
                    .expect("build checks that the batch size is positive"),
                selection: self.selection.clone(),
                max_bytes: self.max_bytes,
                after: None,
            },
            |from, page| {
                Ok(match page {
                    YamlPage::EndOfInput => None,
                    YamlPage::Batch { records, next } => Some((
                        (),
                        records,
                        YamlRead {
                            limit: from.limit,
                            selection: from.selection.clone(),
                            max_bytes: from.max_bytes,
                            after: Some(next),
                        },
                    )),
                })
            },
            move |_, line| decode_line(&decoder, line),
        )
    }
}

fn decode_line<D: YamlDecoder>(decoder: &D, line: YamlLine) -> Result<D::Output, SourceError> {
    match line {
        YamlLine::Record { index, node } => decoder
            .decode(YamlRecord {
                node: &node.0,
                index,
            })
            .map_err(|error| SourceError::Validation(error.locate(index, &node.0))),
        YamlLine::Refused { kind, diagnostic } => Err(SourceError::Terminal { kind, diagnostic }),
        YamlLine::Changed => Err(input_changed()),
    }
}

/// `YamlRead` is the request of one YAML read. A replay and a resume compare it
/// without `limit`, so a build can read pages of another size.
#[derive(Clone, Debug, Serialize)]
pub struct YamlRead {
    #[serde(skip_serializing)]
    pub limit: std::num::NonZeroUsize,
    pub selection: YamlSelection,
    pub max_bytes: usize,
    pub after: Option<QueryBookmark>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum YamlPage {
    Batch {
        records: Vec<YamlLine>,
        next: QueryBookmark,
    },
    EndOfInput,
}

pub(crate) fn read_yaml_page(path: &Path, from: YamlRead) -> Result<YamlPage, QueryError> {
    let read = FileYamlRead {
        batch: from.limit.get(),
        selection: from.selection,
        max_bytes: from.max_bytes,
        at: from.after.as_ref().map(QueryBookmark::decode).transpose()?,
    };
    match read_yaml(path, read) {
        FiniteRead::EndOfInput => Ok(YamlPage::EndOfInput),
        FiniteRead::Batch { records, next } => {
            match records.as_slice() {
                [YamlLine::Refused { kind, diagnostic }] => {
                    return Err(QueryError::terminal(*kind, diagnostic.clone()))
                }
                [YamlLine::Changed] => {
                    return Err(QueryError::terminal(
                        SourcePollErrorKind::Transport,
                        SourceDiagnosticReason::InputChanged,
                    ))
                }
                _ => {}
            }
            let at = next.at.ok_or_else(|| {
                QueryError::terminal(
                    SourcePollErrorKind::Other,
                    SourceDiagnosticReason::UnexpectedShape,
                )
            })?;
            Ok(YamlPage::Batch {
                records,
                next: QueryBookmark::encode(&at)?,
            })
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileYamlRead {
    batch: usize,
    selection: YamlSelection,
    max_bytes: usize,
    at: Option<YamlAt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct YamlAt {
    index: u64,
    version: FileVersion,
}

/// A `YamlLine` is one selected record, a document that the read refused, or a
/// file that changed after a recorded read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum YamlLine {
    Record {
        index: u64,
        node: YamlNode,
    },
    Refused {
        kind: SourcePollErrorKind,
        diagnostic: SourceDiagnostic,
    },
    Changed,
}

/// A `YamlNode` is one parsed YAML value as the reply records it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct YamlNode(Node);
impl YamlNode {
    pub fn parse(bytes: &[u8]) -> Result<Self, QueryError> {
        document::parse(bytes)
            .map(Self)
            .map_err(|e| QueryError::terminal(e.kind(), e.diagnostic().clone()))
    }
}

fn read_yaml(path: &Path, from: FileYamlRead) -> FiniteRead<YamlLine, FileYamlRead> {
    let refused = |error: SourceError| FiniteRead::Batch {
        records: vec![YamlLine::Refused {
            kind: error.kind(),
            diagnostic: error.diagnostic().clone(),
        }],
        next: from.clone(),
    };
    let (bytes, version) = match read_bounded(path, from.max_bytes) {
        Ok(read) => read,
        Err(error) => return refused(error),
    };
    if from.at.as_ref().is_some_and(|at| at.version != version) {
        return FiniteRead::Batch {
            records: vec![YamlLine::Changed],
            next: from.clone(),
        };
    }
    let records =
        match document::parse(&bytes).and_then(|root| document::select(root, &from.selection)) {
            Ok(records) => records,
            Err(error) => return refused(error),
        };
    let start = from
        .at
        .as_ref()
        .map_or(0, |at| usize::try_from(at.index).unwrap_or(usize::MAX));
    if start >= records.len() {
        return FiniteRead::EndOfInput;
    }
    let end = start.saturating_add(from.batch).min(records.len());
    let lines = records
        .into_iter()
        .enumerate()
        .skip(start)
        .take(end - start)
        .map(|(index, node)| YamlLine::Record {
            index: u64::try_from(index).unwrap_or(u64::MAX),
            node: YamlNode(node),
        })
        .collect();
    FiniteRead::Batch {
        records: lines,
        next: FileYamlRead {
            at: Some(YamlAt {
                index: u64::try_from(end).unwrap_or(u64::MAX),
                version,
            }),
            ..from
        },
    }
}

fn read_bounded(path: &Path, max_bytes: usize) -> Result<(Vec<u8>, FileVersion), SourceError> {
    let unavailable = || SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into());
    let limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut file = File::open(path).map_err(|_| unavailable())?;
    let version = FileVersion::of(&file)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(limit)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    version.verify(&file)?;
    if bytes.len() > max_bytes {
        return Err(SourceError::Validation(
            SourceDiagnosticReason::SizeLimitExceeded.into(),
        ));
    }
    Ok((bytes, version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::queries::QueryContext;
    use serde::{Deserialize, Serialize};
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Order {
        order_id: String,
        amount_cents: i64,
    }

    impl TypedPayload for Order {
        const EVENT_TYPE: &'static str = "test.yaml_order";
    }

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

    fn first(selection: YamlSelection, batch: usize, max_bytes: usize) -> FileYamlRead {
        FileYamlRead {
            batch,
            selection,
            max_bytes,
            at: None,
        }
    }

    fn orders() -> YamlSelection {
        YamlSelection::SequenceAt("/orders".into())
    }

    fn lines(read: FiniteRead<YamlLine, FileYamlRead>) -> (Vec<YamlLine>, FileYamlRead) {
        match read {
            FiniteRead::Batch { records, next } => (records, next),
            FiniteRead::EndOfInput => panic!("end of input"),
        }
    }

    fn produce(text: &str, selection: YamlSelection, max_bytes: usize) -> SourceError {
        let input = file(text);
        let (records, _) = lines(read_yaml(input.path(), first(selection, 64, max_bytes)));
        records
            .into_iter()
            .find_map(|line| decode_line(&OrderYaml, line).err())
            .expect("the input is refused")
    }

    struct ScriptedOrders(&'static [&'static str]);

    #[async_trait::async_trait]
    impl YamlFile for ScriptedOrders {
        async fn read(&self, from: &YamlRead, _ctx: &QueryContext) -> Result<YamlPage, QueryError> {
            let start: usize = match &from.after {
                Some(bookmark) => bookmark.decode()?,
                None => 0,
            };
            let end = start.saturating_add(from.limit.get()).min(self.0.len());
            let mut records = Vec::new();
            for (index, text) in self.0.iter().enumerate().take(end).skip(start) {
                records.push(YamlLine::Record {
                    index: index as u64,
                    node: YamlNode::parse(text.as_bytes())?,
                });
            }
            if records.is_empty() {
                return Ok(YamlPage::EndOfInput);
            }
            Ok(YamlPage::Batch {
                records,
                next: QueryBookmark::encode(&end)?,
            })
        }
    }

    #[tokio::test]
    async fn through_port_yaml_decodes_supplier_records() {
        let supplier = Arc::new(ScriptedOrders(&[
            "{order_id: s-1, amount_cents: 700}",
            "{order_id: s-2, amount_cents: 900}",
            "{order_id: s-3, amount_cents: 1100}",
        ]));
        YamlSource::builder(OrderYaml)
            .through(Port::to(supplier.clone()))
            .batch_size(2)
            .build()
            .expect("a port supplies the records");

        let ctx = QueryContext::for_test("orders", 0);
        let mut from = YamlRead {
            limit: std::num::NonZeroUsize::new(2).unwrap(),
            selection: YamlSelection::Document,
            max_bytes: DEFAULT_MAX_BYTES,
            after: None,
        };
        let mut orders = Vec::new();
        while let YamlPage::Batch { records, next } = supplier.read(&from, &ctx).await.unwrap() {
            for line in records {
                orders.push(decode_line(&OrderYaml, line).unwrap());
            }
            from.after = Some(next);
        }
        let ids: Vec<&str> = orders.iter().map(|order| order.order_id.as_str()).collect();
        assert_eq!(ids, ["s-1", "s-2", "s-3"]);
        assert_eq!(orders[1].amount_cents, 900);
    }

    #[test]
    fn yaml_selection_returns_ordered_records() {
        let input = file(
            "orders:\n  - {order_id: a, amount_cents: 1}\n  - {order_id: b, amount_cents: 2}\n  - {order_id: c, amount_cents: 3}\n",
        );
        let (batch, next) = lines(read_yaml(input.path(), first(orders(), 2, 1024)));
        let ids: Vec<String> = batch
            .into_iter()
            .map(|line| decode_line(&OrderYaml, line).unwrap().order_id)
            .collect();
        assert_eq!(ids, ["a", "b"]);
        let (batch, next) = lines(read_yaml(input.path(), next));
        assert_eq!(
            decode_line(&OrderYaml, batch.into_iter().next().unwrap())
                .unwrap()
                .order_id,
            "c"
        );
        assert!(matches!(
            read_yaml(input.path(), next),
            FiniteRead::EndOfInput
        ));
    }

    #[test]
    fn yaml_read_changed_file_records_change() {
        let mut input =
            file("- {order_id: a, amount_cents: 1}\n- {order_id: b, amount_cents: 2}\n");
        let (_, next) = lines(read_yaml(
            input.path(),
            first(YamlSelection::Sequence, 1, 1024),
        ));
        input
            .write_all(b"- {order_id: c, amount_cents: 3}\n")
            .unwrap();
        input.flush().unwrap();
        let (records, _) = lines(read_yaml(input.path(), next));
        assert!(
            matches!(records.as_slice(), [YamlLine::Changed]),
            "{records:?}"
        );
    }

    #[test]
    fn yaml_input_produces_file_reasons() {
        use SourceDiagnosticReason::*;
        let missing = {
            let (records, _) = lines(read_yaml(
                Path::new("/nonexistent/obzenflow/input.yaml"),
                first(YamlSelection::Sequence, 64, 1024),
            ));
            decode_line(&OrderYaml, records.into_iter().next().unwrap()).unwrap_err()
        };
        let cases = [
            (InputUnavailable, missing),
            (MalformedInput, produce("orders: [\n", orders(), 1024)),
            (
                UnsupportedConstruct,
                produce("a: &x 1\nb: *x\n", orders(), 1024),
            ),
            (
                DuplicateKey,
                produce("orders: []\norders: []\n", orders(), 1024),
            ),
            (SizeLimitExceeded, produce("orders: []\n", orders(), 4)),
            (SelectionNotFound, produce("returns: []\n", orders(), 1024)),
            (UnexpectedShape, produce("orders: 3\n", orders(), 1024)),
            (
                MissingField,
                produce("- {order_id: a}\n", YamlSelection::Sequence, 1024),
            ),
            (
                UnknownField,
                produce(
                    "- {order_id: a, amount_cents: 1, extra: 2}\n",
                    YamlSelection::Sequence,
                    1024,
                ),
            ),
            (
                InvalidValue,
                produce(
                    "- {order_id: a, amount_cents: lots}\n",
                    YamlSelection::Sequence,
                    1024,
                ),
            ),
            (
                InputChanged,
                decode_line(&OrderYaml, YamlLine::Changed).unwrap_err(),
            ),
        ];
        for (reason, error) in cases {
            assert_eq!(error.diagnostic().reason(), reason, "{error:?}");
            assert!(
                !format!("{error} {error:?}").contains("nonexistent"),
                "{error:?}"
            );
        }
    }
}
