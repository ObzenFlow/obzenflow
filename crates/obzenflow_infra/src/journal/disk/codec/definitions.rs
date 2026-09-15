// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::primitives::{bytes, text, unsigned, Cursor};
use super::schema::DefinitionKind;
use super::values::{self, ReadDefinitions, Standalone, WriteDefinitions};
use super::{frame, invalid, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// A cache miss can only cause another complete definition or a direct read.
/// The cache is never the durable authority and holds no numeric snapshots.
const CACHE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct Definition {
    kind: DefinitionKind,
    body: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Locator {
    journal: String,
    offset: u64,
    slot: usize,
}

#[derive(Clone)]
struct FileStamp {
    length: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    identity: (u64, u64),
}

impl FileStamp {
    fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.is_file() {
            return Err(invalid("definition carrier must be a regular archive file"));
        }
        Ok(Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt;
                (metadata.dev(), metadata.ino())
            },
        })
    }

    fn preserves(&self, prior: &Self) -> bool {
        #[cfg(unix)]
        if self.identity != prior.identity {
            return false;
        }
        // Journal files are append-only while open. A replacement, truncation,
        // or same-length edit invalidates cached definitions and forces CRC I/O.
        self.length > prior.length
            || (self.length == prior.length && self.modified == prior.modified)
    }
}

#[derive(Clone)]
struct CachedDefinition {
    definition: Definition,
    stamp: Option<FileStamp>,
}

#[derive(Default)]
struct Cache {
    by_value: HashMap<Definition, Locator>,
    by_location: HashMap<Locator, CachedDefinition>,
    bytes: usize,
    stats: StoreStats,
}

#[derive(Default, Debug, Clone, serde::Serialize)]
pub(crate) struct StoreStats {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) carrier_frames: u64,
    pub(crate) carrier_bytes: u64,
    pub(crate) evictions: u64,
    pub(crate) peak_retained_bytes: usize,
}

impl Cache {
    fn insert(&mut self, definition: Definition, locator: Locator, stamp: Option<FileStamp>) {
        if self.by_location.contains_key(&locator) {
            return;
        }
        // Account for both maps, bucket slack, locator strings and Arc handles.
        let charge = definition
            .body
            .len()
            .saturating_add(locator.journal.len() * 2 + 512);
        if charge > CACHE_BYTES {
            return;
        }
        if self.bytes + charge > CACHE_BYTES {
            self.by_value = HashMap::new();
            self.by_location = HashMap::new();
            self.bytes = 0;
            self.stats.evictions += 1;
        }
        self.bytes += charge;
        self.stats.peak_retained_bytes = self.stats.peak_retained_bytes.max(self.bytes);
        self.by_value.insert(definition.clone(), locator.clone());
        self.by_location
            .insert(locator, CachedDefinition { definition, stamp });
    }
}

#[derive(Clone, Default)]
pub(crate) struct DefinitionStore(Arc<Mutex<Cache>>);

impl DefinitionStore {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn stats(&self) -> StoreStats {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stats
            .clone()
    }
    pub(crate) fn for_archive(path: &Path) -> Self {
        type Registry = Mutex<HashMap<PathBuf, Weak<Mutex<Cache>>>>;
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let key = std::fs::canonicalize(parent)
            .unwrap_or_else(|_| std::path::absolute(parent).unwrap_or_else(|_| parent.into()));
        let mut registry = REGISTRY
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        registry.retain(|_, value| value.strong_count() != 0);
        if let Some(cache) = registry.get(&key).and_then(Weak::upgrade) {
            return Self(cache);
        }
        let store = Self::default();
        registry.insert(key, Arc::downgrade(&store.0));
        store
    }

    fn publish(&self, definition: Definition, locator: Locator, stamp: Option<FileStamp>) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(definition, locator, stamp);
    }
}

enum Entry {
    Local(Definition),
    External(DefinitionKind, Locator),
}

pub(super) struct WriteTable {
    store: DefinitionStore,
    journal: String,
    path: PathBuf,
    entries: Vec<Entry>,
    ordinals: HashMap<Definition, usize>,
    capture: Option<Value>,
}

impl WriteTable {
    pub(super) fn begin_record(&mut self) {
        self.capture = None;
    }
    pub(super) fn new(store: DefinitionStore, path: &Path) -> Result<Self> {
        Ok(Self {
            store,
            journal: journal_name(path)?,
            path: path.into(),
            entries: Vec::new(),
            ordinals: HashMap::new(),
            capture: None,
        })
    }

    pub(super) fn encode(&self, out: &mut Vec<u8>) {
        // Journal names are frame-local metadata. A forwarded record often
        // references several definitions in one source journal; name it once.
        let mut journals = Vec::new();
        for entry in &self.entries {
            if let Entry::External(_, locator) = entry {
                if locator.journal != self.journal && !journals.contains(&locator.journal.as_str())
                {
                    journals.push(locator.journal.as_str());
                }
            }
        }
        unsigned(journals.len() as u64, out);
        for journal in &journals {
            write_journal_name(journal, out);
        }
        unsigned(self.entries.len() as u64, out);
        for entry in &self.entries {
            match entry {
                Entry::Local(definition) => {
                    out.push(definition.kind as u8);
                    out.push(0);
                    bytes(&definition.body, out);
                }
                Entry::External(kind, locator) => {
                    out.push(*kind as u8);
                    out.push(1);
                    let journal = if locator.journal == self.journal {
                        0
                    } else {
                        journals
                            .iter()
                            .position(|name| *name == locator.journal)
                            .unwrap()
                            + 1
                    };
                    unsigned(journal as u64, out);
                    unsigned(locator.offset, out);
                    unsigned(locator.slot as u64, out);
                }
            }
        }
    }

    /// Called only by the append owner after its successful write and flush.
    pub(super) fn commit(self, offset: u64) {
        let stamp = FileStamp::read(&self.path).ok();
        for (slot, entry) in self.entries.into_iter().enumerate() {
            if let Entry::Local(definition) = entry {
                self.store.publish(
                    definition,
                    Locator {
                        journal: self.journal.clone(),
                        offset,
                        slot,
                    },
                    stamp.clone(),
                );
            }
        }
    }
}

impl WriteDefinitions for WriteTable {
    fn remember_capture(&mut self, capture: &Value) {
        self.capture = Some(capture.clone());
    }
    fn matches_capture(&self, capture: &Value) -> bool {
        self.capture.as_ref() == Some(capture)
    }
    fn reference(&mut self, kind: DefinitionKind, value: &Value, out: &mut Vec<u8>) -> Result<()> {
        let mut body = Vec::new();
        values::write(kind.body(), value, &mut body, &mut Standalone)?;
        let definition = Definition {
            kind,
            body: body.into(),
        };
        let slot = if let Some(slot) = self.ordinals.get(&definition) {
            *slot
        } else {
            let prior = self
                .store
                .0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .by_value
                .get(&definition)
                .cloned();
            let slot = self.entries.len();
            self.entries.push(match prior {
                Some(locator) => Entry::External(kind, locator),
                None => Entry::Local(definition.clone()),
            });
            self.ordinals.insert(definition, slot);
            slot
        };
        unsigned(slot as u64, out);
        Ok(())
    }
}

pub(super) struct ReadTable<'a> {
    entries: Vec<Entry>,
    decoded: HashMap<usize, Value>,
    store: &'a DefinitionStore,
    path: &'a Path,
    offset: u64,
    costs: Vec<usize>,
    usage: Vec<u8>,
    section: u8,
    file_stamps: HashMap<String, FileStamp>,
    capture: Option<Value>,
}

impl<'a> ReadTable<'a> {
    pub(super) fn begin_record(&mut self) {
        self.capture = None;
    }
    pub(super) fn new(
        input: &mut Cursor<'_>,
        store: &'a DefinitionStore,
        path: &'a Path,
        offset: u64,
    ) -> Result<Self> {
        let mut costs = Vec::new();
        let entries = read_entries(input, path, Some(&mut costs))?;
        let usage = vec![0; entries.len()];
        Ok(Self {
            entries,
            decoded: HashMap::new(),
            store,
            path,
            offset,
            costs,
            usage,
            section: 0,
            file_stamps: HashMap::new(),
            capture: None,
        })
    }

    pub(super) fn section(&mut self, section: u8) {
        self.section = section;
    }

    pub(super) fn attributed_bytes(&self) -> (usize, usize) {
        let mut provenance = 0;
        let mut observation = 0;
        for (&cost, &usage) in self.costs.iter().zip(&self.usage) {
            match usage {
                1 => provenance += cost,
                2 => observation += cost,
                _ => {}
            }
        }
        (provenance, observation)
    }

    fn definition(&mut self, kind: DefinitionKind, locator: &Locator) -> Result<Definition> {
        if locator.journal == journal_name(self.path)? && locator.offset >= self.offset {
            return Err(invalid(
                "definition reference is not before the consuming frame",
            ));
        }
        let path = self
            .path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&locator.journal);
        let stamp = match self.file_stamps.get(&locator.journal) {
            Some(stamp) => stamp.clone(),
            None => {
                let stamp = FileStamp::read(&path)?;
                self.file_stamps
                    .insert(locator.journal.clone(), stamp.clone());
                stamp
            }
        };
        let cached = self
            .store
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .by_location
            .get(locator)
            .cloned();
        if let Some(cached) = cached {
            if cached.definition.kind != kind {
                return Err(invalid("definition kind mismatch"));
            }
            if cached
                .stamp
                .as_ref()
                .is_some_and(|prior| stamp.preserves(prior))
            {
                self.store
                    .0
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .stats
                    .hits += 1;
                return Ok(cached.definition);
            }
        }
        self.store
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stats
            .misses += 1;
        // No aliases out of the archive through symlinks, even with a valid basename.
        let archive = std::fs::canonicalize(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        let canonical = std::fs::canonicalize(&path)?;
        if canonical.parent() != Some(archive.as_path()) {
            return Err(invalid("definition escapes the archive"));
        }
        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(locator.offset))?;
        let mut header = [0u8; frame::HEADER_LEN];
        file.read_exact(&mut header)?;
        let length = frame::frame_length(&header).map_err(frame::io_error)?;
        let remaining = file.metadata()?.len().saturating_sub(locator.offset);
        if length as u64 > remaining {
            return Err(invalid("definition carrier is uncommitted"));
        }
        let mut bytes = header.to_vec();
        file.take((length - frame::HEADER_LEN) as u64)
            .read_to_end(&mut bytes)?;
        {
            let mut cache = self.store.0.lock().unwrap_or_else(|e| e.into_inner());
            cache.stats.carrier_frames += 1;
            cache.stats.carrier_bytes += bytes.len() as u64;
        }
        let body = frame::validate(&bytes).map_err(frame::io_error)?;
        let mut cursor = Cursor::new(body);
        let entries = read_entries(&mut cursor, &canonical, None)?;
        let definition = match entries.into_iter().nth(locator.slot) {
            Some(Entry::Local(definition)) if definition.kind == kind => definition,
            Some(Entry::Local(_)) => return Err(invalid("definition kind mismatch")),
            Some(Entry::External(..)) => {
                return Err(invalid("definition-to-definition reference forbidden"))
            }
            None => return Err(invalid("missing definition slot")),
        };
        super::validate_carrier(&mut cursor)?;
        // Validate the complete body before putting it in the cache.
        decode_definition(&definition)?;
        self.store
            .publish(definition.clone(), locator.clone(), Some(stamp));
        Ok(definition)
    }
}

impl ReadDefinitions for ReadTable<'_> {
    fn remember_capture(&mut self, capture: &Value) {
        self.capture = Some(capture.clone());
    }
    fn capture(&self) -> Option<Value> {
        self.capture.clone()
    }
    fn resolve(&mut self, kind: DefinitionKind, input: &mut Cursor<'_>) -> Result<Value> {
        let slot = input.length()?;
        let entry = self
            .entries
            .get(slot)
            .ok_or_else(|| invalid("missing definition ordinal"))?;
        self.usage[slot] |= self.section;
        let actual_kind = match entry {
            Entry::Local(definition) => definition.kind,
            Entry::External(kind, _) => *kind,
        };
        if actual_kind != kind {
            return Err(invalid("definition kind mismatch"));
        }
        if let Some(value) = self.decoded.get(&slot) {
            return Ok(value.clone());
        }
        let definition = match entry {
            Entry::Local(definition) => definition.clone(),
            Entry::External(kind, locator) => {
                let kind = *kind;
                let locator = locator.clone();
                self.definition(kind, &locator)?
            }
        };
        let value = decode_definition(&definition)?;
        self.decoded.insert(slot, value.clone());
        Ok(value)
    }
}

fn decode_definition(definition: &Definition) -> Result<Value> {
    let mut cursor = Cursor::new(&definition.body);
    let value = values::read(definition.kind.body(), &mut cursor, &mut Standalone)?;
    cursor.finish()?;
    Ok(value)
}

fn read_entries(
    input: &mut Cursor<'_>,
    path: &Path,
    mut costs: Option<&mut Vec<usize>>,
) -> Result<Vec<Entry>> {
    let journal_count = values::bounded_count(input)?;
    let mut journals = vec![journal_name(path)?];
    for _ in 0..journal_count {
        journals.push(read_journal_name(input)?);
    }
    let count = values::bounded_count(input)?;
    let mut entries = Vec::new();
    for _ in 0..count {
        let start = input.position();
        let kind = DefinitionKind::from_byte(input.byte()?)
            .ok_or_else(|| invalid("unknown definition kind"))?;
        entries.push(match input.byte()? {
            0 => Entry::Local(Definition {
                kind,
                body: Arc::from(input.bytes()?),
            }),
            1 => {
                let journal = journals
                    .get(input.length()?)
                    .ok_or_else(|| invalid("missing journal ordinal"))?
                    .clone();
                Entry::External(
                    kind,
                    Locator {
                        journal,
                        offset: input.unsigned()?,
                        slot: input.length()?,
                    },
                )
            }
            _ => return Err(invalid("unknown definition storage tag")),
        });
        if let Some(costs) = costs.as_mut() {
            costs.push(input.position() - start);
        }
    }
    Ok(entries)
}

fn write_journal_name(name: &str, out: &mut Vec<u8>) {
    if name == "system.log" {
        out.push(1);
        return;
    }
    if let Some((prefix, suffix)) = name.rsplit_once("_stage_") {
        if let Some(id) = suffix
            .strip_suffix(".log")
            .and_then(|id| id.parse::<ulid::Ulid>().ok())
        {
            if format!("{prefix}_stage_{id}.log") == name {
                out.push(2);
                text(prefix, out);
                out.extend_from_slice(&id.to_bytes());
                return;
            }
        }
    }
    out.push(0);
    text(name, out);
}

fn read_journal_name(input: &mut Cursor<'_>) -> Result<String> {
    let name = match input.byte()? {
        0 => input.text()?,
        1 => "system.log".into(),
        2 => {
            let prefix = input.text()?;
            let id = ulid::Ulid::from_bytes(input.take(16)?.try_into().unwrap());
            format!("{prefix}_stage_{id}.log")
        }
        _ => return Err(invalid("unknown journal-name tag")),
    };
    validate_name(&name)?;
    Ok(name)
}

fn journal_name(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid("journal filename is not UTF-8"))?;
    validate_name(name)?;
    Ok(name.into())
}

fn validate_name(name: &str) -> Result<()> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
        || name.contains(['/', '\\'])
    {
        return Err(invalid(
            "definition journal must be an archive-local basename",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_definitions_are_bounded_and_eviction_only_causes_complete_duplicates() {
        let store = DefinitionStore::default();
        let path = Path::new("bounded.log");
        let first = Value::String("a complete immutable descriptor".repeat(80));
        for index in 0..8_000 {
            let mut table = WriteTable::new(store.clone(), path).unwrap();
            let value = if index == 0 {
                first.clone()
            } else {
                Value::String(format!("{index}:{}", "x".repeat(2_000)))
            };
            table
                .reference(DefinitionKind::Descriptor, &value, &mut Vec::new())
                .unwrap();
            table.commit(index);
            assert!(store.0.lock().unwrap().bytes <= CACHE_BYTES);
        }
        let mut table = WriteTable::new(store, path).unwrap();
        table
            .reference(DefinitionKind::Descriptor, &first, &mut Vec::new())
            .unwrap();
        assert!(
            matches!(&table.entries[0], Entry::Local(_)),
            "evicted values are written completely again"
        );
    }

    #[test]
    fn locators_reject_traversal_and_definition_chains_and_wrong_kinds() {
        for name in [
            "",
            ".",
            "..",
            "../outside.log",
            "/outside.log",
            "a/b.log",
            "a\\b.log",
        ] {
            assert!(validate_name(name).is_err(), "{name}");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.log");
        let store = DefinitionStore::default();
        let definition = Definition {
            kind: DefinitionKind::Descriptor,
            body: Arc::from([1, b'x']),
        };
        let mut body = Vec::new();
        let table = WriteTable {
            store: store.clone(),
            journal: "events.log".into(),
            path: path.clone(),
            entries: vec![
                Entry::Local(definition),
                Entry::External(
                    DefinitionKind::Descriptor,
                    Locator {
                        journal: "events.log".into(),
                        offset: 0,
                        slot: 0,
                    },
                ),
            ],
            ordinals: HashMap::new(),
            capture: None,
        };
        table.encode(&mut body);
        std::fs::write(&path, frame::encode(&body)).unwrap();
        let encoded = {
            let mut out = Vec::new();
            let table = WriteTable {
                store: store.clone(),
                journal: "events.log".into(),
                path: path.clone(),
                entries: vec![Entry::External(
                    DefinitionKind::Descriptor,
                    Locator {
                        journal: "events.log".into(),
                        offset: 0,
                        slot: 1,
                    },
                )],
                ordinals: HashMap::new(),
                capture: None,
            };
            table.encode(&mut out);
            out
        };
        let mut table = ReadTable::new(&mut Cursor::new(&encoded), &store, &path, 1000).unwrap();
        assert!(table
            .resolve(DefinitionKind::Descriptor, &mut Cursor::new(&[0]))
            .unwrap_err()
            .to_string()
            .contains("definition-to-definition"));
        assert!(table
            .resolve(DefinitionKind::Writer, &mut Cursor::new(&[0]))
            .unwrap_err()
            .to_string()
            .contains("kind mismatch"));
        let mut future = ReadTable::new(&mut Cursor::new(&encoded), &store, &path, 0).unwrap();
        assert!(future
            .resolve(DefinitionKind::Descriptor, &mut Cursor::new(&[0]))
            .unwrap_err()
            .to_string()
            .contains("not before"));
    }
}
