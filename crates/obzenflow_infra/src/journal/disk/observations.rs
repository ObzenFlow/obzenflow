// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disposable attachment locators. Recovery and checkpoint I/O are deliberately
//! outside the fact writer's panic/error contract.

use super::codec::{frame, Decoder};
use super::scanner::{classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy};
use crate::journal::metrics_tail::{Carrier, MetricsTailIndex};
use crate::journal::observation_index::{
    locate, unavailable, Locator, ObservationIndex, HISTORY_PER_KEY,
};
use async_trait::async_trait;
use obzenflow_core::event::{JournalEvent, JournalRecord};
use obzenflow_core::journal::{
    JournalError, JournalObservationReader, LocatedObservation, ObservationKey, ObservationLookup,
};
use obzenflow_core::WriterId;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, TryLockError, Weak};

const REBUILD_FRAMES: usize = 256;
const MAX_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const NO_WRITER: u64 = u64::MAX;
type SharedIndexes = Mutex<HashMap<PathBuf, Weak<Shared>>>;
static INDEXES: OnceLock<SharedIndexes> = OnceLock::new();

struct Shared {
    state: Mutex<State>,
    metrics_tail: Mutex<MetricsTailIndex>,
    // Commitment is independent of the disposable index, including its mutex.
    // NO_WRITER permits read-only archive inspection until a writer takes over.
    committed_end: AtomicU64,
    opening: Mutex<()>,
    maintenance: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    io_hook: Mutex<Option<IoHook>>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            metrics_tail: Mutex::default(),
            committed_end: AtomicU64::new(NO_WRITER),
            opening: Mutex::default(),
            maintenance: Arc::default(),
            #[cfg(test)]
            io_hook: Mutex::default(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Anchor {
    offset: u64,
    length: u64,
    crc: u32,
}

impl Anchor {
    fn of(offset: u64, bytes: &[u8]) -> Self {
        let trailer = bytes.len() - frame::TRAILER_LEN;
        Self {
            offset,
            length: bytes.len() as u64,
            crc: u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap()),
        }
    }
}

#[derive(Clone, Default)]
struct State {
    loaded: bool,
    // EOF-derived state must be discarded when a writable handle appears.
    confirmed: bool,
    index: ObservationIndex,
    offset: u64,
    // Finish this captured prefix before chasing subsequent appends.
    rebuild_end: Option<u64>,
    first: Option<Anchor>,
    last: Option<Anchor>,
    checkpointed: u64,
    #[cfg(test)]
    rebuilt_frames: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    journal_schema_version: String,
    archive: Option<String>,
    journal: String,
    committed_len: u64,
    first: Anchor,
    last: Anchor,
    entries: Vec<(ObservationKey, VecDeque<Locator>)>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CheckedCheckpoint {
    crc: u32,
    body: String,
}

/// Opens attachment lookup without rebuilding the journal's EventId index or
/// obtaining a writable journal handle. Concurrent readers share rebuild work.
#[derive(Clone)]
pub struct DiskObservationReader<T: JournalEvent> {
    path: PathBuf,
    shared: Arc<Shared>,
    writable: bool,
    _event: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> DiskObservationReader<T> {
    pub fn open(path: PathBuf) -> Result<Self, JournalError> {
        if let Some(parent) = path.parent() {
            if parent
                .join(obzenflow_core::journal::RUN_MANIFEST_FILENAME)
                .exists()
            {
                super::inspect::load_manifest(parent)
                    .map_err(|error| unavailable(error.to_string()))?;
            }
        }

        File::open(&path).map_err(|error| unavailable(error.to_string()))?;
        Ok(Self::new(path, false))
    }

    fn new(path: PathBuf, writable: bool) -> Self {
        let key = std::path::absolute(&path).unwrap_or_else(|_| path.clone());
        let mut registry = INDEXES
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let shared = registry
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let state = Arc::new(Shared::default());
                registry.retain(|_, state| state.strong_count() > 0);
                registry.insert(key, Arc::downgrade(&state));
                state
            });
        Self {
            path,
            shared,
            writable,
            _event: std::marker::PhantomData,
        }
    }

    pub(super) fn open_writer<R>(
        path: PathBuf,
        recover: impl FnOnce(Option<u64>) -> Result<(R, u64), JournalError>,
    ) -> Result<(Self, R), JournalError> {
        let reader = Self::new(path, true);
        // Only constructors acquire this lock. Readers and append completion
        // never wait for it. First-open recovery is serialized; subsequent
        // openers can use the confirmed prefix while an append is in flight.
        let guard = reader
            .shared
            .opening
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let journal_lock = super::journal::shared_state_for_path(&reader.path).0;
        let exclusive = journal_lock.try_write();
        let end = if exclusive.is_ok() {
            // This is protected journal recovery, not observation maintenance.
            // Recover a failed/torn append even if an old reader still exists.
            None
        } else {
            Some(reader.confirmed_end().ok_or_else(|| {
                JournalError::Implementation {
                    message:
                        "Cannot recover a journal in use before its committed prefix is established"
                            .into(),
                    source: "journal is busy".into(),
                }
            })?)
        };
        let (recovered, committed_end) = recover(end)?;
        if end.is_none() {
            if reader.confirmed_end() != Some(committed_end) {
                *reader
                    .shared
                    .metrics_tail
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = MetricsTailIndex::default();
            }
            reader
                .shared
                .committed_end
                .store(committed_end, Ordering::Release);
        }
        drop(exclusive);
        drop(guard);
        Ok((reader, recovered))
    }

    pub(super) fn confirmed_end(&self) -> Option<u64> {
        let end = self.shared.committed_end.load(Ordering::Acquire);
        (end != NO_WRITER).then_some(end)
    }

    pub(super) fn committed(
        &self,
        records: &[JournalRecord<T::Payload>],
        offset: u64,
        next_offset: u64,
        crc: u32,
    ) {
        if self
            .shared
            .committed_end
            .compare_exchange(offset, next_offset, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            // Another handle's indeterminate suffix needs journal recovery.
            // A later successful append cannot certify the intervening bytes.
            return;
        }
        {
            let mut tail = self
                .shared
                .metrics_tail
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for (member, record) in records.iter().enumerate() {
                tail.observe(record, Carrier { offset, member });
            }
        }
        self.maintain(|state| {
            if !state.confirmed {
                *state = State {
                    confirmed: true,
                    ..Default::default()
                };
            }
            if offset == 0 && !state.loaded {
                state.loaded = true;
            }
            // An old prefix needs a shared rebuild before newer locators can be
            // assigned portable positions. Never guess from EventIds or clocks.
            if !state.loaded || state.offset != offset {
                return Ok(());
            }
            for (member, record) in records.iter().enumerate() {
                state
                    .index
                    .observe(record.envelope.observability.as_ref(), offset, member)?;
            }
            let anchor = Anchor {
                offset,
                length: next_offset - offset,
                crc,
            };
            state.first.get_or_insert_with(|| anchor.clone());
            state.offset = offset + anchor.length;
            state.last = Some(anchor);
            Ok(())
        });
    }

    fn maintain(&self, operation: impl FnOnce(&mut State) -> Result<(), JournalError>) {
        let mut state = match self.shared.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        if !matches!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&mut state))),
            Ok(Ok(()))
        ) {
            // Optional faults never escape into retain_commit and never poison
            // the journal. The next reader can rebuild from committed facts.
            *state = State::default();
        }
    }

    pub(super) async fn metrics_tail(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let carriers = self
            .shared
            .metrics_tail
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .carriers();
        let Some(end) = self.confirmed_end() else {
            return Ok(Vec::new());
        };
        if carriers.is_empty() {
            return Ok(Vec::new());
        }
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut frames = HashMap::new();
            let mut decoder = Decoder::new(&path);
            let mut result = Vec::with_capacity(carriers.len());
            for carrier in carriers {
                let records = match frames.entry(carrier.offset) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let (bytes, termination) = read_at(&path, carrier.offset, end)?;
                        let Disposition::Yield(frame) = dispose(
                            classify_frame::<T>(&bytes, &mut decoder, carrier.offset),
                            termination,
                            ReadPolicy::SealedScan {
                                tolerate_torn_tail: false,
                            },
                        ) else {
                            return Err(unavailable("invalid metrics tail carrier"));
                        };
                        entry.insert(frame.into_records())
                    }
                };
                result.push(
                    records
                        .get(carrier.member)
                        .ok_or_else(|| unavailable("missing metrics tail member"))?
                        .clone(),
                );
            }
            Ok(result)
        })
        .await
        .map_err(|error| unavailable(error.to_string()))?
    }

    async fn lookup(
        &self,
        key: Option<ObservationKey>,
        observer: WriterId,
    ) -> Result<ObservationLookup<Vec<LocatedObservation>>, JournalError> {
        let this = self.clone();
        // Wait asynchronously, before occupying a blocking worker. The worker
        // owns this guard even if its caller cancels the lookup.
        let guard = this.shared.maintenance.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut state = this
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let original = (state.offset, state.confirmed);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                this.lookup_blocking(&mut state, key, observer)
            }))
            .unwrap_or_else(|_| Err(unavailable("observation maintenance panicked")));
            if result.is_err() {
                let mut live = this.shared.state.lock().unwrap_or_else(|e| e.into_inner());
                // Do not erase append-side progress made during failed I/O.
                if (live.offset, live.confirmed) == original {
                    *live = State::default();
                }
            }
            result
        })
        .await
        .unwrap_or_else(|error| Err(unavailable(error.to_string())))
    }

    fn lookup_blocking(
        &self,
        state: &mut State,
        key: Option<ObservationKey>,
        observer: WriterId,
    ) -> Result<ObservationLookup<Vec<LocatedObservation>>, JournalError> {
        let confirmed_end = self.confirmed_end();
        let confirmed = confirmed_end.is_some();
        let end = match confirmed_end {
            Some(end) => end,
            None => std::fs::metadata(&self.path)
                .map_err(|error| unavailable(error.to_string()))?
                .len(),
        };
        if state.confirmed != confirmed || state.offset > end {
            *state = State::default();
        }
        if !state.loaded {
            #[cfg(test)]
            self.pause_io(IoPhase::CheckpointLoad);
            *state = load_checkpoint(&self.path, end).unwrap_or_default();
            state.loaded = true;
            state.confirmed = confirmed;
        }
        let target = state.rebuild_end.unwrap_or(end).max(state.offset).min(end);
        #[cfg(test)]
        self.pause_io(IoPhase::Rebuild);
        let progress = if state.offset == target {
            RebuildProgress::AtCommittedEnd
        } else {
            rebuild::<T>(&self.path, state, target)?
        };
        let rebuilding = matches!(progress, RebuildProgress::BudgetExhausted);
        state.rebuild_end = rebuilding.then_some(target);
        let mut observation = Vec::new();
        if !rebuilding {
            // Decode each selected physical carrier once, even when it owns
            // several families. Locators themselves never retain packets.
            let mut carriers = HashMap::new();
            let selected: Vec<_> = match key.as_ref() {
                Some(key) => state.index.entries.get_key_value(key).into_iter().collect(),
                None => state
                    .index
                    .entries
                    .iter()
                    .filter(|(family, _)| family.observer == observer)
                    .collect(),
            };
            for (family, history) in selected {
                let locator = history
                    .back()
                    .ok_or_else(|| unavailable("empty locator history"))?;
                let records = match carriers.entry(locator.frame_offset) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        #[cfg(test)]
                        self.pause_io(IoPhase::CarrierRead);
                        let (bytes, termination) =
                            read_at(&self.path, locator.frame_offset, state.offset)?;
                        let frame = dispose(
                            classify_frame::<T>(
                                &bytes,
                                &mut Decoder::new(&self.path),
                                locator.frame_offset,
                            ),
                            termination,
                            ReadPolicy::SealedScan {
                                tolerate_torn_tail: false,
                            },
                        );
                        let Disposition::Yield(frame) = frame else {
                            return Err(unavailable("invalid attachment carrier frame"));
                        };
                        entry.insert(frame.into_records())
                    }
                };
                let packet = records
                    .get(locator.member)
                    .and_then(|record| record.envelope.observability.clone())
                    .ok_or_else(|| unavailable("missing attachment carrier member"))?;
                observation.push(locate(family, locator, packet)?);
            }
        }
        #[cfg(test)]
        self.pause_io(IoPhase::Publish);
        // A read-only scan may have overlapped the first writable opener. Its
        // EOF-derived work is speculative across that transition; retry using
        // the writer's boundary, even if the byte counts happen to agree.
        {
            let mut live = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            if !confirmed && self.confirmed_end().is_some() {
                return Ok(ObservationLookup::Rebuilding {
                    examined_through: 0,
                    committed_len: None,
                });
            }
            if live.confirmed != confirmed || live.offset <= state.offset {
                *live = state.clone();
            }
        }
        if rebuilding {
            return Ok(ObservationLookup::Rebuilding {
                examined_through: state.index.examined_through,
                committed_len: None,
            });
        }
        if self.writable && state.offset != state.checkpointed {
            #[cfg(test)]
            self.pause_io(IoPhase::CheckpointWrite);
            // Serialization, manifest reads, and persistence all use the owned
            // snapshot, outside the index mutex and the fact writer's locks.
            if write_checkpoint(&self.path, state).is_ok() {
                let mut live = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
                if live.confirmed == confirmed && live.offset >= state.offset {
                    live.checkpointed = state.offset;
                }
            }
        }
        Ok(ObservationLookup::Ready {
            committed_len: state.index.examined_through,
            observation,
        })
    }
}

#[cfg(test)]
type IoHook = Arc<dyn Fn(IoPhase) + Send + Sync>;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IoPhase {
    CheckpointLoad,
    Rebuild,
    CarrierRead,
    Publish,
    CheckpointWrite,
}

#[cfg(test)]
impl<T: JournalEvent> DiskObservationReader<T> {
    fn pause_io(&self, phase: IoPhase) {
        let hook = self.shared.io_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(phase);
        }
    }
}

#[async_trait]
impl<T: JournalEvent> JournalObservationReader for DiskObservationReader<T> {
    async fn latest_observation(
        &self,
        key: &ObservationKey,
    ) -> Result<ObservationLookup, JournalError> {
        Ok(match self.lookup(Some(key.clone()), key.observer).await? {
            ObservationLookup::Ready {
                committed_len,
                mut observation,
            } => ObservationLookup::Ready {
                committed_len,
                observation: observation.pop(),
            },
            ObservationLookup::Rebuilding {
                examined_through,
                committed_len,
            } => ObservationLookup::Rebuilding {
                examined_through,
                committed_len,
            },
        })
    }
    async fn latest_observations(
        &self,
        observer: WriterId,
    ) -> Result<ObservationLookup<Vec<LocatedObservation>>, JournalError> {
        self.lookup(None, observer).await
    }
}

fn read_at(
    path: &Path,
    offset: u64,
    end: u64,
) -> Result<(Vec<u8>, super::scanner::FrameTermination), JournalError> {
    let mut file = File::open(path).map_err(|error| unavailable(error.to_string()))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| unavailable(error.to_string()))?;
    let mut bytes = Vec::new();
    let remaining = end
        .checked_sub(offset)
        .ok_or_else(|| unavailable("carrier exceeds committed prefix"))?;
    let (_, termination) = read_frame_sync(&mut BufReader::new(file.take(remaining)), &mut bytes)
        .map_err(|error| unavailable(error.to_string()))?
        .ok_or_else(|| unavailable("missing committed frame"))?;
    Ok((bytes, termination))
}

enum RebuildProgress {
    AtCommittedEnd,
    BudgetExhausted,
}

fn rebuild<T: JournalEvent>(
    path: &Path,
    state: &mut State,
    end: u64,
) -> Result<RebuildProgress, JournalError> {
    let mut file = File::open(path).map_err(|error| unavailable(error.to_string()))?;
    file.seek(SeekFrom::Start(state.offset))
        .map_err(|error| unavailable(error.to_string()))?;
    let remaining = end
        .checked_sub(state.offset)
        .ok_or_else(|| unavailable("indexed prefix exceeds scan boundary"))?;
    let mut reader = BufReader::new(file.take(remaining));
    let mut decoder = Decoder::new(path);
    let mut bytes = Vec::new();
    for _ in 0..REBUILD_FRAMES {
        if state.offset >= end {
            return Ok(RebuildProgress::AtCommittedEnd);
        }
        let Some((_, termination)) = read_frame_sync(&mut reader, &mut bytes)
            .map_err(|error| unavailable(error.to_string()))?
        else {
            return Err(unavailable("journal ended before captured scan boundary"));
        };
        let frame = dispose(
            classify_frame::<T>(&bytes, &mut decoder, state.offset),
            termination,
            if state.confirmed {
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: false,
                }
            } else {
                ReadPolicy::LiveTail
            },
        );
        let records = match frame {
            Disposition::Yield(frame) => frame.into_records(),
            Disposition::Skip | Disposition::EndOfCommittedRecords => {
                if bytes.len() as u64 != end - state.offset {
                    return Err(unavailable("journal ended before captured scan boundary"));
                }
                // A torn final frame contributes no committed positions. Keep
                // the cursor before it so later lookups can retry its completion.
                return Ok(RebuildProgress::AtCommittedEnd);
            }
            Disposition::Corrupt(problem) => return Err(unavailable(problem.to_string())),
        };
        #[cfg(test)]
        {
            state.rebuilt_frames += 1;
        }
        for (member, record) in records.iter().enumerate() {
            state
                .index
                .observe(record.envelope.observability.as_ref(), state.offset, member)?;
        }
        let anchor = Anchor::of(state.offset, &bytes);
        state.first.get_or_insert_with(|| anchor.clone());
        state.offset += anchor.length;
        state.last = Some(anchor);
    }
    Ok(if state.offset == end {
        RebuildProgress::AtCommittedEnd
    } else {
        RebuildProgress::BudgetExhausted
    })
}

fn archive_id(path: &Path) -> Option<String> {
    let bytes = std::fs::read(
        path.parent()?
            .join(obzenflow_core::journal::RUN_MANIFEST_FILENAME),
    )
    .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("flow_id")?.as_str().map(str::to_owned)
}

fn checkpoint_path(path: &Path) -> PathBuf {
    path.with_extension("observations.json")
}

fn write_checkpoint(path: &Path, state: &State) -> Result<(), JournalError> {
    let (Some(first), Some(last)) = (&state.first, &state.last) else {
        return Ok(());
    };
    let checkpoint = Checkpoint {
        journal_schema_version: obzenflow_core::journal::JOURNAL_SCHEMA_VERSION.to_string(),
        archive: archive_id(path),
        journal: path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        committed_len: state.index.examined_through,
        first: first.clone(),
        last: last.clone(),
        entries: state
            .index
            .entries
            .iter()
            .map(|(key, history)| (key.clone(), history.clone()))
            .collect(),
    };
    let body =
        serde_json::to_string(&checkpoint).map_err(|error| unavailable(error.to_string()))?;
    let checked = CheckedCheckpoint {
        crc: crc32fast::hash(body.as_bytes()),
        body,
    };
    let bytes = serde_json::to_vec(&checked).map_err(|error| unavailable(error.to_string()))?;
    if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
        return Err(unavailable("checkpoint capacity reached"));
    }
    let destination = checkpoint_path(path);
    let temporary = destination.with_extension("tmp");
    std::fs::write(&temporary, bytes)
        .and_then(|()| std::fs::rename(temporary, destination))
        .map_err(|error| unavailable(error.to_string()))
}

fn load_checkpoint(path: &Path, committed_end: u64) -> Option<State> {
    let checkpoint_path = checkpoint_path(path);
    if std::fs::metadata(&checkpoint_path).ok()?.len() > MAX_CHECKPOINT_BYTES {
        return None;
    }
    let checked: CheckedCheckpoint =
        serde_json::from_slice(&std::fs::read(checkpoint_path).ok()?).ok()?;
    if crc32fast::hash(checked.body.as_bytes()) != checked.crc {
        return None;
    }
    let checkpoint: Checkpoint = serde_json::from_str(&checked.body).ok()?;
    let end = checkpoint.last.offset.checked_add(checkpoint.last.length)?;
    if checkpoint.journal_schema_version != obzenflow_core::journal::JOURNAL_SCHEMA_VERSION
        || checkpoint.archive != archive_id(path)
        || checkpoint.journal != path.file_name()?.to_str()?
        || end > committed_end
        || checkpoint.first.offset != 0
    {
        return None;
    }
    for anchor in [&checkpoint.first, &checkpoint.last] {
        let (bytes, _) = read_at(path, anchor.offset, end).ok()?;
        frame::validate(&bytes).ok()?;
        if Anchor::of(anchor.offset, &bytes) != *anchor {
            return None;
        }
    }
    let mut entries = HashMap::new();
    for (key, history) in checkpoint.entries {
        if history.is_empty() || history.len() > HISTORY_PER_KEY {
            return None;
        }
        let mut previous = None;
        for locator in &history {
            if locator.position >= checkpoint.committed_len
                || locator.frame_offset >= end
                || previous.is_some_and(|seq| seq >= locator.capture_seq)
            {
                return None;
            }
            previous = Some(locator.capture_seq);
        }
        if entries.insert(key, history).is_some() {
            return None;
        }
    }
    Some(State {
        loaded: true,
        index: ObservationIndex {
            examined_through: checkpoint.committed_len,
            entries,
        },
        offset: end,
        first: Some(checkpoint.first),
        last: Some(checkpoint.last),
        checkpointed: end,
        ..Default::default()
    })
}

#[cfg(test)]
mod cardinality_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::observability::tests::event;
    use crate::journal::{DiskJournal, MemoryJournal};
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::observability::families::ObservationFamily;
    use obzenflow_core::event::observability::{CaptureSeq, ExecutionProgress, RuntimeSnapshot};
    use obzenflow_core::{ChainEvent, Journal, JournalOwner, StageId};
    use std::sync::{atomic::AtomicBool, Condvar};
    use std::time::Duration;

    #[derive(Default)]
    struct IoLatch {
        entered: tokio::sync::Notify,
        released: Mutex<bool>,
        resume: Condvar,
    }

    impl IoLatch {
        fn wait(&self) {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.resume.wait(released).unwrap();
            }
        }

        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.resume.notify_all();
        }
    }

    // Release the blocking worker even if a timeout assertion fails.
    struct ReleaseOnDrop(Arc<IoLatch>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    fn stall(reader: &DiskObservationReader<ChainEvent>, phase: IoPhase) -> ReleaseOnDrop {
        let latch = Arc::new(IoLatch::default());
        let hook_latch = latch.clone();
        let once = AtomicBool::new(false);
        *reader.shared.io_hook.lock().unwrap() = Some(Arc::new(move |at| {
            if at == phase && !once.swap(true, Ordering::SeqCst) {
                hook_latch.wait();
            }
        }));
        ReleaseOnDrop(latch)
    }

    async fn completes<F: std::future::Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("operation must finish while the optional work is still stalled")
    }

    fn recapture(original: &ChainEvent, seq: u64) -> ChainEvent {
        let mut next = original.clone();
        next.envelope
            .observability
            .as_mut()
            .unwrap()
            .capture
            .capture_seq = CaptureSeq(seq);
        next
    }

    #[tokio::test]
    async fn metrics_tail_never_rebuilds_or_checkpoints_an_unindexed_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live-tail.log");
        let stage = StageId::new();
        {
            let journal =
                DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
            journal
                .append(event(stage, 1), Default::default())
                .await
                .unwrap();
            journal
                .append_group(
                    "old-history",
                    (0..1000)
                        .map(|_| {
                            ChainEventFactory::data_event(
                                stage.into(),
                                "test.noise",
                                serde_json::json!({}),
                            )
                        })
                        .collect(),
                    Default::default(),
                )
                .await
                .unwrap();
        }
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let reader = DiskObservationReader::<ChainEvent>::new(path.clone(), true);
        // Holding archive maintenance would deadlock an accidental archive lookup.
        let _maintenance = reader.shared.maintenance.lock().await;
        *reader.shared.io_hook.lock().unwrap() = Some(Arc::new(|_| {
            panic!("live lookup entered archive maintenance")
        }));
        assert!(completes(journal.read_metrics_tail())
            .await
            .unwrap()
            .is_empty());
        let latest = journal
            .append(event(stage, 2), Default::default())
            .await
            .unwrap();
        let rows = completes(journal.read_metrics_tail()).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id(), latest.id());
        let state = reader.shared.state.lock().unwrap();
        assert!(!state.loaded);
        assert_eq!(state.index.examined_through, 0);
        assert!(!checkpoint_path(&path).exists());
    }

    #[tokio::test]
    async fn stalled_observation_io_does_not_block_facts_terminal_groups_or_destruction() {
        for phase in [
            IoPhase::CheckpointLoad,
            IoPhase::Rebuild,
            IoPhase::CarrierRead,
            IoPhase::CheckpointWrite,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("stalled.log");
            let stage = StageId::new();
            let journal =
                DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
            let observed = event(stage, 1);
            let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
            let mut expected = vec![journal
                .append(observed.clone(), Default::default())
                .await
                .unwrap()];
            let first_end = std::fs::metadata(&path).unwrap().len();
            let reader = DiskObservationReader::<ChainEvent>::new(path.clone(), true);
            if matches!(phase, IoPhase::CheckpointLoad | IoPhase::Rebuild) {
                *reader.shared.state.lock().unwrap() = State::default();
            }
            let latch = stall(&reader, phase);
            let query_reader = reader.clone();
            let query_key = key.clone();
            let lookup =
                tokio::spawn(async move { query_reader.latest_observation(&query_key).await });
            completes(latch.0.entered.notified()).await;

            expected.push(
                completes(journal.append(recapture(&observed, 2), Default::default()))
                    .await
                    .unwrap(),
            );
            let eof = ChainEventFactory::eof_event(stage.into(), true).with_observability_context(
                recapture(&observed, 3).envelope.observability.unwrap(),
            );
            expected.extend(
                completes(journal.append_group(
                    "terminal",
                    vec![ChainEventFactory::drain_event(stage.into()), eof],
                    Default::default(),
                ))
                .await
                .unwrap(),
            );
            let records = completes(journal.read_all_unordered()).await.unwrap();
            assert_eq!(
                serde_json::to_value(&records).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            for (member, record) in records[2..].iter().enumerate() {
                assert_eq!(
                    record
                        .envelope
                        .provenance
                        .journal
                        .journal_group_id
                        .as_deref(),
                    Some("terminal")
                );
                assert_eq!(
                    record.envelope.provenance.journal.journal_group_member,
                    Some(obzenflow_core::event::provenance::JournalGroupMember {
                        index: member as u32,
                        size: 2
                    })
                );
            }
            assert_eq!(
                reader.confirmed_end(),
                Some(std::fs::metadata(&path).unwrap().len())
            );
            assert!(reader.shared.maintenance.try_lock().is_err());
            if matches!(phase, IoPhase::CarrierRead | IoPhase::CheckpointWrite) {
                assert_eq!(
                    reader.shared.state.lock().unwrap().index.examined_through,
                    4
                );
            }

            // Cancellation must not release maintenance serialization while the
            // blocking checkpoint writer still owns the temporary file.
            let lookup = if phase == IoPhase::CheckpointWrite {
                lookup.abort();
                assert!(lookup.await.unwrap_err().is_cancelled());
                assert!(reader.shared.maintenance.try_lock().is_err());
                let mut waiting = Box::pin(reader.latest_observation(&key));
                assert!(futures::poll!(waiting.as_mut()).is_pending());
                None
            } else {
                Some(lookup)
            };
            // This is the last writable journal handle, while optional I/O is paused.
            completes(tokio::task::spawn_blocking(move || drop(journal)))
                .await
                .unwrap();
            latch.0.release();
            if let Some(lookup) = lookup {
                assert!(matches!(
                    completes(lookup).await.unwrap().unwrap(),
                    ObservationLookup::Ready {
                        committed_len: 1,
                        ..
                    }
                ));
            }
            // Also wait for a cancelled lookup's retained blocking worker.
            drop(completes(reader.shared.maintenance.lock()).await);
            let checkpoint = load_checkpoint(&path, reader.confirmed_end().unwrap()).unwrap();
            assert_eq!(checkpoint.index.examined_through, 1);
            assert_eq!(reader.shared.state.lock().unwrap().checkpointed, first_end);
            if matches!(phase, IoPhase::CarrierRead | IoPhase::CheckpointWrite) {
                assert_eq!(
                    reader.shared.state.lock().unwrap().index.examined_through,
                    4,
                    "an older maintenance snapshot must not replace newer append progress"
                );
            }
            // The worker can release maintenance before dropping its captured
            // reader. Retire every shared owner so this is a cold reopen.
            let shared = Arc::downgrade(&reader.shared);
            drop(reader);
            completes(async {
                while shared.strong_count() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let cold = DiskObservationReader::<ChainEvent>::open(path).unwrap();
            let ObservationLookup::Ready {
                committed_len,
                observation,
            } = cold.latest_observation(&key).await.unwrap()
            else {
                panic!("the suffix after the checkpoint must be recoverable");
            };
            assert_eq!(committed_len, 4);
            let observation = observation.unwrap();
            assert_eq!(observation.position, 3);
            assert_eq!(observation.observation.capture.capture_seq, CaptureSeq(3));
            assert_eq!(cold.shared.state.lock().unwrap().rebuilt_frames, 2);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)] // The held optional mutex is the injected failure.
    async fn held_index_mutex_skips_updates_and_cancelled_appends_still_commit() {
        for grouped in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("held-index.log");
            let stage = StageId::new();
            let journal =
                DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
            let observed = event(stage, 1);
            let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
            let reader = DiskObservationReader::<ChainEvent>::new(path.clone(), true);
            let guard = reader.shared.state.lock().unwrap();
            let append_journal = journal.clone();
            let fact = observed.clone();
            let mut receipt = Box::pin(async move {
                if grouped {
                    append_journal
                        .append_group(
                            "cancelled",
                            vec![fact.clone(), recapture(&fact, 2)],
                            Default::default(),
                        )
                        .await
                        .map(|_| ())
                } else {
                    append_journal
                        .append(fact, Default::default())
                        .await
                        .map(|_| ())
                }
            });
            assert!(futures::poll!(receipt.as_mut()).is_pending());
            drop(receipt);
            completes(async {
                while reader.confirmed_end() == Some(0) {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            completes(journal.append(recapture(&observed, 3), Default::default()))
                .await
                .unwrap();
            completes(journal.append_group(
                "terminal",
                vec![
                    ChainEventFactory::drain_event(stage.into()),
                    ChainEventFactory::eof_event(stage.into(), true),
                ],
                Default::default(),
            ))
            .await
            .unwrap();
            assert_eq!(
                guard.offset, 0,
                "all append-side index updates were skipped"
            );
            let records = completes(journal.read_all_unordered()).await.unwrap();
            assert_eq!(records.len(), if grouped { 5 } else { 4 });
            // Fixed-prefix readers must work while optional indexing is held
            // unavailable, including the buffered terminal atomic group.
            let mut prefix = completes(journal.reader()).await.unwrap();
            for record in &records {
                assert!(!prefix.initial_prefix_complete().unwrap());
                assert_eq!(
                    completes(prefix.next()).await.unwrap().unwrap().id(),
                    record.id()
                );
            }
            assert!(prefix.initial_prefix_complete().unwrap());
            completes(tokio::task::spawn_blocking(move || drop(journal)))
                .await
                .unwrap();
            assert!(
                !checkpoint_path(&path).exists(),
                "destruction must not persist an index"
            );
            drop(guard);
            let ObservationLookup::Ready {
                committed_len,
                observation,
            } = reader.latest_observation(&key).await.unwrap()
            else {
                panic!("skipped updates must be recovered from the committed journal");
            };
            assert_eq!(committed_len, records.len() as u64);
            assert_eq!(
                observation.unwrap().observation.capture.capture_seq,
                CaptureSeq(3)
            );
        }
    }

    #[tokio::test]
    async fn suffix_recovery_finishes_its_captured_prefix_while_appends_continue() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("moving-tail.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        for seq in 1..=600 {
            journal
                .append(recapture(&observed, seq), Default::default())
                .await
                .unwrap();
        }
        let reader = DiskObservationReader::<ChainEvent>::new(path, true);
        *reader.shared.state.lock().unwrap() = State::default();
        for examined in [256, 512] {
            assert!(matches!(reader.latest_observation(&key).await.unwrap(),
                ObservationLookup::Rebuilding { examined_through, committed_len: None } if examined_through == examined));
            // Each interval adds more frames than one lookup can examine.
            for seq in 0..300 {
                journal
                    .append(
                        recapture(&observed, 1000 + examined + seq),
                        Default::default(),
                    )
                    .await
                    .unwrap();
            }
        }
        let ObservationLookup::Ready {
            committed_len,
            observation,
        } = reader.latest_observation(&key).await.unwrap()
        else {
            panic!("new appends must not move the captured rebuild target");
        };
        assert_eq!(committed_len, 600);
        assert_eq!(
            observation.unwrap().observation.capture.capture_seq,
            CaptureSeq(600)
        );
        assert_eq!(reader.shared.state.lock().unwrap().rebuilt_frames, 600);
        for examined in [856, 1112] {
            assert!(matches!(reader.latest_observation(&key).await.unwrap(),
                ObservationLookup::Rebuilding { examined_through, .. } if examined_through == examined));
        }
        let ObservationLookup::Ready {
            committed_len,
            observation,
        } = reader.latest_observation(&key).await.unwrap()
        else {
            panic!("the next captured suffix must also be recoverable");
        };
        assert_eq!(committed_len, 1200);
        assert_eq!(
            observation.unwrap().observation.capture.capture_seq,
            CaptureSeq(1811)
        );
    }

    #[tokio::test]
    async fn read_only_bootstrap_discards_work_when_a_writer_appears() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bootstrap.log");
        let stage = StageId::new();
        let original = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        original
            .append(observed.clone(), Default::default())
            .await
            .unwrap();
        drop(original);
        let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        assert_eq!(reader.confirmed_end(), None);
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        let latch = stall(&reader, IoPhase::Publish);
        let query_reader = reader.clone();
        let query_key = key.clone();
        let lookup = tokio::spawn(async move { query_reader.latest_observation(&query_key).await });
        completes(latch.0.entered.notified()).await;
        let journal = DiskJournal::with_owner(path, JournalOwner::stage(stage)).unwrap();
        completes(journal.append(recapture(&observed, 2), Default::default()))
            .await
            .unwrap();
        latch.0.release();
        assert!(matches!(
            completes(lookup).await.unwrap().unwrap(),
            ObservationLookup::Rebuilding {
                examined_through: 0,
                committed_len: None
            }
        ));
        assert_eq!(
            ready(&reader, &key).await.observation.capture.capture_seq,
            CaptureSeq(2)
        );
        assert_eq!(
            reader.shared.state.lock().unwrap().index.examined_through,
            2
        );
    }

    #[tokio::test]
    async fn lookup_panic_does_not_erase_newer_append_progress() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("panic.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        journal
            .append(observed.clone(), Default::default())
            .await
            .unwrap();
        let reader = DiskObservationReader::<ChainEvent>::new(path, true);
        let latch = ReleaseOnDrop(Arc::new(IoLatch::default()));
        let hook_latch = latch.0.clone();
        *reader.shared.io_hook.lock().unwrap() = Some(Arc::new(move |phase| {
            if phase == IoPhase::CarrierRead {
                hook_latch.wait();
                panic!("optional carrier decoder fault");
            }
        }));
        let query_reader = reader.clone();
        let lookup =
            tokio::spawn(async move { query_reader.latest_observations(stage.into()).await });
        completes(latch.0.entered.notified()).await;
        completes(journal.append(recapture(&observed, 2), Default::default()))
            .await
            .unwrap();
        latch.0.release();
        assert!(completes(lookup).await.unwrap().is_err());
        assert_eq!(
            reader.shared.state.lock().unwrap().index.examined_through,
            2
        );
        *reader.shared.io_hook.lock().unwrap() = None;
        completes(journal.append(recapture(&observed, 3), Default::default()))
            .await
            .unwrap();
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        assert_eq!(ready(&reader, &key).await.position, 2);
    }

    #[tokio::test]
    async fn checkpoints_and_group_rebuilds_respect_the_confirmed_byte_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("boundary.log");
        let (observed, bytes, tail_offset) = archive_with_tail(&path, 1, 2).await;
        assert!(load_checkpoint(&path, bytes.len() as u64).is_some());
        assert!(
            load_checkpoint(&path, tail_offset as u64).is_none(),
            "a valid checkpoint at physical EOF must not certify unconfirmed bytes"
        );
        let mut state = State {
            confirmed: true,
            ..Default::default()
        };
        assert!(rebuild::<ChainEvent>(&path, &mut state, bytes.len() as u64 - 1).is_err());
        assert_eq!(
            state.index.examined_through, 1,
            "a partial group contributes no positions"
        );
        assert_eq!(state.offset, tail_offset as u64);
        let reader = DiskObservationReader::<ChainEvent>::new(path.clone(), false);
        // Inject a published boundary before a complete-looking, unconfirmed group.
        reader
            .shared
            .committed_end
            .store(tail_offset as u64, Ordering::Release);
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        assert!(matches!(reader.latest_observation(&key).await.unwrap(),
            ObservationLookup::Ready { committed_len: 1, observation: Some(observation) } if observation.position == 0));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes.len() as u64);
    }

    fn key(event: &ChainEvent, kind: ObservationFamily) -> ObservationKey {
        let stamp = event.envelope.observability.as_ref().unwrap().capture;
        ObservationKey {
            capture_scope: stamp.capture_scope,
            observer: stamp.observer,
            kind,
        }
    }

    async fn ready(
        reader: &dyn JournalObservationReader,
        key: &ObservationKey,
    ) -> LocatedObservation {
        match reader.latest_observation(key).await.unwrap() {
            ObservationLookup::Ready {
                observation: Some(observation),
                ..
            } => observation,
            other => panic!("expected indexed observation, got {other:?}"),
        }
    }

    async fn archive_with_tail(
        path: &Path,
        prefix_frames: usize,
        tail_members: usize,
    ) -> (ChainEvent, Vec<u8>, usize) {
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.to_owned(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        for index in 0..prefix_frames {
            let mut fact = observed.clone();
            if index != 0 {
                fact.envelope.observability = None;
            }
            journal.append(fact, Default::default()).await.unwrap();
        }
        let tail_offset = std::fs::metadata(path).unwrap().len() as usize;
        let mut tail: Vec<_> = (0..tail_members)
            .map(|member| {
                let mut fact = observed.clone();
                fact.envelope
                    .observability
                    .as_mut()
                    .unwrap()
                    .capture
                    .capture_seq = CaptureSeq(member as u64 + 2);
                fact
            })
            .collect();
        if tail_members == 1 {
            journal
                .append(tail.pop().unwrap(), Default::default())
                .await
                .unwrap();
        } else {
            journal
                .append_group("tail", tail, Default::default())
                .await
                .unwrap();
        }
        journal
            .observation_reader()
            .unwrap()
            .latest_observations(stage.into())
            .await
            .unwrap();
        drop(journal);
        (observed, std::fs::read(path).unwrap(), tail_offset)
    }

    #[tokio::test]
    async fn read_only_torn_tail_preserves_prefix_and_retries_completed_frame() {
        use std::io::Write;

        let directory = tempfile::tempdir().unwrap();
        for tail_members in [1, 2] {
            let path = directory.path().join(format!("tail-{tail_members}.log"));
            let (observed, bytes, tail_offset) = archive_with_tail(&path, 1, tail_members).await;
            let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
            let capture = observed.envelope.observability.as_ref().unwrap().capture;
            let checkpoint = std::fs::read(checkpoint_path(&path)).unwrap();
            for keep_checkpoint in [false, true] {
                for cut in [
                    tail_offset + 1,
                    tail_offset + frame::HEADER_LEN + 1,
                    bytes.len() - 1,
                ] {
                    std::fs::write(&path, &bytes[..cut]).unwrap();
                    if keep_checkpoint {
                        // This checkpoint includes the now-incomplete frame and must be rejected.
                        std::fs::write(checkpoint_path(&path), &checkpoint).unwrap();
                    } else if checkpoint_path(&path).exists() {
                        std::fs::remove_file(checkpoint_path(&path)).unwrap();
                    }
                    let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
                    for _ in 0..2 {
                        let ObservationLookup::Ready {
                            committed_len,
                            observation,
                        } = reader.latest_observation(&key).await.unwrap()
                        else {
                            panic!("torn tail must not prevent complete prefix coverage");
                        };
                        assert_eq!(committed_len, 1);
                        let observation = observation.unwrap();
                        assert_eq!(observation.position, 0);
                        assert_eq!(observation.observation.capture, capture);
                    }
                    let mut absent = key.clone();
                    absent.kind = ObservationFamily::new("runtime_snapshot");
                    assert!(matches!(
                        reader.latest_observation(&absent).await.unwrap(),
                        ObservationLookup::Ready {
                            committed_len: 1,
                            observation: None
                        }
                    ));
                    assert_eq!(std::fs::read(&path).unwrap(), bytes[..cut]);
                    if keep_checkpoint {
                        assert_eq!(std::fs::read(checkpoint_path(&path)).unwrap(), checkpoint);
                    } else {
                        assert!(!checkpoint_path(&path).exists());
                    }

                    // Completing the same frame must remain visible through the existing reader.
                    std::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .unwrap()
                        .write_all(&bytes[cut..])
                        .unwrap();
                    let ObservationLookup::Ready {
                        committed_len,
                        observation,
                    } = reader.latest_observation(&key).await.unwrap()
                    else {
                        panic!("completed tail must become visible");
                    };
                    assert_eq!(committed_len, 1 + tail_members as u64);
                    let observation = observation.unwrap();
                    assert_eq!(observation.position, tail_members as u64);
                    let mut expected = capture;
                    expected.capture_seq = CaptureSeq(tail_members as u64 + 1);
                    assert_eq!(observation.observation.capture, expected);
                }
            }
        }
    }

    #[tokio::test]
    async fn read_only_incomplete_first_frame_has_an_empty_committed_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("first.log");
        let (observed, bytes, _) = archive_with_tail(&path, 0, 2).await;
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        std::fs::remove_file(checkpoint_path(&path)).unwrap();
        for cut in [0, 1, frame::HEADER_LEN + 1, bytes.len() - 1] {
            std::fs::write(&path, &bytes[..cut]).unwrap();
            let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
            assert!(matches!(
                reader.latest_observation(&key).await.unwrap(),
                ObservationLookup::Ready {
                    committed_len: 0,
                    observation: None
                }
            ));
            assert_eq!(std::fs::read(&path).unwrap(), bytes[..cut]);
        }
    }

    #[tokio::test]
    async fn read_only_torn_tail_distinguishes_budget_exhaustion_from_completion() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("budget.log");
        let (observed, bytes, _) = archive_with_tail(&path, REBUILD_FRAMES, 2).await;
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        std::fs::remove_file(checkpoint_path(&path)).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
        let first = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        let second = DiskObservationReader::<ChainEvent>::open(path).unwrap();
        assert!(matches!(
            first.latest_observation(&key).await.unwrap(),
            ObservationLookup::Rebuilding { examined_through, committed_len: None }
                if examined_through == REBUILD_FRAMES as u64
        ));
        let ObservationLookup::Ready {
            committed_len,
            observation,
        } = second.latest_observation(&key).await.unwrap()
        else {
            panic!("the next shared rebuild must certify the prefix before the torn group");
        };
        assert_eq!(committed_len, REBUILD_FRAMES as u64);
        assert_eq!(observation.unwrap().position, 0);
        assert_eq!(
            first.shared.state.lock().unwrap().rebuilt_frames,
            REBUILD_FRAMES
        );
    }

    #[tokio::test]
    async fn read_only_corrupt_tail_remains_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("corrupt-tail.log");
        let (observed, mut bytes, tail_offset) = archive_with_tail(&path, 1, 2).await;
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        std::fs::remove_file(checkpoint_path(&path)).unwrap();
        bytes[tail_offset + frame::HEADER_LEN] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        for _ in 0..2 {
            let error = reader.latest_observation(&key).await.unwrap_err();
            assert!(error.to_string().contains("frame body checksum mismatch"));
        }
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[tokio::test]
    async fn rebuild_rejects_a_file_shorter_than_its_captured_end() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shortened.log");
        let (_, bytes, tail_offset) = archive_with_tail(&path, 1, 1).await;
        for cut in [tail_offset, bytes.len() - 1] {
            std::fs::write(&path, &bytes[..cut]).unwrap();
            assert!(
                rebuild::<ChainEvent>(&path, &mut State::default(), bytes.len() as u64).is_err()
            );
        }
    }

    #[tokio::test]
    async fn index_uses_each_family_stamp_and_retains_partial_packets_and_repeated_ids() {
        let directory = tempfile::tempdir().unwrap();
        let stage = StageId::new();
        let foreign = StageId::new();
        let disk = DiskJournal::with_owner(
            directory.path().join("families.log"),
            JournalOwner::stage(stage),
        )
        .unwrap();
        let memory = MemoryJournal::with_owner(JournalOwner::stage(stage));
        for journal in [&disk as &dyn Journal<ChainEvent>, &memory] {
            let mut first = event(foreign, 900);
            let outer_key = key(&first, ObservationFamily::new("runtime.in_flight"));
            let mut stamp = first.envelope.observability.as_ref().unwrap().capture;
            stamp.observer = stage.into();
            stamp.capture_seq = CaptureSeq(1);
            first = first.with_runtime_snapshot(RuntimeSnapshot {
                capture: stamp,
                progress: ExecutionProgress::default(),
                fsm_state: "Running".into(),
            });
            let local_key = ObservationKey {
                capture_scope: stamp.capture_scope,
                observer: stage.into(),
                kind: ObservationFamily::new("runtime_snapshot"),
            };
            let mut second = first.clone();
            let packet = second.envelope.observability.as_mut().unwrap();
            packet.runtime = None;
            packet.capture.capture_seq = CaptureSeq(901);
            packet
                .runtime_snapshot
                .as_mut()
                .unwrap()
                .capture
                .capture_seq = CaptureSeq(2);
            journal
                .append_group("same-event-id", vec![first, second], Default::default())
                .await
                .unwrap();
            let reader = journal.observation_reader().unwrap();
            let local = ready(reader, &local_key).await;
            assert_eq!(local.position, 1);
            assert_eq!(local.observation.capture.capture_seq, CaptureSeq(2));
            let foreign = ready(reader, &outer_key).await;
            assert_eq!(foreign.position, 0);
            assert_eq!(foreign.observation.runtime.unwrap().in_flight, Some(900));
        }
    }

    #[tokio::test]
    async fn checkpoint_reopens_read_only_behind_one_million_unobserved_records() {
        use std::io::Write;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("million.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        let mut noise = observed.clone();
        noise.envelope.observability = None;
        journal.append(observed, Default::default()).await.unwrap();
        let noise_records = journal
            .append_group("quiet", vec![noise; 1000], Default::default())
            .await
            .unwrap();
        let checkpoint_writer = DiskObservationReader::<ChainEvent>::new(path.clone(), true);
        drop(journal);

        // Build the checkpoint fixture by reusing a real group's encoding with
        // self-contained codec definitions. Preserve all one million physical
        // noise records and deliberately repeated EventIds. Each copy advances
        // the production observation index through the writer's commit callback.
        let bytes =
            super::super::log_record::serialize_atomic_group("quiet", &noise_records).unwrap();
        let frame = Anchor::of(0, &bytes);
        let mut offset = checkpoint_writer.confirmed_end().unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        for _ in 1..1000 {
            file.write_all(&bytes).unwrap();
            let end = offset + frame.length;
            checkpoint_writer.committed(&noise_records, offset, end, frame.crc);
            offset = end;
        }
        file.flush().unwrap();
        assert_eq!(file.metadata().unwrap().len(), offset);
        drop(file);

        // Independently decode the last copied frame at its actual offset. The
        // checkpoint's large record count must describe a readable archive.
        let last_offset = offset - frame.length;
        let (last, _) = read_at(&path, last_offset, offset).unwrap();
        let records = Decoder::cold(&path)
            .decode::<ChainEvent>(frame::validate(&last).unwrap(), last_offset)
            .unwrap()
            .into_records();
        assert_eq!(records.len(), 1000);
        assert!(records
            .iter()
            .all(|record| record.envelope.observability.is_none()));

        ready(&checkpoint_writer, &key).await;
        let cached = Arc::downgrade(&checkpoint_writer.shared);
        drop(checkpoint_writer);
        assert!(
            cached.upgrade().is_none(),
            "reopen must use the on-disk checkpoint"
        );
        let reader = DiskObservationReader::<ChainEvent>::open(path).unwrap();
        let ObservationLookup::Ready {
            committed_len,
            observation,
        } = reader.latest_observation(&key).await.unwrap()
        else {
            panic!("valid checkpoint must be ready")
        };
        assert_eq!(committed_len, 1_000_001);
        assert_eq!(observation.unwrap().position, 0);
        assert_eq!(
            reader.shared.state.lock().unwrap().rebuilt_frames,
            0,
            "no EventId index or journal scan on checkpoint lookup"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reopened_handles_share_the_gate_and_recover_a_checkpoint_suffix() {
        use obzenflow_core::journal::ObservabilityPolicy;
        use std::time::Duration;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reopened.log");
        let stage = StageId::new();
        let first = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        first
            .configure(obzenflow_core::journal::JournalConfig {
                observability: ObservabilityPolicy::Periodic {
                    interval: Duration::from_millis(250),
                },
            })
            .unwrap();
        let observed = event(stage, 1);
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        first
            .append(observed.clone(), Default::default())
            .await
            .unwrap();
        let second = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        assert_ne!(
            first.id(),
            second.id(),
            "handle identity is not archive identity"
        );
        assert!(second
            .append(observed.clone(), Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_none());
        ready(first.observation_reader().unwrap(), &key).await;
        drop(first);
        drop(second);
        let checkpoint = std::fs::read(checkpoint_path(&path)).unwrap();

        tokio::time::advance(Duration::from_millis(250)).await;
        let reopened = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let mut next = observed;
        next.envelope
            .observability
            .as_mut()
            .unwrap()
            .capture
            .capture_seq = CaptureSeq(2);
        assert!(reopened
            .append(next, Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_some());
        drop(reopened);
        assert_eq!(
            std::fs::read(checkpoint_path(&path)).unwrap(),
            checkpoint,
            "closing a writer must leave persistence to observation maintenance"
        );
        let reader = DiskObservationReader::<ChainEvent>::open(path).unwrap();
        assert_eq!(ready(&reader, &key).await.position, 2);
        assert_eq!(reader.shared.state.lock().unwrap().rebuilt_frames, 1);
    }

    #[tokio::test]
    async fn missing_corrupt_foreign_and_old_key_checkpoints_share_incremental_rebuilds() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("recovery.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        let key = key(&observed, ObservationFamily::new("runtime.in_flight"));
        let mut noise = observed.clone();
        noise.envelope.observability = None;
        journal.append(observed, Default::default()).await.unwrap();
        for _ in 0..600 {
            journal
                .append(noise.clone(), Default::default())
                .await
                .unwrap();
        }
        ready(journal.observation_reader().unwrap(), &key).await;
        drop(journal);
        let valid = std::fs::read(checkpoint_path(&path)).unwrap();
        // Old enum keys must rebuild through the existing recovery path, even
        // when their checkpoint framing and checksum are otherwise valid.
        let mut old: CheckedCheckpoint = serde_json::from_slice(&valid).unwrap();
        let mut body: serde_json::Value = serde_json::from_str(&old.body).unwrap();
        body["entries"][0][0]["kind"] = serde_json::json!("InFlight");
        old.body = serde_json::to_string(&body).unwrap();
        old.crc = crc32fast::hash(old.body.as_bytes());
        let old_keys = serde_json::to_vec(&old).unwrap();
        let mut old_schema: CheckedCheckpoint = serde_json::from_slice(&valid).unwrap();
        let mut body: serde_json::Value = serde_json::from_str(&old_schema.body).unwrap();
        assert_eq!(
            body["journal_schema_version"],
            obzenflow_core::journal::JOURNAL_SCHEMA_VERSION
        );
        body["journal_schema_version"] = serde_json::json!("4.0");
        old_schema.body = serde_json::to_string(&body).unwrap();
        old_schema.crc = crc32fast::hash(old_schema.body.as_bytes());
        let old_schema = serde_json::to_vec(&old_schema).unwrap();
        let foreign_directory = tempfile::tempdir().unwrap();
        let foreign_path = foreign_directory.path().join("recovery.log");
        let foreign =
            DiskJournal::with_owner(foreign_path.clone(), JournalOwner::stage(stage)).unwrap();
        foreign
            .append(event(stage, 1), Default::default())
            .await
            .unwrap();
        foreign
            .observation_reader()
            .unwrap()
            .latest_observations(stage.into())
            .await
            .unwrap();
        drop(foreign);
        let foreign_checkpoint = std::fs::read(checkpoint_path(&foreign_path)).unwrap();
        for checkpoint in [
            None,
            Some(b"broken".to_vec()),
            Some(foreign_checkpoint),
            Some(old_keys),
            Some(old_schema),
        ] {
            match checkpoint {
                None => std::fs::remove_file(checkpoint_path(&path)).unwrap(),
                Some(bytes) => std::fs::write(checkpoint_path(&path), bytes).unwrap(),
            }
            let first = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
            let second = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
            assert!(matches!(
                first.latest_observation(&key).await.unwrap(),
                ObservationLookup::Rebuilding {
                    examined_through: 256,
                    ..
                }
            ));
            assert!(matches!(
                second.latest_observation(&key).await.unwrap(),
                ObservationLookup::Rebuilding {
                    examined_through: 512,
                    ..
                }
            ));
            assert_eq!(ready(&first, &key).await.position, 0);
            assert_eq!(first.shared.state.lock().unwrap().rebuilt_frames, 601);
            drop(first);
            drop(second);
            std::fs::write(checkpoint_path(&path), &valid).unwrap();
        }
    }

    #[tokio::test]
    async fn optional_index_panic_and_checkpoint_write_failure_never_fail_or_poison_facts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("faults.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observation = event(stage, 1);
        let key = key(&observation, ObservationFamily::new("runtime.in_flight"));
        journal
            .append(observation.clone(), Default::default())
            .await
            .unwrap();
        let reader = DiskObservationReader::<ChainEvent>::new(path.clone(), true);
        reader.maintain(|_| panic!("optional index maintenance fault"));
        journal
            .append(observation.clone(), Default::default())
            .await
            .unwrap();
        std::fs::create_dir(checkpoint_path(&path)).unwrap();
        assert_eq!(ready(&reader, &key).await.position, 0);
        journal
            .append(observation, Default::default())
            .await
            .unwrap();
        assert_eq!(journal.read_all_unordered().await.unwrap().len(), 3);
    }
}
