// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disposable attachment locators. Recovery and checkpoint I/O are deliberately
//! outside the fact writer's panic/error contract.

use super::codec::{frame, Decoder};
use super::scanner::{classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy};
use crate::journal::observation_index::{
    locate, unavailable, Locator, ObservationIndex, HISTORY_PER_KEY, MAX_KEYS,
};
use async_trait::async_trait;
use obzenflow_core::event::{JournalEvent, JournalRecord};
use obzenflow_core::journal::{
    JournalError, JournalObservationReader, LocatedObservation, ObservationKey, ObservationLookup,
};
use obzenflow_core::WriterId;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::RwLock;

const REBUILD_FRAMES: usize = 256;
const MAX_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
type SharedIndexes = Mutex<HashMap<PathBuf, Weak<Mutex<State>>>>;
static INDEXES: OnceLock<SharedIndexes> = OnceLock::new();

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

#[derive(Default)]
struct State {
    loaded: bool,
    index: ObservationIndex,
    offset: u64,
    first: Option<Anchor>,
    last: Option<Anchor>,
    checkpointed: u64,
    #[cfg(test)]
    rebuilt_frames: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    format: u32,
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
    state: Arc<Mutex<State>>,
    lock: Arc<RwLock<()>>,
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

    pub(super) fn new(path: PathBuf, writable: bool) -> Self {
        let key = std::path::absolute(&path).unwrap_or_else(|_| path.clone());
        let mut registry = INDEXES
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let state = registry
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let state = Arc::new(Mutex::new(State::default()));
                registry.retain(|_, state| state.strong_count() > 0);
                registry.insert(key, Arc::downgrade(&state));
                state
            });
        Self {
            lock: super::journal::shared_state_for_path(&path).0,
            path,
            state,
            writable,
            _event: std::marker::PhantomData,
        }
    }

    pub(super) fn committed(
        &self,
        records: &[JournalRecord<T::Payload>],
        offset: u64,
        next_offset: u64,
        crc: u32,
    ) {
        self.maintain(|state| {
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
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !matches!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&mut state))),
            Ok(Ok(()))
        ) {
            // Optional faults never escape into retain_commit and never poison
            // the journal. The next reader can rebuild from committed facts.
            *state = State::default();
        }
    }

    pub(super) fn checkpoint(&self) {
        if self.writable {
            self.maintain(|state| {
                if state.loaded && state.offset != state.checkpointed {
                    // Checkpoint write failure needs no reset of the valid live index.
                    if write_checkpoint(&self.path, state).is_ok() {
                        state.checkpointed = state.offset;
                    }
                }
                Ok(())
            });
        }
    }

    async fn lookup(
        &self,
        key: Option<ObservationKey>,
        observer: WriterId,
    ) -> Result<ObservationLookup<Vec<LocatedObservation>>, JournalError> {
        let this = self.clone();
        let recovery = this.state.clone();
        let guard = this.lock.clone().read_owned().await;
        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut state = this.state.lock().unwrap_or_else(|error| error.into_inner());
            if !state.loaded {
                *state = load_checkpoint(&this.path).unwrap_or_default();
                state.loaded = true;
            }
            let end = std::fs::metadata(&this.path)
                .map_err(|error| unavailable(error.to_string()))?
                .len();
            if state.offset > end {
                *state = State {
                    loaded: true,
                    ..Default::default()
                };
            }
            if state.offset < end {
                rebuild::<T>(&this.path, &mut state, end)?;
            }
            if state.offset < end {
                return Ok(ObservationLookup::Rebuilding {
                    examined_through: state.index.examined_through,
                    committed_len: None,
                });
            }
            let mut observation = Vec::new();
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
                        let (bytes, termination) = read_at(&this.path, locator.frame_offset)?;
                        let frame = dispose(
                            classify_frame::<T>(
                                &bytes,
                                &mut Decoder::new(&this.path),
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
            let committed_len = state.index.examined_through;
            drop(state);
            this.checkpoint();
            Ok(ObservationLookup::Ready {
                committed_len,
                observation,
            })
        })
        .await
        .unwrap_or_else(|error| Err(unavailable(error.to_string())));
        if result.is_err() {
            *recovery.lock().unwrap_or_else(|error| error.into_inner()) = State {
                loaded: true,
                ..Default::default()
            };
        }
        result
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
) -> Result<(Vec<u8>, super::scanner::FrameTermination), JournalError> {
    let mut file = File::open(path).map_err(|error| unavailable(error.to_string()))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| unavailable(error.to_string()))?;
    let mut bytes = Vec::new();
    let (_, termination) = read_frame_sync(&mut BufReader::new(file), &mut bytes)
        .map_err(|error| unavailable(error.to_string()))?
        .ok_or_else(|| unavailable("missing committed frame"))?;
    Ok((bytes, termination))
}

fn rebuild<T: JournalEvent>(path: &Path, state: &mut State, end: u64) -> Result<(), JournalError> {
    let mut file = File::open(path).map_err(|error| unavailable(error.to_string()))?;
    file.seek(SeekFrom::Start(state.offset))
        .map_err(|error| unavailable(error.to_string()))?;
    let mut reader = BufReader::new(file);
    let mut decoder = Decoder::new(path);
    let mut bytes = Vec::new();
    for _ in 0..REBUILD_FRAMES {
        if state.offset >= end {
            break;
        }
        let Some((_, termination)) = read_frame_sync(&mut reader, &mut bytes)
            .map_err(|error| unavailable(error.to_string()))?
        else {
            break;
        };
        let frame = dispose(
            classify_frame::<T>(&bytes, &mut decoder, state.offset),
            termination,
            ReadPolicy::LiveTail,
        );
        let records = match frame {
            Disposition::Yield(frame) => frame.into_records(),
            Disposition::Skip | Disposition::EndOfCommittedRecords => break,
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
    Ok(())
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
        format: obzenflow_core::journal::JOURNAL_FORMAT_VERSION,
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

fn load_checkpoint(path: &Path) -> Option<State> {
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
    if checkpoint.format != obzenflow_core::journal::JOURNAL_FORMAT_VERSION
        || checkpoint.archive != archive_id(path)
        || checkpoint.journal != path.file_name()?.to_str()?
        || end > std::fs::metadata(path).ok()?.len()
        || checkpoint.first.offset != 0
        || checkpoint.entries.len() > MAX_KEYS
    {
        return None;
    }
    for anchor in [&checkpoint.first, &checkpoint.last] {
        let (bytes, _) = read_at(path, anchor.offset).ok()?;
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
        #[cfg(test)]
        rebuilt_frames: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::observability::tests::event;
    use crate::journal::{DiskJournal, MemoryJournal};
    use obzenflow_core::event::observability::families::ObservationKind;
    use obzenflow_core::event::observability::{CaptureSeq, ExecutionProgress, RuntimeSnapshot};
    use obzenflow_core::{ChainEvent, Journal, JournalOwner, StageId};

    fn key(event: &ChainEvent, kind: ObservationKind) -> ObservationKey {
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
            let outer_key = key(&first, ObservationKind::InFlight);
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
                kind: ObservationKind::RuntimeSnapshot,
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
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("million.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        let key = key(&observed, ObservationKind::InFlight);
        let mut noise = observed.clone();
        noise.envelope.observability = None;
        journal.append(observed, Default::default()).await.unwrap();
        // Deliberately repeated EventIds: the lookup must use physical positions.
        for group in 0..1000 {
            journal
                .append_group(
                    &format!("quiet-{group}"),
                    vec![noise.clone(); 1000],
                    Default::default(),
                )
                .await
                .unwrap();
        }
        drop(journal);
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
            reader.state.lock().unwrap().rebuilt_frames,
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
        let key = key(&observed, ObservationKind::InFlight);
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
        // Simulate interruption after the append, before checkpoint maintenance.
        std::fs::write(checkpoint_path(&path), checkpoint).unwrap();
        let reader = DiskObservationReader::<ChainEvent>::open(path).unwrap();
        assert_eq!(ready(&reader, &key).await.position, 2);
        assert_eq!(reader.state.lock().unwrap().rebuilt_frames, 1);
    }

    #[tokio::test]
    async fn missing_corrupt_and_foreign_checkpoints_share_incremental_rebuilds() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("recovery.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        let observed = event(stage, 1);
        let key = key(&observed, ObservationKind::InFlight);
        let mut noise = observed.clone();
        noise.envelope.observability = None;
        journal.append(observed, Default::default()).await.unwrap();
        for _ in 0..600 {
            journal
                .append(noise.clone(), Default::default())
                .await
                .unwrap();
        }
        drop(journal);
        let valid = std::fs::read(checkpoint_path(&path)).unwrap();
        let foreign_directory = tempfile::tempdir().unwrap();
        let foreign_path = foreign_directory.path().join("recovery.log");
        let foreign =
            DiskJournal::with_owner(foreign_path.clone(), JournalOwner::stage(stage)).unwrap();
        foreign
            .append(event(stage, 1), Default::default())
            .await
            .unwrap();
        drop(foreign);
        let foreign_checkpoint = std::fs::read(checkpoint_path(&foreign_path)).unwrap();
        for checkpoint in [None, Some(b"broken".to_vec()), Some(foreign_checkpoint)] {
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
            assert_eq!(first.state.lock().unwrap().rebuilt_frames, 601);
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
        let key = key(&observation, ObservationKind::InFlight);
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
        reader.checkpoint();
        journal
            .append(observation, Default::default())
            .await
            .unwrap();
        assert_eq!(journal.read_all_unordered().await.unwrap().len(), 3);
    }
}
