// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only access to the production frame decoder. No alternate codec.
//! Callers retain the archive and prepare fixtures outside the measured loop.

use crate::journal::disk::codec::Decoder;
use crate::journal::disk::identity::CommitmentAdmission;
use crate::journal::disk::scanner::{
    classify_frame, read_frame_sync, FrameTermination, ParseOutcome,
};
use obzenflow_core::event::{ChainEvent, EventId};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub use crate::journal::disk::codec::benchmark::{EncodingCursor, ReconstructionInput};

/// Same seek/write/flush/rollback boundary as DiskJournal, without preparation.
pub async fn write_preencoded(
    file: Arc<std::sync::Mutex<std::fs::File>>,
    path: PathBuf,
    bytes: Arc<Vec<u8>>,
) -> u64 {
    #[cfg(feature = "bench-instrumentation")]
    obzenflow_core::benchmark::add(obzenflow_core::benchmark::Counter::AppendBlockingJobs, 1);
    tokio::task::spawn_blocking(move || {
        crate::journal::disk::journal::benchmark_append_frame(&mut file.lock().unwrap(), &bytes, &path)
    }).await.unwrap().unwrap()
}

/// Evict just this fixture archive's definition cache outside measured work.
/// All its readers/writers must be quiescent; OS page cache is unaffected.
pub fn clear_definition_cache(path: &Path) {
    crate::journal::disk::codec::DefinitionStore::for_archive(path).clear_for_benchmark();
}

struct Frame {
    offset: u64,
    bytes: Vec<u8>,
}

/// Encoded, committed physical frames from a real provider-created journal.
pub struct FrameCorpus {
    path: PathBuf,
    frames: Vec<Frame>,
    encoded_bytes: usize,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct DecodeWork {
    pub frames: usize,
    pub records: usize,
    pub sequence_sum: u64,
}

impl FrameCorpus {
    pub fn load(path: &Path, expected_ids: &[EventId]) -> Result<Arc<Self>, Error> {
        let mut input = BufReader::new(std::fs::File::open(path)?);
        let mut frames = Vec::new();
        let mut offset = 0;
        loop {
            let mut bytes = Vec::new();
            let Some((consumed, termination)) = read_frame_sync(&mut input, &mut bytes)? else {
                break;
            };
            if termination != FrameTermination::Committed {
                return Err("benchmark fixture contains an incomplete frame".into());
            }
            frames.push(Frame { offset, bytes });
            offset += consumed as u64;
        }
        let corpus = Arc::new(Self {
            path: path.to_owned(),
            frames,
            encoded_bytes: offset as usize,
        });
        // Exact identity/order validation and cache warming are untimed.
        let mut decoder = Decoder::new(path);
        let mut admission = CommitmentAdmission::open(path)?;
        let mut found = Vec::new();
        for frame in &corpus.frames {
            let ParseOutcome::Complete(decoded) =
                classify_frame::<ChainEvent>(&frame.bytes, &mut decoder, frame.offset)
            else {
                return Err("benchmark fixture failed production decoding".into());
            };
            for record in decoded.into_records() {
                admission.admit(&record)?;
                found.push(*record.id());
            }
        }
        if found != expected_ids || found.is_empty() {
            return Err("benchmark fixture identities/order differ from append receipts".into());
        }
        Ok(corpus)
    }

    pub fn frames(&self) -> usize {
        self.frames.len()
    }

    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub fn encoded_frames(&self) -> Vec<Arc<Vec<u8>>> {
        self.frames.iter().map(|f| Arc::new(f.bytes.clone())).collect()
    }

    pub fn reconstruction_input(&self, index: usize, event: obzenflow_core::event::provenance::ChainEventProvenance) -> ReconstructionInput {
        let frame = &self.frames[index];
        ReconstructionInput::new(self.path.clone(), frame.bytes.clone(), frame.offset, event)
    }

    pub fn decode_selected_frame(&self, index: usize) -> Vec<obzenflow_core::event::journal_record::ChainJournalRecord> {
        let frame = &self.frames[index];
        let body = crate::journal::disk::codec::frame::validate(&frame.bytes).unwrap();
        Decoder::new(&self.path).decode_selected::<ChainEvent>(body, frame.offset).unwrap().records
    }

    /// Isolated dependency probe, not an admitted selective journal reader.
    /// A cold decoder must fetch referenced definitions from their real carrier.
    pub fn decode_frame(
        &self,
        index: usize,
        cold: bool,
    ) -> Result<Vec<obzenflow_core::event::journal_record::ChainJournalRecord>, Error> {
        let mut decoder = if cold {
            Decoder::cold(&self.path)
        } else {
            Decoder::new(&self.path)
        };
        let frame = &self.frames[index];
        match classify_frame::<ChainEvent>(&frame.bytes, &mut decoder, frame.offset) {
            ParseOutcome::Complete(decoded) => Ok(decoded.into_records()),
            _ => Err("benchmark dependency frame failed decoding".into()),
        }
    }

    pub fn cursor(self: &Arc<Self>) -> DecodeCursor {
        DecodeCursor {
            corpus: self.clone(),
            decoder: Decoder::new(&self.path),
            admission: CommitmentAdmission::open(&self.path).expect("benchmark journal identity"),
            next: 0,
            work: DecodeWork::default(),
        }
    }
}

/// A fresh sequential cursor; batching changes only the experimental dispatch
/// boundary. Classification and optional continuity admission are production code.
pub struct DecodeCursor {
    corpus: Arc<FrameCorpus>,
    decoder: Decoder,
    admission: CommitmentAdmission,
    next: usize,
    work: DecodeWork,
}

impl DecodeCursor {
    pub fn decode(&mut self, max_frames: usize, admit: bool) -> Result<(), Error> {
        assert!(max_frames > 0);
        let end = self
            .next
            .saturating_add(max_frames)
            .min(self.corpus.frames.len());
        for frame in &self.corpus.frames[self.next..end] {
            let decoded =
                match classify_frame::<ChainEvent>(&frame.bytes, &mut self.decoder, frame.offset) {
                    ParseOutcome::Complete(decoded) => decoded,
                    _ => return Err("benchmark decoding failed".into()),
                };
            for record in decoded.into_records() {
                if admit {
                    self.admission.admit(&record)?;
                }
                self.work.records += 1;
                self.work.sequence_sum += record.local_sequence();
                std::hint::black_box(record);
            }
            self.work.frames += 1;
        }
        self.next = end;
        Ok(())
    }

    pub fn finished(&self) -> bool {
        self.next == self.corpus.frames.len()
    }

    /// Selected decoding and binary continuity on the identical physical corpus.
    pub fn decode_selected(&mut self, max_frames: usize) -> Result<(), Error> {
        let end = self.next.saturating_add(max_frames).min(self.corpus.frames.len());
        for frame in &self.corpus.frames[self.next..end] {
            let body = crate::journal::disk::codec::frame::validate(&frame.bytes)
                .map_err(crate::journal::disk::codec::frame::io_error)?;
            let selected = self.decoder.decode_selected::<ChainEvent>(body, frame.offset)?;
            self.admission.admit_range(selected.routing.first, selected.routing.previous, selected.routing.last)?;
            for record in selected.records {
                self.work.records += 1;
                self.work.sequence_sum += record.local_sequence();
                std::hint::black_box(record);
            }
            self.work.frames += 1;
        }
        self.next = end;
        Ok(())
    }

    pub fn work(&self) -> &DecodeWork {
        &self.work
    }
}
