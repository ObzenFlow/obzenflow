// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::io;

pub(crate) const MAGIC: [u8; 4] = *b"OJF4";
pub(crate) const COMMIT_MAGIC: [u8; 4] = *b"4FJO";
pub(crate) const HEADER_LEN: usize = 16;
pub(crate) const TRAILER_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FrameProblem {
    Incomplete(String),
    Corrupt(String),
}

pub(crate) fn encode(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + TRAILER_LEN);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&out).to_le_bytes());
    out.extend_from_slice(body);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&((HEADER_LEN + body.len() + TRAILER_LEN) as u64).to_le_bytes());
    out.extend_from_slice(&COMMIT_MAGIC);
    out
}

/// Validate a header before its untrusted length controls any read/allocation.
pub(crate) fn frame_length(header: &[u8]) -> Result<usize, FrameProblem> {
    if header
        .iter()
        .take(4)
        .zip(MAGIC)
        .any(|(actual, expected)| *actual != expected)
    {
        return Err(FrameProblem::Corrupt(
            "invalid format-4 magic; re-record older journal formats".into(),
        ));
    }
    if header.len() < HEADER_LEN {
        return Err(FrameProblem::Incomplete(
            "incomplete format-4 header".into(),
        ));
    }
    let expected = u32::from_le_bytes(header[12..16].try_into().unwrap());
    if crc32fast::hash(&header[..12]) != expected {
        return Err(FrameProblem::Corrupt(
            "frame header checksum mismatch".into(),
        ));
    }
    let body_length = u64::from_le_bytes(header[4..12].try_into().unwrap());
    usize::try_from(body_length)
        .ok()
        .and_then(|length| length.checked_add(HEADER_LEN + TRAILER_LEN))
        .ok_or_else(|| FrameProblem::Corrupt("frame length overflow".into()))
}

pub(crate) fn validate(bytes: &[u8]) -> Result<&[u8], FrameProblem> {
    let length = frame_length(bytes)?;
    if bytes.len() < length {
        return Err(FrameProblem::Incomplete(format!(
            "incomplete frame: expected {length} bytes, found {}",
            bytes.len()
        )));
    }
    if bytes.len() != length {
        return Err(FrameProblem::Corrupt("trailing bytes after frame".into()));
    }
    let trailer = length - TRAILER_LEN;
    if bytes[length - 4..] != COMMIT_MAGIC {
        return Err(FrameProblem::Corrupt("invalid commit trailer".into()));
    }
    if u64::from_le_bytes(bytes[trailer + 4..trailer + 12].try_into().unwrap()) != length as u64 {
        return Err(FrameProblem::Corrupt(
            "commit trailer length mismatch".into(),
        ));
    }
    if crc32fast::hash(&bytes[..trailer])
        != u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap())
    {
        return Err(FrameProblem::Corrupt("frame body checksum mismatch".into()));
    }
    Ok(&bytes[HEADER_LEN..trailer])
}

pub(crate) fn io_error(problem: FrameProblem) -> io::Error {
    match problem {
        FrameProblem::Incomplete(message) => io::Error::new(io::ErrorKind::UnexpectedEof, message),
        FrameProblem::Corrupt(message) => io::Error::new(io::ErrorKind::InvalidData, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn previous_format_is_rejected_even_as_an_incomplete_tail() {
        assert_eq!(obzenflow_core::journal::JOURNAL_FORMAT_VERSION, 4);
        assert!(matches!(
            frame_length(b"OJF3"),
            Err(FrameProblem::Corrupt(message)) if message.contains("format-4")
        ));
    }

    #[test]
    fn every_truncation_remains_uncommitted_and_every_byte_is_checked() {
        let frame = encode(b"absolute values\0\n\xff");
        for end in 0..frame.len() {
            assert!(matches!(
                validate(&frame[..end]),
                Err(FrameProblem::Incomplete(_))
            ));
        }
        assert_eq!(validate(&frame).unwrap(), b"absolute values\0\n\xff");
        for index in 0..frame.len() {
            let mut corrupt = frame.clone();
            corrupt[index] ^= 1;
            assert!(
                matches!(validate(&corrupt), Err(FrameProblem::Corrupt(_))),
                "byte {index}"
            );
        }
    }
}
