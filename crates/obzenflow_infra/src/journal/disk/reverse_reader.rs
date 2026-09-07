// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Byte-oriented reverse framing for best-effort journal tail reads.
//!
//! I/O chunks are not record boundaries. Retain fragments until a newline or
//! BOF identifies the complete frame, then let the shared scanner validate it.

use super::scanner::LineTermination;
use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

const BUFFER_SIZE: usize = 64 * 1024;

pub(super) struct ReverseFrameReader<R> {
    reader: R,
    /// Start of the buffered chunk; bytes before this offset remain unread.
    offset: u64,
    buffer: Vec<u8>,
    /// Unconsumed prefix of the buffered chunk.
    cursor: usize,
    termination: LineTermination,
}

impl<R: AsyncRead + AsyncSeek + Unpin> ReverseFrameReader<R> {
    /// The caller holds the journal read lock over this fixed EOF snapshot.
    pub(super) fn new(reader: R, end: u64) -> Self {
        Self {
            reader,
            offset: end,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            cursor: 0,
            termination: LineTermination::Unterminated,
        }
    }

    /// Return the next nonempty frame, newest first, without its newline.
    /// Fragments accumulate in reverse byte order so even a frame spanning
    /// many chunks takes linear work, without repeatedly prepending/copying it.
    pub(super) async fn read_frame(
        &mut self,
        frame: &mut Vec<u8>,
    ) -> std::io::Result<Option<LineTermination>> {
        frame.clear();
        loop {
            if self.cursor == 0 {
                if self.offset == 0 {
                    if frame.is_empty() {
                        return Ok(None);
                    }
                    frame.reverse();
                    return Ok(Some(self.termination));
                }

                let start = self.offset.saturating_sub(BUFFER_SIZE as u64);
                self.buffer.resize((self.offset - start) as usize, 0);
                self.reader.seek(SeekFrom::Start(start)).await?;
                // AsyncRead may legally return a short read. Advancing by the
                // requested size after one read would silently skip bytes.
                self.reader.read_exact(&mut self.buffer).await?;
                self.offset = start;
                self.cursor = self.buffer.len();
            }

            let bytes = &self.buffer[..self.cursor];
            if let Some(newline) = bytes.iter().rposition(|byte| *byte == b'\n') {
                frame.extend(bytes[newline + 1..].iter().rev());
                self.cursor = newline;
                let termination =
                    std::mem::replace(&mut self.termination, LineTermination::Terminated);
                if frame.is_empty() {
                    // Includes the delimiter at EOF and empty physical lines.
                    continue;
                }
                frame.reverse();
                return Ok(Some(termination));
            }

            frame.extend(bytes.iter().rev());
            self.cursor = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    #[tokio::test]
    async fn reconstructs_bytes_and_commit_markers_across_multiple_chunks() {
        let frames: Vec<_> = [
            1,
            BUFFER_SIZE - 1,
            BUFFER_SIZE,
            BUFFER_SIZE + 1,
            3 * BUFFER_SIZE,
        ]
        .into_iter()
        .map(|size| "é🙂:".repeat(size / 7 + 1).into_bytes())
        .collect();
        for terminated in [false, true] {
            let mut input = Vec::new();
            for (index, frame) in frames.iter().enumerate() {
                input.extend(frame);
                if index + 1 < frames.len() || terminated {
                    input.push(b'\n');
                }
            }
            let len = input.len() as u64;
            let mut reader = ReverseFrameReader::new(Cursor::new(input), len);
            let mut actual = Vec::new();
            for (index, expected) in frames.iter().rev().enumerate() {
                let ending = reader.read_frame(&mut actual).await.unwrap().unwrap();
                assert_eq!(&actual, expected);
                assert_eq!(
                    ending,
                    if index == 0 && !terminated {
                        LineTermination::Unterminated
                    } else {
                        LineTermination::Terminated
                    }
                );
            }
            assert_eq!(reader.read_frame(&mut actual).await.unwrap(), None);
        }
    }

    struct ShortReads(Cursor<Vec<u8>>);

    impl AsyncRead for ShortReads {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let limit = buf.remaining().min(3);
            let mut limited = ReadBuf::new(buf.initialize_unfilled_to(limit));
            let result = Pin::new(&mut self.0).poll_read(cx, &mut limited);
            let filled = limited.filled().len();
            buf.advance(filled);
            result
        }
    }

    impl AsyncSeek for ShortReads {
        fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
            Pin::new(&mut self.0).start_seek(position)
        }

        fn poll_complete(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<u64>> {
            Pin::new(&mut self.0).poll_complete(cx)
        }
    }

    #[tokio::test]
    async fn short_reads_do_not_skip_bytes() {
        let input = "first\né🙂:0:1372:\nlast\n".as_bytes().to_vec();
        let len = input.len() as u64;
        let mut reader = ReverseFrameReader::new(ShortReads(Cursor::new(input)), len);
        let mut frame = Vec::new();
        for expected in ["last", "é🙂:0:1372:", "first"] {
            assert_eq!(
                reader.read_frame(&mut frame).await.unwrap(),
                Some(LineTermination::Terminated)
            );
            assert_eq!(frame, expected.as_bytes());
        }
        assert_eq!(reader.read_frame(&mut frame).await.unwrap(), None);
    }

    #[tokio::test]
    async fn unexpected_eof_is_an_io_error() {
        let mut reader = ReverseFrameReader::new(Cursor::new(b"short\n".to_vec()), 20);
        let error = reader.read_frame(&mut Vec::new()).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn empty_files_and_empty_lines_have_no_frames() {
        for input in [b"".as_slice(), b"\n", b"\n\n"] {
            let mut reader = ReverseFrameReader::new(Cursor::new(input), input.len() as u64);
            assert_eq!(reader.read_frame(&mut Vec::new()).await.unwrap(), None);
        }
    }
}
