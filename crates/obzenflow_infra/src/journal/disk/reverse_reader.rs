// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Reverse reads use fixed binary trailers. A damaged or torn trailer falls
//! back to checked forward lengths, never delimiter searches through payloads.

use super::codec::frame;
use super::scanner::FrameTermination;
use std::io::{self, SeekFrom};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

pub(super) struct ReverseFrameReader<R> {
    reader: R,
    end: u64,
    offset: u64,
    initial_anchor: Option<u64>,
}

impl<R: AsyncRead + AsyncSeek + Unpin> ReverseFrameReader<R> {
    pub(super) fn new(reader: R, end: u64, anchor: u64) -> Self {
        Self {
            reader,
            end,
            offset: end,
            initial_anchor: Some(anchor),
        }
    }
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    async fn read_at(&mut self, offset: u64, bytes: &mut [u8]) -> io::Result<()> {
        self.reader.seek(SeekFrom::Start(offset)).await?;
        self.reader.read_exact(bytes).await?;
        Ok(())
    }

    /// Exceptional recovery only. Following validated header lengths prevents a
    /// trailer-shaped byte string inside an opaque payload becoming a boundary.
    async fn recover_start(&mut self, anchor: u64) -> io::Result<u64> {
        let mut position = anchor;
        let mut previous = anchor;
        while position < self.end {
            let count = (self.end - position).min(frame::HEADER_LEN as u64) as usize;
            let mut header = [0; frame::HEADER_LEN];
            self.read_at(position, &mut header[..count]).await?;
            let Ok(length) = frame::frame_length(&header[..count]) else {
                return Ok(position);
            };
            let Some(next) = position.checked_add(length as u64) else {
                return Ok(position);
            };
            if next > self.end {
                return Ok(position);
            }
            previous = position;
            position = next;
        }
        Ok(previous)
    }

    pub(super) async fn read_frame(
        &mut self,
        bytes: &mut Vec<u8>,
    ) -> io::Result<Option<FrameTermination>> {
        bytes.clear();
        if self.end == 0 {
            return Ok(None);
        }
        // The physical EOF can be inside an opaque payload which happens to
        // end in a valid embedded frame. Establish the first boundary by
        // following headers from a known journal offset before using trailers.
        let mut start = match self.initial_anchor.take() {
            Some(anchor) => Some(self.recover_start(anchor).await?),
            None => None,
        };
        if start.is_none() && self.end >= (frame::HEADER_LEN + frame::TRAILER_LEN) as u64 {
            let mut trailer = [0u8; frame::TRAILER_LEN];
            self.read_at(self.end - frame::TRAILER_LEN as u64, &mut trailer)
                .await?;
            let length = u64::from_le_bytes(trailer[4..12].try_into().unwrap());
            if trailer[12..] == frame::COMMIT_MAGIC
                && length >= (frame::HEADER_LEN + frame::TRAILER_LEN) as u64
                && length <= self.end
            {
                let candidate = self.end - length;
                let mut header = [0u8; frame::HEADER_LEN];
                self.read_at(candidate, &mut header).await?;
                if frame::frame_length(&header).is_ok_and(|actual| actual as u64 == length) {
                    start = Some(candidate);
                }
            }
        }
        let start = match start {
            Some(start) => start,
            None => self.recover_start(0).await?,
        };
        self.reader.seek(SeekFrom::Start(start)).await?;
        let length = self.end - start;
        (&mut self.reader).take(length).read_to_end(bytes).await?;
        if bytes.len() as u64 != length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "journal changed during reverse read",
            ));
        }
        self.end = start;
        self.offset = start;
        let termination =
            if frame::frame_length(bytes).is_ok_and(|expected| expected == bytes.len()) {
                FrameTermination::Committed
            } else {
                FrameTermination::Incomplete
            };
        Ok(Some(termination))
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
    async fn reconstructs_binary_frames_and_commit_markers_across_multiple_chunks() {
        let frames: Vec<_> = [1, 65535, 65536, 65537, 3 * 65536]
            .into_iter()
            .map(|size| frame::encode("é🙂:\n\0".repeat(size / 9 + 1).as_bytes()))
            .collect();
        for torn in [false, true] {
            let mut input = Vec::new();
            for (index, frame) in frames.iter().enumerate() {
                let end = frame.len() - usize::from(torn && index + 1 == frames.len());
                input.extend_from_slice(&frame[..end]);
            }
            let length = input.len() as u64;
            let mut reader = ReverseFrameReader::new(Cursor::new(input), length, 0);
            let mut actual = Vec::new();
            for (index, expected) in frames.iter().rev().enumerate() {
                let incomplete = torn && index == 0;
                assert_eq!(
                    reader.read_frame(&mut actual).await.unwrap(),
                    Some(if incomplete {
                        FrameTermination::Incomplete
                    } else {
                        FrameTermination::Committed
                    })
                );
                assert_eq!(actual, expected[..expected.len() - usize::from(incomplete)]);
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
        ) -> Poll<io::Result<()>> {
            let limit = buf.remaining().min(3);
            let mut limited = ReadBuf::new(buf.initialize_unfilled_to(limit));
            let result = Pin::new(&mut self.0).poll_read(cx, &mut limited);
            let filled = limited.filled().len();
            buf.advance(filled);
            result
        }
    }
    impl AsyncSeek for ShortReads {
        fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
            Pin::new(&mut self.0).start_seek(position)
        }
        fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
            Pin::new(&mut self.0).poll_complete(cx)
        }
    }

    #[tokio::test]
    async fn short_reads_do_not_skip_bytes() {
        let frames: Vec<_> = ["first", "é🙂:0:1372:\n", "last"]
            .map(|body| frame::encode(body.as_bytes()))
            .into();
        let input = frames.concat();
        let length = input.len() as u64;
        let mut reader = ReverseFrameReader::new(ShortReads(Cursor::new(input)), length, 0);
        let mut actual = Vec::new();
        for expected in frames.iter().rev() {
            assert_eq!(
                reader.read_frame(&mut actual).await.unwrap(),
                Some(FrameTermination::Committed)
            );
            assert_eq!(&actual, expected);
        }
        assert_eq!(reader.read_frame(&mut actual).await.unwrap(), None);
    }

    #[tokio::test]
    async fn torn_payload_with_embedded_valid_frame_does_not_invent_a_boundary() {
        let first = frame::encode(b"first");
        let embedded = frame::encode(b"this is only opaque payload");
        let second = frame::encode(&embedded);
        for cut in [second.len() - 7, frame::HEADER_LEN + embedded.len()] {
            for anchor in [0, first.len() as u64] {
                let mut bytes = first.clone();
                bytes.extend_from_slice(&second[..cut]);
                let length = bytes.len() as u64;
                let mut reader = ReverseFrameReader::new(Cursor::new(bytes), length, anchor);
                let mut actual = Vec::new();
                assert_eq!(
                    reader.read_frame(&mut actual).await.unwrap(),
                    Some(FrameTermination::Incomplete)
                );
                assert_eq!(reader.offset(), first.len() as u64);
                assert_eq!(actual, second[..cut]);
                assert_eq!(
                    reader.read_frame(&mut actual).await.unwrap(),
                    Some(FrameTermination::Committed)
                );
                assert_eq!(actual, first);
            }
        }
    }

    #[tokio::test]
    async fn unexpected_eof_is_an_io_error_and_empty_file_has_no_frames() {
        let mut reader = ReverseFrameReader::new(Cursor::new(b"short".to_vec()), 20, 0);
        assert_eq!(
            reader.read_frame(&mut Vec::new()).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        let mut empty = ReverseFrameReader::new(Cursor::new(Vec::<u8>::new()), 0, 0);
        assert_eq!(empty.read_frame(&mut Vec::new()).await.unwrap(), None);
    }
}
