// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{invalid, Result};

pub(super) fn unsigned(mut value: u64, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(super) fn bytes(value: &[u8], out: &mut Vec<u8>) {
    unsigned(value.len() as u64, out);
    out.extend_from_slice(value);
}

pub(super) fn text(value: &str, out: &mut Vec<u8>) {
    bytes(value.as_bytes(), out);
}

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(super) fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    pub(super) fn position(&self) -> usize {
        self.position
    }

    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid("length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("truncated compact value"))?;
        self.position = end;
        Ok(value)
    }

    pub(super) fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn unsigned(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.byte()?;
            if index == 9 && byte > 1 {
                return Err(invalid("unsigned integer overflow"));
            }
            value |= u64::from(byte & 127) << (7 * index);
            if byte & 128 == 0 {
                if index != 0 && byte == 0 {
                    return Err(invalid("non-canonical unsigned integer"));
                }
                return Ok(value);
            }
        }
        Err(invalid("unterminated unsigned integer"))
    }

    pub(super) fn length(&mut self) -> Result<usize> {
        usize::try_from(self.unsigned()?).map_err(|_| invalid("length exceeds address space"))
    }

    pub(super) fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.length()?;
        self.take(length)
    }

    pub(super) fn text(&mut self) -> Result<String> {
        std::str::from_utf8(self.bytes()?)
            .map(str::to_owned)
            .map_err(|_| invalid("invalid UTF-8"))
    }

    pub(super) fn finish(self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(invalid("trailing compact bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_values_cover_boundaries_and_reject_noncanonical_numbers() {
        for value in [0, 1, 127, 128, 1000, 1001, u32::MAX as u64, u64::MAX] {
            let mut encoded = Vec::new();
            unsigned(value, &mut encoded);
            let mut cursor = Cursor::new(&encoded);
            assert_eq!(cursor.unsigned().unwrap(), value);
            cursor.finish().unwrap();
        }
        for invalid in [&[128, 0][..], &[255; 10][..], &[128][..]] {
            assert!(Cursor::new(invalid).unsigned().is_err());
        }
    }
}
