// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// src/monitoring/metrics/utilities.rs
//! Core metric collection utilities for monitoring primitives

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A sliding window for calculating rates
pub struct SlidingWindow {
    window: Arc<Mutex<VecDeque<Instant>>>,
    window_size: Duration,
}

impl SlidingWindow {
    pub fn new(window_size: Duration) -> Self {
        Self {
            window: Arc::new(Mutex::new(VecDeque::new())),
            window_size,
        }
    }

    pub fn add(&self, timestamp: Instant) {
        let mut window = self.window.lock().unwrap();
        window.push_back(timestamp);

        // Remove old entries
        let cutoff = timestamp - self.window_size;
        while let Some(&front) = window.front() {
            if front < cutoff {
                window.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn rate_per_sec(&self) -> f64 {
        let window = self.window.lock().unwrap();
        let count = window.len() as f64;
        count / self.window_size.as_secs_f64()
    }

    pub fn count(&self) -> usize {
        self.window.lock().unwrap().len()
    }
}

/// Simple histogram for duration tracking
pub struct SimpleHistogram {
    buckets: Arc<Mutex<Vec<f64>>>,
    sorted: Arc<Mutex<bool>>,
}

impl Default for SimpleHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleHistogram {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(Mutex::new(Vec::new())),
            sorted: Arc::new(Mutex::new(true)),
        }
    }

    pub fn observe(&self, value: Duration) {
        let mut buckets = self.buckets.lock().unwrap();
        buckets.push(value.as_secs_f64());
        *self.sorted.lock().unwrap() = false;
    }

    pub fn quantile(&self, q: f64) -> Duration {
        let mut buckets = self.buckets.lock().unwrap();
        let mut sorted = self.sorted.lock().unwrap();

        if !*sorted {
            buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());
            *sorted = true;
        }

        if buckets.is_empty() {
            return Duration::ZERO;
        }

        let index = ((buckets.len() - 1) as f64 * q) as usize;
        Duration::from_secs_f64(buckets[index])
    }

    pub fn p50(&self) -> Duration {
        self.quantile(0.5)
    }

    pub fn p99(&self) -> Duration {
        self.quantile(0.99)
    }

    pub fn p999(&self) -> Duration {
        self.quantile(0.999)
    }
}

/// Circular buffer for recent events
pub struct CircularBuffer<T> {
    buffer: Arc<Mutex<VecDeque<T>>>,
    capacity: usize,
}

impl<T> CircularBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            capacity,
        }
    }

    pub fn push(&self, item: T) {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.len() >= self.capacity {
            buffer.pop_front();
        }
        buffer.push_back(item);
    }

    pub fn len(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.lock().unwrap().is_empty()
    }

    pub fn iter_recent<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&VecDeque<T>) -> R,
    {
        let buffer = self.buffer.lock().unwrap();
        f(&buffer)
    }
}

impl CircularBuffer<(Instant, String)> {
    /// Calculate error rate from recent errors
    pub fn error_rate_per_sec(&self, window: Duration) -> f64 {
        let cutoff = Instant::now() - window;
        self.iter_recent(|buffer| {
            let recent_errors = buffer
                .iter()
                .filter(|(timestamp, _)| *timestamp >= cutoff)
                .count();
            recent_errors as f64 / window.as_secs_f64()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliding_window() {
        let window = SlidingWindow::new(Duration::from_secs(1));
        let start = Instant::now();

        window.add(start);
        window.add(start + Duration::from_millis(100));
        window.add(start + Duration::from_millis(200));

        assert_eq!(window.count(), 3);
        assert!(window.rate_per_sec() > 0.0);
    }

    #[test]
    fn test_simple_histogram() {
        let hist = SimpleHistogram::new();
        hist.observe(Duration::from_millis(10));
        hist.observe(Duration::from_millis(20));
        hist.observe(Duration::from_millis(30));

        assert!(hist.p50() >= Duration::from_millis(10));
        assert!(hist.p99() >= Duration::from_millis(20));
    }

    #[test]
    fn test_circular_buffer() {
        let buffer = CircularBuffer::new(2);
        buffer.push("first");
        buffer.push("second");
        buffer.push("third"); // Should evict "first"

        assert_eq!(buffer.len(), 2);
        buffer.iter_recent(|buf| {
            assert_eq!(buf.len(), 2);
        });
    }
}
