use super::TimeUnit;
use std::sync::atomic::{AtomicU64, Ordering};

/// Histogram for tracking distributions
///
/// Thread-safe histogram with fixed buckets for observing value distributions.
/// Each bucket is an atomic counter, allowing lock-free concurrent updates.
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<AtomicU64>,
    bucket_boundaries: Vec<f64>,
    sum: AtomicU64, // Store sum as fixed-point * 1000
    count: AtomicU64,
}

impl Histogram {
    /// Create a new histogram with OpenTelemetry-style default buckets
    ///
    /// Based on industry research from Prometheus, OpenTelemetry, and Micrometer:
    /// - OpenTelemetry defaults: [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, +Inf]
    /// - Designed for measuring duration in milliseconds (despite OTel recommending seconds)
    /// - Covers typical web service latency from sub-millisecond to 10+ seconds
    /// - Works well for 90th, 95th, 99th percentile calculations
    pub fn new() -> Self {
        let boundaries = vec![
            0.0,     // 0ms
            5.0,     // 5ms - excellent response
            10.0,    // 10ms - very good
            25.0,    // 25ms - good
            50.0,    // 50ms - acceptable
            75.0,    // 75ms
            100.0,   // 100ms - getting slow
            250.0,   // 250ms - slow
            500.0,   // 500ms - very slow
            750.0,   // 750ms
            1000.0,  // 1s - timeout territory
            2500.0,  // 2.5s - really slow
            5000.0,  // 5s - user likely gone
            7500.0,  // 7.5s
            10000.0, // 10s - definitely timeout
            f64::INFINITY,
        ];
        let buckets = (0..boundaries.len()).map(|_| AtomicU64::new(0)).collect();

        Self {
            buckets,
            bucket_boundaries: boundaries,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Create a histogram for measuring durations in seconds (Prometheus-style)
    ///
    /// Buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, +Inf]
    /// Use this when measuring Duration::as_secs_f64() or similar
    pub fn for_seconds() -> Self {
        let boundaries = vec![
            0.005, // 5ms
            0.01,  // 10ms
            0.025, // 25ms
            0.05,  // 50ms
            0.1,   // 100ms
            0.25,  // 250ms
            0.5,   // 500ms
            1.0,   // 1s
            2.5,   // 2.5s
            5.0,   // 5s
            10.0,  // 10s
            f64::INFINITY,
        ];
        let buckets = (0..boundaries.len()).map(|_| AtomicU64::new(0)).collect();

        Self {
            buckets,
            bucket_boundaries: boundaries,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Create a histogram with custom bucket boundaries
    pub fn with_buckets(boundaries: Vec<f64>) -> Self {
        let buckets = (0..boundaries.len()).map(|_| AtomicU64::new(0)).collect();

        Self {
            buckets,
            bucket_boundaries: boundaries,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Observe a value and place it in the appropriate bucket
    pub fn observe(&self, value: f64) {
        // Find the appropriate bucket
        for (i, &boundary) in self.bucket_boundaries.iter().enumerate() {
            if value <= boundary {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }

        // Update sum and count
        let value_fixed = (value * 1000.0) as u64;
        self.sum.fetch_add(value_fixed, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the count for a specific bucket
    pub fn bucket_count(&self, bucket_index: usize) -> u64 {
        if bucket_index < self.buckets.len() {
            self.buckets[bucket_index].load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// Get all bucket counts
    pub fn bucket_counts(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect()
    }

    /// Get bucket boundaries
    pub fn bucket_boundaries(&self) -> &[f64] {
        &self.bucket_boundaries
    }

    /// Get the sum of all observed values
    pub fn sum(&self) -> f64 {
        self.sum.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Get the total count of observations
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Reset all buckets and counters to zero
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.sum.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }

    /// Observe a time duration value
    ///
    /// This method accepts any TimeUnit and converts it to the appropriate
    /// representation for storage in the histogram buckets.
    pub fn observe_time(&self, time: TimeUnit) {
        // Convert to milliseconds for bucket matching (our default buckets are in ms)
        self.observe(time.as_millis());
    }

    /// Get the sum of all observed time values as a TimeUnit
    ///
    /// Returns the total time observed, preserving unit semantics.
    /// The sum is returned in milliseconds since that's our internal storage format.
    pub fn sum_time(&self) -> TimeUnit {
        TimeUnit::from_millis(self.sum() as u64)
    }

    /// Create a snapshot of the histogram for export
    pub fn snapshot(&self) -> crate::metrics::HistogramSnapshot {
        use crate::metrics::{HistogramSnapshot, Percentile};
        use std::collections::HashMap;

        let count = self.count();
        let sum = self.sum();

        // Calculate percentiles based on bucket counts
        let mut percentiles = HashMap::new();

        if count > 0 {
            let bucket_counts = self.bucket_counts();

            // Helper to find value at percentile
            let find_percentile = |target_count: u64| -> f64 {
                let mut cumulative = 0u64;
                for (i, &bucket_count) in bucket_counts.iter().enumerate() {
                    cumulative += bucket_count;
                    if cumulative >= target_count {
                        // Return the upper bound of this bucket
                        return self.bucket_boundaries[i];
                    }
                }
                self.bucket_boundaries.last().copied().unwrap_or(0.0)
            };

            // Calculate standard percentiles
            percentiles.insert(
                Percentile::P50,
                find_percentile((count as f64 * 0.5) as u64),
            );
            percentiles.insert(
                Percentile::P90,
                find_percentile((count as f64 * 0.9) as u64),
            );
            percentiles.insert(
                Percentile::P95,
                find_percentile((count as f64 * 0.95) as u64),
            );
            percentiles.insert(
                Percentile::P99,
                find_percentile((count as f64 * 0.99) as u64),
            );
        }

        HistogramSnapshot {
            count,
            sum,
            min: if count > 0 { 0.0 } else { f64::INFINITY }, // We don't track min
            max: if count > 0 {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            }, // We don't track max
            percentiles,
        }
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_basic_operations() {
        let histogram = Histogram::new();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.sum(), 0.0);

        histogram.observe(50.0); // 50ms
        assert_eq!(histogram.count(), 1);
        assert!((histogram.sum() - 50.0).abs() < 0.001);

        histogram.observe(150.0); // 150ms
        assert_eq!(histogram.count(), 2);
        assert!((histogram.sum() - 200.0).abs() < 0.001);

        histogram.reset();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.sum(), 0.0);
    }

    #[test]
    fn test_histogram_otel_style_buckets() {
        let histogram = Histogram::new();

        histogram.observe(3.0); // Should go in 5ms bucket (index 1)
        histogram.observe(15.0); // Should go in 25ms bucket (index 3)
        histogram.observe(80.0); // Should go in 100ms bucket (index 6)
        histogram.observe(1500.0); // Should go in 2500ms bucket (index 11)

        let counts = histogram.bucket_counts();
        assert_eq!(counts[1], 1); // 3ms in 5ms bucket
        assert_eq!(counts[3], 1); // 15ms in 25ms bucket
        assert_eq!(counts[6], 1); // 80ms in 100ms bucket
        assert_eq!(counts[11], 1); // 1500ms in 2500ms bucket
    }

    #[test]
    fn test_histogram_seconds_style_buckets() {
        let histogram = Histogram::for_seconds();

        histogram.observe(0.008); // 8ms - should go in 0.01s bucket (index 1)
        histogram.observe(0.03); // 30ms - should go in 0.05s bucket (index 3)
        histogram.observe(0.15); // 150ms - should go in 0.25s bucket (index 5)
        histogram.observe(3.0); // 3s - should go in 5s bucket (index 9)

        let counts = histogram.bucket_counts();
        assert_eq!(counts[1], 1); // 8ms in 10ms bucket
        assert_eq!(counts[3], 1); // 30ms in 50ms bucket
        assert_eq!(counts[5], 1); // 150ms in 250ms bucket
        assert_eq!(counts[9], 1); // 3s in 5s bucket
    }

    #[test]
    fn test_histogram_custom_buckets() {
        let boundaries = vec![10.0, 50.0, 100.0, f64::INFINITY];
        let histogram = Histogram::with_buckets(boundaries);

        histogram.observe(5.0); // Should go in 10ms bucket (index 0)
        histogram.observe(30.0); // Should go in 50ms bucket (index 1)
        histogram.observe(75.0); // Should go in 100ms bucket (index 2)
        histogram.observe(500.0); // Should go in +Inf bucket (index 3)

        let counts = histogram.bucket_counts();
        assert_eq!(counts[0], 1);
        assert_eq!(counts[1], 1);
        assert_eq!(counts[2], 1);
        assert_eq!(counts[3], 1);
    }
}
