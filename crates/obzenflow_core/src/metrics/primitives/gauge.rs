use std::sync::atomic::{AtomicU64, Ordering};

/// Gauge for current values that can go up or down
///
/// Thread-safe atomic gauge that stores current state values.
/// Uses fixed-point representation (value * 1000) for precision.
#[derive(Debug)]
pub struct Gauge {
    value: AtomicU64, // Store as fixed-point * 1000 for precision
}

impl Gauge {
    /// Create a new gauge starting at zero
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    /// Set the gauge to a specific value
    pub fn set(&self, value: f64) {
        let fixed_point = (value * 1000.0) as u64;
        self.value.store(fixed_point, Ordering::Relaxed);
    }

    /// Get the current gauge value
    pub fn get(&self) -> f64 {
        self.value.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Reset the gauge to zero
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gauge_basic_operations() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0.0);

        gauge.set(0.5);
        assert!((gauge.get() - 0.5).abs() < 0.001);

        gauge.reset();
        assert_eq!(gauge.get(), 0.0);
    }

    #[test]
    fn test_gauge_precision() {
        let gauge = Gauge::new();

        // Test precision with fixed-point representation
        gauge.set(0.123);
        assert!((gauge.get() - 0.123).abs() < 0.001);

        gauge.set(99.999);
        assert!((gauge.get() - 99.999).abs() < 0.001);
    }
}

