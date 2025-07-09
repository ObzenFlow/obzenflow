//! Time unit type for primitives layer
//!
//! Provides a lightweight, unit-aware time representation without depending on std::time::Duration.
//! This maintains proper layer separation in the FLOWIP-004 architecture.

/// Lightweight time unit representation for primitives
///
/// This type allows primitives to be unit-aware without depending on higher-level types
/// like std::time::Duration. Each variant stores the value in its native unit for precision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeUnit {
    /// Nanoseconds (highest precision, good for very fast operations)
    Nanoseconds(u64),
    /// Microseconds (high precision, good for system calls, small computations)
    Microseconds(u64),
    /// Milliseconds (standard precision, good for API calls, user operations)
    Milliseconds(u64),
    /// Seconds (lower precision, good for long-running operations)
    Seconds(f64),
}

impl TimeUnit {
    /// Convert any time unit to seconds as f64
    ///
    /// This is useful for histogram buckets and mathematical operations
    /// where floating-point seconds are the standard representation.
    pub fn as_seconds(&self) -> f64 {
        match self {
            TimeUnit::Nanoseconds(ns) => *ns as f64 / 1_000_000_000.0,
            TimeUnit::Microseconds(us) => *us as f64 / 1_000_000.0,
            TimeUnit::Milliseconds(ms) => *ms as f64 / 1_000.0,
            TimeUnit::Seconds(s) => *s,
        }
    }

    /// Convert any time unit to milliseconds as f64
    ///
    /// This is useful for histogram storage where milliseconds provide
    /// a good balance of precision and reasonable bucket ranges.
    pub fn as_millis(&self) -> f64 {
        match self {
            TimeUnit::Nanoseconds(ns) => *ns as f64 / 1_000_000.0,
            TimeUnit::Microseconds(us) => *us as f64 / 1_000.0,
            TimeUnit::Milliseconds(ms) => *ms as f64,
            TimeUnit::Seconds(s) => *s * 1_000.0,
        }
    }

    /// Convert any time unit to microseconds as u64
    ///
    /// This is useful for high-precision timing and atomic storage
    /// where integer microseconds provide good precision without floating-point.
    pub fn as_micros(&self) -> u64 {
        match self {
            TimeUnit::Nanoseconds(ns) => ns / 1_000,
            TimeUnit::Microseconds(us) => *us,
            TimeUnit::Milliseconds(ms) => ms * 1_000,
            TimeUnit::Seconds(s) => (*s * 1_000_000.0) as u64,
        }
    }

    /// Convert any time unit to nanoseconds as u64
    ///
    /// This provides the highest precision representation,
    /// useful for very precise timing measurements.
    pub fn as_nanos(&self) -> u64 {
        match self {
            TimeUnit::Nanoseconds(ns) => *ns,
            TimeUnit::Microseconds(us) => us * 1_000,
            TimeUnit::Milliseconds(ms) => ms * 1_000_000,
            TimeUnit::Seconds(s) => (*s * 1_000_000_000.0) as u64,
        }
    }

    /// Create a TimeUnit from nanoseconds
    pub fn from_nanos(nanos: u64) -> Self {
        TimeUnit::Nanoseconds(nanos)
    }

    /// Create a TimeUnit from microseconds
    pub fn from_micros(micros: u64) -> Self {
        TimeUnit::Microseconds(micros)
    }

    /// Create a TimeUnit from milliseconds
    pub fn from_millis(millis: u64) -> Self {
        TimeUnit::Milliseconds(millis)
    }

    /// Create a TimeUnit from seconds
    pub fn from_secs(secs: f64) -> Self {
        TimeUnit::Seconds(secs)
    }

    /// Check if this time unit represents zero time
    pub fn is_zero(&self) -> bool {
        match self {
            TimeUnit::Nanoseconds(ns) => *ns == 0,
            TimeUnit::Microseconds(us) => *us == 0,
            TimeUnit::Milliseconds(ms) => *ms == 0,
            TimeUnit::Seconds(s) => *s == 0.0,
        }
    }
}

/// Convert from std::time::Duration to TimeUnit
///
/// This conversion function allows metrics layer to convert from Duration
/// to TimeUnit when calling primitive operations, maintaining layer separation.
impl From<std::time::Duration> for TimeUnit {
    fn from(duration: std::time::Duration) -> Self {
        // Choose the most appropriate representation based on duration magnitude
        let nanos = duration.as_nanos();
        
        if nanos == 0 {
            TimeUnit::Nanoseconds(0)
        } else if nanos < 1_000_000 {
            // Less than 1ms - use nanoseconds for precision
            TimeUnit::Nanoseconds(nanos as u64)
        } else if nanos < 1_000_000_000 {
            // Less than 1s - use microseconds for good precision
            TimeUnit::Microseconds(duration.as_micros() as u64)
        } else if nanos < 60_000_000_000 {
            // Less than 1 minute - use milliseconds
            TimeUnit::Milliseconds(duration.as_millis() as u64)
        } else {
            // 1 minute or more - use seconds with fractional precision
            TimeUnit::Seconds(duration.as_secs_f64())
        }
    }
}

/// Convert from TimeUnit to std::time::Duration
///
/// This allows metrics layer to convert back to Duration when needed
/// for external APIs or timer operations.
impl From<TimeUnit> for std::time::Duration {
    fn from(time_unit: TimeUnit) -> Self {
        match time_unit {
            TimeUnit::Nanoseconds(ns) => std::time::Duration::from_nanos(ns),
            TimeUnit::Microseconds(us) => std::time::Duration::from_micros(us),
            TimeUnit::Milliseconds(ms) => std::time::Duration::from_millis(ms),
            TimeUnit::Seconds(s) => std::time::Duration::from_secs_f64(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_time_unit_conversions() {
        // Test nanoseconds
        let ns = TimeUnit::Nanoseconds(1_500_000_000); // 1.5 seconds
        assert_eq!(ns.as_seconds(), 1.5);
        assert_eq!(ns.as_millis(), 1500.0);
        assert_eq!(ns.as_micros(), 1_500_000);
        assert_eq!(ns.as_nanos(), 1_500_000_000);

        // Test microseconds  
        let us = TimeUnit::Microseconds(2_500_000); // 2.5 seconds
        assert_eq!(us.as_seconds(), 2.5);
        assert_eq!(us.as_millis(), 2500.0);
        assert_eq!(us.as_micros(), 2_500_000);
        assert_eq!(us.as_nanos(), 2_500_000_000);

        // Test milliseconds
        let ms = TimeUnit::Milliseconds(3500); // 3.5 seconds
        assert_eq!(ms.as_seconds(), 3.5);
        assert_eq!(ms.as_millis(), 3500.0);
        assert_eq!(ms.as_micros(), 3_500_000);
        assert_eq!(ms.as_nanos(), 3_500_000_000);

        // Test seconds
        let s = TimeUnit::Seconds(4.5);
        assert_eq!(s.as_seconds(), 4.5);
        assert_eq!(s.as_millis(), 4500.0);
        assert_eq!(s.as_micros(), 4_500_000);
        assert_eq!(s.as_nanos(), 4_500_000_000);
    }

    #[test]
    fn test_time_unit_creation() {
        assert_eq!(TimeUnit::from_nanos(1000), TimeUnit::Nanoseconds(1000));
        assert_eq!(TimeUnit::from_micros(1000), TimeUnit::Microseconds(1000));
        assert_eq!(TimeUnit::from_millis(1000), TimeUnit::Milliseconds(1000));
        assert_eq!(TimeUnit::from_secs(1.5), TimeUnit::Seconds(1.5));
    }

    #[test]
    fn test_is_zero() {
        assert!(TimeUnit::Nanoseconds(0).is_zero());
        assert!(TimeUnit::Microseconds(0).is_zero());
        assert!(TimeUnit::Milliseconds(0).is_zero());
        assert!(TimeUnit::Seconds(0.0).is_zero());

        assert!(!TimeUnit::Nanoseconds(1).is_zero());
        assert!(!TimeUnit::Microseconds(1).is_zero());
        assert!(!TimeUnit::Milliseconds(1).is_zero());
        assert!(!TimeUnit::Seconds(0.1).is_zero());
    }

    #[test]
    fn test_duration_to_time_unit_conversion() {
        // Test automatic unit selection based on magnitude
        
        // Very short duration -> nanoseconds
        let short_duration = Duration::from_nanos(500_000); // 500μs
        let time_unit: TimeUnit = short_duration.into();
        assert_eq!(time_unit, TimeUnit::Nanoseconds(500_000));

        // Medium duration -> microseconds  
        let medium_duration = Duration::from_millis(50); // 50ms
        let time_unit: TimeUnit = medium_duration.into();
        assert_eq!(time_unit, TimeUnit::Microseconds(50_000));

        // Long duration -> milliseconds
        let long_duration = Duration::from_secs(5); // 5s
        let time_unit: TimeUnit = long_duration.into();
        assert_eq!(time_unit, TimeUnit::Milliseconds(5000));

        // Very long duration -> seconds
        let very_long_duration = Duration::from_secs(120); // 2 minutes
        let time_unit: TimeUnit = very_long_duration.into();
        assert_eq!(time_unit, TimeUnit::Seconds(120.0));
    }

    #[test]
    fn test_time_unit_to_duration_conversion() {
        // Test round-trip conversions
        let original_duration = Duration::from_millis(1500);
        let time_unit: TimeUnit = original_duration.into();
        let converted_duration: Duration = time_unit.into();
        
        // Should be very close (within nanosecond precision)
        assert!((original_duration.as_nanos() as i64 - converted_duration.as_nanos() as i64).abs() < 1000);
    }

    #[test]
    fn test_precision_preservation() {
        // Test that we don't lose precision in common use cases
        let precise_duration = Duration::from_nanos(1_234_567_890); // ~1.23 seconds
        let time_unit: TimeUnit = precise_duration.into();
        
        // This duration is > 1 second, so it should be represented as milliseconds
        match time_unit {
            TimeUnit::Milliseconds(ms) => {
                // Should preserve millisecond precision
                assert_eq!(ms, 1_234);
            }
            _ => panic!("Expected milliseconds for this duration, got {:?}", time_unit),
        }
    }

    #[test]
    fn test_edge_cases() {
        // Zero duration
        let zero = Duration::ZERO;
        let time_unit: TimeUnit = zero.into();
        assert!(time_unit.is_zero());

        // Maximum values (within reason)
        let max_nanos = TimeUnit::Nanoseconds(u64::MAX);
        assert!(max_nanos.as_seconds() > 0.0);
        
        let max_micros = TimeUnit::Microseconds(u64::MAX);
        assert!(max_micros.as_seconds() > 0.0);
    }
}