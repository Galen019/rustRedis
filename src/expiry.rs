//! Relative expiry arithmetic for the command layer.
//!
//! - units are seconds or milliseconds from a caller-supplied `now`
//! - integers follow Redis's strict `string2ll` rules
//! - no replies, no store access

use std::time::{Duration, Instant};

use crate::store::Ttl;

/// Expiry unit for `EXPIRE`/`PEXPIRE`, `TTL`/`PTTL`, and `SET ... EX/PX`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeUnit {
    Seconds,
    Millis,
}

/// Parses an ASCII integer the way Redis's `string2ll` does.
///
/// - optional leading `-`, no `+`, whitespace, or leading zeros
/// - rejects `-0` and values that do not fit `i64`
pub(crate) fn parse_int(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes.split_first() {
        Some((&b'-', digits)) => (true, digits),
        Some(_) => (false, bytes),
        None => return None,
    };

    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if digits == b"0" {
        return (!negative).then_some(0);
    }
    if !(b'1'..=b'9').contains(&digits[0]) {
        return None;
    }

    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// Converts an expiry value to a duration.
///
/// - `Ok(None)` for a non-positive value
/// - `Err(())` when seconds overflow the millisecond range Redis enforces
pub(crate) fn duration(unit: TimeUnit, value: i64) -> Result<Option<Duration>, ()> {
    if unit == TimeUnit::Seconds && !(i64::MIN / 1000..=i64::MAX / 1000).contains(&value) {
        return Err(());
    }
    Ok((value > 0).then(|| match unit {
        TimeUnit::Seconds => Duration::from_secs(value as u64),
        TimeUnit::Millis => Duration::from_millis(value as u64),
    }))
}

/// Resolves an expiry value to a deadline from `now`.
///
/// - `Ok(None)` for a non-positive value, which `EXPIRE` treats as "delete now"
/// - `Err(())` for an unrepresentable value or a `now + duration` overflow
pub(crate) fn deadline(unit: TimeUnit, value: i64, now: Instant) -> Result<Option<Instant>, ()> {
    match duration(unit, value)? {
        Some(duration) => now.checked_add(duration).map(Some).ok_or(()),
        None => Ok(None),
    }
}

/// Formats a store TTL as the integer Redis replies with.
///
/// - `-2` missing/expired, `-1` persistent
/// - seconds round to the nearest second; milliseconds truncate
pub(crate) fn ttl_value(ttl: Ttl, unit: TimeUnit) -> i64 {
    match ttl {
        Ttl::Missing => -2,
        Ttl::Persistent => -1,
        Ttl::Expiring(remaining) => {
            let millis = remaining.as_millis().min(i64::MAX as u128) as i64;
            match unit {
                TimeUnit::Millis => millis,
                TimeUnit::Seconds => millis.saturating_add(500) / 1000,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_parsing_matches_string2ll() {
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"10"), Some(10));
        assert_eq!(parse_int(b"-10"), Some(-10));
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));

        for raw in [
            b"".as_slice(),
            b"+1",
            b"01",
            b"-0",
            b"00",
            b" 1",
            b"1 ",
            b"1.0",
            b"9223372036854775808",
        ] {
            assert_eq!(parse_int(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn non_positive_values_have_no_duration() {
        for unit in [TimeUnit::Seconds, TimeUnit::Millis] {
            assert_eq!(duration(unit, 0), Ok(None));
            assert_eq!(duration(unit, -1), Ok(None));
        }
    }

    #[test]
    fn seconds_that_overflow_milliseconds_are_rejected() {
        assert_eq!(duration(TimeUnit::Seconds, i64::MAX), Err(()));
        assert_eq!(duration(TimeUnit::Seconds, i64::MIN), Err(()));
        assert_eq!(
            duration(TimeUnit::Seconds, i64::MAX / 1000),
            Ok(Some(Duration::from_secs((i64::MAX / 1000) as u64)))
        );
    }

    #[test]
    fn deadline_is_relative_to_now() {
        let now = Instant::now();
        assert_eq!(
            deadline(TimeUnit::Seconds, 10, now),
            Ok(Some(now + Duration::from_secs(10)))
        );
        assert_eq!(deadline(TimeUnit::Millis, 0, now), Ok(None));
    }

    #[test]
    fn seconds_round_to_nearest_and_millis_truncate() {
        for (millis, ttl) in [(1499, 1), (1500, 2), (499, 0)] {
            let remaining = Ttl::Expiring(Duration::from_millis(millis));
            assert_eq!(ttl_value(remaining, TimeUnit::Seconds), ttl);
        }
        assert_eq!(
            ttl_value(Ttl::Expiring(Duration::from_millis(1499)), TimeUnit::Millis),
            1499
        );
        assert_eq!(ttl_value(Ttl::Missing, TimeUnit::Millis), -2);
        assert_eq!(ttl_value(Ttl::Persistent, TimeUnit::Millis), -1);
    }
}
