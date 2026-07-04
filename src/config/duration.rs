use std::time::Duration;

use anyhow::{Context, Result, bail};

/// Parses a duration like `"45s"`, `"10m"`, `"2h"`, `"1d"`, or a compound
/// value like `"1h30m"` (each unit at most once, in descending order),
/// matching common duration notations (e.g. Go's `time.Duration` strings).
pub(crate) fn parse_duration(input: &str) -> Result<Duration> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("duration cannot be empty");
    }

    let mut total_seconds: u64 = 0;
    let mut rest = trimmed;
    let mut matched_any = false;
    while !rest.is_empty() {
        let digits_len = rest
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(rest.len());
        if digits_len == 0 {
            bail!("invalid duration '{trimmed}': expected a number before the unit");
        }
        let (number, remainder) = rest.split_at(digits_len);
        let mut unit_chars = remainder.chars();
        let Some(unit) = unit_chars.next() else {
            bail!("invalid duration '{trimmed}': missing unit (use s, m, h, or d)");
        };
        let value: u64 = number
            .parse()
            .with_context(|| format!("invalid duration '{trimmed}'"))?;
        let seconds_per_unit: u64 = match unit {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86400,
            other => {
                bail!("invalid duration '{trimmed}': unknown unit '{other}' (use s, m, h, or d)")
            }
        };
        total_seconds = total_seconds.saturating_add(value.saturating_mul(seconds_per_unit));
        matched_any = true;
        rest = unit_chars.as_str();
    }

    if !matched_any {
        bail!("invalid duration '{trimmed}'");
    }

    Ok(Duration::from_secs(total_seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_handles_single_and_compound_units() {
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("10m").unwrap(), Duration::from_secs(600));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86400));
        assert_eq!(
            parse_duration("1h30m").unwrap(),
            Duration::from_secs(3600 + 1800)
        );
    }

    #[test]
    fn parse_duration_rejects_malformed_input() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("m10").is_err());
        assert!(parse_duration("10x").is_err());
    }
}
