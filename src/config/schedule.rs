use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, Local, Timelike};
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone)]
pub struct Schedule {
    pub seconds: Vec<String>,
    pub minutes: Vec<String>,
    pub hours: Vec<String>,
    pub days_of_week: Vec<String>,
    pub days_of_month: Vec<String>,
    pub months: Vec<String>,
}

impl<'de> Deserialize<'de> for Schedule {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let expression = String::deserialize(deserializer)?;
        Self::from_cron(&expression).map_err(D::Error::custom)
    }
}

impl Schedule {
    fn from_cron(expression: &str) -> Result<Self> {
        let fields = expression.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 6 {
            bail!(
                "schedule must have 6 cron fields: second minute hour day-of-month month day-of-week"
            );
        }
        let schedule = Self {
            seconds: vec![fields[0].to_string()],
            minutes: vec![fields[1].to_string()],
            hours: vec![fields[2].to_string()],
            days_of_month: vec![fields[3].to_string()],
            months: vec![fields[4].to_string()],
            days_of_week: vec![fields[5].to_string()],
        };
        schedule.validate()?;
        Ok(schedule)
    }

    pub fn validate(&self) -> Result<()> {
        require_field(&self.seconds, "seconds")?;
        require_field(&self.minutes, "minutes")?;
        require_field(&self.hours, "hours")?;
        parse_field("seconds", &self.seconds, 0, 59, &[])?;
        parse_field("minutes", &self.minutes, 0, 59, &[])?;
        parse_field("hours", &self.hours, 0, 23, &[])?;
        parse_field("days_of_week", &self.days_of_week, 1, 7, &weekday_aliases())?;
        parse_field("days_of_month", &self.days_of_month, 1, 31, &[])?;
        parse_field("months", &self.months, 1, 12, &month_aliases())?;
        Ok(())
    }

    pub fn matches(&self, time: DateTime<Local>) -> bool {
        let compiled = CompiledSchedule::compile(self);
        let second = time.second();
        let minute = time.minute();
        let hour = time.hour();
        let day_of_month = time.day();
        let month = time.month();
        let day_of_week = time.weekday().number_from_monday();

        CompiledSchedule::contains(&compiled.seconds, second)
            && CompiledSchedule::contains(&compiled.minutes, minute)
            && CompiledSchedule::contains(&compiled.hours, hour)
            && compiled.day_matches(month, day_of_month, day_of_week)
    }

    /// Finds the next `count` run times at or after `after`.
    ///
    /// Searches day-by-day (bounded to 5 years out) rather than second-by-second,
    /// so a schedule that can never fire (e.g. day 31 restricted to February)
    /// returns quickly instead of scanning ~150 million seconds.
    pub fn next_runs(&self, after: DateTime<Local>, count: usize) -> Vec<DateTime<Local>> {
        if count == 0 {
            return Vec::new();
        }

        let compiled = CompiledSchedule::compile(self);
        if compiled.seconds.is_empty()
            || compiled.minutes.is_empty()
            || compiled.hours.is_empty()
            || compiled.days_of_week.is_empty()
            || compiled.days_of_month.is_empty()
            || compiled.months.is_empty()
        {
            return Vec::new();
        }

        let start = after.with_nanosecond(0).unwrap_or(after) + chrono::Duration::seconds(1);
        let mut date = start.date_naive();
        let last_date = date + chrono::Duration::days(366 * 5);
        let mut runs = Vec::new();

        while date <= last_date {
            if compiled.day_matches(
                date.month(),
                date.day(),
                date.weekday().number_from_monday(),
            ) {
                'times: for &hour in &compiled.hours {
                    for &minute in &compiled.minutes {
                        for &second in &compiled.seconds {
                            let Some(time) = chrono::NaiveTime::from_hms_opt(hour, minute, second)
                            else {
                                continue;
                            };
                            let Some(candidate) =
                                date.and_time(time).and_local_timezone(Local).single()
                            else {
                                continue;
                            };
                            if candidate < start {
                                continue;
                            }
                            runs.push(candidate);
                            if runs.len() == count {
                                break 'times;
                            }
                        }
                    }
                }
                if runs.len() == count {
                    break;
                }
            }
            date += chrono::Duration::days(1);
        }

        runs
    }
}

/// Pre-parsed schedule fields as sorted value lists, computed once per
/// `matches`/`next_runs` call instead of re-parsing the schedule strings on
/// every second-level comparison.
struct CompiledSchedule {
    seconds: Vec<u32>,
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days_of_week: Vec<u32>,
    days_of_month: Vec<u32>,
    months: Vec<u32>,
    dom_restricted: bool,
    dow_restricted: bool,
}

impl CompiledSchedule {
    fn compile(schedule: &Schedule) -> Self {
        let days_of_month = compile_field(&schedule.days_of_month, 1, 31, &[]);
        let days_of_week = compile_field(&schedule.days_of_week, 1, 7, &weekday_aliases());
        Self {
            seconds: compile_field(&schedule.seconds, 0, 59, &[]),
            minutes: compile_field(&schedule.minutes, 0, 59, &[]),
            hours: compile_field(&schedule.hours, 0, 23, &[]),
            months: compile_field(&schedule.months, 1, 12, &month_aliases()),
            dom_restricted: days_of_month.len() < 31,
            dow_restricted: days_of_week.len() < 7,
            days_of_month,
            days_of_week,
        }
    }

    fn contains(list: &[u32], value: u32) -> bool {
        list.binary_search(&value).is_ok()
    }

    /// Standard cron semantics: if both day-of-month and day-of-week are
    /// restricted (not `*`/full-range), a day matches if *either* is
    /// satisfied. Otherwise the single restricted field (or neither) applies.
    fn day_matches(&self, month: u32, day_of_month: u32, day_of_week: u32) -> bool {
        if !Self::contains(&self.months, month) {
            return false;
        }

        let dom_matches = Self::contains(&self.days_of_month, day_of_month);
        let dow_matches = Self::contains(&self.days_of_week, day_of_week);

        if self.dom_restricted && self.dow_restricted {
            dom_matches || dow_matches
        } else {
            dom_matches && dow_matches
        }
    }
}

fn compile_field(values: &[String], min: u32, max: u32, aliases: &[(&str, u32)]) -> Vec<u32> {
    let mut set = std::collections::BTreeSet::new();
    for value in values {
        for part in value.split(',') {
            if let Ok(expanded) = expand_part(part.trim(), min, max, aliases) {
                set.extend(expanded);
            }
        }
    }
    set.into_iter().collect()
}

fn parse_field(
    name: &str,
    values: &[String],
    min: u32,
    max: u32,
    aliases: &[(&str, u32)],
) -> Result<()> {
    for value in values {
        for part in value.split(',') {
            expand_part(part.trim(), min, max, aliases)
                .with_context(|| format!("invalid schedule.{name} value '{part}'"))?;
        }
    }
    Ok(())
}

fn require_field(values: &[String], name: &str) -> Result<()> {
    if values.is_empty() {
        bail!("schedule.{name} is required for non-manual jobs");
    }
    Ok(())
}

fn expand_part(part: &str, min: u32, max: u32, aliases: &[(&str, u32)]) -> Result<Vec<u32>> {
    if part.is_empty() {
        bail!("empty schedule field");
    }

    let (base, step) = if let Some((base, step)) = part.split_once('/') {
        let step = step
            .parse::<u32>()
            .with_context(|| format!("invalid step in '{part}'"))?;
        if step == 0 {
            bail!("step cannot be zero in '{part}'");
        }
        (base, step)
    } else {
        (part, 1)
    };

    let (start, end) = if base == "*" {
        (min, max)
    } else if let Some((start, end)) = base.split_once('-') {
        (parse_atom(start, aliases)?, parse_atom(end, aliases)?)
    } else {
        let exact = parse_atom(base, aliases)?;
        (exact, exact)
    };

    if start < min || end > max || start > end {
        bail!("schedule value '{part}' is outside {min}-{max}");
    }

    Ok((start..=end)
        .filter(|value| (value - start) % step == 0)
        .collect())
}

fn parse_atom(value: &str, aliases: &[(&str, u32)]) -> Result<u32> {
    let normalized = value.to_ascii_lowercase();
    if let Some((_, mapped)) = aliases.iter().find(|(alias, _)| *alias == normalized) {
        return Ok(*mapped);
    }
    value
        .parse::<u32>()
        .with_context(|| format!("invalid schedule value '{value}'"))
}

fn weekday_aliases() -> [(&'static str, u32); 7] {
    [
        ("mon", 1),
        ("tue", 2),
        ("wed", 3),
        ("thu", 4),
        ("fri", 5),
        ("sat", 6),
        ("sun", 7),
    ]
}

fn month_aliases() -> [(&'static str, u32); 12] {
    [
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("may", 5),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn schedule_matches_aliases_ranges_and_steps() {
        let schedule = Schedule {
            seconds: vec!["0,30".to_string()],
            minutes: vec!["*/15".to_string()],
            hours: vec!["9-17".to_string()],
            days_of_week: vec!["mon-fri".to_string()],
            days_of_month: vec!["*".to_string()],
            months: vec!["jan,Jul".to_string()],
        };

        let hit = Local.with_ymd_and_hms(2026, 7, 3, 9, 30, 30).unwrap();
        let miss = Local.with_ymd_and_hms(2026, 7, 4, 9, 30, 30).unwrap();

        assert!(schedule.matches(hit));
        assert!(!schedule.matches(miss));
        assert!(!schedule.matches(Local.with_ymd_and_hms(2026, 7, 3, 9, 30, 5).unwrap()));
    }

    #[test]
    fn schedule_rejects_out_of_range_values() {
        let schedule = Schedule {
            seconds: vec!["*".to_string()],
            minutes: vec!["60".to_string()],
            hours: vec!["*".to_string()],
            days_of_week: vec!["*".to_string()],
            days_of_month: vec!["*".to_string()],
            months: vec!["*".to_string()],
        };

        assert!(schedule.validate().is_err());
    }

    #[test]
    fn schedule_rejects_out_of_range_seconds() {
        let schedule = Schedule {
            seconds: vec!["60".to_string()],
            minutes: vec!["*".to_string()],
            hours: vec!["*".to_string()],
            days_of_week: vec!["*".to_string()],
            days_of_month: vec!["*".to_string()],
            months: vec!["*".to_string()],
        };

        assert!(schedule.validate().is_err());
    }

    #[test]
    fn schedule_requires_six_cron_fields() {
        let config = serde_yaml::from_str::<crate::config::SundialdConfig>(
            r#"
jobs:
  - name: every-minute
    command: "true"
    trigger:
      schedule: "* * *"
"#,
        );

        assert!(config.is_err());
    }

    #[test]
    fn schedule_accepts_six_field_cron_expression() {
        let config: crate::config::SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: every-minute
    command: "true"
    trigger:
      schedule: "0 * * * * *"
"#,
        )
        .unwrap();
        let schedule = config.jobs[0].trigger.schedule().unwrap();

        assert!(schedule.validate().is_ok());
        assert!(schedule.matches(Local.with_ymd_and_hms(2026, 7, 3, 9, 30, 0).unwrap()));
        assert!(!schedule.matches(Local.with_ymd_and_hms(2026, 7, 3, 9, 30, 1).unwrap()));
    }

    #[test]
    fn next_runs_handles_seconds() {
        let schedule = Schedule {
            seconds: vec!["*/15".to_string()],
            minutes: vec!["*".to_string()],
            hours: vec!["*".to_string()],
            days_of_week: vec!["sat".to_string()],
            days_of_month: vec!["*".to_string()],
            months: vec!["jul".to_string()],
        };
        let after = Local.with_ymd_and_hms(2026, 7, 4, 13, 36, 44).unwrap();

        let runs = schedule.next_runs(after, 3);

        assert_eq!(
            runs.iter()
                .map(|run| run.format("%H:%M:%S").to_string())
                .collect::<Vec<_>>(),
            vec!["13:36:45", "13:37:00", "13:37:15"]
        );
    }

    #[test]
    fn next_runs_returns_empty_quickly_for_an_impossible_schedule() {
        // Day 31 restricted to February can never occur; this must not hang
        // brute-forcing seconds for years (regression test for the old
        // second-by-second scan over ~150 million iterations).
        let schedule = Schedule {
            seconds: vec!["0".to_string()],
            minutes: vec!["0".to_string()],
            hours: vec!["0".to_string()],
            days_of_week: vec!["*".to_string()],
            days_of_month: vec!["31".to_string()],
            months: vec!["feb".to_string()],
        };
        let now = Local.with_ymd_and_hms(2026, 7, 4, 12, 0, 0).unwrap();

        assert!(schedule.next_runs(now, 3).is_empty());
    }

    #[test]
    fn schedule_ors_day_of_month_and_day_of_week_when_both_restricted() {
        // Standard cron semantics: when both fields are restricted, a day
        // matches if *either* is satisfied (union), not both (intersection).
        let schedule = Schedule {
            seconds: vec!["0".to_string()],
            minutes: vec!["0".to_string()],
            hours: vec!["0".to_string()],
            days_of_week: vec!["mon".to_string()],
            days_of_month: vec!["1".to_string()],
            months: vec!["*".to_string()],
        };

        // 2026-07-01 is a Wednesday but is the 1st of the month.
        let first_but_not_monday = Local.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        assert!(schedule.matches(first_but_not_monday));

        // 2026-07-06 is a Monday but not the 1st.
        let monday_but_not_first = Local.with_ymd_and_hms(2026, 7, 6, 0, 0, 0).unwrap();
        assert!(schedule.matches(monday_but_not_first));

        // 2026-07-07 is neither the 1st nor a Monday.
        let neither = Local.with_ymd_and_hms(2026, 7, 7, 0, 0, 0).unwrap();
        assert!(!schedule.matches(neither));
    }
}
