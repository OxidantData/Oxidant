//! A five-field cron expression and the "when does this next fire" question.
//!
//! `reconcile --cron '0 6 * * *'` needs two answers and no more: is this expression well formed,
//! and given the last time it ran, is it due now? That is small enough to own rather than to take
//! a dependency for, and owning it keeps the error messages in the same voice as the rest of the
//! CLI — a cron expression is something an operator types by hand at 2 a.m., and
//! `"minute": expected 0-59, got 61` is worth more than a parse failure.
//!
//! The dialect is the standard one: `minute hour day-of-month month day-of-week`, each field a
//! comma-separated list of `*`, `N`, `A-B`, or any of those with a `/step` suffix. Day-of-week is
//! `0-6` with `0` = Sunday (`7` is also accepted for Sunday, as crontab does). Names (`MON`,
//! `JAN`) are accepted case-insensitively.
//!
//! The one rule that surprises people is inherited deliberately from Vixie cron: when **both**
//! day-of-month and day-of-week are restricted, a day matches if **either** does — `0 0 1 * MON`
//! is "the first of the month *and* every Monday", not their intersection. Implementing the
//! intuitive-looking intersection instead would silently skip runs an operator scheduled.
//!
//! Everything is evaluated in **UTC**. A schedule that drifted an hour twice a year against the
//! host's local time would be a genuinely confusing thing to debug from a drift report.

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use oxidant_common::{Error, Result};

/// How far ahead [`Cron::next_after`] will look before giving up.
///
/// Four years rather than one: `0 0 29 2 *` is a legal expression that fires on a leap day, and a
/// one-year horizon would call it unschedulable.
const HORIZON_DAYS: i64 = 366 * 4;

/// A parsed cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    /// The expression as written, kept so a schedule file round-trips what the operator typed.
    expr: String,
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days_of_month: Vec<u32>,
    months: Vec<u32>,
    days_of_week: Vec<u32>,
    /// True when the field was something other than `*` — see the Vixie rule above.
    dom_restricted: bool,
    dow_restricted: bool,
}

impl Cron {
    /// Parse `minute hour day-of-month month day-of-week`.
    pub fn parse(expr: &str) -> Result<Self> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(Error::Io(format!(
                "`{expr}` is not a cron expression: expected 5 fields (minute hour day-of-month \
                 month day-of-week), got {}",
                fields.len()
            )));
        }
        let minutes = field(fields[0], "minute", 0, 59, &[])?;
        let hours = field(fields[1], "hour", 0, 23, &[])?;
        let days_of_month = field(fields[2], "day-of-month", 1, 31, &[])?;
        let months = field(fields[3], "month", 1, 12, MONTH_NAMES)?;
        // 7 is accepted and folded onto 0: `SUN` is spelled both ways in the wild.
        let mut days_of_week = field(fields[4], "day-of-week", 0, 7, DAY_NAMES)?;
        days_of_week = days_of_week.into_iter().map(|d| d % 7).collect();
        days_of_week.sort_unstable();
        days_of_week.dedup();

        Ok(Self {
            expr: fields.join(" "),
            minutes,
            hours,
            days_of_month,
            months,
            days_of_week,
            dom_restricted: fields[2].trim() != "*",
            dow_restricted: fields[4].trim() != "*",
        })
    }

    /// The expression, normalized to single spaces.
    pub fn expr(&self) -> &str {
        &self.expr
    }

    /// The first firing strictly after `after`, or `None` when nothing fires within four years.
    ///
    /// Strictly after, not at or after: the caller's `after` is the last time this schedule ran,
    /// and a firing at exactly that instant is the one that already happened.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Seconds and below are not part of the dialect, so the search walks whole minutes.
        let start = after.with_second(0)?.with_nanosecond(0)? + Duration::minutes(1);
        let last = after + Duration::days(HORIZON_DAYS);
        let mut day = start.date_naive();
        while day <= last.date_naive() {
            if self.matches_day(day) {
                for hour in &self.hours {
                    for minute in &self.minutes {
                        let at = Utc.from_utc_datetime(&day.and_hms_opt(*hour, *minute, 0)?);
                        if at >= start {
                            return Some(at);
                        }
                    }
                }
            }
            day = day.succ_opt()?;
        }
        None
    }

    /// Whether a run anchored at `since` is due by `now`.
    ///
    /// `since` is the last run, or — before there has been one — when the schedule was
    /// registered. Anchoring on registration rather than firing immediately is what makes
    /// `--cron '0 6 * * *'` mean 6 a.m. instead of "6 a.m., and also right now".
    pub fn is_due(&self, since: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        self.next_after(since).is_some_and(|next| next <= now)
    }

    fn matches_day(&self, day: chrono::NaiveDate) -> bool {
        if !self.months.contains(&day.month()) {
            return false;
        }
        let dom = self.days_of_month.contains(&day.day());
        let dow = self
            .days_of_week
            .contains(&day.weekday().num_days_from_sunday());
        // Vixie's rule: restricted on both sides means union, not intersection.
        if self.dom_restricted && self.dow_restricted {
            dom || dow
        } else {
            dom && dow
        }
    }
}

const MONTH_NAMES: &[&str] = &[
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAY_NAMES: &[&str] = &["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// Expand one field into the sorted, deduplicated set of values it matches.
fn field(text: &str, name: &str, min: u32, max: u32, names: &[&str]) -> Result<Vec<u32>> {
    let text = text.trim();
    if text.is_empty() {
        return Err(Error::Io(format!("cron `{name}` field is empty")));
    }
    let mut out: Vec<u32> = Vec::new();
    for item in text.split(',') {
        let item = item.trim();
        let (range, step) = match item.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step.trim().parse().ok().filter(|s| *s > 0).ok_or_else(|| {
                    Error::Io(format!(
                        "cron `{name}` field: `{item}` has step `{step}`, which must be a \
                         positive number"
                    ))
                })?;
                (range.trim(), step)
            }
            None => (item, 1),
        };
        // `*/15` and `5-30/5` both mean "every step-th value of the range"; a bare `*` is the
        // whole range with step 1.
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((lo, hi)) = range.split_once('-') {
            (
                value(lo, name, min, max, names)?,
                value(hi, name, min, max, names)?,
            )
        } else {
            let single = value(range, name, min, max, names)?;
            // `7/2` with no `-` end is "from 7 to the top of the range", as crontab reads it.
            if step > 1 {
                (single, max)
            } else {
                (single, single)
            }
        };
        if lo > hi {
            return Err(Error::Io(format!(
                "cron `{name}` field: range `{range}` runs backwards"
            )));
        }
        let mut at = lo;
        while at <= hi {
            out.push(at);
            at += step;
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// One number or name in a field.
fn value(text: &str, name: &str, min: u32, max: u32, names: &[&str]) -> Result<u32> {
    let text = text.trim();
    let parsed = match text.parse::<u32>() {
        Ok(number) => number,
        Err(_) => {
            let lower = text.to_ascii_lowercase();
            let index = names
                .iter()
                .position(|n| *n == lower)
                .ok_or_else(|| bad_value(text, name, min, max))? as u32;
            // Month names start at 1, day names at 0 — the field's own `min` says which.
            index + min
        }
    };
    if parsed < min || parsed > max {
        return Err(bad_value(text, name, min, max));
    }
    Ok(parsed)
}

fn bad_value(text: &str, name: &str, min: u32, max: u32) -> Error {
    Error::Io(format!(
        "cron `{name}` field: expected {min}-{max}, got `{text}`"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("a test timestamp")
            .with_timezone(&Utc)
    }

    fn next(expr: &str, from: &str) -> String {
        Cron::parse(expr)
            .expect("parses")
            .next_after(at(from))
            .expect("fires within the horizon")
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    #[test]
    fn a_daily_schedule_fires_at_its_hour_the_next_day_once_it_has_passed() {
        assert_eq!(
            next("0 6 * * *", "2026-08-23T05:00:00Z"),
            "2026-08-23T06:00:00Z"
        );
        assert_eq!(
            next("0 6 * * *", "2026-08-23T06:00:00Z"),
            "2026-08-24T06:00:00Z"
        );
        assert_eq!(
            next("0 6 * * *", "2026-08-23T07:30:00Z"),
            "2026-08-24T06:00:00Z"
        );
    }

    #[test]
    fn the_next_firing_is_strictly_after_the_instant_asked_about() {
        // The caller passes the *last run*. Returning that same instant would make every tick
        // due forever, which is how a "scheduled" reconcile turns into a hot loop.
        assert_eq!(
            next("*/5 * * * *", "2026-08-23T10:05:00Z"),
            "2026-08-23T10:10:00Z"
        );
        // Sub-minute precision is not part of the dialect, and must not push the answer a whole
        // step further out.
        assert_eq!(
            next("*/5 * * * *", "2026-08-23T10:05:30Z"),
            "2026-08-23T10:10:00Z"
        );
    }

    #[test]
    fn steps_ranges_and_lists_expand_the_way_crontab_reads_them() {
        assert_eq!(
            next("*/15 * * * *", "2026-08-23T10:02:00Z"),
            "2026-08-23T10:15:00Z"
        );
        assert_eq!(
            next("0 */6 * * *", "2026-08-23T07:00:00Z"),
            "2026-08-23T12:00:00Z"
        );
        assert_eq!(
            next("30 9-17 * * *", "2026-08-23T08:00:00Z"),
            "2026-08-23T09:30:00Z"
        );
        assert_eq!(
            next("0,30 * * * *", "2026-08-23T10:10:00Z"),
            "2026-08-23T10:30:00Z"
        );
    }

    #[test]
    fn day_of_month_and_day_of_week_union_rather_than_intersect() {
        // 2026-08-23 is a Sunday. `1 * MON` means the 1st *or* any Monday — restricting to
        // Mondays that also fall on the 1st would silently drop most of the schedule.
        assert_eq!(
            next("0 0 1 * MON", "2026-08-23T00:00:00Z"),
            "2026-08-24T00:00:00Z"
        );
        assert_eq!(
            next("0 0 1 * MON", "2026-08-25T00:00:00Z"),
            "2026-08-31T00:00:00Z"
        );
        // Only one restricted: that one decides on its own.
        assert_eq!(
            next("0 0 1 * *", "2026-08-23T00:00:00Z"),
            "2026-09-01T00:00:00Z"
        );
        assert_eq!(
            next("0 0 * * MON", "2026-08-23T00:00:00Z"),
            "2026-08-24T00:00:00Z"
        );
    }

    #[test]
    fn names_are_accepted_for_months_and_days_in_any_case() {
        assert_eq!(
            next("0 0 * JAN *", "2026-08-23T00:00:00Z"),
            "2027-01-01T00:00:00Z"
        );
        assert_eq!(
            next("0 0 * * sun", "2026-08-24T00:00:00Z"),
            "2026-08-30T00:00:00Z"
        );
        assert_eq!(
            Cron::parse("0 0 * * 7").unwrap().days_of_week,
            vec![0],
            "7 folds onto Sunday rather than being a sixth weekday"
        );
    }

    #[test]
    fn a_leap_day_schedule_still_resolves_within_the_horizon() {
        // A one-year search horizon would call this unschedulable and quietly never fire it.
        assert_eq!(
            next("0 0 29 2 *", "2026-08-23T00:00:00Z"),
            "2028-02-29T00:00:00Z"
        );
    }

    #[test]
    fn malformed_expressions_say_which_field_and_what_was_expected() {
        for (expr, needle) in [
            ("0 6 * *", "expected 5 fields"),
            ("61 6 * * *", "minute"),
            ("0 24 * * *", "hour"),
            ("0 6 0 * *", "day-of-month"),
            ("0 6 * 13 *", "month"),
            ("0 6 * * 8", "day-of-week"),
            ("0 6 * * FUN", "day-of-week"),
            ("*/0 6 * * *", "positive"),
            ("30-10 6 * * *", "backwards"),
        ] {
            let err = Cron::parse(expr).expect_err(expr).to_string();
            assert!(
                err.contains(needle),
                "`{expr}` should mention `{needle}`: {err}"
            );
        }
    }

    #[test]
    fn due_is_anchored_on_the_last_run_not_on_the_clock() {
        let cron = Cron::parse("0 6 * * *").expect("parses");
        // Registered at 05:00; at 05:59 nothing has fired yet.
        assert!(!cron.is_due(at("2026-08-23T05:00:00Z"), at("2026-08-23T05:59:00Z")));
        assert!(cron.is_due(at("2026-08-23T05:00:00Z"), at("2026-08-23T06:00:00Z")));
        // Having just run at 06:00, it is not due again until tomorrow.
        assert!(!cron.is_due(at("2026-08-23T06:00:00Z"), at("2026-08-23T23:59:00Z")));
        assert!(cron.is_due(at("2026-08-23T06:00:00Z"), at("2026-08-24T06:00:00Z")));
    }

    #[test]
    fn the_expression_round_trips_with_its_whitespace_normalized() {
        assert_eq!(
            Cron::parse("  0   6  *  * *  ").unwrap().expr(),
            "0 6 * * *"
        );
    }
}
