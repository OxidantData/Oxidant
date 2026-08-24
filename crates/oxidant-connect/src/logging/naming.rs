//! UTC names for rolled exec logs, and the grammar `?file=` parses (§3 "Naming", §6).
//!
//! Three rules, all of them there because the obvious spelling is wrong:
//!
//! 1. **Every name is computed in UTC.** Operators read local wall-clock in the log *body*; the
//!    *name* never carries an offset. That removes the whole DST class at a stroke — no repeated
//!    01:00 hour, no missing spring-forward hour, and no ambiguous name a prune cannot parse.
//! 2. **Weekly is ISO year + ISO week** (`%G-W%V`), never `%Y` + `%W`. 2019-12-30 and
//!    2019-12-31 are ISO **2020**-W01; the `%Y`+`%W` spelling writes `oxidant-2019-W52` for
//!    them and `oxidant-2020-W01` for the January days of the *same week*, so one ISO week
//!    becomes two files — and 2021-01-01..03, which is ISO **2020**-W53, lands under
//!    `oxidant-2021-W00`, silently overwriting the next January's first file.
//!    (§3's worked example says 2026-12-28..31 is 2027-W01. It is not — that is ISO 2026-W53,
//!    because 2026 starts on a Thursday and therefore has 53 ISO weeks. The rule the example
//!    illustrates is right; the dates are corrected here and in the doc.)
//! 3. **`.N` is the size-split sequence**, present only on the second and later files of a
//!    period. A clock roll and a size roll therefore never produce the same name.
//!
//! Ordering is **`(period end, split)`, never lexicographic**: a `.N` split is the *newer*
//! generation of its period but its name sorts *before* the plain one (`'2'` < `'l'`). Every
//! consumer — `disk::rolled_by_period`, `disk::rolled_event_logs`,
//! `AppStateStore::load_event_log` and `GET /api/v1/logs/files` (PR4) — computes that key rather
//! than sorting the names.

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc};

/// `OXIDANT_LOG_ROLL`: which UTC boundary closes the live file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LogRoll {
    #[default]
    Daily,
    Hourly,
    Weekly,
    /// No rolling writer at all — nothing under `logs/` is written.
    ///
    /// **Deviation from §3's knob table**, which lists only `daily|hourly|weekly`. The rolling
    /// writer is on by default and writes 30 days of every enabled `tracing` field to disk;
    /// an operator who wants the engine's logs to stay on stderr needs a way to say so that is
    /// not "turn off statement history too".
    Off,
}

impl LogRoll {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Hourly => "hourly",
            Self::Weekly => "weekly",
            Self::Off => "off",
        }
    }

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "daily" | "day" => Some(Self::Daily),
            "hourly" | "hour" => Some(Self::Hourly),
            "weekly" | "week" => Some(Self::Weekly),
            "off" | "none" | "0" | "false" => Some(Self::Off),
            _ => None,
        }
    }
}

/// The UTC period one rolled file covers.
///
/// This is a *typed* value, not a string: `?file=` is parsed into one of these and the filename
/// is then **reconstructed** from it (§6, F12). `..`, `/`, an extension and an absolute path all
/// fail the grammar by construction, so no traversal shape ever reaches a path join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogPeriod {
    Daily {
        year: i32,
        month: u32,
        day: u32,
    },
    Hourly {
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
    },
    /// ISO year + ISO week (`%G-W%V`).
    Weekly {
        year: i32,
        week: u32,
    },
}

/// Filename prefix for every exec log this engine writes. The live file is exactly
/// `oxidant.log`; every rolled one is `oxidant-<period>[.N].<ext>`.
pub(crate) const PREFIX: &str = "oxidant-";

impl LogPeriod {
    /// The period `now` falls in under `roll`. `LogRoll::Off` has no period and answers `None`.
    pub(crate) fn of(now: DateTime<Utc>, roll: LogRoll) -> Option<Self> {
        Some(match roll {
            LogRoll::Off => return None,
            LogRoll::Daily => Self::Daily {
                year: now.year(),
                month: now.month(),
                day: now.day(),
            },
            LogRoll::Hourly => Self::Hourly {
                year: now.year(),
                month: now.month(),
                day: now.day(),
                hour: now.hour(),
            },
            LogRoll::Weekly => {
                let iso = now.iso_week();
                Self::Weekly {
                    year: iso.year(),
                    week: iso.week(),
                }
            }
        })
    }

    /// The period's name, without prefix, split or extension: `2026-08-23`, `2026-08-23-14`,
    /// `2026-W34`.
    pub(crate) fn stem(&self) -> String {
        match self {
            Self::Daily { year, month, day } => format!("{year:04}-{month:02}-{day:02}"),
            Self::Hourly {
                year,
                month,
                day,
                hour,
            } => format!("{year:04}-{month:02}-{day:02}-{hour:02}"),
            Self::Weekly { year, week } => format!("{year:04}-W{week:02}"),
        }
    }

    /// The full filename: `oxidant-<stem>[.N].<ext>`. `split` is 1-based and the suffix is
    /// **omitted** for 1, so the first file of a period keeps the plain name an operator expects.
    pub(crate) fn file_name(&self, split: u32, ext: &str) -> String {
        if split <= 1 {
            format!("{PREFIX}{}.{ext}", self.stem())
        } else {
            format!("{PREFIX}{}.{split}.{ext}", self.stem())
        }
    }

    /// Parse the `?file=` grammar's period part, with its optional `.N`.
    ///
    /// ```text
    /// file := YYYY "-" MM "-" DD          [ "." N ]        # daily
    ///       | YYYY "-" MM "-" DD "-" HH   [ "." N ]        # hourly
    ///       | YYYY "-W" ww                [ "." N ]        # weekly, ISO
    /// YYYY := 4DIGIT   MM,DD,HH,ww := 2DIGIT   N := 1*3DIGIT, 2..999
    /// ```
    ///
    /// Every rejection is a `400`, never a `404`: a caller who typed `../../etc/passwd` is told
    /// their input was invalid, not that the engine looked for it and did not find it.
    pub(crate) fn parse(raw: &str) -> Option<(Self, u32)> {
        // The split is the *last* dot-separated component when it is a 2..999 number. A period
        // stem never contains a `.`, so this cannot eat part of the date.
        let (stem, split) = match raw.rsplit_once('.') {
            Some((head, tail))
                if !tail.is_empty()
                    && tail.len() <= 3
                    && tail.bytes().all(|b| b.is_ascii_digit()) =>
            {
                let n: u32 = tail.parse().ok()?;
                if !(2..=999).contains(&n) {
                    return None;
                }
                (head, n)
            }
            _ => (raw, 1),
        };
        let period = Self::parse_stem(stem)?;
        Some((period, split))
    }

    fn parse_stem(stem: &str) -> Option<Self> {
        let parts: Vec<&str> = stem.split('-').collect();
        let year = digits(parts.first()?, 4)? as i32;
        match parts.len() {
            // 2026-W34
            2 => {
                let week = parts[1].strip_prefix('W')?;
                let week = digits(week, 2)?;
                // Validated against the calendar, not just the digit count: `2026-W53` is not a
                // week that exists, and reconstructing a filename from it would name a file the
                // writer can never have produced.
                NaiveDate::from_isoywd_opt(year, week, chrono::Weekday::Mon)?;
                Some(Self::Weekly { year, week })
            }
            // 2026-08-23
            3 => {
                let month = digits(parts[1], 2)?;
                let day = digits(parts[2], 2)?;
                NaiveDate::from_ymd_opt(year, month, day)?;
                Some(Self::Daily { year, month, day })
            }
            // 2026-08-23-14
            4 => {
                let month = digits(parts[1], 2)?;
                let day = digits(parts[2], 2)?;
                let hour = digits(parts[3], 2)?;
                NaiveDate::from_ymd_opt(year, month, day)?;
                if hour > 23 {
                    return None;
                }
                Some(Self::Hourly {
                    year,
                    month,
                    day,
                    hour,
                })
            }
            _ => None,
        }
    }

    /// The instant the period ends, exclusive.
    ///
    /// Retention is evaluated against this, never against the name parsed as a day: **a rolled
    /// file is deleted only when its whole period is older than `OXIDANT_LOG_KEEP_DAYS`.**
    /// Weekly therefore rounds *up*, keeping up to six extra days rather than discarding days
    /// that are inside the window.
    pub(crate) fn end(&self) -> Option<DateTime<Utc>> {
        let naive = match self {
            Self::Daily { year, month, day } => NaiveDate::from_ymd_opt(*year, *month, *day)?
                .succ_opt()?
                .and_hms_opt(0, 0, 0)?,
            Self::Hourly {
                year,
                month,
                day,
                hour,
            } => {
                let start =
                    NaiveDate::from_ymd_opt(*year, *month, *day)?.and_hms_opt(*hour, 0, 0)?;
                start + chrono::Duration::hours(1)
            }
            Self::Weekly { year, week } => {
                NaiveDate::from_isoywd_opt(*year, *week, chrono::Weekday::Mon)?
                    .and_hms_opt(0, 0, 0)?
                    + chrono::Duration::days(7)
            }
        };
        Some(Utc.from_utc_datetime(&naive))
    }
}

/// A fixed-width run of ASCII digits. `"8"` is not a month here — `MM` is two digits, and
/// accepting `2026-8-3` would let two spellings name one file.
fn digits(raw: &str, width: usize) -> Option<u32> {
    if raw.len() != width || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

/// Split the rolled-file name `oxidant-<stem>[.N].<ext>` back into its parts.
///
/// The prune reads this to answer "what period does this file cover", and the boot scan reads it
/// to answer "is there a `.log` here that never became a `.parquet`". Returns `None` for
/// anything that is not a name this writer produced — including the live `oxidant.log`.
pub(crate) fn parse_file_name(name: &str) -> Option<(LogPeriod, u32, &str)> {
    let rest = name.strip_prefix(PREFIX)?;
    let (body, ext) = rest.rsplit_once('.')?;
    if body.is_empty() {
        return None;
    }
    let (period, split) = LogPeriod::parse(body)?;
    Some((period, split, ext))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).unwrap()
    }

    /// The year boundary `%Y`+`%W` gets wrong, pinned in both directions.
    ///
    /// December days that belong to *next* year's W01, and January days that belong to *last*
    /// year's W53, must each write **one** file with the rest of their ISO week — which is
    /// exactly what the `%Y`+`%W` spelling breaks.
    #[test]
    fn iso_week_naming_pins_the_year_boundary() {
        // 2019-12-30 (Mon) opens ISO 2020-W01; so do 2019-12-31 and 2020-01-01..05.
        for (y, m, d) in [(2019, 12, 30), (2019, 12, 31), (2020, 1, 1), (2020, 1, 5)] {
            assert_eq!(
                LogPeriod::of(utc(y, m, d, 12), LogRoll::Weekly)
                    .expect("weekly")
                    .stem(),
                "2020-W01",
                "{y}-{m:02}-{d:02} is ISO 2020-W01"
            );
        }
        // And the other direction: 2021-01-01..03 is still ISO 2020-W53. `%Y`+`%W` would call
        // it `2021-W00` and overwrite it with the next January's first file.
        for d in [1, 2, 3] {
            assert_eq!(
                LogPeriod::of(utc(2021, 1, d, 12), LogRoll::Weekly)
                    .expect("weekly")
                    .stem(),
                "2020-W53",
                "2021-01-{d:02} is ISO 2020-W53"
            );
        }
        assert_eq!(
            LogPeriod::of(utc(2021, 1, 4, 0), LogRoll::Weekly)
                .expect("weekly")
                .stem(),
            "2021-W01",
            "the Monday after is the first week of the new ISO year"
        );
    }

    #[test]
    fn names_carry_the_split_only_past_the_first() {
        let period = LogPeriod::of(utc(2026, 8, 23, 14), LogRoll::Hourly).expect("hourly");
        assert_eq!(period.file_name(1, "log"), "oxidant-2026-08-23-14.log");
        assert_eq!(period.file_name(2, "log"), "oxidant-2026-08-23-14.2.log");
        assert_eq!(
            period.file_name(3, "parquet"),
            "oxidant-2026-08-23-14.3.parquet"
        );
        let daily = LogPeriod::of(utc(2026, 8, 23, 14), LogRoll::Daily).expect("daily");
        assert_eq!(daily.file_name(1, "log"), "oxidant-2026-08-23.log");
    }

    /// Lexicographic order equals chronological order — what the prune's oldest-first pass and
    /// PR4's file listing both rely on.
    #[test]
    fn names_sort_chronologically() {
        let mut names = vec![
            LogPeriod::of(utc(2026, 8, 23, 14), LogRoll::Daily)
                .unwrap()
                .file_name(2, "log"),
            LogPeriod::of(utc(2026, 8, 23, 14), LogRoll::Daily)
                .unwrap()
                .file_name(1, "log"),
            LogPeriod::of(utc(2026, 9, 1, 0), LogRoll::Daily)
                .unwrap()
                .file_name(1, "log"),
            LogPeriod::of(utc(2025, 12, 31, 0), LogRoll::Daily)
                .unwrap()
                .file_name(1, "log"),
        ];
        names.sort();
        assert_eq!(
            names,
            vec![
                "oxidant-2025-12-31.log",
                "oxidant-2026-08-23.2.log",
                "oxidant-2026-08-23.log",
                "oxidant-2026-09-01.log",
            ],
            "within one period the split sorts before the plain name; across periods the date wins"
        );
    }

    #[test]
    fn the_grammar_round_trips_every_valid_form() {
        for (raw, split) in [
            ("2026-08-23", 1),
            ("2026-08-23.2", 2),
            ("2026-08-23.999", 999),
            ("2026-08-23-14", 1),
            ("2026-08-23-14.7", 7),
            ("2026-W34", 1),
            ("2026-W34.12", 12),
        ] {
            let (period, n) = LogPeriod::parse(raw).unwrap_or_else(|| panic!("{raw} must parse"));
            assert_eq!(n, split, "{raw}");
            let reconstructed = period.file_name(n, "log");
            let expected = format!("oxidant-{raw}.log");
            assert_eq!(reconstructed, expected, "{raw} must reconstruct verbatim");
        }
    }

    /// The traversal shapes, the extensions callers must not name, and the near-misses. Each is
    /// a `400`, and none of them ever reaches a path join.
    #[test]
    fn the_grammar_refuses_traversal_extensions_and_near_misses() {
        for bad in [
            "..",
            "../../etc/passwd",
            "2026-08-23/../../etc/passwd",
            "/var/log/oxidant",
            "2026-08-23.log",
            "2026-08-23.parquet",
            "oxidant-2026-08-23",
            "2026-8-23",
            "2026-08-23-24",
            "2026-13-01",
            "2026-02-30",
            "2027-W53",
            "2026-W00",
            "2026-08-23.1",
            "2026-08-23.0",
            "2026-08-23.1000",
            "current",
            "",
            "2026",
            "2026-08-23-14-30",
        ] {
            assert!(
                LogPeriod::parse(bad).is_none(),
                "{bad:?} must not parse into a period"
            );
        }
    }

    /// Retention is period-based: a file is deleted only when its *whole* period is older than
    /// the window, and weekly rounds up.
    #[test]
    fn period_end_rounds_up_for_weekly_and_is_exclusive() {
        let daily = LogPeriod::Daily {
            year: 2026,
            month: 8,
            day: 23,
        };
        assert_eq!(daily.end().unwrap(), utc(2026, 8, 24, 0));
        let hourly = LogPeriod::Hourly {
            year: 2026,
            month: 8,
            day: 23,
            hour: 23,
        };
        assert_eq!(hourly.end().unwrap(), utc(2026, 8, 24, 0));
        // ISO 2026-W34 is Mon 2026-08-17 .. Sun 2026-08-23; it survives until 2026-08-24.
        let weekly = LogPeriod::Weekly {
            year: 2026,
            week: 34,
        };
        assert_eq!(weekly.end().unwrap(), utc(2026, 8, 24, 0));
    }

    #[test]
    fn file_names_parse_back_and_the_live_file_never_does() {
        let (period, split, ext) =
            parse_file_name("oxidant-2026-08-23-14.2.parquet").expect("rolled name");
        assert_eq!(split, 2);
        assert_eq!(ext, "parquet");
        assert_eq!(period.stem(), "2026-08-23-14");
        assert!(parse_file_name("oxidant.log").is_none(), "the live file");
        assert!(parse_file_name("syslog").is_none());
        assert!(parse_file_name("oxidant-.log").is_none());
        assert!(parse_file_name("oxidant-2026-08-23").is_none(), "no ext");
    }

    #[test]
    fn roll_modes_parse_and_render() {
        for (raw, roll) in [
            ("daily", LogRoll::Daily),
            ("HOURLY", LogRoll::Hourly),
            (" weekly ", LogRoll::Weekly),
            ("off", LogRoll::Off),
        ] {
            assert_eq!(LogRoll::parse(raw), Some(roll), "{raw}");
        }
        assert_eq!(LogRoll::parse("monthly"), None);
        assert_eq!(LogRoll::default().as_str(), "daily");
    }
}
