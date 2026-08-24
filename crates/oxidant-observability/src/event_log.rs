//! The replay order of a rolled `events.jsonl` directory (`docs/query-history-durability.md` §8).
//!
//! **Replay order is a correctness property, not a cosmetic one.** `AppStateStore::apply_event` is
//! last-write-wins and `JobStarted` overwrites the *whole* job — status `Running`,
//! `completion_time_ms: None`, `error: None`. So a job whose `JobStarted` landed in one generation
//! and whose `JobFinished` landed in a later one comes back **`Running`, with its completion time
//! and error erased**, if the later generation is replayed first. That is precisely the data loss
//! the roll exists to avoid, one level down.
//!
//! And `sort()` over the file names gets it wrong: `events-2026-08-24.2.jsonl` sorts *before*
//! `events-2026-08-24.jsonl`, because `'2'` (0x32) < `'j'` (0x6a). The `.N` split is the *newer*
//! generation of its period, so a lexicographic sort replays the newest split of a period ahead of
//! the plain file that preceded it.
//!
//! The order is therefore `(period end, split)`, matching what the sweeper that writes these names
//! uses to decide which generation to prune (`oxidant_connect::history::disk::rolled_event_logs`).
//! The grammar is duplicated here rather than shared because `oxidant-connect` depends on this
//! crate, not the other way round; the tests below pin the two spellings against the same worked
//! examples.

use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};

/// The live event log every other tool tails. Replayed **last**: it is the newest generation.
pub(crate) const LIVE_EVENT_LOG: &str = "events.jsonl";
const ROLLED_PREFIX: &str = "events-";
const ROLLED_SUFFIX: &str = ".jsonl";

/// The replay order of one rolled generation: the instant its period ends, then its `.N` split.
pub(crate) type OrderKey = (Option<DateTime<Utc>>, u32);

/// The sort key of a rolled `events-<period>[.N].jsonl`, or `None` for a name this engine did not
/// write (a Spark history server's own `application_*` files live in this directory).
///
/// The key is `(period end, split)`. A name whose period does not parse sorts first — there is no
/// evidence it is recent, and replaying an unknown generation before the known ones is the
/// direction that cannot clobber a finished job with a stale `JobStarted`.
pub(crate) fn rolled_order_key(name: &str) -> Option<OrderKey> {
    let body = name
        .strip_prefix(ROLLED_PREFIX)?
        .strip_suffix(ROLLED_SUFFIX)?;
    if body.is_empty() {
        return None;
    }
    // The split is the last dot-separated component when it is a number; a period stem never
    // contains a `.`, so this cannot eat part of the date.
    let (stem, split) = match body.rsplit_once('.') {
        Some((head, tail))
            if !tail.is_empty() && tail.len() <= 3 && tail.bytes().all(|b| b.is_ascii_digit()) =>
        {
            (head, tail.parse().ok()?)
        }
        _ => (body, 1u32),
    };
    Some((period_end(stem), split))
}

/// The instant the period named by `stem` ends, exclusive. `2026-08-23`, `2026-08-23-14` and
/// `2026-W34` — §3's three roll modes, in UTC, ISO weeks included.
fn period_end(stem: &str) -> Option<DateTime<Utc>> {
    let parts: Vec<&str> = stem.split('-').collect();
    let year = digits(parts.first()?, 4)? as i32;
    let naive = match parts.len() {
        // 2026-W34 — ISO year + ISO week, so the file a Sunday wrote and the file the following
        // Monday wrote are ordered by the week they belong to, not by the calendar year.
        2 => {
            let week = digits(parts[1].strip_prefix('W')?, 2)?;
            NaiveDate::from_isoywd_opt(year, week, chrono::Weekday::Mon)?.and_hms_opt(0, 0, 0)?
                + Duration::days(7)
        }
        // 2026-08-23
        3 => NaiveDate::from_ymd_opt(year, digits(parts[1], 2)?, digits(parts[2], 2)?)?
            .succ_opt()?
            .and_hms_opt(0, 0, 0)?,
        // 2026-08-23-14
        4 => {
            let hour = digits(parts[3], 2)?;
            if hour > 23 {
                return None;
            }
            NaiveDate::from_ymd_opt(year, digits(parts[1], 2)?, digits(parts[2], 2)?)?
                .and_hms_opt(hour, 0, 0)?
                + Duration::hours(1)
        }
        _ => return None,
    };
    Some(Utc.from_utc_datetime(&naive))
}

/// A fixed-width run of ASCII digits — `2026-8-3` is not a period stem, and accepting it would let
/// two spellings name one generation.
fn digits(raw: &str, width: usize) -> Option<u32> {
    if raw.len() != width || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `'2' < 'j'` inversion, stated as an ordering rather than as a string comparison.
    #[test]
    fn a_split_replays_after_the_plain_file_of_its_period() {
        let plain = rolled_order_key("events-2026-08-24.jsonl").expect("grammar");
        let split = rolled_order_key("events-2026-08-24.2.jsonl").expect("grammar");
        assert!(
            plain < split,
            "the `.2` split is the newer generation, but its name sorts first: {plain:?} vs {split:?}"
        );
        assert!(
            "events-2026-08-24.2.jsonl" < "events-2026-08-24.jsonl",
            "…which is exactly what a lexicographic sort() gets wrong"
        );
    }

    /// Ordering across periods and roll modes, including the ISO-week boundary `%Y`+`%W` gets
    /// wrong: 2021-01-01 is ISO 2020-W53, so `2020-W53` must sort *after* `2020-12-25`.
    #[test]
    fn periods_order_chronologically_across_roll_modes() {
        let names = [
            "events-2026-08-23.jsonl",
            "events-2026-08-24.jsonl",
            "events-2026-08-24.2.jsonl",
            "events-2026-08-24.10.jsonl",
        ];
        let mut keys: Vec<_> = names.iter().map(|n| rolled_order_key(n).unwrap()).collect();
        let sorted = keys.clone();
        keys.sort();
        assert_eq!(keys, sorted, "the names are already in chronological order");

        assert!(
            rolled_order_key("events-2020-12-25.jsonl").unwrap()
                < rolled_order_key("events-2020-W53.jsonl").unwrap(),
            "2020-W53 ends 2021-01-04, after 2020-12-25"
        );
        assert!(
            rolled_order_key("events-2026-08-23-14.jsonl").unwrap()
                < rolled_order_key("events-2026-08-23-15.jsonl").unwrap(),
            "hourly generations order by the hour"
        );
    }

    /// Only the shape this engine writes is ordered; a history server's own files are not ours to
    /// interpret, and the live file is not a rolled generation.
    #[test]
    fn foreign_names_have_no_order_key() {
        for name in [
            "application_1_0001",
            "events.jsonl",
            "events-.jsonl",
            "events-2026-08-23.jsonl.gz",
            "notes.txt",
        ] {
            assert!(
                rolled_order_key(name).is_none(),
                "{name} is not a rolled event log"
            );
        }
        // A name with our prefix and suffix but an unparseable period *is* ours — the sweeper
        // treats `events-*.jsonl` as its own and may prune it — so it is replayed, first, where a
        // stale `JobStarted` in it cannot clobber a finished job from a known generation.
        assert_eq!(
            rolled_order_key("events-2026-8-3.jsonl"),
            Some((None, 1)),
            "an unparseable period replays before every dated one"
        );
    }
}
