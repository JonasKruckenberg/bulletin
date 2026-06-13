//! The digest's opening line: a short, warm greeting keyed to the subscriber's local time-of-day
//! and cadence. It stands in for the reference design's "big picture" lead until the digest
//! produces a real summary. The phrasing is picked from [`VARIANTS`] by a seed derived from the
//! digest's identity (subscriber + window), so re-rendering the same digest yields the same line
//! (idempotent — design §9.2) while consecutive windows vary.

use std::hash::{Hash, Hasher};

use chrono::{DateTime, NaiveTime, Timelike, Utc};
use uuid::Uuid;

use crate::digest::subscriber::Recurrence;

/// The bare time-of-day phrase for the subscriber's local delivery time — what the wall clock would
/// say when the mail lands. Late nights and the small hours greet as "evening" (a 2am "good morning"
/// reads odd).
fn time_of_day(t: NaiveTime) -> &'static str {
    match t.hour() {
        5..=11 => "Good morning",
        12..=16 => "Good afternoon",
        _ => "Good evening",
    }
}

/// Salutation for the subscriber's local delivery time, personalized with their `name` when we have
/// one: "Good morning" → "Good morning, Alice". A blank/whitespace name is treated as absent (the
/// store normalizes those to `None`, but we guard here too). Shared with the empty-digest mail,
/// which splices it onto its own "all caught up" copy.
pub(crate) fn salutation(t: NaiveTime, name: Option<&str>) -> String {
    let base = time_of_day(t);
    match name.map(str::trim).filter(|n| !n.is_empty()) {
        Some(name) => format!("{base}, {name}"),
        None => base.to_string(),
    }
}

fn cadence_word(recurrence: Recurrence) -> &'static str {
    match recurrence {
        Recurrence::Daily => "daily",
        Recurrence::Weekly { .. } => "weekly",
    }
}

/// Interchangeable phrasings, each built from a `{salutation}` and the `{cadence}` word so every one
/// works across all time-of-day × cadence combinations. Kept short, warm, and em-dash-free; reword
/// or add freely — the only contract (enforced by tests) is the two placeholders and no em-dash.
const VARIANTS: &[&str] = &[
    "{salutation}. Here's your {cadence} digest.",
    "{salutation}! Your {cadence} digest just landed.",
    "{salutation}, your {cadence} digest is ready.",
    "{salutation}! Here's what's new in your {cadence} digest.",
    "{salutation}. Fresh off the press: your {cadence} digest.",
    "{salutation}! Time for your {cadence} digest.",
    "{salutation}. Let's dive into your {cadence} digest.",
    "{salutation}! Your {cadence} dose of the news has arrived.",
    // Calm, meditational phrasings — the unhurried, breathe-easy tone the app is going for, with a
    // little life so they land warm rather than sleepy.
    "{salutation}. Take a breath; here's your {cadence} digest.",
    "{salutation}. Your {cadence} digest is ready whenever you are.",
    "{salutation}. Settle in with your {cadence} digest.",
    "{salutation}. Breathe easy; your {cadence} digest is here.",
    "{salutation}. Find a quiet moment for your {cadence} digest.",
    "{salutation}. Ease into your {cadence} digest.",
    "{salutation}. When you're ready, your {cadence} digest is waiting.",
    "{salutation}. Inhale, exhale; your {cadence} digest is ready.",
    "{salutation}. Drop your shoulders; your {cadence} digest is here.",
    "{salutation}. Find your flow with your {cadence} digest.",
    "{salutation}. Unwind a little; your {cadence} digest is ready.",
    "{salutation}. Channel your inner calm; your {cadence} digest awaits.",
];

/// Builds the greeting for one digest: `digest_time` chooses the salutation, `name` personalizes it,
/// `recurrence` the cadence word, and `seed` the phrasing (pass [`seed_for`] for a
/// stable-per-digest choice).
pub(crate) fn greeting(
    digest_time: NaiveTime,
    recurrence: Recurrence,
    seed: u64,
    name: Option<&str>,
) -> String {
    VARIANTS[(seed % VARIANTS.len() as u64) as usize]
        .replace("{salutation}", &salutation(digest_time, name))
        .replace("{cadence}", cadence_word(recurrence))
}

/// A stable seed from the digest's identity, so the same digest renders the same greeting while
/// consecutive windows rotate. Not persisted or security-sensitive — it only needs to spread.
pub(crate) fn seed_for(subscriber_id: Uuid, window_end: DateTime<Utc>) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    subscriber_id.hash(&mut h);
    window_end.timestamp().hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn salutation_tracks_time_of_day() {
        let at = |h| salutation(NaiveTime::from_hms_opt(h, 0, 0).unwrap(), None);
        assert_eq!(at(5), "Good morning");
        assert_eq!(at(11), "Good morning");
        assert_eq!(at(12), "Good afternoon");
        assert_eq!(at(16), "Good afternoon");
        assert_eq!(at(17), "Good evening");
        assert_eq!(at(2), "Good evening"); // small hours stay "evening"
    }

    #[test]
    fn salutation_personalizes_with_name() {
        let nine = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        assert_eq!(salutation(nine, Some("Alice")), "Good morning, Alice");
        // A blank or whitespace name falls back to the bare salutation.
        assert_eq!(salutation(nine, Some("")), "Good morning");
        assert_eq!(salutation(nine, Some("  ")), "Good morning");
        // Surrounding whitespace is trimmed.
        assert_eq!(salutation(nine, Some("  Bob ")), "Good morning, Bob");
    }

    #[test]
    fn greeting_threads_the_name_into_the_salutation() {
        let nine = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        for s in 0..VARIANTS.len() as u64 {
            let line = greeting(nine, Recurrence::Daily, s, Some("Alice"));
            assert!(
                line.starts_with("Good morning, Alice"),
                "name should open the greeting: {line}"
            );
            assert!(line.contains("daily"));
        }
    }

    #[test]
    fn every_variant_is_well_formed() {
        let nine = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        let mut seen = std::collections::HashSet::new();
        for s in 0..VARIANTS.len() as u64 {
            let line = greeting(nine, Recurrence::Daily, s, None);
            // Opens with the salutation, names the cadence, and is a single clean line...
            assert!(line.starts_with("Good morning"));
            assert!(line.contains("daily"));
            assert!(greeting(nine, Recurrence::Weekly { weekday: 2 }, s, None).contains("weekly"));
            assert!(!line.contains('\n'));
            // ...with no leaked placeholder and no em-dash (a deliberate house-style choice).
            assert!(
                !line.contains('{') && !line.contains('}'),
                "placeholder leaked: {line}"
            );
            assert!(!line.contains('—'), "em-dash in greeting: {line}");
            seen.insert(line);
        }
        // The seed reaches every distinct phrasing.
        assert_eq!(seen.len(), VARIANTS.len());
    }

    #[test]
    fn seed_is_stable_per_digest() {
        let id = Uuid::nil();
        let window = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let later = DateTime::from_timestamp(1_700_086_400, 0).unwrap();
        assert_eq!(seed_for(id, window), seed_for(id, window)); // idempotent re-render
        assert_ne!(seed_for(id, window), seed_for(id, later)); // next window rotates
    }
}
