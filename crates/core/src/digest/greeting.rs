//! The digest's opening line: a short, warm greeting keyed to *when* the subscriber receives the
//! digest (their local time-of-day) and *how often* (daily / weekly). It stands in for the
//! reference design's "big picture" lead until the digest produces a real summary — a friendly
//! hello rather than a paragraph of analysis.
//!
//! A handful of interchangeable phrasings keep consecutive digests from reading identically. The
//! choice is seeded from the digest's identity (subscriber + window) via [`seed_for`], so
//! re-rendering the *same* digest yields the *same* greeting — rendering stays idempotent (design
//! §9.2) — while the next window varies.

use std::hash::{Hash, Hasher};

use chrono::{DateTime, NaiveTime, Timelike, Utc};
use uuid::Uuid;

use crate::digest::subscriber::Recurrence;

/// Coarse part of the day, bucketed from the subscriber's local `digest_time`. Boundaries are the
/// conventional ones — picked so the salutation matches what the wall clock would say when the mail
/// lands, not some abstract "AM/PM".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DayPart {
    Morning,
    Afternoon,
    Evening,
}

impl DayPart {
    fn from_local_time(t: NaiveTime) -> Self {
        match t.hour() {
            5..=11 => DayPart::Morning,
            12..=16 => DayPart::Afternoon,
            // Evenings, late nights and the small hours all greet as "evening": a 2am digest
            // wishing "good morning" would read worse than "good evening".
            _ => DayPart::Evening,
        }
    }

    fn salutation(self) -> &'static str {
        match self {
            DayPart::Morning => "Good morning",
            DayPart::Afternoon => "Good afternoon",
            DayPart::Evening => "Good evening",
        }
    }
}

/// The cadence word that slots into a greeting — how a subscriber would describe their own digest
/// ("your daily digest", "your weekly digest").
fn cadence_word(recurrence: Recurrence) -> &'static str {
    match recurrence {
        Recurrence::Daily => "daily",
        Recurrence::Weekly { .. } => "weekly",
    }
}

/// Interchangeable phrasings, each built from a `{salutation}` and the `{cadence}` word so every
/// one works across all time-of-day × cadence combinations. Kept short and warm — one line, no
/// analysis, and no em-dashes. Reword or add freely; the only contract is the two placeholders.
const VARIANTS: &[&str] = &[
    "{salutation}. Here's your {cadence} digest.",
    "{salutation}! Your {cadence} digest just landed.",
    "{salutation}, your {cadence} digest is ready.",
    "{salutation}! Here's what's new in your {cadence} digest.",
    "{salutation}. Fresh off the press: your {cadence} digest.",
    "{salutation}! Time for your {cadence} digest.",
    "{salutation}. Let's dive into your {cadence} digest.",
    "{salutation}! Your {cadence} dose of the news has arrived.",
    // Calm, meditational phrasings — the unhurried, breathe-easy tone the app is going for.
    "{salutation}. Take a breath; here's your {cadence} digest.",
    "{salutation}. Your {cadence} digest is ready whenever you are.",
    "{salutation}. Settle in with your {cadence} digest.",
    "{salutation}. Breathe easy; your {cadence} digest is here.",
    "{salutation}. Find a quiet moment for your {cadence} digest.",
    "{salutation}. Ease into your {cadence} digest.",
    "{salutation}. No rush; your {cadence} digest will keep.",
    "{salutation}. When you're ready, your {cadence} digest is waiting.",
];

/// Builds the greeting for one digest. `digest_time` is the subscriber's local delivery time (which
/// time-of-day to greet for), `recurrence` supplies the cadence word, and `seed` selects the
/// phrasing — pass [`seed_for`] of the digest's identity for a stable-per-digest, varies-across-
/// digests choice.
pub(crate) fn greeting(digest_time: NaiveTime, recurrence: Recurrence, seed: u64) -> String {
    let salutation = DayPart::from_local_time(digest_time).salutation();
    let cadence = cadence_word(recurrence);
    let template = VARIANTS[(seed % VARIANTS.len() as u64) as usize];
    template
        .replace("{salutation}", salutation)
        .replace("{cadence}", cadence)
}

/// A stable seed from the digest's identity (subscriber + window). The same digest always renders
/// the same greeting (idempotent re-render); consecutive windows hash differently, so the phrasing
/// rotates. Not persisted and not security-sensitive, so the std hasher's cross-version instability
/// doesn't matter — it only needs to spread.
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
        let at = |h, m| NaiveTime::from_hms_opt(h, m, 0).unwrap();
        // Morning band.
        assert!(greeting(at(6, 0), Recurrence::Daily, 0).starts_with("Good morning"));
        assert!(greeting(at(9, 0), Recurrence::Daily, 0).starts_with("Good morning"));
        assert!(greeting(at(11, 59), Recurrence::Daily, 0).starts_with("Good morning"));
        // Afternoon band.
        assert!(greeting(at(12, 0), Recurrence::Daily, 0).starts_with("Good afternoon"));
        assert!(greeting(at(16, 59), Recurrence::Daily, 0).starts_with("Good afternoon"));
        // Evening / night band — including the small hours.
        assert!(greeting(at(17, 0), Recurrence::Daily, 0).starts_with("Good evening"));
        assert!(greeting(at(23, 30), Recurrence::Daily, 0).starts_with("Good evening"));
        assert!(greeting(at(2, 0), Recurrence::Daily, 0).starts_with("Good evening"));
    }

    #[test]
    fn cadence_word_matches_recurrence() {
        let nine = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        // Every phrasing names the cadence (it's a required placeholder), regardless of wording.
        for s in 0..VARIANTS.len() as u64 {
            assert!(greeting(nine, Recurrence::Daily, s).contains("daily"));
            assert!(greeting(nine, Recurrence::Weekly { weekday: 2 }, s).contains("weekly"));
        }
    }

    #[test]
    fn seed_selects_among_all_variants() {
        let nine = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        // Walking the seed across the variant count must surface every distinct phrasing, and every
        // result is a non-empty single line that opens with the salutation.
        let rendered: std::collections::HashSet<String> = (0..VARIANTS.len() as u64)
            .map(|s| greeting(nine, Recurrence::Daily, s))
            .collect();
        assert_eq!(rendered.len(), VARIANTS.len());
        for line in &rendered {
            assert!(line.starts_with("Good morning"));
            assert!(line.contains("daily"));
            assert!(!line.contains('\n'));
        }
    }

    #[test]
    fn variants_are_well_formed() {
        // Two contracts every variant must honour: consume both `{…}` placeholders (or the
        // subscriber sees a raw token), and carry no em-dash (a deliberate house-style choice).
        let nine = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        for s in 0..VARIANTS.len() as u64 {
            let line = greeting(nine, Recurrence::Weekly { weekday: 0 }, s);
            assert!(!line.contains('{'), "placeholder leaked: {line}");
            assert!(!line.contains('}'), "placeholder leaked: {line}");
            assert!(!line.contains('—'), "em-dash in greeting: {line}");
        }
    }

    #[test]
    fn seed_is_stable_per_digest() {
        let id = Uuid::nil();
        let window = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        // Same identity → same seed (idempotent re-render); a different window rotates it.
        assert_eq!(seed_for(id, window), seed_for(id, window));
        let later = DateTime::from_timestamp(1_700_086_400, 0).unwrap();
        assert_ne!(seed_for(id, window), seed_for(id, later));
    }
}
