//! The **salience / importance scale** — the single home for "how big a deal is this item",
//! the ranking signal that lifts the digest off a pure-recency sort (design §8.3 priority boost).
//!
//! Importance is a small integer `0..=3` written onto an event's `severity_hint` *before* it is
//! clustered, so it rides the existing rollup (`cluster.max_severity` = max over the group →
//! `story.max_severity` → the `severity_weight · max_severity` term in [`crate::digest::select`]).
//! No new ranking plumbing — the severity term was built for exactly this and sat idle (no v1 source
//! emitted a hint).
//!
//! **Ground-truth-first** (`docs/local-ml-options.md` §0): the model never emits the *number*. A
//! structured source maps deterministically from its activity kind ([`github_importance`]); a
//! free-text source has the enrichment LLM *classify* into a closed vocabulary it calibrates far
//! better than a raw integer ([`impact_score`]), and this module turns that class into the score.
//! So importance is always a deterministic, tunable, inspectable function of a classification — never
//! a scalar hallucinated by the model.

/// A major, breaking development — a war escalation, a mass-casualty event, a critical security
/// advisory, a release. The top of the scale.
pub const MAJOR: i16 = 3;
/// A significant development — a major policy change, a serious incident, a notable market/org move.
pub const SIGNIFICANT: i16 = 2;
/// A notable item worth surfacing but not dominating — a policy debate, a routine release, a local
/// incident, an opened issue/PR.
pub const NOTABLE: i16 = 1;
/// Routine churn — chatter, a push, a watch/fork, a market wrap, a minor cultural note. Contributes
/// no priority boost (the un-tuned baseline), so it ranks on recency alone.
pub const ROUTINE: i16 = 0;

/// The closed impact vocabulary the enrichment LLM classifies a free-text item into — ordered
/// least→most significant. A *closed* vocab is what a 3–4B model calibrates reliably (unlike a bare
/// integer), and it is re-validated here exactly like the comprehension pass re-checks `event_type`
/// (defense in depth: an out-of-vocab class degrades to [`ROUTINE`], never an error).
pub const IMPACT_VOCAB: [&str; 4] = ["routine", "notable", "significant", "major"];

/// Map the LLM's classified `impact` word to its importance score, re-validating against
/// [`IMPACT_VOCAB`]. An empty, unknown, or garbled value is treated as [`ROUTINE`] — the safe floor,
/// so a flaky classification only forgoes a *boost*, it can never inflate an item.
pub fn impact_score(impact: &str) -> i16 {
    match impact.trim().to_ascii_lowercase().as_str() {
        "major" => MAJOR,
        "significant" => SIGNIFICANT,
        "notable" => NOTABLE,
        _ => ROUTINE,
    }
}

/// Structural importance for a GitHub activity, by its REST event `kind` — the deterministic map for a
/// source whose semantics are exact (no LLM needed, and enrichment is deliberately *not* run over
/// GitHub, see [`crate::common::kind::SourceKind::benefits_from_enrichment`]). A release ships
/// something; an issue/PR is substantive activity; chatter, pushes, and stars are routine churn. Coarse
/// and tunable — the weight that scales it lives in `digest_config`.
pub fn github_importance(kind: &str) -> i16 {
    match kind {
        "ReleaseEvent" => SIGNIFICANT,
        "IssuesEvent" | "PullRequestEvent" => NOTABLE,
        _ => ROUTINE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impact_vocab_round_trips_to_an_ascending_scale() {
        // The vocabulary is the calibration anchor the model sees; its scores must be strictly
        // ascending so "major" always outweighs "notable".
        let scores: Vec<i16> = IMPACT_VOCAB.iter().map(|w| impact_score(w)).collect();
        assert_eq!(scores, vec![ROUTINE, NOTABLE, SIGNIFICANT, MAJOR]);
        assert!(scores.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn unknown_or_garbled_impact_floors_to_routine() {
        // A hallucinated/out-of-vocab class can never inflate an item — it forgoes the boost.
        assert_eq!(impact_score("catastrophic"), ROUTINE);
        assert_eq!(impact_score(""), ROUTINE);
        assert_eq!(impact_score("  MAJOR "), MAJOR); // tolerant of case/whitespace
    }

    #[test]
    fn github_releases_outrank_issues_outrank_churn() {
        assert!(github_importance("ReleaseEvent") > github_importance("IssuesEvent"));
        assert!(github_importance("PullRequestEvent") > github_importance("PushEvent"));
        assert_eq!(github_importance("WatchEvent"), ROUTINE);
        assert_eq!(github_importance("ForkEvent"), ROUTINE);
    }
}
