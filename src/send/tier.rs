// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The one customer-facing knob: `NEW_RELIC_EXTENSION_SEND_TIER` (`high` or `default`).
//! `ExtensionConfig.extension.send_tier` holds the parsed value; the process-wide breaker
//! in `event_loop.rs` reads `SendTier::from_env()` on its own, which is harmless since it
//! is a pure function of the same env var.

pub const SEND_TIER_ENV_VAR: &str = "NEW_RELIC_EXTENSION_SEND_TIER";

/// The one customer-facing tier. `Low` is explicitly out of scope for now
/// (deferred) — adding it
/// later means adding one more match arm here, not a new knob shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendTier {
    /// Today's behavior: synchronous sends, strong delivery guarantee.
    /// Safe default — zero behavior change for existing customers.
    #[default]
    Default,
    /// High-throughput and/or ultra-fast-function customers: deferred
    /// sends, resolved at the start of the next invocation.
    High,
}

impl SendTier {
    /// Parse from the raw env var value. Case-insensitive, trims
    /// whitespace. Unknown/empty values fall back to `Default` — this
    /// function never panics, per the "warn and fall back, don't crash a
    /// customer's Lambda" requirement.
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "high" => SendTier::High,
            "default" | "" => SendTier::Default,
            other => {
                tracing::warn!(
                    "Unknown {} value '{}' - falling back to 'default'",
                    SEND_TIER_ENV_VAR,
                    other
                );
                SendTier::Default
            }
        }
    }

    /// Reads `NEW_RELIC_EXTENSION_SEND_TIER` from the environment. Missing env var is
    /// not a warning case (it's the expected default-on-upgrade path) -
    /// only a *present but unrecognized* value warns.
    pub fn from_env() -> Self {
        match std::env::var(SEND_TIER_ENV_VAR) {
            Ok(raw) => Self::parse(&raw),
            Err(_) => SendTier::Default,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive_and_trims_whitespace() {
        assert_eq!(SendTier::parse("high"), SendTier::High);
        assert_eq!(SendTier::parse("HIGH"), SendTier::High);
        assert_eq!(SendTier::parse("  High  "), SendTier::High);
        assert_eq!(SendTier::parse("default"), SendTier::Default);
        assert_eq!(SendTier::parse("DEFAULT"), SendTier::Default);
    }

    #[test]
    fn parse_empty_string_falls_back_to_default() {
        assert_eq!(SendTier::parse(""), SendTier::Default);
    }

    #[test]
    fn parse_unknown_value_falls_back_to_default_without_panicking() {
        assert_eq!(SendTier::parse("ultra-mega-fast"), SendTier::Default);
        assert_eq!(SendTier::parse("low"), SendTier::Default); // not built yet
    }

    #[test]
    fn default_impl_matches_default_variant() {
        assert_eq!(SendTier::default(), SendTier::Default);
    }
}
