//! Pure validation of extension claims and emitted channels.
//!
//! Namespace enforcement [D-ns]: an extension may only emit under its
//! claimed path ids and registered channel prefixes, and registration
//! claims must not collide with any other extension's routes or prefixes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, bail};
use tracing::warn;
use whdr_proto::{Event, validate_channel};

pub(crate) fn filter_owned_events(
    ext_id: &str,
    paths: &[String],
    registered_prefixes: &[String],
    violations: &AtomicUsize,
    events: Vec<Event>,
) -> Vec<Event> {
    events
        .into_iter()
        .filter_map(|event| {
            match validate_emitted_channel(&event.channel, ext_id, paths, registered_prefixes) {
                Ok(()) => Some(event),
                Err(err) => {
                    let count = violations.fetch_add(1, Ordering::Relaxed) + 1;
                    warn!(
                        ext = ext_id,
                        channel = event.channel,
                        count,
                        error = %err,
                        "extension emitted unauthorized channel"
                    );
                    None
                }
            }
        })
        .collect()
}

pub(crate) fn validate_registration_claims(
    id: &str,
    claims: &[String],
    channel_prefixes: &[String],
    routes: &HashMap<String, String>,
    existing_prefixes: &HashMap<String, String>,
) -> Result<()> {
    let mut seen_claims = HashMap::new();
    for claim in claims {
        validate_path_claim(claim)?;
        if seen_claims.insert(claim.as_str(), ()).is_some() {
            bail!("duplicate path claim: {claim}");
        }
        if let Some(owner) = routes.get(claim) {
            bail!("path collision: {claim} already claimed by {owner}");
        }
        for (prefix, owner) in existing_prefixes {
            if owner != id && first_channel_segment(prefix) == claim {
                bail!("path claim {claim} collides with channel prefix {prefix} owned by {owner}");
            }
        }
    }

    let mut seen_prefixes: Vec<&str> = Vec::new();
    for prefix in channel_prefixes {
        validate_channel(prefix).with_context(|| format!("invalid channel prefix: {prefix}"))?;
        for seen in &seen_prefixes {
            if channel_prefixes_overlap(prefix, seen) {
                bail!("channel prefix {prefix} collides with channel prefix {seen}");
            }
        }
        seen_prefixes.push(prefix.as_str());

        let first = first_channel_segment(prefix);
        if let Some(owner) = routes.get(first)
            && owner != id
        {
            bail!("channel prefix {prefix} collides with route {first} owned by {owner}");
        }
        for (existing, owner) in existing_prefixes {
            if owner != id && channel_prefixes_overlap(prefix, existing) {
                bail!(
                    "channel prefix {prefix} collides with channel prefix {existing} owned by {owner}"
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_emitted_channel(
    channel: &str,
    ext_id: &str,
    claims: &[String],
    registered_prefixes: &[String],
) -> Result<()> {
    validate_channel(channel).with_context(|| format!("invalid event channel: {channel}"))?;
    let first = first_channel_segment(channel);
    if claims.iter().any(|claim| claim == first)
        || registered_prefixes
            .iter()
            .any(|prefix| channel_is_in_prefix(channel, prefix))
    {
        return Ok(());
    }
    bail!("channel {channel} is not owned by extension {ext_id}");
}

pub(crate) fn first_channel_segment(channel: &str) -> &str {
    channel.split('.').next().unwrap_or_default()
}

fn channel_is_in_prefix(channel: &str, prefix: &str) -> bool {
    channel == prefix
        || channel
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn channel_prefixes_overlap(left: &str, right: &str) -> bool {
    channel_is_in_prefix(left, right) || channel_is_in_prefix(right, left)
}

pub(crate) fn validate_path_claim(claim: &str) -> Result<()> {
    if claim.is_empty()
        || claim.contains('/')
        || !claim
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        bail!("invalid path claim: {claim}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_rejects_channel_prefix_that_collides_with_existing_route() {
        let mut routes = HashMap::new();
        routes.insert("github".to_string(), "github".to_string());
        let prefixes = HashMap::new();

        let err = validate_registration_claims(
            "teams",
            &["teams".to_string()],
            &["github.notifications".to_string()],
            &routes,
            &prefixes,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("channel prefix github.notifications collides with route github")
        );
    }

    #[test]
    fn emitted_event_channel_must_be_valid_and_owned() {
        let registered_prefixes = vec!["alerts.ops".to_string()];

        assert!(
            validate_emitted_channel(
                "teams.message",
                "teams",
                &["teams".to_string()],
                &registered_prefixes
            )
            .is_ok()
        );
        assert!(
            validate_emitted_channel(
                "alerts.ops.high",
                "teams",
                &["teams".to_string()],
                &registered_prefixes
            )
            .is_ok()
        );

        let err = validate_emitted_channel(
            "github.push",
            "teams",
            &["teams".to_string()],
            &registered_prefixes,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("channel github.push is not owned by extension teams")
        );
    }
}
