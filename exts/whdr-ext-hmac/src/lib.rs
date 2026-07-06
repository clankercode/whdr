//! Generic HMAC signature-verification extension.
//!
//! Verifies an HMAC signature over the exact raw request body and, on success,
//! emits a whdr event carrying that raw body. It lets whdr ingest webhooks from
//! any provider whose scheme is "HMAC of the body, encoded in a header" (Stripe,
//! Linear, Shopify, ...) without a bespoke Rust extension per provider.
//!
//! Non-secret behaviour (header name, algorithm, encoding, prefix to strip,
//! channel prefix) is configured via environment variables read once at startup
//! (see [`HmacConfig::from_env`]). The provider secret is NEVER configured here:
//! it arrives per-request on stdin (keyed by ext id in `secrets.toml`), is never
//! placed on argv, and is never logged (SPEC §12 G4).

use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use subtle::ConstantTimeEq;
use whdr_ext_kit::{decode_body_b64, header_value, text_reply};
use whdr_proto::{Event, HttpReply, SrvMsg};

/// Digest used for the HMAC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    Sha1,
    Sha256,
    Sha512,
}

impl Algorithm {
    fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "sha1" => Ok(Algorithm::Sha1),
            "sha256" => Ok(Algorithm::Sha256),
            "sha512" => Ok(Algorithm::Sha512),
            other => Err(anyhow!(
                "unsupported HMAC algorithm {other:?} (expected sha1, sha256, or sha512)"
            )),
        }
    }

    /// Raw HMAC bytes over `body` keyed by `secret`.
    fn mac(self, secret: &[u8], body: &[u8]) -> Vec<u8> {
        macro_rules! mac {
            ($digest:ty) => {{
                let mut mac =
                    Hmac::<$digest>::new_from_slice(secret).expect("HMAC accepts keys of any size");
                mac.update(body);
                mac.finalize().into_bytes().to_vec()
            }};
        }
        match self {
            Algorithm::Sha1 => mac!(Sha1),
            Algorithm::Sha256 => mac!(Sha256),
            Algorithm::Sha512 => mac!(Sha512),
        }
    }
}

/// How the signature bytes are rendered in the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Hex,
    Base64,
}

impl Encoding {
    fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "hex" => Ok(Encoding::Hex),
            "base64" | "b64" => Ok(Encoding::Base64),
            other => Err(anyhow!(
                "unsupported signature encoding {other:?} (expected hex or base64)"
            )),
        }
    }

    /// Encode raw MAC bytes for display / signing.
    fn encode(self, bytes: &[u8]) -> String {
        match self {
            Encoding::Hex => hex::encode(bytes),
            Encoding::Base64 => STANDARD.encode(bytes),
        }
    }

    /// Decode a header signature into raw bytes. `None` on malformed input.
    fn decode(self, value: &str) -> Option<Vec<u8>> {
        match self {
            Encoding::Hex => hex::decode(value).ok(),
            Encoding::Base64 => STANDARD.decode(value).ok(),
        }
    }
}

/// Non-secret configuration for the extension.
#[derive(Debug, Clone)]
pub struct HmacConfig {
    /// Header carrying the signature (case-insensitive lookup).
    pub header: String,
    /// Digest algorithm (default sha256).
    pub algorithm: Algorithm,
    /// Signature encoding (default hex).
    pub encoding: Encoding,
    /// Optional literal prefix stripped from the header value before decoding
    /// (e.g. `sha256=`). If set and absent from the header, the request is rejected.
    pub prefix: Option<String>,
    /// First channel segment for emitted events (default `hmac`).
    pub channel_prefix: String,
}

impl Default for HmacConfig {
    fn default() -> Self {
        HmacConfig {
            header: "X-Signature".to_string(),
            algorithm: Algorithm::Sha256,
            encoding: Encoding::Hex,
            prefix: None,
            channel_prefix: "hmac".to_string(),
        }
    }
}

impl HmacConfig {
    /// Build config from `WHDR_HMAC_*` environment variables, falling back to
    /// defaults. Returns an error on an unparseable algorithm or encoding so the
    /// operator sees a clear failure at startup instead of silent misbehaviour.
    ///
    /// - `WHDR_HMAC_HEADER` (default `X-Signature`)
    /// - `WHDR_HMAC_ALGORITHM` (`sha256` default; `sha1`, `sha512`)
    /// - `WHDR_HMAC_ENCODING` (`hex` default; `base64`)
    /// - `WHDR_HMAC_PREFIX` (optional; empty is treated as unset)
    /// - `WHDR_HMAC_CHANNEL_PREFIX` (default `hmac`)
    pub fn from_env() -> Result<Self> {
        let default = HmacConfig::default();
        let header = non_empty_env("WHDR_HMAC_HEADER").unwrap_or(default.header);
        let algorithm = match non_empty_env("WHDR_HMAC_ALGORITHM") {
            Some(value) => Algorithm::parse(&value)?,
            None => default.algorithm,
        };
        let encoding = match non_empty_env("WHDR_HMAC_ENCODING") {
            Some(value) => Encoding::parse(&value)?,
            None => default.encoding,
        };
        let prefix = non_empty_env("WHDR_HMAC_PREFIX");
        let channel_prefix = non_empty_env("WHDR_HMAC_CHANNEL_PREFIX")
            .map(|raw| channel_token(&raw))
            .unwrap_or(default.channel_prefix);
        Ok(HmacConfig {
            header,
            algorithm,
            encoding,
            prefix,
            channel_prefix,
        })
    }
}

/// Produce the header value a provider would send for `body`: the encoded MAC,
/// with the configured prefix reapplied. Primarily useful for tests and tooling.
pub fn sign(config: &HmacConfig, secret: &str, body: &[u8]) -> String {
    let mac = config.algorithm.mac(secret.as_bytes(), body);
    let encoded = config.encoding.encode(&mac);
    match &config.prefix {
        Some(prefix) => format!("{prefix}{encoded}"),
        None => encoded,
    }
}

/// Verify the signature on a dispatch and, on success, emit one event carrying
/// the raw body. All verification failures return `401` with no events, matching
/// the rejection semantics of the github/teams extensions.
pub fn handle_hmac_dispatch(config: &HmacConfig, msg: SrvMsg) -> Result<(HttpReply, Vec<Event>)> {
    let SrvMsg::Dispatch {
        path,
        headers,
        body_b64,
        secret,
        ..
    } = msg
    else {
        return Ok((text_reply(200, "shutdown ignored"), vec![]));
    };

    let body = decode_body_b64(&body_b64)?;

    let Some(secret) = secret else {
        return Ok((text_reply(401, "missing secret"), vec![]));
    };
    let Some(header) = header_value(&headers, &config.header) else {
        return Ok((text_reply(401, "missing signature header"), vec![]));
    };

    // Strip the configured prefix; its absence is a rejection, not a silent pass.
    let signature = match &config.prefix {
        Some(prefix) => match header.strip_prefix(prefix.as_str()) {
            Some(rest) => rest,
            None => return Ok((text_reply(401, "malformed signature"), vec![])),
        },
        None => header,
    };

    let Some(provided) = config.encoding.decode(signature.trim()) else {
        return Ok((text_reply(401, "malformed signature"), vec![]));
    };

    let expected = config.algorithm.mac(secret.as_bytes(), &body);

    // Constant-time compare of raw MAC bytes. A length mismatch can't be
    // compared in constant time and is treated as an invalid signature; we use
    // the same generic message so length isn't a distinguishing oracle.
    let matches = provided.len() == expected.len() && provided.ct_eq(&expected).unwrap_u8() == 1;
    if !matches {
        return Ok((text_reply(401, "invalid signature"), vec![]));
    }

    let channel = derive_channel(&config.channel_prefix, &path);
    Ok((
        text_reply(200, "ok"),
        vec![Event {
            channel,
            payload_b64: STANDARD.encode(&body),
        }],
    ))
}

/// Channel = configured prefix, plus any request-path segments beyond the first
/// (the routing mount). `/hmac/Stripe/Foo` with prefix `hmac` -> `hmac.stripe.foo`.
/// Each segment is tokenised to the channel grammar; empty segments are dropped.
fn derive_channel(channel_prefix: &str, path: &str) -> String {
    let mut channel = channel_prefix.to_string();
    for segment in path.split('/').skip(2) {
        let token = channel_token(segment);
        if !token.is_empty() {
            channel.push('.');
            channel.push_str(&token);
        }
    }
    channel
}

/// Normalise an arbitrary string to a single channel-grammar token.
fn channel_token(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        match ch {
            'a'..='z' | '0'..='9' | '_' | '-' => out.push(ch),
            'A'..='Z' => out.push(ch.to_ascii_lowercase()),
            _ => out.push('-'),
        }
    }
    out.trim_matches('-').to_string()
}

fn non_empty_env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}
