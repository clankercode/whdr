use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use rand::random;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use whdr_proto::TokenSummary;

use crate::config::enforce_0600;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRecord {
    pub hash: String,
    pub created: String,
}

#[derive(Clone, Debug)]
pub struct TokenStore {
    path: PathBuf,
    tokens: BTreeMap<String, TokenRecord>,
}

impl TokenStore {
    pub fn load_or_empty(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.exists() {
            return Ok(Self {
                path,
                tokens: BTreeMap::new(),
            });
        }
        enforce_0600(&path, "token store")?;
        let text = fs::read_to_string(&path)
            .with_context(|| format!("read token store {}", path.display()))?;
        let tokens = if text.trim().is_empty() {
            BTreeMap::new()
        } else {
            toml::from_str(&text).context("parse token store toml")?
        };
        Ok(Self { path, tokens })
    }

    pub fn add(&mut self, name: &str) -> Result<String> {
        validate_name(name)?;
        if self.tokens.contains_key(name) {
            bail!("token name already exists: {name}");
        }
        let token = mint_token();
        self.tokens.insert(name.to_string(), record_for(&token));
        self.persist()?;
        Ok(token)
    }

    pub fn rotate(&mut self, name: &str) -> Result<String> {
        validate_name(name)?;
        if !self.tokens.contains_key(name) {
            bail!("unknown token name: {name}");
        }
        let token = mint_token();
        self.tokens.insert(name.to_string(), record_for(&token));
        self.persist()?;
        Ok(token)
    }

    pub fn revoke(&mut self, name: &str) -> Result<()> {
        if self.tokens.remove(name).is_none() {
            bail!("unknown token name: {name}");
        }
        self.persist()
    }

    pub fn authenticate(&self, presented: &str) -> Option<String> {
        let hash = hash_token(presented);
        self.tokens.iter().find_map(|(name, record)| {
            if record.hash.as_bytes().ct_eq(hash.as_bytes()).into() {
                Some(name.clone())
            } else {
                None
            }
        })
    }

    pub fn list(&self, active_conns: &BTreeMap<String, usize>) -> Vec<TokenSummary> {
        self.tokens
            .iter()
            .map(|(name, record)| TokenSummary {
                name: name.clone(),
                fingerprint: fingerprint(&record.hash),
                created: record.created.clone(),
                active_conns: active_conns.get(name).copied().unwrap_or_default(),
            })
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.tokens.keys().cloned().collect()
    }

    pub fn invalidated_names(&self, replacement: &Self) -> Vec<String> {
        self.tokens
            .iter()
            .filter_map(|(name, record)| {
                if replacement.tokens.get(name) == Some(record) {
                    None
                } else {
                    Some(name.clone())
                }
            })
            .collect()
    }

    fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create token store dir {}", parent.display()))?;
        }
        let tmp = self.path.with_extension("toml.tmp");
        let text = toml::to_string_pretty(&self.tokens).context("serialize token store")?;
        {
            let mut file =
                File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .with_context(|| format!("chmod {}", tmp.display()))?;
            file.write_all(text.as_bytes())
                .with_context(|| format!("write {}", tmp.display()))?;
            file.sync_all()
                .with_context(|| format!("fsync {}", tmp.display()))?;
        }
        fs::rename(&tmp, &self.path)
            .with_context(|| format!("rename {} to {}", tmp.display(), self.path.display()))?;
        if let Some(parent) = self.path.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).with_context(|| format!("open dir {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync dir {}", path.display()))?;
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("invalid token name: {name}");
    }
    Ok(())
}

fn mint_token() -> String {
    let bytes: [u8; 32] = random();
    format!("tok_{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn record_for(token: &str) -> TokenRecord {
    TokenRecord {
        hash: hash_token(token),
        created: Utc::now().to_rfc3339(),
    }
}

fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    format!("sha256:{}", hex::encode(digest))
}

fn fingerprint(hash: &str) -> String {
    hash.chars()
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}
