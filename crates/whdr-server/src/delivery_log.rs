//! Durable delivery log ([D-store]): a single-writer, crash-safe append log
//! over an embedded `redb` file. Every fanned-out event is appended under a
//! gapless global `seq` before delivery, so a reconnecting subscriber can
//! resume from a cursor (§9.4). Retention is TTL + size bounded and pruned
//! from the front (lowest seq = oldest).
//!
//! Never persists tokens or provider secrets: a stored row is only
//! `{seq, id, ts_ms, channel, payload_b64}`. At rest the file is `0600` in a
//! `0700` state dir; on reopen we enforce `0600` and refuse to start
//! otherwise, mirroring the token/secret stores ([D-dursec]).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use redb::{Database, ReadableTable, TableDefinition, TableError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use whdr_proto::{Event, Pattern};

use crate::config::{DeliveryConfig, enforce_0600};

const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");

/// On-disk value bytes for one stored event. Deliberately excludes any
/// token/secret/identity — only the fields a replay needs.
#[derive(Serialize, Deserialize)]
struct StoredEvent {
    id: Uuid,
    ts_ms: u64,
    channel: String,
    payload_b64: String,
}

/// An event with its durable identity/sequence stamped by the log.
pub(crate) struct StampedEvent {
    pub(crate) seq: u64,
    pub(crate) id: Uuid,
    pub(crate) ts_ms: u64,
    pub(crate) channel: String,
    pub(crate) payload_b64: String,
}

struct Shared {
    db: Database,
    /// Serialises writes so seq allocation + commit is atomic and gapless.
    /// A `std::sync::Mutex` because writes run on a `spawn_blocking` thread
    /// (append) or synchronously (prune), never held across an `.await`.
    writer: std::sync::Mutex<()>,
    head: AtomicU64,
    floor: AtomicU64,
    retained_count: AtomicU64,
    retained_bytes: AtomicU64,
    retention_secs: u64,
    max_bytes: u64,
    max_events: u64,
}

pub(crate) struct DeliveryLog {
    shared: Arc<Shared>,
}

impl DeliveryLog {
    pub(crate) fn open(cfg: &DeliveryConfig) -> Result<Self> {
        let path = cfg.store_path.as_path();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("create delivery dir {}", parent.display()))?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("chmod 0700 delivery dir {}", parent.display()))?;
        }

        let existed = path.exists();
        if existed {
            enforce_0600(path, "delivery log")?;
        }
        let db = Database::create(path)
            .with_context(|| format!("open delivery log {}", path.display()))?;
        if !existed {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .with_context(|| format!("chmod 0600 delivery log {}", path.display()))?;
        }

        // Ensure the table exists so read transactions never race a missing
        // table, then scan once to recover head/floor/counters.
        {
            let write_txn = db.begin_write()?;
            write_txn.open_table(EVENTS)?;
            write_txn.commit()?;
        }
        let (head, floor, count, bytes) = {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(EVENTS)?;
            let mut head = 0u64;
            let mut floor = 0u64;
            let mut count = 0u64;
            let mut bytes = 0u64;
            for item in table.iter()? {
                let (key, value) = item?;
                let seq = key.value();
                if floor == 0 {
                    floor = seq;
                }
                head = seq;
                count += 1;
                bytes += value.value().len() as u64;
            }
            (head, floor, count, bytes)
        };

        let shared = Arc::new(Shared {
            db,
            writer: std::sync::Mutex::new(()),
            head: AtomicU64::new(head),
            floor: AtomicU64::new(floor),
            retained_count: AtomicU64::new(count),
            retained_bytes: AtomicU64::new(bytes),
            retention_secs: cfg.retention_secs,
            max_bytes: cfg.max_bytes,
            max_events: cfg.max_events,
        });
        let log = Self { shared };
        // One prune pass on boot to shed anything already past its window.
        log.prune(crate::subscribers::now_unix_ms())?;
        Ok(log)
    }

    /// Serialised single writer: allocate a contiguous seq run, stamp
    /// id/ts, write all rows in one redb transaction, commit (one fsync),
    /// and return the stamped events. Gapless by construction.
    pub(crate) async fn append(
        &self,
        events: Vec<Event>,
        ts_ms: u64,
    ) -> Result<Vec<StampedEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || shared.append_blocking(events, ts_ms))
            .await
            .context("delivery append task")?
    }

    /// Range scan `(after_seq, head]`, optionally filtered by patterns, in
    /// ascending seq order. No lock: uses a redb read transaction.
    pub(crate) fn read_after(
        &self,
        after_seq: u64,
        patterns: Option<&[Pattern]>,
    ) -> Result<Vec<StampedEvent>> {
        let read_txn = self.shared.db.begin_read()?;
        let table = match read_txn.open_table(EVENTS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let start = after_seq.saturating_add(1);
        let mut out = Vec::new();
        for item in table.range(start..)? {
            let (key, value) = item?;
            let seq = key.value();
            let stored: StoredEvent = serde_json::from_slice(value.value())?;
            if let Some(patterns) = patterns {
                let matches = patterns
                    .iter()
                    .any(|pattern| pattern.matches(&stored.channel).unwrap_or(false));
                if !matches {
                    continue;
                }
            }
            out.push(StampedEvent {
                seq,
                id: stored.id,
                ts_ms: stored.ts_ms,
                channel: stored.channel,
                payload_b64: stored.payload_b64,
            });
        }
        Ok(out)
    }

    /// Prune the front (oldest) while it is past the TTL or the size/count
    /// caps are exceeded. Returns the number of events removed.
    pub(crate) fn prune(&self, now_ms: u64) -> Result<u64> {
        self.shared.prune_blocking(now_ms)
    }

    pub(crate) fn head_seq(&self) -> u64 {
        self.shared.head.load(Ordering::Relaxed)
    }

    pub(crate) fn floor_seq(&self) -> u64 {
        self.shared.floor.load(Ordering::Relaxed)
    }

    pub(crate) fn retained_count(&self) -> u64 {
        self.shared.retained_count.load(Ordering::Relaxed)
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.shared.retained_bytes.load(Ordering::Relaxed)
    }
}

impl Shared {
    fn append_blocking(&self, events: Vec<Event>, ts_ms: u64) -> Result<Vec<StampedEvent>> {
        let _guard = self.writer.lock().expect("delivery writer poisoned");
        let base = self.head.load(Ordering::Relaxed);
        let mut stamped = Vec::with_capacity(events.len());
        let mut rows: Vec<(u64, Vec<u8>)> = Vec::with_capacity(events.len());
        let mut bytes_added = 0u64;
        for (offset, event) in events.into_iter().enumerate() {
            let seq = base + 1 + offset as u64;
            let id = Uuid::new_v4();
            let stored = StoredEvent {
                id,
                ts_ms,
                channel: event.channel,
                payload_b64: event.payload_b64,
            };
            let value = serde_json::to_vec(&stored)?;
            bytes_added += value.len() as u64;
            rows.push((seq, value));
            stamped.push(StampedEvent {
                seq,
                id,
                ts_ms,
                channel: stored.channel,
                payload_b64: stored.payload_b64,
            });
        }

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(EVENTS)?;
            for (seq, value) in &rows {
                table.insert(*seq, value.as_slice())?;
            }
        }
        write_txn.commit()?;

        self.head.store(base + rows.len() as u64, Ordering::Relaxed);
        // First-ever append sets the retained floor; later appends leave it.
        let _ = self.floor.compare_exchange(
            0,
            stamped[0].seq,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.retained_count
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        self.retained_bytes.fetch_add(bytes_added, Ordering::Relaxed);
        Ok(stamped)
    }

    fn prune_blocking(&self, now_ms: u64) -> Result<u64> {
        let _guard = self.writer.lock().expect("delivery writer poisoned");
        let mut count = self.retained_count.load(Ordering::Relaxed);
        let mut bytes = self.retained_bytes.load(Ordering::Relaxed);
        let mut pruned = 0u64;
        let new_floor;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(EVENTS)?;
            loop {
                let front = {
                    let mut iter = table.iter()?;
                    match iter.next() {
                        Some(item) => {
                            let (key, value) = item?;
                            let raw = value.value();
                            let stored: StoredEvent = serde_json::from_slice(raw)?;
                            Some((key.value(), raw.len() as u64, stored.ts_ms))
                        }
                        None => None,
                    }
                };
                let Some((seq, value_bytes, ts_ms)) = front else {
                    break;
                };
                // TTL at second granularity (the knob is in seconds; sub-second
                // precision is meaningless for a 24 h window).
                let too_old = now_ms.saturating_sub(ts_ms) / 1000 > self.retention_secs;
                let too_many = count > self.max_events;
                let too_big = bytes > self.max_bytes;
                if !(too_old || too_many || too_big) {
                    break;
                }
                table.remove(seq)?;
                count -= 1;
                bytes = bytes.saturating_sub(value_bytes);
                pruned += 1;
            }
            new_floor = table
                .iter()?
                .next()
                .transpose()?
                .map(|(key, _)| key.value())
                .unwrap_or(0);
        }
        write_txn.commit()?;

        self.retained_count.store(count, Ordering::Relaxed);
        self.retained_bytes.store(bytes, Ordering::Relaxed);
        self.floor.store(new_floor, Ordering::Relaxed);
        Ok(pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ms() -> u64 {
        crate::subscribers::now_unix_ms()
    }

    fn ev(channel: &str, payload: &str) -> Event {
        Event {
            channel: channel.to_string(),
            payload_b64: payload.to_string(),
        }
    }

    fn cfg(dir: &std::path::Path, max_events: u64) -> DeliveryConfig {
        DeliveryConfig {
            enabled: true,
            store_path: dir.join("d.redb"),
            retention_secs: 86_400,
            max_bytes: 1 << 30,
            max_events,
            prune_interval_secs: 300,
        }
    }

    fn open(dir: &std::path::Path) -> DeliveryLog {
        DeliveryLog::open(&cfg(dir, 1_000_000)).unwrap()
    }

    #[tokio::test]
    async fn append_assigns_contiguous_seq_and_reads_back_in_order() {
        let temp = tempfile::tempdir().unwrap();
        let log = open(temp.path());
        let a = log
            .append(vec![ev("dev.one", "MQ=="), ev("dev.two", "Mg==")], now_ms())
            .await
            .unwrap();
        assert_eq!(a[0].seq, 1);
        assert_eq!(a[1].seq, 2);
        let b = log
            .append(vec![ev("dev.three", "Mw==")], now_ms())
            .await
            .unwrap();
        assert_eq!(b[0].seq, 3);

        let read = log.read_after(0, None).unwrap();
        assert_eq!(
            read.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(read[0].id, a[0].id); // stored id == stamped id
    }

    #[tokio::test]
    async fn seq_resumes_across_reopen() {
        let temp = tempfile::tempdir().unwrap();
        {
            let log = open(temp.path());
            log.append(vec![ev("dev.one", "MQ==")], now_ms())
                .await
                .unwrap();
        }
        let log = open(temp.path());
        let next = log
            .append(vec![ev("dev.two", "Mg==")], now_ms())
            .await
            .unwrap();
        assert_eq!(next[0].seq, 2, "seq must not reset on reopen");
        assert_eq!(log.floor_seq(), 1);
        assert_eq!(log.head_seq(), 2);
    }

    #[tokio::test]
    async fn prune_by_ttl_drops_old_events_and_raises_floor() {
        let temp = tempfile::tempdir().unwrap();
        let log = open(temp.path());
        let base = 1_000_000_000_000u64;
        log.append(vec![ev("dev.old", "MQ==")], base).await.unwrap(); // seq 1, old
        log.append(vec![ev("dev.new", "Mg==")], base + 100_000_000)
            .await
            .unwrap(); // seq 2, new
        // TTL 24h; treat "now" as base + 100_000_000 + 24h + 1ms.
        let now = base + 100_000_000 + 86_400_000 + 1;
        let pruned = log.prune(now).unwrap();
        assert_eq!(pruned, 1);
        assert_eq!(log.floor_seq(), 2);
        assert_eq!(
            log.read_after(0, None)
                .unwrap()
                .iter()
                .map(|e| e.seq)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[tokio::test]
    async fn prune_by_max_events_keeps_newest() {
        let temp = tempfile::tempdir().unwrap();
        let log = DeliveryLog::open(&cfg(temp.path(), 2)).unwrap();
        for i in 0..5 {
            log.append(vec![ev("dev.x", "MQ==")], now_ms() + i)
                .await
                .unwrap();
        }
        log.prune(now_ms() + 10).unwrap();
        let seqs: Vec<u64> = log
            .read_after(0, None)
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(seqs, vec![4, 5], "only newest max_events survive");
    }

    #[tokio::test]
    async fn read_after_filters_by_patterns() {
        let temp = tempfile::tempdir().unwrap();
        let log = open(temp.path());
        log.append(
            vec![ev("github.push", "MQ=="), ev("stripe.charge", "Mg==")],
            now_ms(),
        )
        .await
        .unwrap();
        let pats = vec![whdr_proto::Pattern::new("github.>".to_string()).unwrap()];
        let got = log.read_after(0, Some(&pats)).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].channel, "github.push");
    }

    #[test]
    fn on_disk_bytes_never_contain_a_secret_marker() {
        // Payloads are stored, but tokens/secrets never are. Sanity guard.
        let temp = tempfile::tempdir().unwrap();
        let log = open(temp.path());
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            log.append(vec![ev("github.push", "MQ==")], now_ms())
                .await
                .unwrap();
        });
        let raw = std::fs::read(temp.path().join("d.redb")).unwrap();
        assert!(
            !raw.windows(4).any(|w| w == b"tok_"),
            "no subscriber token bytes on disk"
        );
        assert!(
            !raw.windows(6).any(|w| w == b"whsec_"),
            "no provider secret bytes on disk"
        );
    }
}
