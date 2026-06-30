use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use serde_json::{json, Value};

pub const WAL_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq)]
pub enum WalLine {
    Ok(Value),
    Corrupt,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WalReadReport {
    pub wal_version: u64,
    pub selected_event_count: u64,
    pub decoded_record_count: u64,
    pub corrupt_tail_record_count: u64,
    pub truncated_tail_bytes: u64,
    pub unknown_kind_count: u64,
    pub kind_counts: BTreeMap<String, u64>,
    pub unknown_kinds: BTreeMap<String, u64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WalReadResult {
    pub events: Vec<Value>,
    pub report: WalReadReport,
}

// CRC32 (IEEE 802.3, the zlib/PNG polynomial) -- self-contained so the broker
// adds no new crate dependency.
pub fn crc32_ieee(bytes: &[u8]) -> u32 {
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        let mut i = 0usize;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xEDB88320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    });
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

pub fn wal_v1_header_line() -> String {
    serde_json::to_string(&json!({ "kind": "wal_header", "wal_version": WAL_VERSION })).unwrap()
}

pub fn wal_v1_envelope_line(ev: &Value) -> String {
    let payload = serde_json::to_string(ev).unwrap();
    let crc = crc32_ieee(payload.as_bytes());
    serde_json::to_string(&json!({ "_c": crc, "_d": payload })).unwrap()
}

pub fn decode_wal_line(line: &str, v1_mode: bool) -> WalLine {
    if let Ok(v) = serde_json::from_str::<Value>(line) {
        if let (Some(c), Some(d)) = (
            v.get("_c").and_then(|x| x.as_u64()),
            v.get("_d").and_then(|x| x.as_str()),
        ) {
            if crc32_ieee(d.as_bytes()) != c as u32 {
                return WalLine::Corrupt;
            }
            return match serde_json::from_str::<Value>(d) {
                Ok(ev) => WalLine::Ok(ev),
                Err(_) => WalLine::Corrupt,
            };
        }
        if v1_mode {
            return WalLine::Corrupt;
        }
        return WalLine::Ok(v);
    }
    WalLine::Corrupt
}

pub fn wal_event_kind(ev: &Value) -> String {
    ev.get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

pub fn is_known_wal_event_kind(kind: &str) -> bool {
    matches!(
        kind,
        "register"
            | "write"
            | "component_add"
            | "component_remove"
            | "delete_tombstone"
            | "transfer"
            | "authority_epoch"
            | "failover_grant"
            | "block_migration"
            | "component_authority"
            | "threshold_prepare"
            | "threshold_preload_ready"
            | "threshold_commit"
            | "threshold_adopt"
            | "threshold_abort"
            | "mesh_out"
            | "mesh_acked"
            | "partition_config"
            | "reserve_entity_ids"
            | "snapshot_marker"
    )
}

pub fn read_wal_events(path: &str, up_to_offset: Option<u64>) -> WalReadResult {
    let raw_lines: Vec<String> = match File::open(path) {
        Ok(f) => BufReader::new(f).lines().map_while(Result::ok).collect(),
        Err(_) => {
            return WalReadResult {
                events: Vec::new(),
                report: WalReadReport {
                    wal_version: WAL_VERSION,
                    ..WalReadReport::default()
                },
            };
        }
    };

    let mut wal_version: u64 = 0;
    let mut first_idx: Option<usize> = None;
    for (i, raw) in raw_lines.iter().enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        first_idx = Some(i);
        if let Ok(v) = serde_json::from_str::<Value>(raw.trim()) {
            if v.get("kind").and_then(|k| k.as_str()) == Some("wal_header") {
                wal_version = v.get("wal_version").and_then(|x| x.as_u64()).unwrap_or(0);
            }
        }
        break;
    }

    if wal_version > WAL_VERSION {
        return WalReadResult {
            events: Vec::new(),
            report: WalReadReport {
                wal_version,
                error: Some(format!(
                    "VersionReject: WAL wal_version={} is newer than this broker supports (max {}); refusing to recover",
                    wal_version, WAL_VERSION
                )),
                ..WalReadReport::default()
            },
        };
    }

    let v1_mode = wal_version >= 1;
    let header_idx = if v1_mode { first_idx } else { None };

    struct Rec {
        ev: Option<Value>,
        bytes: u64,
        cum_end: u64,
    }

    let mut recs: Vec<Rec> = Vec::with_capacity(raw_lines.len());
    let mut cumulative: u64 = 0;
    for (i, raw) in raw_lines.iter().enumerate() {
        let span = raw.len() as u64 + 1;
        cumulative += span;
        let trimmed = raw.trim();
        if trimmed.is_empty() || Some(i) == header_idx {
            continue;
        }
        let ev = match decode_wal_line(trimmed, v1_mode) {
            WalLine::Ok(v) => {
                if v.get("kind").and_then(|k| k.as_str()) == Some("wal_header") {
                    continue;
                }
                Some(v)
            }
            WalLine::Corrupt => None,
        };
        recs.push(Rec {
            ev,
            bytes: span,
            cum_end: cumulative,
        });
    }

    let mut truncated_tail_bytes: u64 = 0;
    let mut tail_corrupt = 0usize;
    let mut good_prefix_len = recs.len();
    if v1_mode {
        for r in recs.iter().rev() {
            if r.ev.is_none() {
                tail_corrupt += 1;
                truncated_tail_bytes += r.bytes;
            } else {
                break;
            }
        }
        good_prefix_len = recs.len() - tail_corrupt;
        let mid_corrupt = recs[..good_prefix_len].iter().any(|r| r.ev.is_none());
        if mid_corrupt {
            return WalReadResult {
                events: Vec::new(),
                report: WalReadReport {
                    wal_version,
                    error: Some(
                        "RestoreIntegrityError: corrupt WAL record(s) in the MIDDLE of the log (a valid record follows a CRC failure) -- refusing to serve partial/forked state. Repair or restore from a snapshot."
                            .to_string(),
                    ),
                    ..WalReadReport::default()
                },
            };
        }
    }

    let mut events = Vec::new();
    let mut kind_counts = BTreeMap::new();
    let mut unknown_kinds = BTreeMap::new();
    let mut decoded_record_count = 0u64;

    for r in &recs[..good_prefix_len] {
        let include = match up_to_offset {
            Some(off) => r.cum_end <= off,
            None => true,
        };
        if !include {
            continue;
        }
        if let Some(ev) = r.ev.as_ref() {
            decoded_record_count += 1;
            let kind = wal_event_kind(ev);
            *kind_counts.entry(kind.clone()).or_insert(0) += 1;
            if !is_known_wal_event_kind(&kind) {
                *unknown_kinds.entry(kind).or_insert(0) += 1;
            }
            events.push(ev.clone());
        }
    }

    let unknown_kind_count = unknown_kinds.values().sum();
    WalReadResult {
        report: WalReadReport {
            wal_version,
            selected_event_count: events.len() as u64,
            decoded_record_count,
            corrupt_tail_record_count: tail_corrupt as u64,
            truncated_tail_bytes,
            unknown_kind_count,
            kind_counts,
            unknown_kinds,
            error: None,
        },
        events,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_wal(name: &str, lines: &[String]) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "godworks_{name}_{}_{}.wal",
            std::process::id(),
            nanos
        ));
        let mut body = lines.join("\n");
        body.push('\n');
        fs::write(&path, body).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn shared_wal_reader_counts_unknown_kinds() {
        let path = temp_wal(
            "unknown_kind",
            &[
                wal_v1_header_line(),
                wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"})),
                wal_v1_envelope_line(&json!({"kind":"future_transition","entity":"e1"})),
            ],
        );

        let read = read_wal_events(&path, None);
        assert!(read.report.error.is_none());
        assert_eq!(read.events.len(), 2);
        assert_eq!(read.report.unknown_kind_count, 1);
        assert_eq!(
            read.report.unknown_kinds.get("future_transition").copied(),
            Some(1)
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn shared_wal_reader_truncates_corrupt_tail() {
        let path = temp_wal(
            "corrupt_tail",
            &[
                wal_v1_header_line(),
                wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"})),
                "{\"_c\":0,\"_d\":\"{\\\"kind\\\":\\\"write\\\"}\"}".to_string(),
            ],
        );

        let read = read_wal_events(&path, None);
        assert!(read.report.error.is_none());
        assert_eq!(read.events.len(), 1);
        assert_eq!(read.report.corrupt_tail_record_count, 1);
        assert!(read.report.truncated_tail_bytes > 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn shared_wal_reader_refuses_mid_corruption() {
        let path = temp_wal(
            "mid_corruption",
            &[
                wal_v1_header_line(),
                wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"})),
                "{\"_c\":0,\"_d\":\"{\\\"kind\\\":\\\"write\\\"}\"}".to_string(),
                wal_v1_envelope_line(&json!({"kind":"write","entity":"e1"})),
            ],
        );

        let read = read_wal_events(&path, None);
        assert!(read.report.error.is_some());
        assert!(read.events.is_empty());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn shared_wal_reader_refuses_future_version() {
        let path = temp_wal(
            "future_version",
            &[
                serde_json::to_string(&json!({"kind":"wal_header","wal_version":99})).unwrap(),
                wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"})),
            ],
        );

        let read = read_wal_events(&path, None);
        assert_eq!(read.report.wal_version, 99);
        assert!(read
            .report
            .error
            .unwrap_or_default()
            .contains("VersionReject"));
        assert!(read.events.is_empty());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn shared_wal_reader_honors_restore_offset() {
        let header = wal_v1_header_line();
        let first = wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"}));
        let second = wal_v1_envelope_line(&json!({"kind":"register","entity":"e2"}));
        let offset_after_first = (header.len() + 1 + first.len() + 1) as u64;
        let path = temp_wal("restore_offset", &[header, first, second]);

        let read = read_wal_events(&path, Some(offset_after_first));
        assert!(read.report.error.is_none());
        assert_eq!(read.events.len(), 1);
        assert_eq!(read.report.selected_event_count, 1);
        assert_eq!(
            read.events[0].get("entity").and_then(|v| v.as_str()),
            Some("e1")
        );

        let _ = fs::remove_file(path);
    }
}
