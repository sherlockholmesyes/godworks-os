use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread;

use serde_json::Value;

const DEFAULT_TAPE_CAPACITY: usize = 8192;

#[derive(Clone)]
pub struct ReplayTape {
    tx: SyncSender<String>,
    dropped: Arc<AtomicU64>,
}

impl ReplayTape {
    pub fn from_env() -> Option<Self> {
        let path = std::env::var("GW_REPLAY_TAPE")
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        let capacity = std::env::var("GW_REPLAY_TAPE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_TAPE_CAPACITY);
        match Self::open_path(path, capacity) {
            Ok(tape) => Some(tape),
            Err(err) => {
                eprintln!("[replay-tape] disabled: failed to open GW_REPLAY_TAPE: {err}");
                None
            }
        }
    }

    pub fn open_path(path: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (tx, rx) = sync_channel::<String>(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        thread::Builder::new()
            .name("godworks-replay-tape".to_string())
            .spawn(move || {
                let mut file = file;
                while let Ok(line) = rx.recv() {
                    if writeln!(file, "{line}").is_err() {
                        break;
                    }
                    let _ = file.flush();
                }
            })?;
        Ok(Self { tx, dropped })
    }

    pub fn record(&self, event: Value) {
        let event = sanitize_tape_event(event);
        let Ok(line) = serde_json::to_string(&event) else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        match self.tx.try_send(line) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn sanitize_tape_event(event: Value) -> Value {
    match event {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter_map(|(key, value)| {
                    if matches!(
                        key.as_str(),
                        "auth_token" | "value" | "payload" | "components" | "updates"
                    ) {
                        None
                    } else {
                        Some((key, sanitize_tape_event(value)))
                    }
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(sanitize_tape_event).collect()),
        other => other,
    }
}
