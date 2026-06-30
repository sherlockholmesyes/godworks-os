use godworks_broker::wal::read_wal_events;
use serde_json::json;

fn usage() {
    eprintln!("usage: wal_inspect <path> [--up-to-offset <bytes>]");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        usage();
        std::process::exit(64);
    };

    let mut up_to_offset = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--up-to-offset" => {
                let Some(raw) = args.next() else {
                    usage();
                    std::process::exit(64);
                };
                up_to_offset = match raw.parse::<u64>() {
                    Ok(v) => Some(v),
                    Err(_) => {
                        usage();
                        std::process::exit(64);
                    }
                };
            }
            _ => {
                usage();
                std::process::exit(64);
            }
        }
    }

    let scan = read_wal_events(&path, up_to_offset);
    let report = scan.report;
    let out = json!({
        "wal": path,
        "wal_version": report.wal_version,
        "selected_event_count": report.selected_event_count,
        "decoded_record_count": report.decoded_record_count,
        "corrupt_tail_record_count": report.corrupt_tail_record_count,
        "truncated_tail_bytes": report.truncated_tail_bytes,
        "recoverable_prefix_bytes": report.recoverable_prefix_bytes,
        "unknown_kind_count": report.unknown_kind_count,
        "kind_counts": report.kind_counts,
        "unknown_kinds": report.unknown_kinds,
        "error": report.error,
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
    std::process::exit(if out.get("error").and_then(|v| v.as_str()).is_some() {
        2
    } else {
        0
    });
}
