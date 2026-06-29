set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# Show available commands.
default:
    @just --list

# Format the whole repository.
fmt:
    cargo fmt --all

# Check formatting without rewriting files.
fmt-check:
    cargo fmt --all -- --check

# Compile all targets in debug mode.
check:
    cargo check --all-targets

# Run clippy over all targets.
lint:
    cargo clippy --all-targets

# Run the test suite.
test:
    cargo test --all-targets

# Build optimized binaries.
build-release:
    cargo build --release

# Run the complete local baseline gate used before opening product-hardening PRs.
gate: fmt-check check lint test build-release

# Start a local broker with a throwaway WAL.
run-broker:
    mkdir -p .local
    GW_WAL=.local/godworks-dev.wal cargo run --bin godworks_broker

# Start a local W zone worker against the default broker port.
run-worker-w:
    GW_ZW_REGION=W GW_ZW_ID=zw-W cargo run --bin zone_worker

# Start a local E zone worker against the default broker port.
run-worker-e:
    GW_ZW_REGION=E GW_ZW_ID=zw-E cargo run --bin zone_worker

# Run the reality harness against a broker on the default port.
loadgen-single:
    cargo run --bin reality_loadgen
