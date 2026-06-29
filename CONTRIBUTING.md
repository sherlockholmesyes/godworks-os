# Contributing to Godworks OS

Godworks OS is being hardened from a pre-1.0 spatial backend prototype into a product-quality 2D distributed-authority runtime. This document defines the minimum engineering workflow for changes.

## Local gate

Run the full baseline before opening a PR:

```bash
just gate
```

Equivalent commands:

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo clippy --all-targets
cargo test --all-targets
cargo build --release
```

## Change rules

Persistent world-state mutations must go through the durable transition path:

1. validate authority and ACLs;
2. write the WAL record;
3. cross the durability barrier;
4. mutate broker memory;
5. publish to interested workers/clients.

Do not publish persistent state that recovery cannot reproduce.

## Protocol changes

Any new wire operation or field must update:

- `docs/protocol/v1-draft.md`;
- protocol golden tests, once the typed protocol crate exists;
- compatibility notes when behavior changes.

## Runtime safety rules

- No unbounded per-client egress queues.
- No global world-state mutation before authority checks.
- No broker-to-broker handoff without durable source-side state.
- No ghost entity may ever receive authoritative grants.
- No security-sensitive component may be mutable by an unprivileged worker.

## Documentation rules

A product-facing feature is not done until it has:

- a short concept explanation;
- one runnable example or test;
- failure-mode notes when applicable;
- updated configuration documentation.

## Pull request checklist

- [ ] `just gate` passes locally or the PR explains why it cannot yet pass.
- [ ] Persistent-state changes include recovery tests.
- [ ] Protocol changes update protocol docs.
- [ ] New runtime flags/configs update ops docs.
- [ ] New failure modes update runbook docs.
