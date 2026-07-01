# System Laws Gate

Godworks has many typed rails already: authority epochs, WAL envelopes,
snapshot manifests, partition maps, replay tape metadata, security roles, and
client cache contracts. This gate keeps those rails from becoming disconnected
checklists.

The system laws are the small set of invariants that every material change must
respect:

- execution: when a transition becomes visible;
- consistency: which writer or observer can see which state;
- time: which ordering source is authoritative;
- failure: what survives crashes, retries, duplicates, and reconnects;
- data lifecycle: how runtime state becomes telemetry, replay, snapshot, and
  model-plane input without gaining hidden authority.

## CI Gate

CI validates the machine-readable law index:

```bash
python3 tools/system_laws_lint.py --laws docs/ops/system-laws.jsonl
```

The Rust baseline also binds law rows to the current Rust test inventory:

```bash
python3 tools/system_laws_test_inventory.py --laws docs/ops/system-laws.jsonl
```

This runs `cargo test --workspace --all-targets -- --list` and fails if a
`current_gates[].command` names a `cargo test` filter that no longer resolves
to a real test. It does not replace running the tests; it prevents stale or
invented law gates from surviving as documentation.

CI also runs a deliberately malformed fixture and expects it to fail:

```bash
python3 tools/system_laws_lint.py \
  --laws docs/ops/examples/system-laws.invalid-missing-fail-gate.jsonl
```

The linter does not prove the runtime by itself. It prevents a weaker failure:
a PR claiming a law exists while omitting the exact boundary, current gates,
known gaps, or fail-under-broken check that would catch a violation.

## Promotion Rule

A law row can be promoted from seed to protected only when it names:

- the runtime boundary;
- the visibility or ordering rule;
- current CI/tool/runtime gates;
- a fail-under-broken gate with expected failure;
- known gaps and explicit non-scope.

For broad laws, partial coverage is honest only when the row says what remains
uncovered. A public claim that a law is fully protected must point to a concrete
test, fixture, or runtime ruler that would fail when the law is broken.

## Files

- `docs/ops/system-laws.jsonl` is the current law index.
- `docs/ops/examples/system-laws.invalid-missing-fail-gate.jsonl` proves the
  linter rejects a row without a fail-under-broken gate.
- `docs/ops/examples/system-laws.valid-test-inventory.jsonl` and
  `docs/ops/examples/system-laws.invalid-missing-test.jsonl` prove the
  inventory checker accepts an existing cargo test gate and rejects a stale one.
- `tools/system_laws_lint.py` validates the index without external
  dependencies.
- `tools/system_laws_test_inventory.py` validates that law rows pointing at
  Rust tests still point at tests in the current workspace inventory.

