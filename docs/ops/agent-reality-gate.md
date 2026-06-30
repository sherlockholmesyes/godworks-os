# Agent Reality Gate

Godworks OS can use LLM agents and external reviewers, but the product cannot
trust an agent because it produced a plausible explanation. Agent work is
accepted only when it leaves a small, checkable trace and passes a task relative
gate.

This is not a second runtime or a replacement for CI. It is the thin operating
layer around agent contributions:

```text
agent work -> trace -> eval case -> promotion gate -> repo change
```

## CI Gate

CI validates this scaffold directly:

```bash
python3 tools/agent_reality_gate_lint.py \
  --schema docs/ops/agent-trace.schema.json \
  --evals docs/ops/agent-eval-cases.jsonl \
  --trace docs/ops/examples/agent-trace.example.json
```

This keeps the trace schema, eval ledger, example trace, and linter executable.
It does not require every ordinary manual PR to include a new trace. A
PR-specific trace is required only when a maintainer marks the work as
agent-produced, promotes an external-review output into repo rules, or asks for
an explicit reality gate on that change.

## What To Record

Every material agent contribution should have a trace:

- the task and intended invariant;
- the agent role;
- the prompt or skill versions if known;
- the files or subsystems inspected;
- the action type: review, patch, test, docs, or ops;
- the commands or reality gates run;
- the failure tags, if the contribution was invalid or incomplete;
- the decision: accept, revise, reject, or park.

The trace should be concise. It should point to the PR, commit, CI run, or
artifact rather than copying logs.

## Promotion Rule

An agent-derived rule, prompt, checklist, or implementation pattern can be
promoted only when all of these are true:

- it fixes a named failure class;
- it has at least one eval case or regression test that would fail without the
  rule;
- it does not broaden into vague advice;
- it preserves existing successful behavior;
- it does not leak private process, identities, keys, or non-public research
  terms.

## Initial Godworks Failure Classes

The first eval set focuses on failure classes already relevant to this runtime:

- authority handoff without epoch fencing;
- mesh adoption above durable source state;
- AOI/interest changes without enter/leave boundary tests;
- WAL/recovery changes without replay or crash-gate coverage;
- security changes that only add a config flag without a fail-closed test;
- docs that claim product readiness without a runtime proof.

## Files

- `docs/ops/agent-trace.schema.json` defines the trace shape.
- `docs/ops/agent-eval-cases.jsonl` contains the first eval cases.
- `docs/ops/examples/agent-trace.example.json` is a filled example.
- `tools/agent_reality_gate_lint.py` validates the scaffold without external
  dependencies.

Run:

```bash
python tools/agent_reality_gate_lint.py \
  --schema docs/ops/agent-trace.schema.json \
  --evals docs/ops/agent-eval-cases.jsonl \
  --trace docs/ops/examples/agent-trace.example.json
```

The linter is intentionally small. It checks structure and obvious empty gates;
CI and runtime tests remain the source of truth.
