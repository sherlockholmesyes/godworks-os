# Godot Content Load Probe

`client_probes/godot/content_load_probe.gd` is a headless Godot client proof for the content manifest contract.

It is deliberately small and uses only the public broker wire protocol:

1. connect as a worker to a live broker;
2. create one asset-bearing entity;
3. issue `EntityQuery`;
4. consume the returned `asset_manifest` and `content_manifest`;
5. resolve the entity's package ids into package rows;
6. expand package assets;
7. verify every required entity asset has a loadable URI/path and hash in the package plan.

This proves a real Godot runtime can consume the manifest as a client package load-set. It is still a headless contract proof, not a visual render proof.

## Run

Start a broker:

```powershell
$env:GW_BIND="127.0.0.1"
$env:GW_PORT="7777"
cargo run --bin godworks_broker
```

Run the Godot probe:

```powershell
$env:GW_HOST="127.0.0.1"
$env:GW_PORT="7777"
godot --headless --path client_probes/godot --script res://content_load_probe.gd
```

Expected success:

```text
GODOT CONTENT-LOAD | connected=true count=1 content_load_ok=1
GODOT CONTENT-LOAD: PASS -- public EntityQueryResponse resolved to a client package load-set
```

