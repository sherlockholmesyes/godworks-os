param(
    [switch]$Full
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Invoke-Cargo {
    param(
        [string[]]$CargoArgs
    )

    Write-Host "==> cargo $($CargoArgs -join ' ')"
    & cargo @CargoArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo $($CargoArgs -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Assert-ZoneWorkerUsesSdkBoundary {
    $path = "src/bin/zone_worker.rs"
    $source = Get-Content -Raw -LiteralPath $path

    Write-Host "==> assert zone_worker uses Worker SDK read/write boundary"
    if ($source -notmatch "use\s+godworks_worker_sdk::") {
        throw "$path does not import godworks_worker_sdk"
    }
    foreach ($needle in @("read_op", "write_op", "worker_connect_op", "create_entity_op", "fold_op", "heartbeat_op", "disconnect_op")) {
        if ($source -notmatch "\b$needle\b") {
            throw "$path is missing SDK helper '$needle'"
        }
    }
    if (($source | Select-String -Pattern "\bread_op\s*\(" -AllMatches).Matches.Count -lt 1) {
        throw "$path does not call read_op"
    }
    if (($source | Select-String -Pattern "\bwrite_op\s*\(" -AllMatches).Matches.Count -lt 5) {
        throw "$path does not use write_op for the worker protocol path"
    }
    Write-Host "ok: zone_worker uses SDK read/write boundary"
}

function Assert-NoLocalZoneWorkerFrameCodec {
    $pattern = "fn\s+read_frame|fn\s+write_frame|read_frame\(|write_frame\(|write_all\(|serde_json::to_vec|serde_json::from_slice"
    Write-Host "==> rg local zone_worker frame codec"
    & rg $pattern "src/bin/zone_worker.rs"
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        throw "src/bin/zone_worker.rs still contains a local frame codec or direct serde frame I/O"
    }
    if ($code -ne 1) {
        throw "rg failed with exit code $code"
    }
    Write-Host "ok: no local zone_worker frame codec terms found"
}

Assert-ZoneWorkerUsesSdkBoundary
Assert-NoLocalZoneWorkerFrameCodec

Invoke-Cargo -CargoArgs @("test", "-p", "godworks-worker-sdk", "zone_worker_outbound_helpers_match_current_wire_shapes")
Invoke-Cargo -CargoArgs @("test", "-p", "godworks-protocol", "authority_change_preserves_loss_imminent_metadata")
Invoke-Cargo -CargoArgs @("test", "-p", "godworks-protocol", "update_rejected_preserves_admin_stale_ghost_metadata")
Invoke-Cargo -CargoArgs @("test", "-p", "godworks-protocol", "mesh_handoff_preserves_authority_and_components")
Invoke-Cargo -CargoArgs @("test", "-p", "godworks-protocol", "mesh_handoff_roundtrips_current_broker_src_region_wire_shape")
Invoke-Cargo -CargoArgs @("test", "--test", "zone_worker_runtime", "--", "--test-threads=1")
Invoke-Cargo -CargoArgs @("test", "--test", "reality_loadgen_runtime", "cross_broker_reality_loadgen_requires_mesh_adoption", "--", "--nocapture", "--test-threads=1")

if ($Full) {
    Invoke-Cargo -CargoArgs @("fmt", "--all", "--", "--check")
    Invoke-Cargo -CargoArgs @("check", "--workspace", "--all-targets")
    Invoke-Cargo -CargoArgs @("clippy", "--workspace", "--all-targets")
    Invoke-Cargo -CargoArgs @("test", "--workspace", "--all-targets", "--", "--test-threads=1")
    Invoke-Cargo -CargoArgs @("build", "--workspace", "--release")
}

Write-Host "issue16 gate passed"
