param(
  [string]$Godot = $env:GODOT_BIN,
  [switch]$AllowMissingGodot,
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ProbeDir = Join-Path $Repo "client_probes\godot"
$LocalDir = Join-Path $Repo ".local\godot"
$Broker = Join-Path $Repo "target\release\godworks_broker.exe"
$Fixture = Join-Path $Repo "tests\fixtures\client_bridge\godot-resync-contract.json"
$RequiredProbeScripts = @(
  "client_bridge_contract_probe.gd",
  "client_bridge_tcp_resync_probe.gd",
  "cross_broker_handoff_probe.gd"
)
$EnvKeys = @(
  "GW_BIND",
  "GW_PORT",
  "GW_PORT_W",
  "GW_PORT_E",
  "GW_HOST",
  "GW_WAL",
  "GW_BOUNDARY",
  "GW_ADVERTISE",
  "GW_MESH",
  "GW_DURABLE_FLUSH_MS",
  "GW_CLIENT_BRIDGE_FIXTURE",
  "GW_BRIDGE_STALE_ENTITY",
  "GW_BRIDGE_FRESH_ENTITY",
  "GW_CROSS_ENTITY"
)

function Get-GodotExe {
  if ($Godot -and (Test-Path $Godot)) {
    return (Resolve-Path $Godot).Path
  }
  if ($Godot) {
    $cmd = Get-Command $Godot -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
  }
  $fallback = Get-Command "godot" -ErrorAction SilentlyContinue
  if ($fallback) { return $fallback.Source }
  return $null
}

function Set-GateEnv([hashtable]$Values) {
  foreach ($key in $Values.Keys) {
    Set-Item -Path "Env:$key" -Value ([string]$Values[$key])
  }
}

function Clear-GateEnv([string[]]$Keys) {
  foreach ($key in $Keys) {
    Remove-Item -Path "Env:$key" -ErrorAction SilentlyContinue
  }
}

function Assert-FileExists([string]$Path, [string]$Label) {
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "$Label missing: $Path"
  }
}

function Assert-GodotProbeInputs {
  Assert-FileExists $Fixture "client bridge fixture"
  foreach ($script in $RequiredProbeScripts) {
    Assert-FileExists (Join-Path $ProbeDir $script) "Godot probe script"
  }
}

function Invoke-GodotProbe([string]$ScriptName) {
  Push-Location $Repo
  try {
    & $script:GodotExe --headless --path $ProbeDir --script "res://$ScriptName"
    if ($LASTEXITCODE -ne 0) {
      throw "Godot probe $ScriptName failed with exit code $LASTEXITCODE"
    }
  } finally {
    Pop-Location
  }
}

function Start-Broker([string]$Name, [int]$Port, [hashtable]$Env) {
  Clear-GateEnv $EnvKeys
  Set-GateEnv $Env
  $stdout = Join-Path $LocalDir "$Name.out.log"
  $stderr = Join-Path $LocalDir "$Name.err.log"
  $proc = Start-Process -FilePath $Broker `
    -WorkingDirectory $Repo `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -WindowStyle Hidden `
    -PassThru
  Start-Sleep -Milliseconds 800
  if ($proc.HasExited) {
    $err = if (Test-Path $stderr) { Get-Content -Raw $stderr } else { "" }
    throw "broker $Name exited early on port $Port. stderr: $err"
  }
  return $proc
}

function Stop-Brokers([System.Collections.ArrayList]$Procs) {
  foreach ($proc in $Procs) {
    if ($proc -and -not $proc.HasExited) {
      Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
      Wait-Process -Id $proc.Id -Timeout 5 -ErrorAction SilentlyContinue
    }
  }
}

$old = @{}
foreach ($key in $EnvKeys) {
  $old[$key] = (Get-Item -Path "Env:$key" -ErrorAction SilentlyContinue).Value
}

$procs = [System.Collections.ArrayList]::new()

try {
  Assert-GodotProbeInputs
  $script:GodotExe = Get-GodotExe
  if (-not $script:GodotExe) {
    $msg = "Godot 4.x executable was not found. Set GODOT_BIN or pass -Godot <path>."
    if ($AllowMissingGodot) {
      Write-Host "GODOT PROBES: SKIP -- $msg"
      exit 0
    }
    throw $msg
  }

  New-Item -ItemType Directory -Force -Path $LocalDir | Out-Null
  Get-ChildItem -Path $LocalDir -Filter "*.wal" -ErrorAction SilentlyContinue | Remove-Item -Force

  if (-not $SkipBuild -or -not (Test-Path $Broker)) {
    Push-Location $Repo
    try {
      cargo build --bin godworks_broker --release
    } finally {
      Pop-Location
    }
  }

  Write-Host "GODOT PROBES: fixture contract"
  Set-GateEnv @{
    GW_CLIENT_BRIDGE_FIXTURE = (Resolve-Path $Fixture).Path
  }
  Invoke-GodotProbe "client_bridge_contract_probe.gd"

  Write-Host "GODOT PROBES: real broker reconnect/resync"
  $resyncBroker = Start-Broker "godot-bridge-resync" 7811 @{
    GW_BIND = "127.0.0.1"
    GW_PORT = "7811"
    GW_WAL = (Join-Path $LocalDir "godot-bridge-resync.wal")
    GW_DURABLE_FLUSH_MS = "5"
  }
  [void]$procs.Add($resyncBroker)
  Clear-GateEnv $EnvKeys
  Set-GateEnv @{
    GW_HOST = "127.0.0.1"
    GW_PORT = "7811"
  }
  Invoke-GodotProbe "client_bridge_tcp_resync_probe.gd"
  Stop-Brokers $procs
  $procs.Clear()

  Write-Host "GODOT PROBES: cross-broker handoff"
  $eBroker = Start-Broker "godot-cross-e" 7802 @{
    GW_BIND = "127.0.0.1"
    GW_PORT = "7802"
    GW_WAL = (Join-Path $LocalDir "godot-cross-e.wal")
    GW_BOUNDARY = "0"
    GW_ADVERTISE = "E=127.0.0.1:7802"
    GW_DURABLE_FLUSH_MS = "5"
  }
  [void]$procs.Add($eBroker)
  $wBroker = Start-Broker "godot-cross-w" 7801 @{
    GW_BIND = "127.0.0.1"
    GW_PORT = "7801"
    GW_WAL = (Join-Path $LocalDir "godot-cross-w.wal")
    GW_BOUNDARY = "0"
    GW_ADVERTISE = "W=127.0.0.1:7801"
    GW_MESH = "E=127.0.0.1:7802"
    GW_DURABLE_FLUSH_MS = "5"
  }
  [void]$procs.Add($wBroker)
  Clear-GateEnv $EnvKeys
  Set-GateEnv @{
    GW_HOST = "127.0.0.1"
    GW_PORT_W = "7801"
    GW_PORT_E = "7802"
  }
  Invoke-GodotProbe "cross_broker_handoff_probe.gd"

  Write-Host "GODOT PROBES: PASS"
} finally {
  Stop-Brokers $procs
  Clear-GateEnv $EnvKeys
  foreach ($key in $old.Keys) {
    if ($null -ne $old[$key]) {
      Set-Item -Path "Env:$key" -Value $old[$key]
    }
  }
}
