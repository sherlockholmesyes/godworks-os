param(
  [switch]$BuildBroker,
  [switch]$RunGate,
  [switch]$RunOneShotHandoffGate,
  [switch]$StopExisting,
  [switch]$SkipNpmInstall,
  [string]$CloneUrl = "https://github.com/owenashurst/agar.io-clone.git",
  [string]$CloneRoot = "",
  [int]$PortBroker = 7990,
  [int]$PortGame = 3000,
  [int]$PortMonitor = 8091,
  [int]$ControlBase = 8100,
  [int]$GridCols = 4,
  [int]$GridRows = 4,
  [int]$FoodPerWorker = 64
)

$ErrorActionPreference = "Stop"

$ToolsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = (Resolve-Path (Join-Path $ToolsDir "..\..")).Path
if (-not $CloneRoot) { $CloneRoot = Join-Path $Repo ".local\agar_mit_clone_authoritative" }
$LogDir = Join-Path $Repo ".local\agar_authoritative_logs"
$BrokerExe = Join-Path $Repo "target\release\godworks_broker.exe"
$WalPath = Join-Path $LogDir "authoritative.wal"
$BrokerLog = Join-Path $LogDir "broker.log"
$PowerShellExe = (Get-Command powershell.exe).Source
$NodeExe = (Get-Command node.exe).Source

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Test-PortListening([int]$Port) {
  $client = New-Object System.Net.Sockets.TcpClient
  try {
    $iar = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
    if (-not $iar.AsyncWaitHandle.WaitOne(250, $false)) { return $false }
    $client.EndConnect($iar)
    return $true
  } catch {
    return $false
  } finally {
    $client.Close()
  }
}

function Wait-Port([int]$Port, [string]$Name, [int]$Seconds = 30) {
  $deadline = (Get-Date).AddSeconds($Seconds)
  while ((Get-Date) -lt $deadline) {
    if (Test-PortListening $Port) {
      Write-Host "$Name ready on :$Port"
      return
    }
    Start-Sleep -Milliseconds 300
  }
  throw "$Name did not open :$Port within ${Seconds}s"
}

function Start-LoggedPowerShell([string]$Name, [string]$Command, [string]$WorkingDirectory = $Repo) {
  $out = Join-Path $LogDir "$Name.out.log"
  $err = Join-Path $LogDir "$Name.err.log"
  Remove-Item -LiteralPath $out,$err -ErrorAction SilentlyContinue
  $p = Start-Process -FilePath $PowerShellExe `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $Command) `
    -WorkingDirectory $WorkingDirectory `
    -RedirectStandardOutput $out `
    -RedirectStandardError $err `
    -WindowStyle Hidden `
    -PassThru
  Write-Host "$Name pid=$($p.Id) out=$out err=$err"
  return $p
}

function Stop-GodworksAuthoritativeAgar {
  $needles = @(
    "gw_authoritative_zone_worker.js",
    "gw_authoritative_server.js",
    "gw_authoritative_gate.js",
    "gw_authoritative_one_shot_handoff_gate.js",
    "gw_authoritative_bots.js",
    "gw_authoritative_capacity_gate.js",
    "gw_authoritative_monitor.js",
    "_gw_bots.js",
    "_gw_spectator_tap.js",
    "gw_shard_monitor.js",
    "gw_agar_live_gate.js",
    "gw_agar_playable_seam_gate.js",
    "gw_agar_command_bridge.js",
    "gw_agar_broker_command_gate.js",
    "gw_agar_broker_command_capacity_gate.js",
    "gw_agar_capacity_gate.js",
    "gw_agar_mirror_worker.js",
    "gw_broker_view.js"
  )
  $ports = @($PortBroker, $PortGame, $PortMonitor)
  for ($i = 0; $i -lt ($GridCols * $GridRows); $i++) { $ports += ($ControlBase + $i) }
  $listenerPids = @(Get-NetTCPConnection -LocalPort $ports -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique |
    Where-Object { $_ -gt 0 })
  Get-CimInstance Win32_Process |
    Where-Object {
      $cmd = $_.CommandLine
      $_.ProcessId -ne $PID -and $cmd -and (
        ($listenerPids -contains $_.ProcessId) -or
        ($CloneRoot -and $cmd -like "*$CloneRoot*") -or
        ($needles | Where-Object { $cmd -like "*$_*" })
      )
    } |
    Sort-Object ProcessId -Descending |
    ForEach-Object {
      Write-Host "stopping pid=$($_.ProcessId) $($_.Name)"
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Ensure-Clone {
  if (-not (Test-Path -LiteralPath $CloneRoot)) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $CloneRoot) | Out-Null
    & (Get-Command git.exe).Source clone --depth 1 $CloneUrl $CloneRoot
    if ($LASTEXITCODE -ne 0) { throw "failed to clone stock agar.io repo from $CloneUrl" }
  }
  if (-not $SkipNpmInstall -and -not (Test-Path -LiteralPath (Join-Path $CloneRoot "node_modules"))) {
    Push-Location $CloneRoot
    try {
      & (Get-Command npm.cmd).Source install
      if ($LASTEXITCODE -ne 0) { throw "npm install failed in $CloneRoot" }
    } finally {
      Pop-Location
    }
  }
}

function Ensure-ClientBuild {
  $appBundle = Join-Path $CloneRoot "bin\client\js\app.js"
  if (Test-Path -LiteralPath $appBundle) { return }
  $gulp = Join-Path $CloneRoot "node_modules\.bin\gulp.cmd"
  if (-not (Test-Path -LiteralPath $gulp)) {
    throw "gulp executable missing in $CloneRoot; run without -SkipNpmInstall once"
  }
  Push-Location $CloneRoot
  try {
    & $gulp dev
    if ($LASTEXITCODE -ne 0) { throw "client build failed in $CloneRoot" }
  } finally {
    Pop-Location
  }
}

function Copy-Tools {
  foreach ($name in @("gw_authoritative_zone_worker.js", "gw_authoritative_server.js", "gw_authoritative_gate.js", "gw_authoritative_one_shot_handoff_gate.js", "gw_authoritative_bots.js", "gw_authoritative_capacity_gate.js", "gw_authoritative_monitor.js")) {
    Copy-Item -LiteralPath (Join-Path $ToolsDir $name) -Destination (Join-Path $CloneRoot $name) -Force
  }
}

if ($StopExisting) {
  Stop-GodworksAuthoritativeAgar
  Start-Sleep -Seconds 1
}

Ensure-Clone
Ensure-ClientBuild
Copy-Tools

if ($BuildBroker -or -not (Test-Path -LiteralPath $BrokerExe)) {
  Push-Location $Repo
  try {
    cargo build --release --bin godworks_broker
    if ($LASTEXITCODE -ne 0) { throw "broker build failed" }
  } finally {
    Pop-Location
  }
}

$claims = @("obs-token:OBS:observer|inspector", "client-token:CLIENT:role.client")
for ($y = 0; $y -lt $GridRows; $y++) {
  for ($x = 0; $x -lt $GridCols; $x++) {
    $r = "Z${x}_${y}"
    $claims += "$r-token:${r}:"
  }
}

if (-not (Test-PortListening $PortBroker)) {
  Remove-Item -LiteralPath $WalPath -ErrorAction SilentlyContinue
  $claimString = $claims -join ","
  $cmd = "`$env:GW_PORT='$PortBroker'; `$env:GW_GRID2D='${GridCols}x${GridRows}'; `$env:GW_ARENA='5000,5000'; `$env:GW_WAL='$WalPath'; `$env:GW_AUTH_CLAIMS='$claimString'; & '$BrokerExe' 2>&1 | Tee-Object -FilePath '$BrokerLog'"
  Start-LoggedPowerShell "auth_broker_$PortBroker" $cmd $Repo | Out-Null
  Wait-Port $PortBroker "Godworks broker"
}

$cellW = 5000 / $GridCols
$cellH = 5000 / $GridRows
for ($y = 0; $y -lt $GridRows; $y++) {
  for ($x = 0; $x -lt $GridCols; $x++) {
    $idx = $y * $GridCols + $x
    $r = "Z${x}_${y}"
    $wid = "auth-$r"
    $control = $ControlBase + $idx
    if (-not (Test-PortListening $control)) {
      $x0 = [Math]::Round($x * $cellW, 3)
      $x1 = [Math]::Round(($x + 1) * $cellW, 3)
      $y0 = [Math]::Round($y * $cellH, 3)
      $y1 = [Math]::Round(($y + 1) * $cellH, 3)
      $box = "$x0,$x1,$y0,$y1"
      $cmd = "Set-Location '$CloneRoot'; `$env:GW_PORT='$PortBroker'; `$env:GW_REGION='$r'; `$env:GW_WID='$wid'; `$env:GW_CONNECT_TOKEN='$r-token'; `$env:GW_CONTROL_PORT='$control'; `$env:GW_BOX='$box'; `$env:GW_WORLD='0,5000,0,5000'; `$env:GW_FOOD='$FoodPerWorker'; & '$NodeExe' 'gw_authoritative_zone_worker.js'"
      Start-LoggedPowerShell "auth_worker_$r" $cmd $CloneRoot | Out-Null
      Wait-Port $control "$wid control"
    }
  }
}

if (-not (Test-PortListening $PortGame)) {
  $cmd = "Set-Location '$CloneRoot'; `$env:PORT='$PortGame'; `$env:GW_PORT='$PortBroker'; `$env:GW_CONTROL_BASE='$ControlBase'; `$env:GW_COLS='$GridCols'; `$env:GW_ROWS='$GridRows'; `$env:GW_OBS_TOKEN='obs-token'; `$env:GW_CLIENT_TOKEN='client-token'; & '$NodeExe' 'gw_authoritative_server.js'"
  Start-LoggedPowerShell "auth_server_$PortGame" $cmd $CloneRoot | Out-Null
  Wait-Port $PortGame "Godworks authoritative agar server"
}

if (-not (Test-PortListening $PortMonitor)) {
  $cmd = "Set-Location '$CloneRoot'; `$env:GW_MONITOR_PORT='$PortMonitor'; `$env:GW_AGAR_STATE_URL='http://127.0.0.1:$PortGame/state?entities=1'; `$env:GW_WORKER_COLS='$GridCols'; `$env:GW_WORKER_ROWS='$GridRows'; & '$NodeExe' 'gw_authoritative_monitor.js'"
  Start-LoggedPowerShell "auth_monitor_$PortMonitor" $cmd $CloneRoot | Out-Null
  Wait-Port $PortMonitor "Godworks authoritative monitor"
}

Write-Host ""
Write-Host "Godworks authoritative agar:"
Write-Host "  game/client: http://localhost:$PortGame/"
Write-Host "  monitor:     http://localhost:$PortMonitor/"
Write-Host "  state:       http://localhost:$PortGame/state"
Write-Host "  broker:      127.0.0.1:$PortBroker"
Write-Host "  workers:     $($GridCols * $GridRows) region workers, controls $ControlBase-$($ControlBase + $GridCols * $GridRows - 1)"
Write-Host "  wal:         $WalPath"
Write-Host "  clone:       $CloneRoot"
Write-Host "  logs:        $LogDir"

if ($RunGate) {
  Start-Sleep -Seconds 3
  $env:GW_AGAR_GAME_URL = "http://127.0.0.1:$PortGame"
  $env:GW_AGAR_STATE_URL = "http://127.0.0.1:$PortGame/state"
  $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
  Push-Location $CloneRoot
  try {
    & $NodeExe "gw_authoritative_gate.js"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally {
    Pop-Location
  }
}

if ($RunOneShotHandoffGate) {
  Start-Sleep -Seconds 3
  $env:GW_AGAR_GAME_URL = "http://127.0.0.1:$PortGame"
  $env:GW_AGAR_STATE_URL = "http://127.0.0.1:$PortGame/state?entities=1"
  Push-Location $CloneRoot
  try {
    & $NodeExe "gw_authoritative_one_shot_handoff_gate.js"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally {
    Pop-Location
  }
}
