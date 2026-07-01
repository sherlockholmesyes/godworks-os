param(
  [switch]$BuildBroker,
  [switch]$MirrorBroker,
  [switch]$RunGate,
  [switch]$RunPlayableSeamGate,
  [switch]$RunBrokerCommandGate,
  [switch]$RunBrokerCommandCapacityGate,
  [switch]$StartCommandBridge,
  [switch]$RunCapacityGate,
  [switch]$SkipGame,
  [switch]$StopExisting,
  [switch]$SkipNpmInstall,
  [switch]$HerdBots,
  [int]$BotCount = 40,
  [string]$CloneUrl = "https://github.com/owenashurst/agar.io-clone.git",
  [string]$CloneRoot = "",
  [int]$PortBroker = 7990,
  [int]$PortMonitor = 8091,
  [int]$PortView = 8092,
  [int]$PortCommandBridge = 8093,
  [int]$CapacityMs = 15000,
  [int]$CapacityMinPlayers = 30,
  [int]$CapacityMinEntities = 800,
  [int]$CapacityMinWorkers = 16,
  [int]$CapacityMinOkSamples = 8,
  [int]$CommandPlayers = 4,
  [int]$CommandCapacityMinCompleted = 4
)

$ErrorActionPreference = "Stop"

$ToolsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = (Resolve-Path (Join-Path $ToolsDir "..\..")).Path
if (-not $CloneRoot) { $CloneRoot = Join-Path $Repo ".local\agar_mit_clone" }
$LogDir = Join-Path $Repo ".local\agar_mit_clone_logs"
$WalPath = Join-Path $LogDir "mirror.wal"
$BrokerLog = Join-Path $LogDir "broker.log"
$BrokerExe = Join-Path $Repo "target\release\godworks_broker.exe"
$NodeExe = (Get-Command node.exe).Source
$PowerShellExe = (Get-Command powershell.exe).Source

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

function Stop-KnownAgarMitStack {
  $allProcesses = @(Get-CimInstance Win32_Process)
  $needles = @(
    "gw_agar_mirror_worker.js",
    "gw_agar_command_bridge.js",
    "gw_agar_broker_command_gate.js",
    "gw_agar_broker_command_capacity_gate.js",
    "gw_shard_monitor.js",
    "gw_broker_view.js",
    "_gw_bots.js",
    "gw_agar_d2_worker.js"
  )
  $targets = @()

  $targets += $allProcesses |
    Where-Object {
      $cmd = $_.CommandLine
      $_.ProcessId -ne $PID -and $cmd -and (
        ($needles | Where-Object { $cmd -like "*$_*" }) -or
        ($CloneRoot -and $cmd -like "*$CloneRoot*")
      )
    }

  $listenerPids = @(Get-NetTCPConnection -LocalPort 3000,$PortBroker,$PortMonitor,$PortView,$PortCommandBridge -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique |
    Where-Object { $_ -gt 0 })
  foreach ($listenerPid in $listenerPids) {
    $targets += $allProcesses | Where-Object { $_.ProcessId -eq $listenerPid }
  }

  # Start-LoggedPowerShell launches wrapper PowerShell/npm/gulp processes. Killing
  # only the listening child leaves stale wrappers after stress-ladder runs, so
  # walk both directions but keep the scope anchored to this adapter's ports,
  # clone root, or known helper scripts.
  $targetIds = @{}
  foreach ($target in $targets) {
    if ($target -and $target.ProcessId -ne $PID) {
      $targetIds[[int]$target.ProcessId] = $true
    }
  }

  $changed = $true
  while ($changed) {
    $changed = $false
    foreach ($process in $allProcesses) {
      if (-not $process -or $process.ProcessId -eq $PID) { continue }
      $cmd = $process.CommandLine
      $isKnownWrapper = $cmd -and (
        $cmd -like "*$CloneRoot*" -or
        $cmd -like "*npm-cli.js*start*" -or
        $cmd -like "*npm.cmd*start*" -or
        $cmd -like "*gulp.js*run*" -or
        $cmd -like "*gulp run*" -or
        ($needles | Where-Object { $cmd -like "*$_*" })
      )
      if (-not $isKnownWrapper) { continue }

      $pidKnown = $targetIds.ContainsKey([int]$process.ProcessId)
      $parentKnown = $process.ParentProcessId -and $targetIds.ContainsKey([int]$process.ParentProcessId)
      $childKnown = [bool]($allProcesses | Where-Object {
        $_.ParentProcessId -eq $process.ProcessId -and $targetIds.ContainsKey([int]$_.ProcessId)
      } | Select-Object -First 1)

      if (($parentKnown -or $childKnown) -and -not $pidKnown) {
        $targetIds[[int]$process.ProcessId] = $true
        $changed = $true
      }
    }
  }

  $targetIds.Keys |
    Sort-Object -Descending |
    ForEach-Object {
      Write-Host "stopping agar stack pid=$_"
      Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
    }
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

function Test-CommandProcess([string]$Needle) {
  [bool](Get-CimInstance Win32_Process |
    Where-Object {
      $_.CommandLine -and $_.CommandLine -like "*$Needle*"
    } |
    Select-Object -First 1)
}

function Test-CommandProcessAll([string[]]$Needles) {
  [bool](Get-CimInstance Win32_Process |
    Where-Object {
      $cmd = $_.CommandLine
      if (-not $cmd) { return $false }
      foreach ($needle in $Needles) {
        if ($cmd -notlike "*$needle*") { return $false }
      }
      return $true
    } |
    Select-Object -First 1)
}

function Stop-CommandProcessAll([string[]]$Needles) {
  Get-CimInstance Win32_Process |
    Where-Object {
      $cmd = $_.CommandLine
      if (-not $cmd) { return $false }
      foreach ($needle in $Needles) {
        if ($cmd -notlike "*$needle*") { return $false }
      }
      return $true
    } |
    ForEach-Object {
      Write-Host "stopping $($_.ProcessId) $($_.Name)"
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Wait-Port([int]$Port, [string]$Name, [int]$Seconds = 25) {
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

function Ensure-StockClone {
  if ($SkipGame) { return }
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

function Copy-CloneTools {
  if (-not (Test-Path -LiteralPath $CloneRoot)) { return }
  foreach ($name in @("_gw_spectator_tap.js", "_gw_bots.js", "gw_shard_monitor.js", "gw_agar_mirror_worker.js", "gw_agar_command_bridge.js", "gw_agar_playable_seam_gate.js", "gw_agar_broker_command_gate.js", "gw_agar_broker_command_capacity_gate.js", "gw_agar_capacity_gate.js")) {
    Copy-Item -LiteralPath (Join-Path $ToolsDir $name) -Destination (Join-Path $CloneRoot $name) -Force
  }
}

if ($StopExisting) {
  Stop-KnownAgarMitStack
  Start-Sleep -Seconds 1
}

Ensure-StockClone
Copy-CloneTools

if ($RunBrokerCommandGate -and -not $MirrorBroker) {
  throw "-RunBrokerCommandGate requires -MirrorBroker because it verifies broker-routed CommandRequest ownership"
}

if ($RunBrokerCommandCapacityGate -and -not $MirrorBroker) {
  throw "-RunBrokerCommandCapacityGate requires -MirrorBroker because it verifies broker-routed CommandRequest ownership"
}

if (-not $SkipGame -and -not (Test-PortListening 3000)) {
  $npm = (Get-Command npm.cmd).Source
  Start-LoggedPowerShell "agar_game_3000" "Set-Location '$CloneRoot'; & '$npm' start" $CloneRoot | Out-Null
  Wait-Port 3000 "stock agar game"
} else {
  Write-Host "stock agar game already available or skipped"
}

if (-not $SkipGame) {
  $herd = if ($HerdBots) { "1" } else { "0" }
  if (-not (Test-CommandProcess "_gw_bots.js")) {
    Start-LoggedPowerShell "agar_bots" "Set-Location '$CloneRoot'; `$env:GW_BOTS='$BotCount'; `$env:GW_BOT_HERD='$herd'; & '$NodeExe' '_gw_bots.js'" $CloneRoot | Out-Null
  } else {
    Write-Host "agar bot load already running"
  }
}

if (-not (Test-PortListening $PortMonitor)) {
  Start-LoggedPowerShell "agar_monitor_$PortMonitor" "Set-Location '$CloneRoot'; `$env:GW_MONITOR_PORT='$PortMonitor'; `$env:GW_WORKER_COLS='4'; `$env:GW_WORKER_ROWS='4'; & '$NodeExe' 'gw_shard_monitor.js'" $CloneRoot | Out-Null
  Wait-Port $PortMonitor "dynamic shard monitor"
} else {
  Write-Host "dynamic shard monitor already listening on :$PortMonitor"
}

if ($MirrorBroker) {
  if ($BuildBroker -or -not (Test-Path -LiteralPath $BrokerExe)) {
    Push-Location $Repo
    try {
      cargo build --release --bin godworks_broker
      if ($LASTEXITCODE -ne 0) { throw "broker build failed" }
    } finally {
      Pop-Location
    }
  }
  if (-not (Test-Path -LiteralPath $BrokerExe)) { throw "broker exe missing: $BrokerExe" }

  $claims = @("obs-token:OBS:observer|inspector", "client-token:CLIENT:role.client")
  for ($y = 0; $y -lt 4; $y++) {
    for ($x = 0; $x -lt 4; $x++) {
      $r = "Z${x}_${y}"
      $claims += "$r-token:${r}:"
    }
  }

  if (-not (Test-PortListening $PortBroker)) {
    Remove-Item -LiteralPath $WalPath -ErrorAction SilentlyContinue
    $claimString = $claims -join ","
    $cmd = "`$env:GW_PORT='$PortBroker'; `$env:GW_GRID2D='4x4'; `$env:GW_ARENA='5000,5000'; `$env:GW_WAL='$WalPath'; `$env:GW_AUTH_CLAIMS='$claimString'; & '$BrokerExe' 2>&1 | Tee-Object -FilePath '$BrokerLog'"
    Start-LoggedPowerShell "agar_broker_$PortBroker" $cmd $Repo | Out-Null
    Wait-Port $PortBroker "Godworks broker"
  } else {
    Write-Host "Godworks broker already listening on :$PortBroker"
  }

  $NeedsCommandBridge = $StartCommandBridge -or $RunBrokerCommandGate -or $RunBrokerCommandCapacityGate

  if ($RunBrokerCommandCapacityGate -and (Test-PortListening $PortCommandBridge)) {
    Write-Host "broker-command capacity gate needs a fresh multi-player command bridge; restarting :$PortCommandBridge"
    Stop-CommandProcessAll @("gw_agar_command_bridge.js")
    Start-Sleep -Milliseconds 500
  }

  if ($NeedsCommandBridge) {
    if (-not (Test-PortListening $PortCommandBridge)) {
      $cmd = "Set-Location '$CloneRoot'; `$env:GW_AGAR_GAME_URL='http://127.0.0.1:3000'; `$env:GW_AGAR_COMMAND_PORT='$PortCommandBridge'; `$env:GW_AGAR_COMMAND_PLAYERS='$CommandPlayers'; & '$NodeExe' 'gw_agar_command_bridge.js'"
      Start-LoggedPowerShell "agar_command_bridge_$PortCommandBridge" $cmd $CloneRoot | Out-Null
      Wait-Port $PortCommandBridge "agar command bridge"
    } else {
      Write-Host "agar command bridge already listening on :$PortCommandBridge"
    }
  }

  for ($y = 0; $y -lt 4; $y++) {
    for ($x = 0; $x -lt 4; $x++) {
      $r = "Z${x}_${y}"
      $wid = "mit-$r"
      $workerNeedles = @($wid, "GW_PORT='$PortBroker'")
      if ($NeedsCommandBridge) {
        $workerNeedles += "GW_AGAR_COMMAND_URL"
      }
      if (Test-CommandProcessAll $workerNeedles) {
        Write-Host "$wid mirror worker already running"
      } else {
        if ($NeedsCommandBridge -and (Test-CommandProcessAll @($wid, "GW_PORT='$PortBroker'", "gw_agar_mirror_worker.js"))) {
          Write-Host "$wid mirror worker is running without command bridge env; restarting it"
          Stop-CommandProcessAll @($wid, "GW_PORT='$PortBroker'", "gw_agar_mirror_worker.js")
          Start-Sleep -Milliseconds 300
        }
        $commandBridgeEnv = ""
        if ($NeedsCommandBridge) {
          $commandBridgeEnv = "`$env:GW_AGAR_COMMAND_URL='http://127.0.0.1:$PortCommandBridge'; "
        }
        $cmd = "Set-Location '$CloneRoot'; `$env:GW_PORT='$PortBroker'; `$env:GW_GRID2D='4x4'; `$env:GW_ARENA='5000'; `$env:GW_REGION='$r'; `$env:GW_WID='$wid'; `$env:GW_CONNECT_TOKEN='$r-token'; $commandBridgeEnv& '$NodeExe' 'gw_agar_mirror_worker.js'"
        Start-LoggedPowerShell "mirror_$r" $cmd $CloneRoot | Out-Null
      }
    }
  }

  if (-not (Test-PortListening $PortView)) {
    $cmd = "`$env:GW_PORT='$PortBroker'; `$env:GW_HTTP='$PortView'; `$env:GW_ARENA='5000'; `$env:NX='4'; `$env:NY='4'; `$env:GW_OBS_TOKEN='obs-token'; & '$NodeExe' '$ToolsDir\gw_broker_view.js'"
    Start-LoggedPowerShell "broker_view_$PortView" $cmd $Repo | Out-Null
    Wait-Port $PortView "broker view"
  } else {
    Write-Host "broker view already listening on :$PortView"
  }
}

Write-Host ""
Write-Host "Godworks MIT agar.io adapter:"
Write-Host "  game:    http://localhost:3000/"
Write-Host "  monitor: http://localhost:$PortMonitor/"
if ($MirrorBroker) {
  Write-Host "  broker:  127.0.0.1:$PortBroker"
  Write-Host "  view:    http://localhost:$PortView/"
  if ($StartCommandBridge -or $RunBrokerCommandGate) {
    Write-Host "  command: http://localhost:$PortCommandBridge/"
  } elseif ($RunBrokerCommandCapacityGate) {
    Write-Host "  command: http://localhost:$PortCommandBridge/ ($CommandPlayers controlled players)"
  }
  Write-Host "  wal:     $WalPath"
}
Write-Host "  clone:   $CloneRoot"
Write-Host "  logs:    $LogDir"

if ($RunGate) {
  Start-Sleep -Seconds 3
  $env:GW_AGAR_GAME_URL = "http://127.0.0.1:3000/"
  $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
  if ($MirrorBroker) {
    $env:GW_AGAR_BROKER_VIEW_URL = "http://127.0.0.1:$PortView/state"
    $env:GW_AGAR_WAL = $WalPath
  }
  & $NodeExe (Join-Path $ToolsDir "gw_agar_live_gate.js")
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

if ($RunPlayableSeamGate) {
  Start-Sleep -Seconds 3
  $env:GW_AGAR_GAME_URL = "http://127.0.0.1:3000/"
  $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
  if ($MirrorBroker) {
    $env:GW_AGAR_BROKER_VIEW_URL = "http://127.0.0.1:$PortView/state"
  } else {
    Remove-Item Env:\GW_AGAR_BROKER_VIEW_URL -ErrorAction SilentlyContinue
  }
  Push-Location $CloneRoot
  try {
    & $NodeExe "gw_agar_playable_seam_gate.js"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally {
    Pop-Location
  }
}

if ($RunBrokerCommandGate) {
  Start-Sleep -Seconds 3
  $env:GW_HOST = "127.0.0.1"
  $env:GW_PORT = "$PortBroker"
  $env:GW_CLIENT_TOKEN = "client-token"
  $env:GW_AGAR_COMMAND_BRIDGE_URL = "http://127.0.0.1:$PortCommandBridge"
  $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
  $env:GW_AGAR_BROKER_VIEW_URL = "http://127.0.0.1:$PortView/state"
  Push-Location $CloneRoot
  try {
    & $NodeExe "gw_agar_broker_command_gate.js"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally {
    Pop-Location
  }
}

if ($RunBrokerCommandCapacityGate) {
  Start-Sleep -Seconds 3
  $env:GW_HOST = "127.0.0.1"
  $env:GW_PORT = "$PortBroker"
  $env:GW_CLIENT_TOKEN = "client-token"
  $env:GW_AGAR_COMMAND_BRIDGE_URL = "http://127.0.0.1:$PortCommandBridge"
  $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
  $env:GW_AGAR_BROKER_VIEW_URL = "http://127.0.0.1:$PortView/state"
  $env:GW_AGAR_COMMAND_CAPACITY_PLAYERS = "$CommandPlayers"
  $env:GW_AGAR_COMMAND_CAPACITY_MIN_COMPLETED = "$CommandCapacityMinCompleted"
  $env:GW_AGAR_CAPACITY_MIN_PLAYERS = "$CapacityMinPlayers"
  $env:GW_AGAR_CAPACITY_MIN_ENTITIES = "$CapacityMinEntities"
  $env:GW_AGAR_CAPACITY_MIN_WORKERS = "$CapacityMinWorkers"
  $env:GW_AGAR_CAPACITY_MIN_OK_SAMPLES = "$CapacityMinOkSamples"
  Push-Location $CloneRoot
  try {
    & $NodeExe "gw_agar_broker_command_capacity_gate.js"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally {
    Pop-Location
  }
}

if ($RunCapacityGate) {
  Start-Sleep -Seconds 3
  $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
  $env:GW_AGAR_CAPACITY_MS = "$CapacityMs"
  $env:GW_AGAR_CAPACITY_MIN_PLAYERS = "$CapacityMinPlayers"
  $env:GW_AGAR_CAPACITY_MIN_ENTITIES = "$CapacityMinEntities"
  $env:GW_AGAR_CAPACITY_MIN_WORKERS = "$CapacityMinWorkers"
  $env:GW_AGAR_CAPACITY_MIN_OK_SAMPLES = "$CapacityMinOkSamples"
  if ($MirrorBroker) {
    $env:GW_AGAR_BROKER_VIEW_URL = "http://127.0.0.1:$PortView/state"
  } else {
    Remove-Item Env:\GW_AGAR_BROKER_VIEW_URL -ErrorAction SilentlyContinue
  }
  Push-Location $CloneRoot
  try {
    & $NodeExe "gw_agar_capacity_gate.js"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally {
    Pop-Location
  }
}
