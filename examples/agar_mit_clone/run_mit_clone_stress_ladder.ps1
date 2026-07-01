param(
  [int[]]$BotCounts = @(40, 80, 120, 200),
  [int]$CommandPlayers = 8,
  [int]$CommandCapacityMinCompleted = 4,
  [double]$MinPlayerRatio = 0.75,
  [int]$CapacityMinPlayers = 30,
  [int]$CapacityMinEntities = 800,
  [int]$CapacityMinWorkers = 16,
  [int]$CapacityMinOkSamples = 8,
  [int]$CommandTimeoutMs = 90000,
  [int]$StackTimeoutMs = 120000,
  [switch]$BuildEachProfile,
  [switch]$ContinueOnFailure,
  [string]$CloneRoot = "",
  [int]$PortBroker = 7990,
  [int]$PortMonitor = 8091,
  [int]$PortView = 8092,
  [int]$PortCommandBridge = 8093,
  [string]$OutputDir = ""
)

$ErrorActionPreference = "Stop"

$ToolsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = (Resolve-Path (Join-Path $ToolsDir "..\..")).Path
$Runner = Join-Path $ToolsDir "run_mit_clone_adapter.ps1"
$NodeExe = (Get-Command node.exe).Source
$PowerShellExe = (Get-Command powershell.exe).Source
if (-not $CloneRoot) { $CloneRoot = Join-Path $Repo ".local\agar_mit_clone" }
if (-not $OutputDir) { $OutputDir = Join-Path $Repo ".local\agar_mit_clone_ladder" }
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

function Restore-Env([string]$Name, [object]$Value) {
  if ($null -eq $Value) {
    Remove-Item "Env:\$Name" -ErrorAction SilentlyContinue
  } else {
    Set-Item "Env:\$Name" $Value
  }
}

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

function Wait-Port([int]$Port, [string]$Name, [int]$Seconds = 25) {
  $deadline = (Get-Date).AddSeconds($Seconds)
  while ((Get-Date) -lt $deadline) {
    if (Test-PortListening $Port) { return }
    Start-Sleep -Milliseconds 300
  }
  throw "$Name did not open :$Port within ${Seconds}s"
}

function Invoke-CapturedProcess(
  [string]$FilePath,
  [string[]]$Arguments,
  [string]$WorkingDirectory,
  [string]$StdoutPath,
  [string]$StderrPath,
  [int]$TimeoutMs
) {
  Remove-Item -LiteralPath $StdoutPath,$StderrPath -ErrorAction SilentlyContinue
  $process = Start-Process -FilePath $FilePath `
    -ArgumentList $Arguments `
    -WorkingDirectory $WorkingDirectory `
    -RedirectStandardOutput $StdoutPath `
    -RedirectStandardError $StderrPath `
    -PassThru `
    -WindowStyle Hidden
  if (-not $process.WaitForExit($TimeoutMs)) {
    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    return 124
  }
  $process.Refresh()
  if ($null -eq $process.ExitCode) {
    return 0
  }
  return $process.ExitCode
}

function Extract-GateJson([string]$Text) {
  $match = [regex]::Match($Text, '(?s)\{\s*"ok"\s*:\s*(?:true|false).*?\}\s*$')
  if (-not $match.Success) {
    throw "runner output did not end with a gate JSON object"
  }
  return ($match.Value | ConvertFrom-Json)
}

function Save-Summary([string]$Path, [object[]]$Rows) {
  $payload = [ordered]@{
    ok = -not [bool]($Rows | Where-Object { -not $_.ok } | Select-Object -First 1)
    gate = "mit_clone_broker_command_stress_ladder"
    generatedAt = (Get-Date).ToString("o")
    rows = $Rows
  }
  $payload | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Start-CommandBridge([string]$Profile, [int]$CommandPlayersForProfile) {
  if (Test-PortListening $PortCommandBridge) { return }
  $out = Join-Path $OutputDir "$Profile.command_bridge.out.log"
  $err = Join-Path $OutputDir "$Profile.command_bridge.err.log"
  $command = "Set-Location '$CloneRoot'; " +
    "`$env:GW_AGAR_GAME_URL='http://127.0.0.1:3000'; " +
    "`$env:GW_AGAR_COMMAND_PORT='$PortCommandBridge'; " +
    "`$env:GW_AGAR_COMMAND_PLAYERS='$CommandPlayersForProfile'; " +
    "& '$NodeExe' 'gw_agar_command_bridge.js'"
  Remove-Item -LiteralPath $out,$err -ErrorAction SilentlyContinue
  Start-Process -FilePath $PowerShellExe `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command) `
    -WorkingDirectory $CloneRoot `
    -RedirectStandardOutput $out `
    -RedirectStandardError $err `
    -PassThru `
    -WindowStyle Hidden | Out-Null
  Wait-Port $PortCommandBridge "agar command bridge"
}

$summaryPath = Join-Path $OutputDir "ladder_summary.json"
$rows = @()
$profileIndex = 0

foreach ($botCount in $BotCounts) {
  $profile = "bots_$botCount"
  $stackOut = Join-Path $OutputDir "$profile.stack.raw.log"
  $stackErr = Join-Path $OutputDir "$profile.stack.err.log"
  $gateOut = Join-Path $OutputDir "$profile.gate.raw.log"
  $gateErr = Join-Path $OutputDir "$profile.gate.err.log"
  $profileMinPlayers = [Math]::Max($CapacityMinPlayers, [int][Math]::Floor($botCount * $MinPlayerRatio))

  Write-Host ""
  Write-Host "== Godworks MIT agar stress profile: bots=$botCount minPlayers=$profileMinPlayers commandPlayers=$CommandPlayers minCompleted=$CommandCapacityMinCompleted =="

  $runnerArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $Runner,
    "-MirrorBroker",
    "-StartCommandBridge",
    "-CommandPlayers", "$CommandPlayers",
    "-BotCount", "$botCount",
    "-CapacityMinPlayers", "$profileMinPlayers",
    "-CapacityMinEntities", "$CapacityMinEntities",
    "-CapacityMinWorkers", "$CapacityMinWorkers",
    "-CapacityMinOkSamples", "$CapacityMinOkSamples",
    "-PortBroker", "$PortBroker",
    "-PortMonitor", "$PortMonitor",
    "-PortView", "$PortView",
    "-PortCommandBridge", "$PortCommandBridge",
    "-StopExisting"
  )
  if ($BuildEachProfile -or $profileIndex -eq 0) {
    $runnerArgs += "-BuildBroker"
  }

  $stackExit = Invoke-CapturedProcess `
    -FilePath $PowerShellExe `
    -Arguments $runnerArgs `
    -WorkingDirectory $Repo `
    -StdoutPath $stackOut `
    -StderrPath $stackErr `
    -TimeoutMs $StackTimeoutMs

  $gate = $null
  $gateExit = 1
  $parseError = $null
  $bridgeError = $null

  if ($stackExit -eq 0) {
    try {
      Start-CommandBridge $profile $CommandPlayers
      Wait-Port $PortBroker "Godworks broker" 5
      Wait-Port $PortMonitor "dynamic shard monitor" 5
      Wait-Port $PortView "broker view" 5

      $oldEnv = @{
        GW_HOST = $env:GW_HOST
        GW_PORT = $env:GW_PORT
        GW_CLIENT_TOKEN = $env:GW_CLIENT_TOKEN
        GW_AGAR_COMMAND_BRIDGE_URL = $env:GW_AGAR_COMMAND_BRIDGE_URL
        GW_AGAR_MONITOR_URL = $env:GW_AGAR_MONITOR_URL
        GW_AGAR_BROKER_VIEW_URL = $env:GW_AGAR_BROKER_VIEW_URL
        GW_AGAR_COMMAND_CAPACITY_PLAYERS = $env:GW_AGAR_COMMAND_CAPACITY_PLAYERS
        GW_AGAR_COMMAND_CAPACITY_MIN_COMPLETED = $env:GW_AGAR_COMMAND_CAPACITY_MIN_COMPLETED
        GW_AGAR_COMMAND_CAPACITY_TIMEOUT_MS = $env:GW_AGAR_COMMAND_CAPACITY_TIMEOUT_MS
        GW_AGAR_CAPACITY_MIN_PLAYERS = $env:GW_AGAR_CAPACITY_MIN_PLAYERS
        GW_AGAR_CAPACITY_MIN_ENTITIES = $env:GW_AGAR_CAPACITY_MIN_ENTITIES
        GW_AGAR_CAPACITY_MIN_WORKERS = $env:GW_AGAR_CAPACITY_MIN_WORKERS
        GW_AGAR_CAPACITY_MIN_OK_SAMPLES = $env:GW_AGAR_CAPACITY_MIN_OK_SAMPLES
      }

      $env:GW_HOST = "127.0.0.1"
      $env:GW_PORT = "$PortBroker"
      $env:GW_CLIENT_TOKEN = "client-token"
      $env:GW_AGAR_COMMAND_BRIDGE_URL = "http://127.0.0.1:$PortCommandBridge"
      $env:GW_AGAR_MONITOR_URL = "http://127.0.0.1:$PortMonitor/state"
      $env:GW_AGAR_BROKER_VIEW_URL = "http://127.0.0.1:$PortView/state"
      $env:GW_AGAR_COMMAND_CAPACITY_PLAYERS = "$CommandPlayers"
      $env:GW_AGAR_COMMAND_CAPACITY_MIN_COMPLETED = "$CommandCapacityMinCompleted"
      $env:GW_AGAR_COMMAND_CAPACITY_TIMEOUT_MS = "$CommandTimeoutMs"
      $env:GW_AGAR_CAPACITY_MIN_PLAYERS = "$profileMinPlayers"
      $env:GW_AGAR_CAPACITY_MIN_ENTITIES = "$CapacityMinEntities"
      $env:GW_AGAR_CAPACITY_MIN_WORKERS = "$CapacityMinWorkers"
      $env:GW_AGAR_CAPACITY_MIN_OK_SAMPLES = "$CapacityMinOkSamples"

      try {
        $gateExit = Invoke-CapturedProcess `
          -FilePath $NodeExe `
          -Arguments @("gw_agar_broker_command_capacity_gate.js") `
          -WorkingDirectory $CloneRoot `
          -StdoutPath $gateOut `
          -StderrPath $gateErr `
          -TimeoutMs ($CommandTimeoutMs + 15000)
      } finally {
        foreach ($key in $oldEnv.Keys) {
          Restore-Env $key $oldEnv[$key]
        }
      }

      $gateText = @(
        (Get-Content -LiteralPath $gateOut -Raw -ErrorAction SilentlyContinue),
        (Get-Content -LiteralPath $gateErr -Raw -ErrorAction SilentlyContinue)
      ) -join "`n"
      try {
        $gate = Extract-GateJson $gateText
      } catch {
        $parseError = $_.Exception.Message
      }
    } catch {
      $bridgeError = $_.Exception.Message
    }
  }

  $ok = ($stackExit -eq 0) -and ($gateExit -eq 0) -and $gate -and $gate.ok
  $row = [ordered]@{
    ok = [bool]$ok
    botCount = $botCount
    commandPlayers = $CommandPlayers
    minCompleted = $CommandCapacityMinCompleted
    minPlayersRequired = $profileMinPlayers
    stackExitCode = $stackExit
    gateExitCode = $gateExit
    stackLog = $stackOut
    stackErrLog = $stackErr
    gateLog = $gateOut
    gateErrLog = $gateErr
    parseError = $parseError
    bridgeError = $bridgeError
  }

  if ($gate) {
    $row.entitiesMin = $gate.capacity.entitiesMin
    $row.entitiesMax = $gate.capacity.entitiesMax
    $row.playersMin = $gate.capacity.playersMin
    $row.playersMax = $gate.capacity.playersMax
    $row.samples = $gate.capacity.samples
    $row.okSamples = $gate.capacity.okSamples
    $row.workerSlotsMin = $gate.capacity.workersMin
    $row.workerSlotsMax = $gate.capacity.workersMax
    $row.rebalanceDelta = $gate.capacity.rebalanceDelta
    $row.loadPeakToMeanMax = $gate.capacity.loadPeakToMeanMax
    $row.brokerMirrorEntitiesMax = $gate.brokerView.entitiesMax
    $row.brokerMirrorOwnerCountMax = $gate.brokerView.ownerCountMax
    $row.completedPlayers = $gate.brokerCommandCapacity.completedPlayers
    $row.failedPlayers = $gate.brokerCommandCapacity.failedPlayers
    $row.totalCommandResponses = $gate.brokerCommandCapacity.totalCommandResponses
    $row.totalCommandOwnerMatches = $gate.brokerCommandCapacity.totalCommandOwnerMatches
    $row.minPostSeamPath = $gate.brokerCommandCapacity.minPostSeamPath
    $row.allPostSeamCommandOk = $gate.brokerCommandCapacity.allPostSeamCommandOk
  }

  $rows += [pscustomobject]$row
  Save-Summary $summaryPath $rows

  if (-not $ok) {
    Write-Host "profile failed: bots=$botCount stack=$stackExit gate=$gateExit gateLog=$gateOut gateErr=$gateErr"
    if (-not $ContinueOnFailure) {
      Write-Host "summary: $summaryPath"
      exit 1
    }
  } else {
    Write-Host "profile passed: bots=$botCount gateLog=$gateOut"
  }

  $profileIndex += 1
}

Write-Host ""
Write-Host "stress ladder summary: $summaryPath"
Get-Content -LiteralPath $summaryPath
