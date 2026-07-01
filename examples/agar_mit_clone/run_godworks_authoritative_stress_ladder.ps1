param(
  [int[]]$BotCounts = @(20, 50, 100),
  [double]$MinAliveRatio = 0.75,
  [int]$MinEntities = 900,
  [int]$MinWorkers = 16,
  [int]$MinOkSamples = 8,
  [int]$DurationMs = 15000,
  [int]$StackTimeoutMs = 120000,
  [switch]$BuildEachProfile,
  [switch]$ContinueOnFailure,
  [string]$CloneRoot = "",
  [string]$OutputDir = "",
  [int]$PortBroker = 7990,
  [int]$PortGame = 3000,
  [int]$BotPort = 8094
)

$ErrorActionPreference = "Stop"

$ToolsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = (Resolve-Path (Join-Path $ToolsDir "..\..")).Path
$Runner = Join-Path $ToolsDir "run_godworks_authoritative.ps1"
$PowerShellExe = (Get-Command powershell.exe).Source
$NodeExe = (Get-Command node.exe).Source
if (-not $CloneRoot) { $CloneRoot = Join-Path $Repo ".local\agar_mit_clone_authoritative" }
if (-not $OutputDir) { $OutputDir = Join-Path $Repo ".local\agar_authoritative_ladder" }
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

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
  try {
    return [int]$process.ExitCode
  } catch {
    return 1
  }
}

function Start-Bots([string]$Profile, [int]$Count) {
  $out = Join-Path $OutputDir "$Profile.bots.out.log"
  $err = Join-Path $OutputDir "$Profile.bots.err.log"
  Remove-Item -LiteralPath $out,$err -ErrorAction SilentlyContinue
  $oldEnv = @{
    GW_AGAR_GAME_URL = $env:GW_AGAR_GAME_URL
    GW_AUTH_BOTS = $env:GW_AUTH_BOTS
    GW_AUTH_BOT_PORT = $env:GW_AUTH_BOT_PORT
  }
  try {
    $env:GW_AGAR_GAME_URL = "http://127.0.0.1:$PortGame"
    $env:GW_AUTH_BOTS = "$Count"
    $env:GW_AUTH_BOT_PORT = "$BotPort"
    $process = Start-Process -FilePath $NodeExe `
      -ArgumentList @("gw_authoritative_bots.js") `
      -WorkingDirectory $CloneRoot `
      -RedirectStandardOutput $out `
      -RedirectStandardError $err `
      -PassThru `
      -WindowStyle Hidden
  } finally {
    foreach ($key in $oldEnv.Keys) {
      if ($null -eq $oldEnv[$key]) { Remove-Item "Env:\$key" -ErrorAction SilentlyContinue }
      else { Set-Item "Env:\$key" $oldEnv[$key] }
    }
  }
  Wait-Port $BotPort "Godworks authoritative bots"
  return [ordered]@{
    process = $process
    stdout = $out
    stderr = $err
  }
}

function Stop-Bots([object]$BotRun) {
  if ($BotRun -and $BotRun.process) {
    Stop-Process -Id $BotRun.process.Id -Force -ErrorAction SilentlyContinue
  }
}

function Extract-GateJson([string]$Text) {
  $match = [regex]::Match($Text, '(?s)\{\s*"ok"\s*:\s*(?:true|false).*?\}\s*$')
  if (-not $match.Success) {
    throw "gate output did not end with a JSON object"
  }
  return ($match.Value | ConvertFrom-Json)
}

function Get-HostProfile {
  try {
    $cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
    $computer = Get-CimInstance Win32_ComputerSystem | Select-Object -First 1
    $os = Get-CimInstance Win32_OperatingSystem | Select-Object -First 1
    return [ordered]@{
      cpuName = $cpu.Name
      logicalProcessors = [int]$cpu.NumberOfLogicalProcessors
      totalMemoryGiB = [Math]::Round(([double]$computer.TotalPhysicalMemory / 1GB), 3)
      osCaption = $os.Caption
      osArchitecture = $os.OSArchitecture
    }
  } catch {
    return [ordered]@{
      unavailable = $true
      error = $_.Exception.Message
    }
  }
}

function Get-PortProcessMetrics([int[]]$Ports) {
  try {
    $connections = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
      Where-Object { $Ports -contains [int]$_.LocalPort })
    $pids = @($connections | Select-Object -ExpandProperty OwningProcess -Unique |
      Where-Object { $_ -and [int]$_ -gt 0 })
    $items = @()
    foreach ($processId in $pids) {
      $process = Get-Process -Id $processId -ErrorAction Stop
      $cpuSeconds = 0.0
      if ($null -ne $process.CPU) {
        $cpuSeconds = [double]$process.CPU
      }
      $items += [pscustomobject]@{
        pid = [int]$process.Id
        name = $process.ProcessName
        cpuSeconds = [Math]::Round($cpuSeconds, 3)
        workingSetMiB = [Math]::Round(([double]$process.WorkingSet64 / 1MB), 3)
      }
    }
    return [ordered]@{
      capturedAt = (Get-Date).ToString("o")
      ports = $Ports
      processCount = $items.Count
      totalCpuSeconds = [Math]::Round(([double](($items | Measure-Object -Property cpuSeconds -Sum).Sum)), 3)
      totalWorkingSetMiB = [Math]::Round(([double](($items | Measure-Object -Property workingSetMiB -Sum).Sum)), 3)
      processes = @($items)
    }
  } catch {
    return [ordered]@{
      unavailable = $true
      error = $_.Exception.Message
      ports = $Ports
    }
  }
}

function Save-Summary([string]$Path, [object[]]$Rows, [object]$HostProfile) {
  $payload = [ordered]@{
    schemaVersion = 1
    ok = -not [bool]($Rows | Where-Object { -not $_.ok } | Select-Object -First 1)
    gate = "godworks_authoritative_agar_stress_ladder"
    generatedAt = (Get-Date).ToString("o")
    host = $HostProfile
    rows = $Rows
  }
  $payload | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $Path -Encoding UTF8
}

$summaryPath = Join-Path $OutputDir "ladder_summary.json"
$rows = @()
$hostProfile = Get-HostProfile
$profileIndex = 0

foreach ($botCount in $BotCounts) {
  $profile = "auth_bots_$botCount"
  $minAlive = [Math]::Max(1, [int][Math]::Floor($botCount * $MinAliveRatio))
  $stackOut = Join-Path $OutputDir "$profile.stack.out.log"
  $stackErr = Join-Path $OutputDir "$profile.stack.err.log"
  $gateOut = Join-Path $OutputDir "$profile.gate.out.log"
  $gateErr = Join-Path $OutputDir "$profile.gate.err.log"
  Write-Host ""
  Write-Host "== Godworks authoritative agar profile: bots=$botCount minAlive=$minAlive =="

  $runnerArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $Runner,
    "-StopExisting",
    "-PortBroker", "$PortBroker",
    "-PortGame", "$PortGame"
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

  $botRun = $null
  $gate = $null
  $gateExit = 1
  $parseError = $null
  $runError = $null
  $resourceBefore = $null
  $resourceAfter = $null

  try {
    if ($stackExit -ne 0) {
      throw "stack runner failed with exit $stackExit"
    }
    $botRun = Start-Bots $profile $botCount
    Start-Sleep -Seconds 4
    $resourceBefore = Get-PortProcessMetrics -Ports @($PortBroker, $PortGame, $BotPort)

    $oldEnv = @{
      GW_AGAR_STATE_URL = $env:GW_AGAR_STATE_URL
      GW_AUTH_BOTS_URL = $env:GW_AUTH_BOTS_URL
      GW_AUTH_CAPACITY_MS = $env:GW_AUTH_CAPACITY_MS
      GW_AUTH_CAPACITY_MIN_PLAYERS = $env:GW_AUTH_CAPACITY_MIN_PLAYERS
      GW_AUTH_CAPACITY_MIN_ENTITIES = $env:GW_AUTH_CAPACITY_MIN_ENTITIES
      GW_AUTH_CAPACITY_MIN_WORKERS = $env:GW_AUTH_CAPACITY_MIN_WORKERS
      GW_AUTH_CAPACITY_MIN_OK_SAMPLES = $env:GW_AUTH_CAPACITY_MIN_OK_SAMPLES
      GW_AUTH_CAPACITY_MIN_COMMAND_DELTA = $env:GW_AUTH_CAPACITY_MIN_COMMAND_DELTA
      GW_AUTH_CAPACITY_MAX_REJECT_DELTA = $env:GW_AUTH_CAPACITY_MAX_REJECT_DELTA
      GW_AUTH_CAPACITY_MAX_TRANSIENT_REJECT_DELTA = $env:GW_AUTH_CAPACITY_MAX_TRANSIENT_REJECT_DELTA
      GW_AUTH_CAPACITY_MIN_BOT_ALIVE = $env:GW_AUTH_CAPACITY_MIN_BOT_ALIVE
      GW_AUTH_CAPACITY_MIN_BOT_FRAME_DELTA = $env:GW_AUTH_CAPACITY_MIN_BOT_FRAME_DELTA
    }
    try {
      $env:GW_AGAR_STATE_URL = "http://127.0.0.1:$PortGame/state"
      $env:GW_AUTH_BOTS_URL = "http://127.0.0.1:$BotPort/state"
      $env:GW_AUTH_CAPACITY_MS = "$DurationMs"
      $env:GW_AUTH_CAPACITY_MIN_PLAYERS = "$minAlive"
      $env:GW_AUTH_CAPACITY_MIN_ENTITIES = "$MinEntities"
      $env:GW_AUTH_CAPACITY_MIN_WORKERS = "$MinWorkers"
      $env:GW_AUTH_CAPACITY_MIN_OK_SAMPLES = "$MinOkSamples"
      $env:GW_AUTH_CAPACITY_MIN_COMMAND_DELTA = "$minAlive"
      $env:GW_AUTH_CAPACITY_MAX_REJECT_DELTA = "0"
      $env:GW_AUTH_CAPACITY_MAX_TRANSIENT_REJECT_DELTA = "$([Math]::Max($minAlive * 2, 50))"
      $env:GW_AUTH_CAPACITY_MIN_BOT_ALIVE = "$minAlive"
      $env:GW_AUTH_CAPACITY_MIN_BOT_FRAME_DELTA = "$minAlive"

      $gateExit = Invoke-CapturedProcess `
        -FilePath $NodeExe `
        -Arguments @("gw_authoritative_capacity_gate.js") `
        -WorkingDirectory $CloneRoot `
        -StdoutPath $gateOut `
        -StderrPath $gateErr `
        -TimeoutMs ($DurationMs + 30000)
      $resourceAfter = Get-PortProcessMetrics -Ports @($PortBroker, $PortGame, $BotPort)
    } finally {
      foreach ($key in $oldEnv.Keys) {
        if ($null -eq $oldEnv[$key]) { Remove-Item "Env:\$key" -ErrorAction SilentlyContinue }
        else { Set-Item "Env:\$key" $oldEnv[$key] }
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
    $runError = $_.Exception.Message
  } finally {
    Stop-Bots $botRun
  }

  $ok = ($stackExit -eq 0) -and ($gateExit -eq 0) -and $gate -and $gate.ok
  $row = [ordered]@{
    ok = [bool]$ok
    botCount = $botCount
    minAliveRequired = $minAlive
    stackExitCode = $stackExit
    gateExitCode = $gateExit
    stackLog = $stackOut
    stackErrLog = $stackErr
    gateLog = $gateOut
    gateErrLog = $gateErr
    botLog = if ($botRun) { $botRun.stdout } else { $null }
    botErrLog = if ($botRun) { $botRun.stderr } else { $null }
    parseError = $parseError
    runError = $runError
    resourceBefore = $resourceBefore
    resourceAfter = $resourceAfter
  }
  if ($gate) {
    $row.thresholds = $gate.thresholds
    $row.samples = $gate.samples
    $row.deltas = $gate.deltas
    $row.initialState = $gate.initialState
    $row.initialBots = $gate.initialBots
    $row.latestState = $gate.latestState
    $row.latestBots = $gate.latestBots
  }
  $rows += [pscustomobject]$row
  Save-Summary $summaryPath $rows $hostProfile

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
Write-Host "authoritative stress ladder summary: $summaryPath"
Get-Content -LiteralPath $summaryPath
