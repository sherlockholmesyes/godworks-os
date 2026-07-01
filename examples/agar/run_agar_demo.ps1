param(
  [int]$Port = 7777,
  [int]$HttpPort = 8091,
  [string]$Grid = "4x4",
  [double]$Arena = 120,
  [switch]$GateOnly
)

$ErrorActionPreference = "Stop"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$LogDir = Join-Path $Repo ".local\agar"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$broker = Join-Path $Repo "target\debug\godworks_broker.exe"
if (!(Test-Path $broker)) {
  Push-Location $Repo
  try { cargo build --bin godworks_broker } finally { Pop-Location }
}

$cols, $rows = $Grid -split "x" | ForEach-Object { [int]$_ }
$regions = for ($y = 0; $y -lt $rows; $y++) { for ($x = 0; $x -lt $cols; $x++) { "Z${x}_${y}" } }
$claims = @("obs-token:OBS:observer|inspector", "spawn-token:AGAR_SPAWNER:", "browser-token:CLIENT:role.client")
foreach ($r in $regions) { $claims += "$r-token:${r}:" }

$old = @{
  GW_PORT = $env:GW_PORT
  GW_BIND = $env:GW_BIND
  GW_WAL = $env:GW_WAL
  GW_GRID2D = $env:GW_GRID2D
  GW_GRID = $env:GW_GRID
  GW_ARENA = $env:GW_ARENA
  GW_WORLD = $env:GW_WORLD
  GW_AUTH_CLAIMS = $env:GW_AUTH_CLAIMS
  GW_HTTP = $env:GW_HTTP
  GW_SPEED = $env:GW_SPEED
}

$procs = @()
function Start-HiddenProc([string]$FilePath, [string[]]$ProcArgs, [string]$OutPath, [string]$ErrPath) {
  if ($ProcArgs -and $ProcArgs.Count -gt 0) {
    Start-Process -FilePath $FilePath -ArgumentList $ProcArgs -WorkingDirectory $Repo -WindowStyle Hidden -PassThru -RedirectStandardOutput $OutPath -RedirectStandardError $ErrPath
  } else {
    Start-Process -FilePath $FilePath -WorkingDirectory $Repo -WindowStyle Hidden -PassThru -RedirectStandardOutput $OutPath -RedirectStandardError $ErrPath
  }
}

try {
  $env:GW_PORT = "$Port"
  $env:GW_BIND = "127.0.0.1"
  $env:GW_WAL = (Join-Path $LogDir "agar.wal")
  $env:GW_GRID2D = $Grid
  $env:GW_GRID = $Grid
  $env:GW_ARENA = "$Arena,$Arena"
  $env:GW_WORLD = "0,$Arena,0,$Arena"
  $env:GW_AUTH_CLAIMS = ($claims -join ",")
  $env:GW_SPEED = "18"
  Remove-Item $env:GW_WAL -ErrorAction SilentlyContinue

  $procs += Start-HiddenProc -FilePath $broker -ProcArgs @() -OutPath (Join-Path $LogDir "broker.out.log") -ErrPath (Join-Path $LogDir "broker.err.log")
  Start-Sleep -Milliseconds 800

  $worker = Join-Path $PSScriptRoot "gw_agar_zone_worker.js"
  foreach ($r in $regions) {
    $env:GW_REGION = $r
    $env:GW_WID = "agar-$r"
    $env:GW_CONNECT_TOKEN = "$r-token"
    $env:GW_SPAWN = "4"
    $env:GW_FOOD = "22"
    $procs += Start-HiddenProc -FilePath "node" -ProcArgs @($worker) -OutPath (Join-Path $LogDir "$r.out.log") -ErrPath (Join-Path $LogDir "$r.err.log")
  }

  $env:GW_HTTP = "$HttpPort"
  $env:GW_OBS_TOKEN = "obs-token"
  $env:GW_CLIENT_TOKEN = "spawn-token"
  $env:GW_BROWSER_TOKEN = "browser-token"
  $gateway = Join-Path $PSScriptRoot "gw_agar_gateway.js"
  $procs += Start-HiddenProc -FilePath "node" -ProcArgs @($gateway) -OutPath (Join-Path $LogDir "gateway.out.log") -ErrPath (Join-Path $LogDir "gateway.err.log")
  Start-Sleep -Seconds 2

  Write-Host "Godworks agar.io demo: http://localhost:$HttpPort"

  if ($GateOnly) {
    $env:GW_AGAR_URL = "http://127.0.0.1:$HttpPort"
    $env:GW_GATE_MIN_OWNERS = [Math]::Min(4, $regions.Count)
    node (Join-Path $PSScriptRoot "gw_agar_gate.js")
    if ($LASTEXITCODE -ne 0) { throw "agar reality gate failed with exit code $LASTEXITCODE" }
  } else {
    Write-Host "Press Ctrl+C to stop. Logs: $LogDir"
    while ($true) { Start-Sleep -Seconds 1 }
  }
}
finally {
  foreach ($p in $procs) {
    if ($p -and !$p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
  }
  foreach ($k in $old.Keys) {
    if ($null -eq $old[$k]) { Remove-Item "Env:$k" -ErrorAction SilentlyContinue } else { Set-Item "Env:$k" $old[$k] }
  }
  Remove-Item Env:GW_REGION -ErrorAction SilentlyContinue
  Remove-Item Env:GW_WID -ErrorAction SilentlyContinue
  Remove-Item Env:GW_CONNECT_TOKEN -ErrorAction SilentlyContinue
  Remove-Item Env:GW_SPAWN -ErrorAction SilentlyContinue
  Remove-Item Env:GW_FOOD -ErrorAction SilentlyContinue
  Remove-Item Env:GW_OBS_TOKEN -ErrorAction SilentlyContinue
  Remove-Item Env:GW_CLIENT_TOKEN -ErrorAction SilentlyContinue
  Remove-Item Env:GW_BROWSER_TOKEN -ErrorAction SilentlyContinue
  Remove-Item Env:GW_AGAR_URL -ErrorAction SilentlyContinue
  Remove-Item Env:GW_GATE_MIN_OWNERS -ErrorAction SilentlyContinue
}
