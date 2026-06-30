param(
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Clear-GwEnv {
    foreach ($key in @(
        "GW_HOST", "GW_PORT", "GW_WAL", "GW_DURABLE_FLUSH_MS", "GW_BOUNDARY", "GW_BOUNDARIES",
        "GW_GRID2D", "GW_MESH", "GW_MESH_EAST", "GW_ADVERTISE", "GW_TARGET", "GW_TARGET_E",
        "GW_ENTITIES", "GW_TICKS", "GW_HZ", "GW_EVENT_BURST", "GW_REQUIRE_MESH", "GW_SLOW_VIEWER",
        "GW_ZW_HOST", "GW_ZW_PORT", "GW_ZW_REGION", "GW_ZW_ID", "GW_ZW_SPAWN",
        "GW_ZW_SPAWN_BOX", "GW_ZW_SPAWN_SPEED", "GW_ZW_SPAWN_VEL", "GW_ZW_RADIUS",
        "GW_ZW_DURATION", "GW_ZW_HZ", "GW_ZW_SEED"
    )) {
        Set-Item -Path "env:$key" -Value $null -ErrorAction SilentlyContinue
    }
}

function Invoke-Cargo {
    param([string[]]$CargoArgs)
    Write-Host "==> cargo $($CargoArgs -join ' ')"
    & cargo @CargoArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo $($CargoArgs -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Get-FreePort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse("127.0.0.1"), 0)
    $listener.Start()
    try {
        return [int]$listener.LocalEndpoint.Port
    } finally {
        $listener.Stop()
    }
}

function Wait-ForPort {
    param([int]$Port)
    for ($i = 0; $i -lt 80; $i++) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $client.Connect("127.0.0.1", $Port)
            return
        } catch {
            Start-Sleep -Milliseconds 50
        } finally {
            $client.Close()
        }
    }
    throw "broker did not open port $Port"
}

function Start-LoggedProcess {
    param(
        [string]$Exe,
        [string]$Stdout,
        [string]$Stderr
    )
    Start-Process `
        -FilePath $Exe `
        -WorkingDirectory $repoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $Stdout `
        -RedirectStandardError $Stderr `
        -PassThru
}

function Stop-SmokeProcess {
    param($Process)
    if ($null -ne $Process) {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 50
    }
}

function Wait-SmokeProcess {
    param(
        $Process,
        [int]$TimeoutMs,
        [string]$Name,
        [string]$Stdout,
        [string]$Stderr
    )
    $Process.WaitForExit($TimeoutMs) | Out-Null
    if (-not $Process.HasExited) {
        Stop-SmokeProcess $Process
        throw "$Name timed out; stdout=$Stdout stderr=$Stderr"
    }
    $Process.Refresh()
    $exitCode = $Process.ExitCode
    if ($null -eq $exitCode) {
        $exitCode = 0
    }
    if ($exitCode -ne 0) {
        $out = if (Test-Path $Stdout) { Get-Content -Raw $Stdout } else { "" }
        $err = if (Test-Path $Stderr) { Get-Content -Raw $Stderr } else { "" }
        throw "$Name failed with exit code $exitCode`n$out`n$err"
    }
}

function Get-ResultLine {
    param([string]$Path, [string]$Prefix)
    $line = Get-Content $Path | Where-Object { $_.StartsWith($Prefix) } | Select-Object -Last 1
    if (-not $line) {
        throw "missing line prefix '$Prefix' in $Path"
    }
    return $line
}

function Convert-MetricLine {
    param([string]$Line)
    $result = @{}
    foreach ($part in $Line.Split(" ")) {
        $kv = $part.Split("=", 2)
        if ($kv.Count -eq 2) {
            $result[$kv[0]] = $kv[1]
        }
    }
    return $result
}

function Convert-ZoneWorkerSummary {
    param([string]$Line)
    return ($Line.Substring("zone_worker_summary ".Length) | ConvertFrom-Json)
}

function Assert-OnlyOldOwnerRejects {
    param(
        $Summary,
        [string]$Owner,
        [int]$Max
    )
    $props = @($Summary.reject_classes.PSObject.Properties)
    $total = 0
    foreach ($prop in $props) {
        $total += [int]$prop.Value
        if ($prop.Name -notmatch "^comp=(pos|vel)\|reason=not_authoritative\|owner=$([regex]::Escape($Owner))$") {
            throw "unexpected reject class '$($prop.Name)': $($Summary | ConvertTo-Json -Compress)"
        }
    }
    if ($total -ne [int]$Summary.rejects) {
        throw "reject class total $total does not match rejects $($Summary.rejects): $($Summary | ConvertTo-Json -Compress)"
    }
    if ($total -gt $Max) {
        throw "old-owner reject storm total=$total max=${Max}: $($Summary | ConvertTo-Json -Compress)"
    }
}

function Assert-NoRejectClasses {
    param($Summary)
    $props = @($Summary.reject_classes.PSObject.Properties)
    if ($props.Count -ne 0 -or [int]$Summary.rejects -ne 0) {
        throw "unexpected rejects: $($Summary | ConvertTo-Json -Compress)"
    }
}

function Invoke-RealityLoadgenSmoke {
    param([string]$OutDir)
    Write-Host "==> runtime smoke: broker + reality_loadgen"

    $port = Get-FreePort
    $broker = $null
    $brokerOut = Join-Path $OutDir "loadgen_broker.out.log"
    $brokerErr = Join-Path $OutDir "loadgen_broker.err.log"
    $loadOut = Join-Path $OutDir "loadgen.out.log"
    $loadErr = Join-Path $OutDir "loadgen.err.log"

    try {
        Clear-GwEnv
        $env:GW_HOST = "127.0.0.1"
        $env:GW_PORT = "$port"
        $env:GW_WAL = Join-Path $OutDir "loadgen_smoke.wal"
        $env:GW_DURABLE_FLUSH_MS = "5"
        $env:GW_BOUNDARY = "0"
        $broker = Start-LoggedProcess (Join-Path $repoRoot "target\release\godworks_broker.exe") $brokerOut $brokerErr
        Wait-ForPort $port

        Clear-GwEnv
        $env:GW_HOST = "127.0.0.1"
        $env:GW_TARGET = "$port"
        $env:GW_TARGET_E = "$port"
        $env:GW_ENTITIES = "12"
        $env:GW_TICKS = "90"
        $env:GW_HZ = "30"
        $env:GW_SLOW_VIEWER = "1"
        $loadgen = Start-LoggedProcess (Join-Path $repoRoot "target\release\reality_loadgen.exe") $loadOut $loadErr
        Wait-SmokeProcess $loadgen 30000 "reality_loadgen" $loadOut $loadErr

        $line = Get-ResultLine $loadOut "reality_loadgen "
        $metrics = Convert-MetricLine $line
        if ($metrics["result"] -ne "pass" -or $metrics["failures"] -ne "none") {
            throw "reality_loadgen smoke did not pass: $line"
        }
        return $line
    } finally {
        Stop-SmokeProcess $broker
        Clear-GwEnv
    }
}

function Invoke-ZoneWorkerSmoke {
    param([string]$OutDir)
    Write-Host "==> runtime smoke: broker + W/E zone_workers"

    $port = Get-FreePort
    $broker = $null
    $west = $null
    $east = $null
    $brokerOut = Join-Path $OutDir "zone_broker.out.log"
    $brokerErr = Join-Path $OutDir "zone_broker.err.log"
    $westOut = Join-Path $OutDir "worker_w.out.log"
    $westErr = Join-Path $OutDir "worker_w.err.log"
    $eastOut = Join-Path $OutDir "worker_e.out.log"
    $eastErr = Join-Path $OutDir "worker_e.err.log"

    try {
        Clear-GwEnv
        $env:GW_HOST = "127.0.0.1"
        $env:GW_PORT = "$port"
        $env:GW_WAL = Join-Path $OutDir "zone_worker_smoke.wal"
        $env:GW_DURABLE_FLUSH_MS = "5"
        $env:GW_BOUNDARY = "0"
        $broker = Start-LoggedProcess (Join-Path $repoRoot "target\release\godworks_broker.exe") $brokerOut $brokerErr
        Wait-ForPort $port

        Clear-GwEnv
        $env:GW_ZW_HOST = "127.0.0.1"
        $env:GW_ZW_PORT = "$port"
        $env:GW_ZW_REGION = "E"
        $env:GW_ZW_ID = "zw-E-smoke"
        $env:GW_ZW_SPAWN = "0"
        $env:GW_ZW_DURATION = "5.0"
        $env:GW_ZW_HZ = "30"
        $env:GW_ZW_SPAWN_SPEED = "0"
        $env:GW_ZW_SPAWN_VEL = "0,0"
        $env:GW_ZW_RADIUS = "0.05"
        $east = Start-LoggedProcess (Join-Path $repoRoot "target\release\zone_worker.exe") $eastOut $eastErr
        Start-Sleep -Milliseconds 250

        Clear-GwEnv
        $env:GW_ZW_HOST = "127.0.0.1"
        $env:GW_ZW_PORT = "$port"
        $env:GW_ZW_REGION = "W"
        $env:GW_ZW_ID = "zw-W-smoke"
        $env:GW_ZW_SPAWN = "24"
        $env:GW_ZW_SPAWN_BOX = "-4,-2,-12,12"
        $env:GW_ZW_SPAWN_SPEED = "0"
        $env:GW_ZW_SPAWN_VEL = "10,0"
        $env:GW_ZW_RADIUS = "0.05"
        $env:GW_ZW_DURATION = "5.0"
        $env:GW_ZW_HZ = "30"
        $west = Start-LoggedProcess (Join-Path $repoRoot "target\release\zone_worker.exe") $westOut $westErr

        Wait-SmokeProcess $west 15000 "zone_worker W" $westOut $westErr
        Wait-SmokeProcess $east 15000 "zone_worker E" $eastOut $eastErr

        $westSummary = Convert-ZoneWorkerSummary (Get-ResultLine $westErr "zone_worker_summary ")
        $eastSummary = Convert-ZoneWorkerSummary (Get-ResultLine $eastErr "zone_worker_summary ")

        if ([int]$westSummary.auth_gain -ne 24) { throw "W auth_gain != 24: $($westSummary | ConvertTo-Json -Compress)" }
        if ([int]$westSummary.auth_loss -ne 24) { throw "W auth_loss != 24: $($westSummary | ConvertTo-Json -Compress)" }
        if ([int]$westSummary.owned -ne 0) { throw "W owned != 0: $($westSummary | ConvertTo-Json -Compress)" }
        if ([int]$eastSummary.auth_gain -lt 24) { throw "E auth_gain < 24: $($eastSummary | ConvertTo-Json -Compress)" }
        if ([int]$eastSummary.rejects -ne 0) { throw "E rejects != 0: $($eastSummary | ConvertTo-Json -Compress)" }
        Assert-OnlyOldOwnerRejects $westSummary "zw-E-smoke" 48
        Assert-NoRejectClasses $eastSummary

        Write-Host "ok: zone_worker smoke"
        return [pscustomobject]@{
            west = @{
                auth_gain = [int]$westSummary.auth_gain
                auth_loss = [int]$westSummary.auth_loss
                owned = [int]$westSummary.owned
                rejects = [int]$westSummary.rejects
                reject_classes = $westSummary.reject_classes
            }
            east = @{
                auth_gain = [int]$eastSummary.auth_gain
                auth_loss = [int]$eastSummary.auth_loss
                owned = [int]$eastSummary.owned
                rejects = [int]$eastSummary.rejects
                reject_classes = $eastSummary.reject_classes
            }
        }
    } finally {
        Write-Host "cleanup: zone worker processes"
        Stop-SmokeProcess $west
        Stop-SmokeProcess $east
        Stop-SmokeProcess $broker
        Clear-GwEnv
        Write-Host "cleanup: zone worker done"
    }
}

if (-not $SkipBuild) {
    Invoke-Cargo -CargoArgs @("build", "--workspace", "--release")
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $repoRoot ".local\runtime_smoke_$stamp"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$loadgenLine = Invoke-RealityLoadgenSmoke $outDir
Write-Host "ok: reality_loadgen smoke"
$zoneSummary = Invoke-ZoneWorkerSmoke $outDir
Write-Host "ok: all smoke stages"

Write-Output "runtime_smoke result=pass log_dir=$outDir"
Write-Output $loadgenLine
Write-Output "zone_worker west_auth_gain=$($zoneSummary.west.auth_gain) west_auth_loss=$($zoneSummary.west.auth_loss) west_owned=$($zoneSummary.west.owned) west_rejects=$($zoneSummary.west.rejects) east_auth_gain=$($zoneSummary.east.auth_gain) east_rejects=$($zoneSummary.east.rejects)"
Write-Host "runtime smoke passed"
