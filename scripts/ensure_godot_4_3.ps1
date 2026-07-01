param(
  [string]$InstallDir = "",
  [switch]$ForceDownload
)

$ErrorActionPreference = "Stop"

$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $InstallDir) {
  $InstallDir = Join-Path $Repo ".local\tools\godot-4.3"
}

$GodotUrl = "https://github.com/godotengine/godot/releases/download/4.3-stable/Godot_v4.3-stable_win64.exe.zip"
$ExpectedZipSha256 = "8F2C75B734BD956027AE3CA92C41F78B5D5A255DACC0F20E4E3C523C545AD410"
$ZipPath = Join-Path $InstallDir "Godot_v4.3-stable_win64.exe.zip"
$ConsoleExe = Join-Path $InstallDir "Godot_v4.3-stable_win64_console.exe"

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

if ($ForceDownload -or -not (Test-Path -LiteralPath $ZipPath)) {
  Remove-Item -LiteralPath $ZipPath -Force -ErrorAction SilentlyContinue
  Write-Host "Downloading Godot 4.3 stable portable zip..."
  Invoke-WebRequest -UseBasicParsing -Uri $GodotUrl -OutFile $ZipPath
}

$actualHash = (Get-FileHash -LiteralPath $ZipPath -Algorithm SHA256).Hash.ToUpperInvariant()
if ($actualHash -ne $ExpectedZipSha256) {
  throw "Godot zip SHA256 mismatch. expected=$ExpectedZipSha256 actual=$actualHash path=$ZipPath"
}

if (-not (Test-Path -LiteralPath $ConsoleExe)) {
  Write-Host "Extracting Godot 4.3 stable..."
  Expand-Archive -LiteralPath $ZipPath -DestinationPath $InstallDir -Force
}

if (-not (Test-Path -LiteralPath $ConsoleExe)) {
  throw "Godot console executable missing after extraction: $ConsoleExe"
}

Write-Output (Resolve-Path $ConsoleExe).Path
