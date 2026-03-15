param(
    [switch]$Clean = $true
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "[1/3] Checking PyInstaller..."
python -m PyInstaller --version | Out-Host

if ($Clean) {
    Write-Host "[2/3] Cleaning previous build artifacts..."
    if (Test-Path build) { Remove-Item build -Recurse -Force }
    if (Test-Path dist) { Remove-Item dist -Recurse -Force }
}

Write-Host "[3/3] Building HFQT.exe ..."
python -m PyInstaller HFQT.spec --noconfirm --clean

Write-Host ""
Write-Host "Build complete:"
Write-Host "  dist\\HFQT.exe"
Write-Host ""
Write-Host "Usage:"
Write-Host "  1. Put your .env next to HFQT.exe"
Write-Host "  2. Double-click HFQT.exe to launch the dashboard"
Write-Host "  3. Or run HFQT.exe <command> to use CLI mode"
