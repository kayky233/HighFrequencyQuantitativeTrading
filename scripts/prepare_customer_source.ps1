param(
    [string]$OutputDir = "output/customer_source_minimal"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$target = Join-Path $root $OutputDir

if (Test-Path $target) {
    Remove-Item -Recurse -Force $target
}

New-Item -ItemType Directory -Force -Path $target | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $target "var") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $target "config") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $target "cache") | Out-Null

$copyItems = @(
    "src",
    "fixtures",
    "docs",
    "pyproject.toml",
    ".env.client.minimal.example",
    ".env.client.usmart.minimal.example",
    ".env.usmart.example",
    "scripts/ignite_local.ps1",
    "scripts/ignite_analysis.ps1",
    "scripts/ignite_public_no_key.ps1"
    "scripts/ignite_usmart.ps1"
)

foreach ($item in $copyItems) {
    $sourcePath = Join-Path $root $item
    if (-not (Test-Path $sourcePath)) {
        Write-Warning "Skip missing path: $item"
        continue
    }
    $destinationPath = Join-Path $target $item
    $destinationParent = Split-Path -Parent $destinationPath
    if ($destinationParent) {
        New-Item -ItemType Directory -Force -Path $destinationParent | Out-Null
    }
    Copy-Item -Recurse -Force $sourcePath $destinationPath
}

Get-ChildItem -Path $target -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Path $target -Recurse -File -Filter "*.pyc" | Remove-Item -Force

Write-Host "Prepared customer source bundle at: $target"
