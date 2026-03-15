$ErrorActionPreference = "Stop"

Write-Host "== HFQT uSmart ignition =="
Write-Host "1. Static healthcheck"
$env:PYTHONPATH = "src"
python -m hfqt.app healthcheck --broker usmart

Write-Host ""
Write-Host "2. Read-only smoke"
python -m hfqt.app usmart-smoke --market us
