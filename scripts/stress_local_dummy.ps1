$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"
$env:HFQT_DATABASE_PATH = "var/stress_local_dummy.sqlite3"
if (Test-Path $env:HFQT_DATABASE_PATH) {
    Remove-Item $env:HFQT_DATABASE_PATH -Force
}

Write-Host "[1/2] Run dummy gateway style continuous push through local_paper"
python -m hfqt.app run-dummy-stream --broker local_paper --symbols US.AAPL --events 40 --concurrency 6 --interval-ms 20

Write-Host "[2/2] Show current local_paper portfolio PnL"
python -m hfqt.app portfolio-summary --broker local_paper --symbols US.AAPL
