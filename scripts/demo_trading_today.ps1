$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"
$env:HFQT_DATABASE_PATH = "var/demo_trading_today.sqlite3"
if (Test-Path $env:HFQT_DATABASE_PATH) {
    Remove-Item $env:HFQT_DATABASE_PATH -Force
}

Write-Host "[1/3] Scan current watchlist and pick a trade target"
python -m hfqt.app scan-watchlist --symbols US.AAPL

Write-Host "[2/3] Place one positive-control simulated trade on local_paper"
python -m hfqt.app run-manual-event --symbol US.AAPL --headline "Apple supplier checks improve materially and channel inventory tightens" --sentiment 0.82 --qty 1 --broker local_paper

Write-Host "[3/3] Show current portfolio PnL"
python -m hfqt.app portfolio-summary --broker local_paper --symbols US.AAPL
