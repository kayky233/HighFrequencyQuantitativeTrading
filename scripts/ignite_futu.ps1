$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"

Write-Host "[1/4] Checking OpenD TCP reachability"
$reachable = Test-NetConnection 127.0.0.1 -Port 11111 -WarningAction SilentlyContinue
if (-not $reachable.TcpTestSucceeded) {
    Write-Host "OpenD is not listening on 127.0.0.1:11111."
    Write-Host "Please start FutuOpenD, log in interactively, then rerun this script."
    exit 1
}

Write-Host "[2/4] Broker healthcheck"
python -m hfqt.app healthcheck --broker futu_sim

Write-Host "[3/4] Listing simulated accounts"
python -m hfqt.app list-accounts --broker futu_sim

Write-Host "[4/4] Pulling one quote snapshot"
python -m hfqt.app quote --symbol US.AAPL
