$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"

Write-Host "[1/4] Local broker healthcheck"
python -m hfqt.app healthcheck --broker local_paper

Write-Host "[2/4] Public no-key market data + network intel + local simulated execution"
python -m hfqt.app run-network-event --symbol US.AAPL --query "AAPL stock earnings" --broker local_paper

Write-Host "[3/4] Positive control to verify the simulated broker can place and fill"
python -m hfqt.app run-manual-event --symbol US.AAPL --headline "Apple supplier checks improve materially and channel inventory tightens" --sentiment 0.82 --qty 1 --broker local_paper

Write-Host "[4/4] Optional LLM check with the same no-key market data path"
if ($env:HFQT_LLM_BASE_URL -and $env:HFQT_LLM_MODEL) {
    python -m hfqt.app run-manual-event --symbol US.AAPL --headline "Apple supplier checks improve and channel inventory tightens" --sentiment 0.68 --qty 1 --broker local_paper --llm
} else {
    Write-Host "    Skipped: HFQT_LLM_BASE_URL / HFQT_LLM_MODEL not set."
}
