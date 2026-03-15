$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"

Write-Host "[1/3] Local analysis healthcheck"
python -m hfqt.app healthcheck --broker local_paper

if ($env:HFQT_LLM_BASE_URL -and $env:HFQT_LLM_MODEL) {
    Write-Host "[2/3] Running replay through the LLM reasoner"
    python -m hfqt.app run-replay --fixture fixtures\sample_news_event.json --broker local_paper --llm
} else {
    Write-Host "[2/3] No HFQT_LLM_BASE_URL / HFQT_LLM_MODEL found, falling back to MockReasoner"
    python -m hfqt.app run-replay --fixture fixtures\sample_news_event.json --broker local_paper
}

Write-Host "[3/3] To accept scraper events over HTTP, run:"
Write-Host "    python -m hfqt.app serve-api --host 127.0.0.1 --port 8000"
Write-Host "    POST /signals/manual?broker=local_paper&llm=true"
