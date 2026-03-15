$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"

Write-Host "[1/3] Local broker healthcheck"
python -m hfqt.app healthcheck --broker local_paper

Write-Host "[2/3] Replay sample fixture through the full pipeline"
python -m hfqt.app run-replay --fixture fixtures\sample_news_event.json --broker local_paper

Write-Host "[3/3] FastAPI can be started with:"
Write-Host "    python -m hfqt.app serve-api --host 127.0.0.1 --port 8000"
