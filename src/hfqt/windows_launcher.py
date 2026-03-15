from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser

import uvicorn

from hfqt.app import create_fastapi_app, main as cli_main
from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger, setup_logging


logger = get_logger("windows_launcher")


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _browser_url(host: str, port: int) -> str:
    if host in {"0.0.0.0", "::"}:
        return f"http://127.0.0.1:{port}/"
    return f"http://{host}:{port}/"


def _open_browser_later(url: str) -> None:
    def _task() -> None:
        time.sleep(1.5)
        try:
            webbrowser.open(url)
        except Exception:  # noqa: BLE001
            logger.exception("failed to open browser", extra={"event": "launcher_open_browser_failed", "url": url})

    threading.Thread(target=_task, daemon=True).start()


def main() -> None:
    if len(sys.argv) > 1:
        cli_main()
        return

    config = AppConfig.from_env()
    setup_logging(config)
    host = config.api_host
    port = config.api_port
    url = _browser_url(host, port)

    logger.info(
        "starting packaged dashboard",
        extra={
            "event": "launcher_start_dashboard",
            "host": host,
            "port": port,
            "url": url,
        },
    )
    print(f"HFQT dashboard starting on {url}")
    print("You can place a local .env next to the .exe and the app will load it automatically.")

    if _env_flag("HFQT_OPEN_BROWSER", True):
        _open_browser_later(url)

    uvicorn.run(create_fastapi_app(), host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
