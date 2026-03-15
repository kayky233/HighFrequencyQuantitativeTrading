from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hfqt.config import AppConfig
from hfqt.marketdata.futu_quote import FutuQuoteAdapter


async def main_async(symbol: str) -> None:
    config = AppConfig.from_env()
    adapter = FutuQuoteAdapter(config)
    snapshot = await adapter.get_snapshot(symbol)
    print(json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-test Futu quote access.")
    parser.add_argument("--symbol", default="US.AAPL")
    args = parser.parse_args()
    asyncio.run(main_async(args.symbol))


if __name__ == "__main__":
    main()
