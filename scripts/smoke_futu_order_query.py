from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hfqt.brokers.futu_sim import FutuSimBrokerAdapter
from hfqt.config import AppConfig


async def main_async() -> None:
    config = AppConfig.from_env()
    broker = FutuSimBrokerAdapter(config)
    await broker.connect()
    try:
        orders = await broker.list_orders()
        print(json.dumps([item.model_dump(mode="json") for item in orders], ensure_ascii=False, indent=2))
    finally:
        await broker.close()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
