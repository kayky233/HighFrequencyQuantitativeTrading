from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hfqt.brokers.futu_sim import FutuSimBrokerAdapter
from hfqt.config import AppConfig
from hfqt.schemas import OrderRequest, OrderType, TradeAction


async def main_async(symbol: str, price: float, qty: float) -> None:
    config = AppConfig.from_env()
    broker = FutuSimBrokerAdapter(config)
    await broker.connect()
    try:
        accounts = await broker.get_accounts()
        order = OrderRequest(
            intent_id="smoke-futu-trade",
            broker=broker.name,
            symbol=symbol,
            side=TradeAction.BUY,
            quantity=qty,
            order_type=OrderType.LIMIT,
            price=price,
        )
        submitted = await broker.submit_order(order)
        latest = await broker.get_order(submitted.broker_order_id)
        print(
            json.dumps(
                {
                    "accounts": [item.model_dump(mode="json") for item in accounts],
                    "submitted": submitted.model_dump(mode="json"),
                    "latest": latest.model_dump(mode="json") if latest else None,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        await broker.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-test Futu simulated trade.")
    parser.add_argument("--symbol", default="US.AAPL")
    parser.add_argument("--price", type=float, default=150.0)
    parser.add_argument("--qty", type=float, default=1.0)
    args = parser.parse_args()
    asyncio.run(main_async(args.symbol, args.price, args.qty))


if __name__ == "__main__":
    main()
