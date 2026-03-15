from __future__ import annotations

import importlib

from hfqt.config import AppConfig
from hfqt.marketdata.base import MarketDataAdapter
from hfqt.schemas import MarketSnapshot


class FutuQuoteAdapter(MarketDataAdapter):
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def _load_sdk(self):
        for module_name in ("futu", "moomoo"):
            try:
                return importlib.import_module(module_name)
            except ModuleNotFoundError:
                continue
        raise RuntimeError("Neither 'futu' nor 'moomoo' SDK is installed.")

    async def get_snapshot(self, symbol: str) -> MarketSnapshot:
        sdk = self._load_sdk()
        ctx = sdk.OpenQuoteContext(host=self.config.futu_host, port=self.config.futu_port)
        try:
            ret_sub, err = ctx.subscribe([symbol], [sdk.SubType.QUOTE], subscribe_push=False)
            if ret_sub != sdk.RET_OK:
                raise RuntimeError(str(err))
            ret, data = ctx.get_stock_quote([symbol])
            if ret != sdk.RET_OK:
                raise RuntimeError(str(data))
            row = data.iloc[0].to_dict()
            return MarketSnapshot(
                symbol=symbol,
                last_price=float(row.get("last_price") or row.get("cur_price")),
                bid_price=float(row.get("bid_price")) if row.get("bid_price") is not None else None,
                ask_price=float(row.get("ask_price")) if row.get("ask_price") is not None else None,
                raw=row,
            )
        finally:
            ctx.close()
