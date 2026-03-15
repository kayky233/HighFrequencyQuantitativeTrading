from __future__ import annotations

from dataclasses import dataclass

from hfqt.marketdata import PublicPriceActionAdapter
from hfqt.schemas import OrderRecord, OrderStatus, PortfolioSummary, PositionSummary, TradeAction
from hfqt.store.sqlite_store import SQLiteAuditStore


@dataclass
class _PositionState:
    quantity: float = 0.0
    avg_cost: float = 0.0
    realized_pnl: float = 0.0


class PortfolioAnalyzer:
    def __init__(self, store: SQLiteAuditStore, marketdata: PublicPriceActionAdapter) -> None:
        self.store = store
        self.marketdata = marketdata

    async def summarize(self, broker: str, symbols: list[str] | None = None) -> PortfolioSummary:
        records = await self.store.load_order_records(broker=broker, status=OrderStatus.FILLED.value)
        symbol_filter = {item.upper() for item in symbols} if symbols else None
        states: dict[str, _PositionState] = {}
        for record in records:
            if symbol_filter and record.symbol.upper() not in symbol_filter:
                continue
            self._apply_fill(states.setdefault(record.symbol, _PositionState()), record)

        positions: list[PositionSummary] = []
        realized_total = 0.0
        unrealized_total = 0.0
        cost_basis_total = 0.0
        gross_market_value_total = 0.0
        net_market_value_total = 0.0
        winning_positions = 0
        losing_positions = 0

        for symbol, state in states.items():
            last_price = await self._mark_price(symbol, state.avg_cost)
            market_value = state.quantity * last_price if last_price is not None else 0.0
            unrealized = 0.0
            if last_price is not None and state.quantity != 0:
                if state.quantity > 0:
                    unrealized = (last_price - state.avg_cost) * state.quantity
                else:
                    unrealized = (state.avg_cost - last_price) * abs(state.quantity)

            total = state.realized_pnl + unrealized
            realized_total += state.realized_pnl
            unrealized_total += unrealized
            cost_basis_total += abs(state.quantity) * state.avg_cost
            gross_market_value_total += abs(market_value)
            net_market_value_total += market_value
            if total > 0:
                winning_positions += 1
            elif total < 0:
                losing_positions += 1
            positions.append(
                PositionSummary(
                    symbol=symbol,
                    net_quantity=state.quantity,
                    avg_cost=round(state.avg_cost, 6),
                    last_price=round(last_price, 6) if last_price is not None else None,
                    market_value=round(market_value, 6),
                    realized_pnl=round(state.realized_pnl, 6),
                    unrealized_pnl=round(unrealized, 6),
                    total_pnl=round(total, 6),
                )
            )

        positions.sort(key=lambda item: abs(item.total_pnl), reverse=True)
        return PortfolioSummary(
            broker=broker,
            positions=positions,
            filled_orders=len(records),
            realized_pnl=round(realized_total, 6),
            unrealized_pnl=round(unrealized_total, 6),
            total_pnl=round(realized_total + unrealized_total, 6),
            cost_basis=round(cost_basis_total, 6),
            gross_market_value=round(gross_market_value_total, 6),
            net_market_value=round(net_market_value_total, 6),
            return_pct=round((realized_total + unrealized_total) / cost_basis_total, 6) if cost_basis_total else 0.0,
            winning_positions=winning_positions,
            losing_positions=losing_positions,
        )

    async def _mark_price(self, symbol: str, fallback: float) -> float | None:
        try:
            features = await self.marketdata.get_intraday_features(symbol)
            return features.last_price or fallback
        except Exception:  # noqa: BLE001
            return fallback if fallback > 0 else None

    @staticmethod
    def _apply_fill(state: _PositionState, record: OrderRecord) -> None:
        qty = float(record.filled_qty or record.quantity or 0.0)
        if qty <= 0:
            return
        price = float(record.avg_fill_price or record.price or 0.0)
        if price <= 0:
            return

        side_mult = 1.0 if record.side == TradeAction.BUY else -1.0
        trade_qty_signed = side_mult * qty
        prior_qty = state.quantity

        if prior_qty == 0 or prior_qty * trade_qty_signed > 0:
            new_qty = prior_qty + trade_qty_signed
            if abs(new_qty) > 0:
                weighted_cost = abs(prior_qty) * state.avg_cost + qty * price
                state.avg_cost = weighted_cost / abs(new_qty)
            else:
                state.avg_cost = 0.0
            state.quantity = new_qty
            return

        closing_qty = min(abs(prior_qty), qty)
        if prior_qty > 0 and side_mult < 0:
            state.realized_pnl += (price - state.avg_cost) * closing_qty
        elif prior_qty < 0 and side_mult > 0:
            state.realized_pnl += (state.avg_cost - price) * closing_qty

        new_qty = prior_qty + trade_qty_signed
        if new_qty == 0:
            state.quantity = 0.0
            state.avg_cost = 0.0
            return

        if prior_qty * new_qty < 0:
            state.quantity = new_qty
            state.avg_cost = price
            return

        state.quantity = new_qty
