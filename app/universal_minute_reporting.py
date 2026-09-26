"""Result projections for the universal minute engine."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from .kernel import stable_hash
from .minute_replay import FillRecord


def drawdown_curve(equity: list[dict[str, Any]]) -> list[dict[str, Any]]:
    peak: float | None = None
    output: list[dict[str, Any]] = []
    for point in equity:
        value = float(point["value"])
        peak = value if peak is None else max(peak, value)
        output.append(
            {
                "date": point["date"],
                "value": value / peak - 1.0 if peak else 0.0,
            }
        )
    return output


def closed_trades(fills: tuple[FillRecord, ...]) -> list[dict[str, Any]]:
    """按平均成本法把单边 Fill 投影为已闭合往返，Fill 仍是成交事实。"""

    states: dict[str, dict[str, Any]] = {}
    deals: list[dict[str, Any]] = []
    for fill in fills:
        direction = Decimal("1") if fill.side == "buy" else Decimal("-1")
        state = states.get(fill.symbol)
        close_fee = Decimal("0")
        open_fee = fill.commission
        if fill.closed_quantity > 0 and state is not None:
            close_ratio = fill.closed_quantity / fill.quantity
            close_fee = fill.commission * close_ratio
            open_fee = fill.commission - close_fee
            entry_ratio = fill.closed_quantity / state["quantity"]
            entry_fee = state["commission"] * entry_ratio
            deal = {
                "trade_id": stable_hash(
                    {
                        "entry_fill_id": state["entry_fill_id"],
                        "exit_fill_id": fill.fill_id,
                        "quantity": str(fill.closed_quantity),
                        "index": len(deals) + 1,
                    }
                ),
                "symbol": fill.symbol,
                "side": "long" if state["direction"] > 0 else "short",
                "entry_time": state["entry_time"],
                "exit_time": fill.event_time,
                "entry_price": str(state["average_price"]),
                "exit_price": str(fill.price),
                "quantity": str(fill.closed_quantity),
                "gross_pnl": str(fill.realized_pnl),
                "commission": str(entry_fee + close_fee),
                "funding_pnl": "0",
                "pnl_amount": str(fill.realized_pnl - entry_fee - close_fee),
                "entry_fill_id": state["entry_fill_id"],
                "exit_fill_id": fill.fill_id,
            }
            deals.append(deal)
            state["quantity"] -= fill.closed_quantity
            state["commission"] -= entry_fee
            if state["quantity"] <= 0:
                states.pop(fill.symbol, None)

        if fill.opened_quantity <= 0:
            continue
        state = states.get(fill.symbol)
        if state is None or state["direction"] != direction:
            states[fill.symbol] = {
                "direction": direction,
                "quantity": fill.opened_quantity,
                "average_price": fill.price,
                "entry_time": fill.event_time,
                "entry_fill_id": fill.fill_id,
                "commission": open_fee,
            }
            continue
        combined_quantity = state["quantity"] + fill.opened_quantity
        state["average_price"] = (
            state["average_price"] * state["quantity"]
            + fill.price * fill.opened_quantity
        ) / combined_quantity
        state["quantity"] = combined_quantity
        state["commission"] += open_fee
    return deals
