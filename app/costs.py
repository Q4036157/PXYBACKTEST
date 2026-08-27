"""可组合的股票/ETF 交易成本模型。

这是对常见交易成本口径的独立实现：佣金最低收费、卖出印花税、沪市过户费、
固定每笔费用，以及按比例或最小跳动计算的滑点。它只返回成交成本，不负责撮合或下单。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FillCost:
    requested_price: float
    fill_price: float
    notional: float
    commission: float
    stamp_tax: float
    transfer_fee: float
    fixed_fee: float
    slippage_cost: float

    @property
    def total_fee(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee + self.fixed_fee


def calculate_fill_cost(
    *,
    price: float,
    quantity: float,
    side: str,
    symbol: str = "",
    commission_rate: float = 0.0003,
    min_commission: float = 5.0,
    stamp_tax_rate: float = 0.001,
    transfer_fee_rate: float = 0.00001,
    fixed_fee: float = 0.0,
    slippage_mode: str = "ratio",
    slippage_ratio: float = 0.0,
    slippage_ticks: int = 0,
    tick_size: float = 0.01,
) -> FillCost:
    """计算一笔成交的价格和费用；参数错误会明确抛出 ``ValueError``。"""
    if price <= 0 or quantity <= 0:
        raise ValueError("price 和 quantity 必须大于 0")
    side = side.strip().lower()
    if side not in {"buy", "sell"}:
        raise ValueError("side 必须是 buy 或 sell")
    if any(value < 0 for value in (commission_rate, min_commission, stamp_tax_rate, transfer_fee_rate, fixed_fee, slippage_ratio, slippage_ticks, tick_size)):
        raise ValueError("费用、滑点和 tick 参数不能为负数")
    if slippage_mode not in {"ratio", "ticks", "none"}:
        raise ValueError("slippage_mode 必须是 ratio、ticks 或 none")

    adverse = 1.0 if side == "buy" else -1.0
    if slippage_mode == "ratio":
        fill_price = price * (1.0 + adverse * slippage_ratio)
    elif slippage_mode == "ticks":
        fill_price = price + adverse * slippage_ticks * tick_size
    else:
        fill_price = price
    notional = fill_price * quantity
    commission = max(notional * commission_rate, min_commission) if quantity > 0 else 0.0
    stamp_tax = notional * stamp_tax_rate if side == "sell" else 0.0
    normalized = symbol.strip().upper()
    transfer_fee = (
        notional * transfer_fee_rate
        if normalized.startswith("SH.") or normalized.endswith(".SH")
        else 0.0
    )
    return FillCost(
        requested_price=price,
        fill_price=fill_price,
        notional=notional,
        commission=commission,
        stamp_tax=stamp_tax,
        transfer_fee=transfer_fee,
        fixed_fee=fixed_fee,
        slippage_cost=abs(fill_price - price) * quantity,
    )


def cost_kwargs(execution: dict[str, Any]) -> dict[str, Any]:
    """从任务 execution 配置提取成本参数，便于适配器复用。"""
    return {
        "commission_rate": float(execution.get("commission") or execution.get("rate") or 0.0),
        "min_commission": float(execution.get("min_commission") or 0.0),
        "stamp_tax_rate": float(execution.get("stamp_tax") or 0.0),
        "transfer_fee_rate": float(execution.get("transfer_fee") or 0.0),
        "fixed_fee": float(execution.get("fixed_fee") or 0.0),
        "slippage_mode": str(execution.get("slippage_mode") or "none"),
        "slippage_ratio": float(execution.get("slippage_ratio") or 0.0),
        "slippage_ticks": int(execution.get("slippage_ticks") or 0),
        "tick_size": float(execution.get("tick_size") or 0.01),
    }


__all__ = ["FillCost", "calculate_fill_cost", "cost_kwargs"]
