"""在固定合成 K 线上执行真实 vn.py CTA 引擎的验收向量。"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .kernel import stable_hash
from .replay import ReplayEvent, ResultReplayController


VNPY_ACCEPTANCE_VECTOR_ID = "vnpy-cta-native-v1"


def _scalar(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _strategy_source_sha256() -> str:
    path = Path(__file__).parent / "acceptance_strategies" / "vnpy_cta_v1.py"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_vnpy_acceptance_vector() -> dict[str, Any]:
    """返回可被三维验收器直接消费的统一结果片段。"""

    try:
        import vnpy
        import vnpy_ctastrategy
        from vnpy.trader.constant import Exchange, Interval
        from vnpy.trader.object import BarData
        from vnpy_ctastrategy.backtesting import BacktestingEngine
    except ModuleNotFoundError as exc:
        raise RuntimeError("当前 Python 未加载 PXYLH vn.py 运行时") from exc

    from .acceptance_strategies.vnpy_cta_v1 import VnpyCtaAcceptanceV1

    start = datetime(2026, 1, 5, 9, 0)
    close_prices = [100, 99, 98, 100, 102, 101, 99, 97]
    bars = [
        BarData(
            symbol="TEST",
            exchange=Exchange.LOCAL,
            datetime=start + timedelta(minutes=index),
            interval=Interval.MINUTE,
            volume=100 + index,
            turnover=0,
            open_price=close_price,
            high_price=close_price + 1,
            low_price=close_price - 1,
            close_price=close_price,
            gateway_name="PXYBACKTEST",
        )
        for index, close_price in enumerate(close_prices)
    ]
    bar_payloads = [
        {
            "symbol": bar.vt_symbol,
            "datetime": bar.datetime.isoformat(),
            "open": float(bar.open_price),
            "high": float(bar.high_price),
            "low": float(bar.low_price),
            "close": float(bar.close_price),
            "volume": float(bar.volume),
        }
        for bar in bars
    ]
    data_manifest_sha256 = stable_hash(bar_payloads)

    engine = BacktestingEngine()
    engine.output = lambda _message: None
    engine.set_parameters(
        vt_symbol="TEST.LOCAL",
        interval="1m",
        start=start,
        end=start + timedelta(minutes=10),
        rate=0.0001,
        slippage=0.2,
        size=10,
        pricetick=0.2,
        capital=100_000,
    )
    engine.add_strategy(VnpyCtaAcceptanceV1, {})
    engine.history_data = bars
    engine.run_backtesting()
    daily = engine.calculate_result()
    engine.calculate_statistics(output=False)

    orders = [
        {
            "order_id": str(order.orderid),
            "symbol": order.vt_symbol,
            "datetime": order.datetime.isoformat() if order.datetime else None,
            "type": order.type.name.lower(),
            "direction": order.direction.name.lower(),
            "offset": order.offset.name.lower(),
            "price": float(order.price),
            "volume": float(order.volume),
            "traded": float(order.traded),
            "status": order.status.name.lower(),
        }
        for order in engine.limit_orders.values()
    ]
    deals = [
        {
            "trade_id": str(trade.tradeid),
            "order_id": str(trade.orderid),
            "symbol": trade.vt_symbol,
            "datetime": trade.datetime.isoformat(),
            "direction": trade.direction.name.lower(),
            "offset": trade.offset.name.lower(),
            "price": float(trade.price),
            "volume": float(trade.volume),
        }
        for trade in engine.trades.values()
    ]
    account_curve: list[dict[str, Any]] = []
    if daily is not None:
        for row in daily.reset_index().to_dict("records"):
            account_curve.append(
                {
                    "date": str(_scalar(row.get("date") or row.get("index"))),
                    "balance": float(_scalar(row["balance"])),
                    "net_pnl": float(_scalar(row["net_pnl"])),
                    "commission": float(_scalar(row["commission"])),
                    "slippage": float(_scalar(row["slippage"])),
                    "turnover": float(_scalar(row["turnover"])),
                    "trade_count": int(_scalar(row["trade_count"])),
                }
            )

    events: list[ReplayEvent] = []
    for index, payload in enumerate(bar_payloads):
        events.append(
            ReplayEvent(
                event_type="market_bar",
                event_time=f"{payload['datetime']}+08:00",
                payload=payload,
                snapshot_id=data_manifest_sha256,
                source="vnpy-native-vector",
                symbol="TEST.LOCAL",
                source_seq=index,
            )
        )
    for index, order in enumerate(orders, start=len(events)):
        events.append(
            ReplayEvent(
                event_type="order",
                event_time=f"{order['datetime']}+08:00",
                payload=order,
                snapshot_id=data_manifest_sha256,
                source="vnpy-native-vector",
                symbol="TEST.LOCAL",
                source_seq=index,
            )
        )
    for index, deal in enumerate(deals, start=len(events)):
        events.append(
            ReplayEvent(
                event_type="fill",
                event_time=f"{deal['datetime']}+08:00",
                payload=deal,
                snapshot_id=data_manifest_sha256,
                source="vnpy-native-vector",
                symbol="TEST.LOCAL",
                source_seq=index,
            )
        )
    for index, account in enumerate(account_curve, start=len(events)):
        events.append(
            ReplayEvent(
                event_type="account",
                event_time=f"{account['date']}T15:00:00+08:00",
                payload=account,
                snapshot_id=data_manifest_sha256,
                source="vnpy-native-vector",
                source_seq=index,
            )
        )
    replay = ResultReplayController(
        run_id=VNPY_ACCEPTANCE_VECTOR_ID,
        snapshot_id=data_manifest_sha256,
        events=events,
        mode="fast",
    ).run(sleep=lambda _seconds: None)
    runtime_identity = (
        f"vnpy-{getattr(vnpy, '__version__', 'unknown')}+"
        f"vnpy_ctastrategy-{getattr(vnpy_ctastrategy, '__version__', 'unknown')}"
    )
    return {
        "strategy": {"source_hash": _strategy_source_sha256()},
        "data_snapshot": {"manifest_sha256": data_manifest_sha256},
        "diagnostics": {"runtime_identity": runtime_identity},
        "orders": orders,
        "deals": deals,
        "execution_snapshot": replay["execution_snapshot"],
        "replay_audit": replay["replay_audit"],
    }


__all__ = ["VNPY_ACCEPTANCE_VECTOR_ID", "run_vnpy_acceptance_vector"]
