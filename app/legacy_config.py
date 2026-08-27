"""将看海量化 ``.kh`` 配置转换为 PXY 回测任务的公共部分。

转换器只处理声明式配置，不加载策略源码、不访问 MiniQMT，也不绕过 snapshot 校验。
生成的结果仍需补齐 PXYDATA snapshot 和已登记的 strategy identity 后才能提交任务。
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


class LegacyConfigError(ValueError):
    pass


def _sha256_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _symbols(data: Mapping[str, Any]) -> list[str]:
    values = data.get("stock_list", data.get("stock_pool", []))
    if isinstance(values, str):
        values = [item.strip() for item in values.replace(";", ",").split(",")]
    if not isinstance(values, Sequence) or isinstance(values, (bytes, bytearray)):
        raise LegacyConfigError(".kh data.stock_list 必须是列表")
    result = [str(value).strip().upper() for value in values if str(value).strip()]
    if not result:
        raise LegacyConfigError(".kh 配置缺少股票池")
    return list(dict.fromkeys(result))


def translate_kh_config(
    payload: Mapping[str, Any], *, strategy_id: str, strategy_source_hash: str | None = None
) -> dict[str, Any]:
    """把 .kh JSON 映射为可继续补全的 PXY task contract。"""
    if not isinstance(payload, Mapping):
        raise LegacyConfigError(".kh 顶层必须是 JSON 对象")
    strategy_id = strategy_id.strip()
    if not strategy_id:
        raise LegacyConfigError("必须提供 strategy_id")
    backtest = dict(payload.get("backtest") or {})
    data = dict(payload.get("data") or {})
    start = str(backtest.get("start_time") or "").strip()
    end = str(backtest.get("end_time") or "").strip()
    if not start or not end or start > end:
        raise LegacyConfigError(".kh 回测起止日期无效")
    costs = dict(backtest.get("trade_cost") or {})
    slippage = dict(costs.get("slippage") or {})
    slippage_type = str(slippage.get("type") or "none").lower()
    if slippage_type not in {"none", "ratio", "tick"}:
        raise LegacyConfigError(".kh slippage.type 必须是 none、ratio 或 tick")
    commission_rate = float(costs.get("commission_rate") or 0.0)
    stamp_tax_rate = float(costs.get("stamp_tax_rate") or 0.0)
    execution: dict[str, Any] = {
        "capital": float(backtest.get("init_capital") or 1_000_000),
        "rate": commission_rate,
        "commission": commission_rate,
        "commission_bps": commission_rate * 10_000,
        "stamp_tax": stamp_tax_rate,
        "stamp_tax_bps": stamp_tax_rate * 10_000,
        "min_commission": float(costs.get("min_commission") or 0.0),
        "fixed_fee": float(costs.get("flow_fee") or 0.0),
        "mode": "BAR",
        "signal_time": "bar_close",
        "entry_fill": "next_bar_open",
        "exit_fill": "next_bar_open",
        "t_plus_one": True,
        "price_adjustment": "forward" if str(data.get("dividend_type") or "none").lower() == "front" else "none",
        "legacy_slippage_mode": slippage_type,
    }
    if slippage_type == "ratio":
        execution["slippage_mode"] = "ratio"
        execution["slippage_ratio"] = float(slippage.get("ratio") or 0.0)
    elif slippage_type == "tick":
        execution["slippage_mode"] = "ticks"
        execution["slippage_ticks"] = int(slippage.get("tick_count") or 0)
        execution["tick_size"] = float(slippage.get("tick_size") or 0.01)
    else:
        execution["slippage_mode"] = "none"
    trigger = dict(backtest.get("trigger") or {})
    return {
        "schema_version": 2,
        "engine_type": "a_share_portfolio",
        "strategy": {
            "id": strategy_id,
            "version": "legacy-kh",
            "entrypoint": strategy_id,
            "source_hash": strategy_source_hash,
        },
        "universe": {"symbols": _symbols(data), "asset_class": "A_SHARE"},
        "period": {
            "start": start,
            "end": end,
            "interval": str(data.get("kline_period") or "1d"),
        },
        "execution": execution,
        "parameters": {
            "fields": list(data.get("fields") or []),
            "dividend_type": data.get("dividend_type") or "none",
            "legacy_trigger": trigger,
            "legacy_risk": dict(payload.get("risk") or {}),
        },
        "compatibility": {
            "source_format": "kh",
            "source_config_sha256": _sha256_payload(payload),
            "requires_snapshot_binding": True,
        },
    }


__all__ = ["LegacyConfigError", "translate_kh_config"]
