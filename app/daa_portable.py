"""DAA 已验证 ``polars_expr`` 策略的跨资产安全运行时。

这里只允许 DAA 版本化 AI 目录中的 ``ai_*`` 文件，并在 worker 内再次校验
SHA256。策略只接收标准行情字段，不能读取任意路径或注入订单执行代码。
"""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from typing import Any

import polars as pl


class DaaPortableStrategyError(ValueError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_strategy(*, strategy: dict[str, Any], daa_root: str | Path):
    strategy_id = str(strategy.get("id") or "").strip()
    if not strategy_id.startswith("ai_"):
        raise DaaPortableStrategyError("portable DAA 策略必须使用 ai_ ID")
    if str(strategy.get("entrypoint") or "") != strategy_id:
        raise DaaPortableStrategyError("DAA strategy.id 与 entrypoint 不一致")
    expected_hash = str(strategy.get("source_hash") or "").strip().lower()
    if len(expected_hash) != 64:
        raise DaaPortableStrategyError("DAA 策略缺少 SHA256 版本锚点")
    path = (Path(daa_root).resolve() / "data" / "strategies" / "ai" / f"{strategy_id}.py").resolve()
    try:
        path.relative_to((Path(daa_root).resolve() / "data" / "strategies" / "ai").resolve())
    except ValueError as exc:
        raise DaaPortableStrategyError("DAA 策略路径越出版本化目录") from exc
    if not path.is_file() or _sha256(path) != expected_hash:
        raise DaaPortableStrategyError("DAA 策略源码 SHA256 不匹配")
    spec = importlib.util.spec_from_file_location(f"pxy_daa_{strategy_id}", path)
    if spec is None or spec.loader is None:
        raise DaaPortableStrategyError("DAA 策略模块无法加载")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if str(getattr(module, "EXECUTION_BACKEND", "polars_expr")) != "polars_expr":
        raise DaaPortableStrategyError("跨资产 DAA 策略必须使用 polars_expr")
    if not callable(getattr(module, "filter", None)):
        raise DaaPortableStrategyError("DAA portable 策略缺少 filter")
    return module


def evaluate_filter(module: Any, frame: pl.DataFrame, params: dict[str, Any]) -> bool:
    """将 DAA filter 的结果归一化为当前行情是否触发。"""
    if frame.is_empty():
        return False
    frame = enrich_market_frame(frame)
    result = module.filter(frame, params)
    if isinstance(result, pl.Expr):
        # CTA 传入完整历史窗口，但信号只取最新一根；不能因为历史上
        # 曾经命中过一次就把后续所有 K 线都当作入场信号。
        evaluated = frame.with_columns(result.alias("__daa_signal")).tail(1)
        return bool(evaluated["__daa_signal"][0]) if evaluated.height else False
    if isinstance(result, pl.DataFrame):
        return result.height > 0
    if isinstance(result, bool):
        return result
    raise DaaPortableStrategyError("DAA filter 必须返回 Polars Expr/DataFrame/bool")


def enrich_market_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """补齐跨资产策略允许使用的确定性技术字段。"""
    expressions = []
    if "close" in frame.columns:
        for window in (5, 10, 20):
            name = f"ma{window}"
            if name not in frame.columns:
                expressions.append(pl.col("close").rolling_mean(window).over("symbol").alias(name) if "symbol" in frame.columns else pl.col("close").rolling_mean(window).alias(name))
        if "prev_close" not in frame.columns:
            expressions.append(pl.col("close").shift(1).over("symbol").alias("prev_close") if "symbol" in frame.columns else pl.col("close").shift(1).alias("prev_close"))
        
    if "volume" in frame.columns and "vol_ma5" not in frame.columns:
        expressions.append(pl.col("volume").rolling_mean(5).over("symbol").alias("vol_ma5") if "symbol" in frame.columns else pl.col("volume").rolling_mean(5).alias("vol_ma5"))
    if expressions:
        frame = frame.with_columns(expressions)
    if "close" in frame.columns and "change_pct" not in frame.columns and "prev_close" in frame.columns:
        frame = frame.with_columns(
            ((pl.col("close") / pl.col("prev_close") - 1.0).fill_nan(0.0).fill_null(0.0)).alias("change_pct")
        )
    return frame


def bar_frame(bar: Any) -> pl.DataFrame:
    """把 vn.py BarData/字典转换为 DAA 通用行情字段。"""
    if isinstance(bar, dict):
        value = dict(bar)
    else:
        value = {
            "datetime": getattr(bar, "datetime", None),
            "open": getattr(bar, "open_price", 0.0),
            "high": getattr(bar, "high_price", 0.0),
            "low": getattr(bar, "low_price", 0.0),
            "close": getattr(bar, "close_price", 0.0),
            "volume": getattr(bar, "volume", 0.0),
            "open_interest": getattr(bar, "open_interest", 0.0),
        }
    value.setdefault("symbol", value.get("vt_symbol", ""))
    value.setdefault("date", value.get("datetime"))
    close = float(value.get("close") or value.get("close_price") or 0.0)
    value.setdefault("last_price", close)
    value.setdefault("mid_price", close)
    value.setdefault("bid_price1", close)
    value.setdefault("ask_price1", close)
    value.setdefault("spread", 0.0)
    value.setdefault("imbalance", 0.0)
    value.setdefault("event_time", value.get("datetime"))
    return pl.DataFrame([value], infer_schema_length=None)


def tick_frame(tick: dict[str, Any]) -> pl.DataFrame:
    value = dict(tick)
    bid = float(value.get("bid_price1") or 0.0)
    ask = float(value.get("ask_price1") or 0.0)
    last = float(value.get("last_price") or (bid + ask) / 2.0)
    value.setdefault("mid_price", (bid + ask) / 2.0)
    value.setdefault("spread", ask - bid)
    total = float(value.get("bid_volume1") or 0.0) + float(value.get("ask_volume1") or 0.0)
    value.setdefault("imbalance", (float(value.get("bid_volume1") or 0.0) - float(value.get("ask_volume1") or 0.0)) / total if total else 0.0)
    value.setdefault("last_price", last)
    value.setdefault("datetime", value.get("exchange_ts"))
    value.setdefault("event_time", value.get("exchange_ts"))
    value.setdefault("close", last)
    value.setdefault("open", last)
    value.setdefault("high", last)
    value.setdefault("low", last)
    value.setdefault("volume", value.get("last_volume", 0.0))
    return pl.DataFrame([value], infer_schema_length=None)
