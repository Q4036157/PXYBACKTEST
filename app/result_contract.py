from __future__ import annotations

import re
from typing import Any

from .kernel import stable_hash
from .metrics import drawdown_curve, enrich_metrics
from .reporting import build_report_projection

RESULT_CONTRACT_VERSION = "pxybacktest.task-result.v2"


def _attach_reproducibility(
    result: dict[str, Any], *, request: dict[str, Any], raw_result: dict[str, Any]
) -> dict[str, Any]:
    """给结果写入稳定的输入、事件和最终结果哈希。"""
    task = dict(request.get("_task_contract") or {})
    snapshot = dict(result.get("data_snapshot") or {})
    events = list(raw_result.get("events") or raw_result.get("replay_events") or [])
    replay_audit = raw_result.get("replay_audit")
    event_log_sha256 = (
        str(replay_audit.get("chain_sha256") or "")
        if isinstance(replay_audit, dict)
        else stable_hash(events)
    )
    result["reproducibility"] = {
        "input_contract_sha256": stable_hash(task),
        "manifest_sha256": snapshot.get("manifest_sha256"),
        "event_log_sha256": event_log_sha256,
        "event_count": int(replay_audit.get("event_count") or 0)
        if isinstance(replay_audit, dict)
        else len(events),
        "engine_version": str(raw_result.get("engine_version") or "unknown"),
    }
    result["reproducibility"]["result_sha256"] = stable_hash(result)
    return result


def build_result_v2(
    *, task_id: str, request: dict[str, Any], raw_result: dict[str, Any]
) -> dict[str, Any]:
    """把现有 vn.py 结果映射成稳定的 v2 结果结构。"""
    task = dict(request.get("_task_contract") or {})
    data = dict(task.get("data") or {})
    requested_snapshot = dict(data.get("snapshot") or {})
    data_provenance = dict(raw_result.get("data_provenance") or {})
    if not data_provenance:
        data_provenance = {
            "loader": "pxylh.services.backtest_service.kline_loader",
            "execution_source": "vnpy_database_compat_cache",
            "population_policy": "pxydata_preferred_with_vnpy_ccxt_fallback",
            "actual_upstream": "not_reported_by_loader",
            "credential_source": "unknown",
            "immutable_snapshot_verified": False,
        }
    execution_snapshot = dict(raw_result.get("data_snapshot") or {})
    immutable_snapshot_verified = (
        data_provenance.get("immutable_snapshot_verified") is True
        and bool(execution_snapshot.get("snapshot_id"))
        and re.fullmatch(
            r"[0-9a-fA-F]{64}",
            str(execution_snapshot.get("manifest_sha256") or ""),
        )
        is not None
    )
    if not immutable_snapshot_verified:
        execution_snapshot = {}

    warnings = list(requested_snapshot.get("warnings") or [])
    warnings.extend(
        [
            "vnpy_cta 首期适配器尚未输出完整订单生命周期。",
            "positions 仅支持实时事件，最终结果暂未物化持仓历史。",
            "当前 vnpy_cta 执行读取 VNPY 兼容缓存；上游优先 PXYDATA，并允许 VNPY/CCXT 回退。",
            "CTA loader 尚不报告实际上游命中分支，未把请求快照声明为执行快照。",
        ]
    )
    trades = list(raw_result.get("trades") or [])
    daily_results = list(raw_result.get("daily_results") or [])
    benchmark_curve = list(raw_result.get("benchmark_curve") or [])
    parameters = dict(task.get("parameters") or {})
    standard_metrics = enrich_metrics(
        dict(raw_result.get("statistics") or {}),
        daily_results,
        deals=trades,
        benchmark=benchmark_curve,
        risk_free_rate=float(parameters.get("risk_free_rate") or 0.0),
        periods_per_year=float(parameters.get("annualization") or 252),
    )
    result = {
        "schema_version": 2,
        "contract_version": RESULT_CONTRACT_VERSION,
        "task_id": task_id,
        "complete": bool(raw_result.get("complete", True)),
        "termination_reason": str(raw_result.get("termination_reason") or "completed"),
        "engine_type": str(task.get("engine_type") or "vnpy_cta"),
        "strategy": task.get("strategy") or {},
        "run": {
            "default_profile": task.get("default_profile"),
            "universe": task.get("universe") or {},
            "period": task.get("period") or {},
            "execution": task.get("execution") or {},
            "parameters": task.get("parameters") or {},
            "random_seed": task.get("random_seed"),
        },
        "metrics": standard_metrics,
        "curves": {
            "daily": daily_results,
            "equity": daily_results,
            "drawdown": [
                {"date": point.get("date"), "value": value}
                for point, value in zip(daily_results, drawdown_curve(daily_results))
                if isinstance(point, dict)
            ],
            "benchmark": benchmark_curve,
        },
        "market": {"bars": list(raw_result.get("bars") or [])},
        "orders": [],
        "deals": trades,
        "positions": [],
        "diagnostics": {
            "data_count": int(raw_result.get("data_count") or 0),
            "progress": float(raw_result.get("progress") or 0.0),
            "processed_bars": int(raw_result.get("processed_bars") or 0),
            "total_bars": int(raw_result.get("total_bars") or 0),
            "current_datetime": str(raw_result.get("current_datetime") or ""),
            "trade_count": int(raw_result.get("trades_count") or len(trades)),
            "quality_accepted": bool(execution_snapshot.get("quality_accepted")),
            "random_seed": task.get("random_seed"),
            "adapter": "vnpy_cta.legacy.v1",
            "software_versions": {"result_contract": RESULT_CONTRACT_VERSION},
            "degraded_capabilities": ["orders", "positions"],
            "data_source_policy": "pxydata_preferred_with_runtime_fallback",
            "snapshot_enforcement": "provenance_only",
            "strictly_reproducible": False,
            "data_provenance": data_provenance,
            "requested_data_snapshot": requested_snapshot or None,
            "replay_audit": raw_result.get("replay_audit"),
            "warnings": warnings,
        },
        "replay_audit": raw_result.get("replay_audit"),
        "artifacts": [],
    }
    if execution_snapshot:
        result["data_snapshot"] = execution_snapshot
    result["report"] = build_report_projection(result)
    return _attach_reproducibility(result, request=request, raw_result=raw_result)


def build_a_share_result_v2(
    *, task_id: str, request: dict[str, Any], raw_result: dict[str, Any]
) -> dict[str, Any]:
    """把 DAA manifest-bound 组合回测结果映射成统一 Result v2。"""
    task = dict(request.get("_task_contract") or {})
    snapshot = dict((task.get("data") or {}).get("snapshot") or {})
    stats = dict(raw_result.get("stats") or {})
    adapter = dict(stats.pop("adapter", {}) or {})
    trades = list(raw_result.get("trades") or [])
    benchmark_curve = list(raw_result.get("benchmark_curve") or [])
    equity_curve = list(raw_result.get("equity_curve") or [])
    parameters = dict(task.get("parameters") or {})
    standard_metrics = enrich_metrics(
        stats,
        equity_curve,
        deals=trades,
        benchmark=benchmark_curve,
        risk_free_rate=float(parameters.get("risk_free_rate") or 0.0),
        periods_per_year=float(parameters.get("annualization") or 252),
    )
    warnings = list(snapshot.get("warnings") or [])
    warnings.extend(
        [
            "当前 A 股日线为未复权价格，长期收益未计入分红送转等公司行动。",
            "A 股首期适配器尚未输出完整订单生命周期和持仓历史。",
        ]
    )
    engine_type = str(task.get("engine_type") or "a_share_portfolio")
    if engine_type in {"factor_matrix", "event_sentiment"}:
        warnings.append(
            "因子结果严格绑定 factor_set/input_snapshot/feature_code 版本；"
            "更换任一版本后不可直接横向比较。"
        )
    if not benchmark_curve:
        warnings.append("当前快照未提供独立基准曲线。")

    input_snapshot = None
    if adapter.get("factor_input_snapshot_id"):
        input_snapshot = {
            "snapshot_id": adapter.get("factor_input_snapshot_id"),
            "manifest_sha256": adapter.get("factor_input_manifest_sha256"),
            "role": "factor_materialization_input",
        }

    result = {
        "schema_version": 2,
        "contract_version": RESULT_CONTRACT_VERSION,
        "task_id": task_id,
        "engine_type": engine_type,
        "strategy": task.get("strategy") or {},
        "data_snapshot": snapshot,
        "input_snapshot": input_snapshot,
        "run": {
            "default_profile": task.get("default_profile"),
            "universe": task.get("universe") or {},
            "period": task.get("period") or {},
            "execution": task.get("execution") or {},
            "parameters": task.get("parameters") or {},
            "random_seed": task.get("random_seed"),
        },
        "metrics": standard_metrics,
        "curves": {
            "equity": equity_curve,
            "drawdown": list(raw_result.get("drawdown_curve") or []),
            "benchmark": benchmark_curve,
        },
        "market": {
            "per_symbol_stats": list(raw_result.get("per_symbol_stats") or []),
            "strategy_info": dict(raw_result.get("strategy_info") or {}),
        },
        "orders": [],
        "deals": trades,
        "positions": [],
        "diagnostics": {
            "data_count": int(adapter.get("loaded_rows") or 0),
            "trade_count": len(trades),
            "quality_accepted": bool(snapshot.get("quality_accepted")),
            "random_seed": task.get("random_seed"),
            "adapter": f"daa.{engine_type}.v1",
            "adapter_contract": adapter.get("contract_version"),
            "strategy_source_sha256": adapter.get("strategy_source_sha256"),
            "snapshot_id": adapter.get("snapshot_id"),
            "manifest_sha256": adapter.get("manifest_sha256"),
            "factor_contract": adapter.get("factor_contract"),
            "factor_set_id": adapter.get("factor_set_id"),
            "factor_set_hash": adapter.get("factor_set_hash"),
            "factor_input_snapshot_id": adapter.get("factor_input_snapshot_id"),
            "factor_input_manifest_sha256": adapter.get(
                "factor_input_manifest_sha256"
            ),
            "feature_code_hash": adapter.get("feature_code_hash"),
            "factor_weights": adapter.get("factor_weights"),
            "verified_file_count": int(adapter.get("verified_file_count") or 0),
            "verified_size_bytes": int(adapter.get("verified_size_bytes") or 0),
            "loaded_symbols": int(adapter.get("loaded_symbols") or 0),
            "data_start": adapter.get("data_start"),
            "data_end": adapter.get("data_end"),
            "elapsed_ms": float(raw_result.get("elapsed_ms") or 0.0),
            "software_versions": {
                "result_contract": RESULT_CONTRACT_VERSION,
                "daa_worker": adapter.get("worker_version") or "unknown",
            },
            "degraded_capabilities": ["orders", "positions", "adjusted_prices"],
            "data_source_policy": "pxydata_snapshot_only",
            "snapshot_enforcement": adapter.get("snapshot_enforcement"),
            "strictly_reproducible": True,
            "replay_audit": raw_result.get("replay_audit"),
            "price_adjustment": adapter.get("price_adjustment"),
            "corporate_actions_applied": bool(adapter.get("corporate_actions_applied")),
            "warnings": warnings,
        },
        "artifacts": [],
        "replay_audit": raw_result.get("replay_audit"),
    }
    if isinstance(raw_result.get("_replay_events"), list):
        result["_replay_events"] = raw_result["_replay_events"]
    result["report"] = build_report_projection(result)
    return _attach_reproducibility(result, request=request, raw_result=raw_result)
