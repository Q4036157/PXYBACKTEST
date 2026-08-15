"""在同一不可变数据快照上执行 Optuna 多目标优化和 Walk-forward。"""

from __future__ import annotations

import copy
import math
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any


class OptimizationError(ValueError):
    """优化配置、目标指标或试验结果不满足契约。"""


Evaluator = Callable[[dict[str, Any]], dict[str, Any]]


def run_task_optimization(
    task: dict[str, Any],
    evaluator: Evaluator,
    *,
    cancel_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    config = task.get("optimization")
    if not isinstance(config, dict):
        return evaluator(task)
    method = str(config.get("method") or "")
    if method == "optuna":
        return _run_optuna_task(task, config, evaluator, cancel_check=cancel_check)
    if method == "walk_forward":
        return _run_walk_forward(task, config, evaluator, cancel_check=cancel_check)
    raise OptimizationError(f"未知优化方法: {method}")


def _run_optuna_task(
    task: dict[str, Any],
    config: dict[str, Any],
    evaluator: Evaluator,
    *,
    cancel_check: Callable[[], bool] | None,
) -> dict[str, Any]:
    study, trial_results = _optimize(
        task,
        config,
        evaluator,
        cancel_check=cancel_check,
    )
    best = _representative_trial(study, list(config["objectives"]))
    if best is None:
        raise OptimizationError("Optuna 没有完成的有效试验")
    best_task = _task_with_parameters(task, dict(best.params))
    best_result = evaluator(best_task)
    best_result["optimization"] = {
        "method": "optuna",
        "n_trials": int(config["n_trials"]),
        "n_completed": len(trial_results),
        "objectives": list(config["objectives"]),
        "best_params": dict(best.params),
        "best_values": list(best.values or []),
        "pareto_front": [
            {
                "trial": trial.number,
                "params": dict(trial.params),
                "values": list(trial.values or []),
            }
            for trial in study.best_trials
        ],
        "trials": trial_results,
    }
    return best_result


def _run_walk_forward(
    task: dict[str, Any],
    config: dict[str, Any],
    evaluator: Evaluator,
    *,
    cancel_check: Callable[[], bool] | None,
) -> dict[str, Any]:
    period = dict(task.get("period") or {})
    start = date.fromisoformat(str(period["start"])[:10])
    end = date.fromisoformat(str(period["end"])[:10])
    folds = generate_folds(
        start,
        end,
        train_days=int(config["train_days"]),
        test_days=int(config["test_days"]),
        step_days=int(config["step_days"]),
    )
    fold_results: list[dict[str, Any]] = []
    combined_deals: list[dict[str, Any]] = []
    combined_equity: list[dict[str, Any]] = []
    base_result: dict[str, Any] | None = None
    compounded = 1.0
    for index, fold in enumerate(folds):
        if cancel_check is not None and cancel_check():
            raise OptimizationError("walk-forward 已取消")
        train_task = _task_with_period(task, fold[0], fold[1])
        train_task["optimization"] = None
        study, trial_rows = _optimize(
            train_task,
            config,
            evaluator,
            cancel_check=cancel_check,
        )
        best = _representative_trial(study, list(config["objectives"]))
        if best is None:
            fold_results.append(
                {
                    "index": index,
                    "train_start": fold[0].isoformat(),
                    "train_end": fold[1].isoformat(),
                    "test_start": fold[2].isoformat(),
                    "test_end": fold[3].isoformat(),
                    "status": "skipped",
                    "reason": "训练窗没有有效试验",
                }
            )
            continue
        oos_task = _task_with_parameters(
            _task_with_period(task, fold[2], fold[3]),
            dict(best.params),
        )
        oos_task["optimization"] = None
        oos = evaluator(oos_task)
        if base_result is None:
            base_result = copy.deepcopy(oos)
        metrics = dict(oos.get("metrics") or {})
        fold_return = float(metrics.get("total_return") or 0.0)
        compounded *= 1 + fold_return
        fold_results.append(
            {
                "index": index,
                "train_start": fold[0].isoformat(),
                "train_end": fold[1].isoformat(),
                "test_start": fold[2].isoformat(),
                "test_end": fold[3].isoformat(),
                "status": "completed",
                "best_params": dict(best.params),
                "is_values": list(best.values or []),
                "oos_metrics": metrics,
                "n_trials": len(trial_rows),
            }
        )
        combined_deals.extend(list(oos.get("deals") or []))
        combined_equity.append({"date": fold[3].isoformat(), "value": compounded})
    valid = [item for item in fold_results if item["status"] == "completed"]
    if not valid or base_result is None:
        raise OptimizationError("walk-forward 没有有效 OOS 折")
    returns = [float(item["oos_metrics"].get("total_return") or 0.0) for item in valid]
    base_result["metrics"] = {
        **dict(base_result.get("metrics") or {}),
        "total_return": compounded - 1.0,
        "n_trades": len(combined_deals),
        "walk_forward_folds": len(valid),
        "walk_forward_consistency": sum(value > 0 for value in returns) / len(returns),
        "walk_forward_average_return": sum(returns) / len(returns),
    }
    base_result["curves"] = {
        "equity": combined_equity,
        "drawdown": _drawdown_curve(combined_equity),
    }
    base_result["deals"] = combined_deals
    base_result["optimization"] = {
        "method": "walk_forward",
        "objectives": list(config["objectives"]),
        "n_planned_folds": len(folds),
        "n_valid_folds": len(valid),
        "folds": fold_results,
        "compounded_oos_return": compounded - 1.0,
    }
    return base_result


def _optimize(
    task: dict[str, Any],
    config: dict[str, Any],
    evaluator: Evaluator,
    *,
    cancel_check: Callable[[], bool] | None,
):
    try:
        import optuna
    except ImportError as exc:
        raise OptimizationError("统一优化任务缺少 optuna 运行依赖") from exc
    objectives = list(config["objectives"])
    directions = [str(item["direction"]) for item in objectives]
    seed = int(config.get("sampler_seed", 42))
    sampler = (
        optuna.samplers.TPESampler(seed=seed)
        if len(objectives) == 1
        else optuna.samplers.NSGAIISampler(seed=seed)
    )
    study = optuna.create_study(directions=directions, sampler=sampler)
    trial_results: list[dict[str, Any]] = []

    def objective(trial):
        if cancel_check is not None and cancel_check():
            study.stop()
            raise optuna.TrialPruned("任务已取消")
        params = {
            name: _suggest(trial, name, dimension)
            for name, dimension in dict(config["search_space"]).items()
        }
        result = evaluator(_task_with_parameters(task, params))
        metrics = dict(result.get("metrics") or {})
        values: list[float] = []
        for item in objectives:
            raw = metrics.get(str(item["metric"]))
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise OptimizationError(
                    f"优化结果缺少数值指标: {item['metric']}"
                ) from exc
            if not math.isfinite(value):
                raise OptimizationError(f"优化指标不是有限数: {item['metric']}")
            values.append(value)
        trial_results.append(
            {
                "trial": trial.number,
                "params": params,
                "values": values,
            }
        )
        return values[0] if len(values) == 1 else tuple(values)

    study.optimize(objective, n_trials=int(config["n_trials"]), n_jobs=1)
    return study, trial_results


def generate_folds(
    start: date,
    end: date,
    *,
    train_days: int,
    test_days: int,
    step_days: int,
) -> list[tuple[date, date, date, date]]:
    folds: list[tuple[date, date, date, date]] = []
    train_start = start
    while True:
        train_end = train_start + timedelta(days=train_days - 1)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=test_days - 1)
        if test_end > end:
            break
        folds.append((train_start, train_end, test_start, test_end))
        train_start += timedelta(days=step_days)
    if not folds:
        raise OptimizationError("数据区间不足以生成一个 walk-forward 折")
    return folds


def _suggest(trial, name: str, dimension: dict[str, Any]) -> Any:
    kind = str(dimension["type"])
    if kind == "categorical":
        return trial.suggest_categorical(name, list(dimension["choices"]))
    if kind == "int":
        kwargs: dict[str, Any] = {"log": bool(dimension.get("log", False))}
        if dimension.get("step") is not None:
            kwargs["step"] = int(dimension["step"])
        return trial.suggest_int(
            name, int(dimension["low"]), int(dimension["high"]), **kwargs
        )
    kwargs = {"log": bool(dimension.get("log", False))}
    if dimension.get("step") is not None:
        kwargs["step"] = float(dimension["step"])
    return trial.suggest_float(
        name, float(dimension["low"]), float(dimension["high"]), **kwargs
    )


def _representative_trial(study, objectives: list[dict[str, Any]]):
    candidates = list(study.best_trials)
    if not candidates:
        return None
    if len(objectives) == 1:
        return candidates[0]
    ranges: list[tuple[float, float]] = []
    for index in range(len(objectives)):
        values = [float(trial.values[index]) for trial in candidates]
        ranges.append((min(values), max(values)))

    def score(trial) -> tuple[float, int]:
        total = 0.0
        for index, objective in enumerate(objectives):
            low, high = ranges[index]
            value = float(trial.values[index])
            normalized = 1.0 if high == low else (value - low) / (high - low)
            total += normalized if objective["direction"] == "maximize" else 1 - normalized
        return total, -int(trial.number)

    return max(candidates, key=score)


def _task_with_parameters(task: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    candidate = copy.deepcopy(task)
    candidate["parameters"] = {**dict(candidate.get("parameters") or {}), **values}
    candidate["optimization"] = None
    return candidate


def _task_with_period(task: dict[str, Any], start: date, end: date) -> dict[str, Any]:
    candidate = copy.deepcopy(task)
    period = dict(candidate.get("period") or {})
    timezone_suffix = "+08:00"
    period["start"] = f"{start.isoformat()}T00:00:00{timezone_suffix}"
    period["end"] = f"{end.isoformat()}T23:59:59{timezone_suffix}"
    candidate["period"] = period
    return candidate


def _drawdown_curve(equity: list[dict[str, Any]]) -> list[dict[str, Any]]:
    peak = 1.0
    result = []
    for point in equity:
        value = float(point["value"])
        peak = max(peak, value)
        result.append({"date": point["date"], "value": value / peak - 1.0})
    return result
