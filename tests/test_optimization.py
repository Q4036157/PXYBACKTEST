from __future__ import annotations

from copy import deepcopy

import pytest

from app.optimization import OptimizationError, generate_folds, run_task_optimization


def _task(method: str = "optuna") -> dict:
    return {
        "period": {
            "start": "2026-01-01T00:00:00+08:00",
            "end": "2026-02-28T23:59:59+08:00",
        },
        "parameters": {"fixed": 1},
        "optimization": {
            "method": method,
            "search_space": {
                "x": {"type": "int", "low": 0, "high": 4, "step": 1},
                "mode": {"type": "categorical", "choices": ["a", "b"]},
            },
            "objectives": [
                {"metric": "total_return", "direction": "maximize"},
                {"metric": "risk", "direction": "minimize"},
            ],
            "n_trials": 12,
            "sampler_seed": 7,
            "train_days": 20,
            "test_days": 10,
            "step_days": 10,
        },
    }


def _evaluate(task: dict) -> dict:
    params = task["parameters"]
    x = int(params.get("x", 0))
    bonus = 1 if params.get("mode") == "b" else 0
    start = task["period"]["start"][:10]
    return {
        "metrics": {
            "total_return": 10 - abs(3 - x) + bonus,
            "risk": abs(x - 2),
            "n_trades": 1,
        },
        "curves": {"equity": [], "drawdown": []},
        "deals": [{"date": start, "pnl_amount": x}],
    }


def test_optuna_multi_objective_returns_pareto_and_best_result() -> None:
    result = run_task_optimization(_task(), _evaluate)

    assert result["optimization"]["method"] == "optuna"
    assert result["optimization"]["n_completed"] == 12
    assert result["optimization"]["pareto_front"]
    assert set(result["optimization"]["best_params"]) == {"x", "mode"}


def test_walk_forward_keeps_train_and_oos_dates_disjoint() -> None:
    task = _task("walk_forward")
    task["optimization"]["n_trials"] = 3
    result = run_task_optimization(task, _evaluate)

    folds = result["optimization"]["folds"]
    assert result["optimization"]["n_valid_folds"] >= 1
    assert all(item["train_end"] < item["test_start"] for item in folds)
    assert result["metrics"]["walk_forward_folds"] == len(folds)
    assert result["curves"]["equity"]


def test_generate_folds_rejects_short_range() -> None:
    task = _task()
    period = deepcopy(task["period"])

    with pytest.raises(OptimizationError, match="不足"):
        generate_folds(
            __import__("datetime").date.fromisoformat(period["start"][:10]),
            __import__("datetime").date.fromisoformat("2026-01-10"),
            train_days=20,
            test_days=10,
            step_days=10,
        )
