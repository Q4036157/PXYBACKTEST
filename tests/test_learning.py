from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import queue

import pytest
from pydantic import ValidationError

from app.learning import (
    LEARNING_CHECKPOINT_ARTIFACT,
    LEARNING_CHECKPOINT_CONTRACT_VERSION,
    LEARNING_METRICS_ARTIFACT,
    ML_STRATEGY_HASH,
    LearningBacktestError,
    LearningCheckpoint,
    _fit_linear_regression,
    _generate_folds,
    run_learning_backtest,
)
from app.models import SubmitBacktestRequestV2
from app.worker_process import run_learning_worker


def test_builtin_learning_model_is_deterministic_and_temporal_folds_are_ordered() -> None:
    rows = []
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(12):
        rows.append(
            {
                "event_time": (start + timedelta(days=index)).isoformat(),
                "available_at": (start + timedelta(days=index)).isoformat(),
                "symbol": "AAA",
                "x": float(index),
                "label": float(index) / 100,
            }
        )
    folds = _generate_folds(rows, {"train_days": 6, "test_days": 2, "step_days": 2})
    assert folds == [
        (start.date() + timedelta(days=5), start.date() + timedelta(days=6), start.date() + timedelta(days=7)),
        (start.date() + timedelta(days=7), start.date() + timedelta(days=8), start.date() + timedelta(days=9)),
        (start.date() + timedelta(days=9), start.date() + timedelta(days=10), start.date() + timedelta(days=11)),
    ]
    predictor = _fit_linear_regression(rows[:6], ["x"], "label", {"epochs": 100})
    assert predictor([6.0]) > predictor([1.0])


def test_learning_contract_requires_features_and_point_in_time_dataset() -> None:
    payload = {
        "schema_version": 2,
        "engine_type": "ml_factor",
        "strategy": {
            "id": "temporal_ml_rank_v1",
            "version": "builtin-v1",
            "source_hash": "a" * 64,
            "entrypoint": "temporal_ml_rank_v1",
        },
        "universe": {"symbols": ["600000.SH"]},
        "period": {
            "start": "2026-01-01T00:00:00+08:00",
            "end": "2026-03-01T00:00:00+08:00",
            "interval": "1d",
            "timezone": "Asia/Shanghai",
        },
        "data": {
            "selection": {
                "datasets": ["kline_daily"],
                "decision_time": "2026-03-01T00:00:00+08:00",
            }
        },
        "parameters": {"feature_columns": ["pe"], "label_column": "label"},
    }
    with pytest.raises(ValidationError, match="ml_features_daily|factor_matrix_daily"):
        SubmitBacktestRequestV2.model_validate(payload)
    payload["data"]["selection"]["datasets"] = ["factor_matrix_daily"]
    validated = SubmitBacktestRequestV2.model_validate(payload)
    assert validated.engine_type == "ml_factor"


def test_lighter_microstructure_factor_contract_supports_ranking_and_sequence_metadata() -> None:
    payload = {
        "schema_version": 2,
        "engine_type": "ml_factor",
        "strategy": {
            "id": "temporal_ml_rank_v1",
            "version": "builtin-v1",
            "source_hash": "a" * 64,
            "entrypoint": "temporal_ml_rank_v1",
        },
        "universe": {"symbols": ["LITUSDT_SWAP_LIGHTER"]},
        "period": {
            "start": "2026-01-01T00:00:00+00:00",
            "end": "2026-01-03T00:00:00+00:00",
            "interval": "1d",
        },
        "data": {
            "selection": {
                "datasets": ["lighter_microstructure_factors"],
                "decision_time": "2026-01-03T00:00:00+00:00",
            }
        },
        "parameters": {
            "feature_columns": ["ofi_normalized", "trade_imbalance"],
            "label_column": "future_mid_return_bps",
            "task_type": "ranking",
            "seq_len": 24,
        },
    }
    validated = SubmitBacktestRequestV2.model_validate(payload)
    assert validated.parameters["task_type"] == "ranking"
    assert validated.parameters["seq_len"] == 24


def test_learning_errors_when_no_fold_can_be_built() -> None:
    rows = [{"event_time": "2026-01-01T00:00:00+00:00"}]
    with pytest.raises(LearningBacktestError, match="不足"):
        _generate_folds(rows, {"train_days": 2, "test_days": 1})


def _checkpoint_rows() -> list[dict]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "event_time": (start + timedelta(days=index)).isoformat(),
            "available_at": (start + timedelta(days=index)).isoformat(),
            "decision_time": (start + timedelta(days=index)).isoformat(),
            "symbol": "AAA",
            "x": float(index),
            "label": float(index + 1) / 100,
        }
        for index in range(12)
    ]


def _checkpoint_task() -> dict:
    return {
        "engine_type": "ml_factor",
        "strategy": {
            "id": "checkpoint-strategy",
            "source_hash": "a" * 64,
            "entrypoint": "checkpoint-strategy",
        },
        "universe": {"symbols": ["AAA"]},
        "period": {
            "start": "2026-01-01T00:00:00+00:00",
            "end": "2026-01-12T23:59:59+00:00",
        },
        "data": {"snapshot": {"snapshot_id": "btsnap_v1_" + "c" * 32}},
        "execution": {"capital": 100_000, "rate": 0.0001},
        "parameters": {
            "feature_columns": ["x"],
            "label_column": "label",
            "model_type": "linear_regression",
            "epochs": 5,
            "learning_rate": 0.02,
            "train_days": 6,
            "test_days": 2,
            "step_days": 2,
            "prediction_threshold": -1,
        },
        "random_seed": 17,
    }


def _run_checkpoint_training(monkeypatch, **kwargs) -> dict:
    monkeypatch.setattr(
        "app.learning.load_manifest_feature_rows",
        lambda **_: copy.deepcopy(_checkpoint_rows()),
    )
    return run_learning_backtest(
        task_id="learning-checkpoint-test",
        task=kwargs.pop("task", _checkpoint_task()),
        manifest=kwargs.pop("manifest", {"snapshot_manifest_version": 1}),
        data_root="unused",
        **kwargs,
    )


def test_learning_checkpoint_round_trip_and_nested_tamper_rejection(
    monkeypatch,
) -> None:
    task = _checkpoint_task()
    task["random_seed"] = 0
    paused = _run_checkpoint_training(monkeypatch, task=task, epoch_budget=2)
    payload = paused["training"]["checkpoint"]
    restored = LearningCheckpoint.from_dict(json.loads(json.dumps(payload)))

    assert restored.contract_version == LEARNING_CHECKPOINT_CONTRACT_VERSION
    assert restored.artifact_logical_name == LEARNING_CHECKPOINT_ARTIFACT
    assert restored.snapshot_id == "btsnap_v1_" + "c" * 32
    assert restored.strategy_id == "checkpoint-strategy"
    assert restored.strategy_source_hash == "a" * 64
    assert restored.random_seed == 0
    assert restored.fold_index == 0
    assert restored.completed_epochs == 2
    assert set(restored.model_state or {}) == {
        "contract_version",
        "learning_rate",
        "means",
        "scales",
        "weights",
    }

    tampered = restored.to_dict()
    tampered["model_state"]["weights"][0] += 1
    with pytest.raises(LearningBacktestError, match="内容哈希"):
        LearningCheckpoint.from_dict(tampered)


def test_learning_resume_is_deterministic_and_reports_live_metrics(monkeypatch) -> None:
    uninterrupted_metrics: list[dict] = []
    uninterrupted = _run_checkpoint_training(
        monkeypatch,
        on_training_metric=uninterrupted_metrics.append,
    )
    checkpoint_events: list[dict] = []
    first_metrics: list[dict] = []
    paused = _run_checkpoint_training(
        monkeypatch,
        epoch_budget=3,
        on_training_metric=first_metrics.append,
        on_checkpoint=checkpoint_events.append,
    )
    checkpoint = json.loads(json.dumps(paused["training"]["checkpoint"]))
    resumed_metrics: list[dict] = []
    resumed = _run_checkpoint_training(
        monkeypatch,
        checkpoint=checkpoint,
        on_training_metric=resumed_metrics.append,
    )

    assert paused["complete"] is False
    assert paused["training"]["metrics_emitted"] == 3
    assert len(first_metrics) == 3
    assert checkpoint_events[-1] == checkpoint
    assert resumed["metrics"] == uninterrupted["metrics"]
    assert resumed["curves"] == uninterrupted["curves"]
    assert resumed["diagnostics"]["folds"] == uninterrupted["diagnostics"]["folds"]
    assert resumed["replay_audit"] == uninterrupted["replay_audit"]
    assert resumed["training"]["checkpoint_summary"] == uninterrupted["training"][
        "checkpoint_summary"
    ]
    assert len(first_metrics) + len(resumed_metrics) == len(uninterrupted_metrics)

    metric = first_metrics[-1]
    assert set(
        [
            "fold",
            "epoch",
            "loss",
            "learning_rate",
            "device",
            "checkpoint_summary",
        ]
    ) <= set(metric)
    assert metric["fold"] == 1
    assert metric["epoch"] == 3
    assert metric["loss"] >= 0
    assert metric["learning_rate"] == 0.02
    assert metric["device"] == "cpu"
    assert metric["artifact_logical_name"] == LEARNING_METRICS_ARTIFACT
    assert metric["checkpoint_summary"]["artifact_logical_name"] == (
        LEARNING_CHECKPOINT_ARTIFACT
    )
    assert set(resumed["training"]["artifact_logical_names"]) == {
        LEARNING_CHECKPOINT_ARTIFACT,
        LEARNING_METRICS_ARTIFACT,
    }
    assert resumed["artifacts"] == []
    assert "优化器" in resumed["diagnostics"]["warnings"][1]


def test_external_model_checkpoint_resumes_only_from_completed_fold(
    monkeypatch,
) -> None:
    task = _checkpoint_task()
    task["parameters"]["model_type"] = "lightgbm"
    task["parameters"]["n_estimators"] = 4
    monkeypatch.setattr(
        "app.learning.learning_runtime_capabilities",
        lambda: {
            "optional": {
                "lightgbm": True,
                "torch": False,
                "qlib": False,
                "rd_agent": False,
            }
        },
    )
    monkeypatch.setattr(
        "app.learning._fit_model",
        lambda *args, **kwargs: lambda values: float(values[0]) / 100,
    )
    uninterrupted = _run_checkpoint_training(monkeypatch, task=task)
    checkpoints: list[dict] = []

    class StopAfterFold(RuntimeError):
        pass

    def stop_after_first_fold(payload: dict) -> None:
        checkpoints.append(payload)
        if payload["fold_index"] == 1:
            raise StopAfterFold

    with pytest.raises(StopAfterFold):
        _run_checkpoint_training(
            monkeypatch,
            task=task,
            on_checkpoint=stop_after_first_fold,
        )

    completed_fold = LearningCheckpoint.from_dict(checkpoints[-1])
    assert completed_fold.fold_index == 1
    assert completed_fold.completed_epochs == 0
    assert completed_fold.model_state is None
    resumed_metrics: list[dict] = []
    resumed = _run_checkpoint_training(
        monkeypatch,
        task=task,
        checkpoint=completed_fold,
        on_training_metric=resumed_metrics.append,
    )

    assert resumed["metrics"] == uninterrupted["metrics"]
    assert resumed["curves"] == uninterrupted["curves"]
    assert resumed_metrics[0]["checkpoint_scope"] == "completed_fold_only"
    assert resumed_metrics[0]["epoch"] == 4
    assert resumed_metrics[0]["loss"] >= 0
    with pytest.raises(LearningBacktestError, match="完整 fold"):
        _run_checkpoint_training(
            monkeypatch,
            task=task,
            epoch_budget=1,
        )


def test_learning_worker_restores_checkpoint_and_emits_persistable_events(
    tmp_path, monkeypatch
) -> None:
    checkpoint = {
        "contract_version": LEARNING_CHECKPOINT_CONTRACT_VERSION,
        "artifact_logical_name": LEARNING_CHECKPOINT_ARTIFACT,
        "fold_index": 1,
        "completed_epochs": 0,
        "metric_count": 2,
        "checkpoint_sha256": "c" * 64,
    }
    captured: dict = {}

    def fake_run_learning_backtest(**kwargs):
        captured.update(kwargs)
        kwargs["on_checkpoint"](checkpoint)
        kwargs["on_training_metric"](
            {
                "artifact_logical_name": LEARNING_METRICS_ARTIFACT,
                "fold": 2,
                "epoch": 1,
                "loss": 0.125,
            }
        )
        return {
            "metrics": {},
            "curves": {},
            "diagnostics": {},
            "artifacts": [],
        }

    monkeypatch.setattr(
        "app.learning.run_learning_backtest", fake_run_learning_backtest
    )
    event_queue: queue.Queue = queue.Queue()
    command_queue: queue.Queue = queue.Queue()
    result_path = tmp_path / "result.json"

    run_learning_worker(
        "learning-worker-test",
        {
            "_task_contract": _checkpoint_task(),
            "_snapshot_manifest": {"snapshot_manifest_version": 1},
            "_learning_checkpoint": checkpoint,
        },
        "unused",
        str(result_path),
        event_queue,
        command_queue,
    )

    events = []
    while not event_queue.empty():
        events.append(event_queue.get_nowait())
    event_types = [event["type"] for event in events]
    assert captured["checkpoint"] == checkpoint
    assert callable(captured["on_checkpoint"])
    assert callable(captured["on_training_metric"])
    assert "learning_checkpoint" in event_types
    assert "training_metric" in event_types
    assert event_types[-1] == "completed"
    metric_event = next(
        event for event in events if event["type"] == "training_metric"
    )
    assert metric_event["data"]["metrics_emitted"] == 3
    assert result_path.is_file()


@pytest.mark.parametrize(
    "binding",
    ["snapshot_id", "strategy_source_hash", "random_seed", "manifest"],
)
def test_learning_checkpoint_rejects_changed_training_bindings(
    monkeypatch, binding: str
) -> None:
    paused = _run_checkpoint_training(monkeypatch, epoch_budget=2)
    task = _checkpoint_task()
    manifest = {"snapshot_manifest_version": 1}
    if binding == "snapshot_id":
        task["data"]["snapshot"]["snapshot_id"] = "btsnap_v1_" + "d" * 32
    elif binding == "strategy_source_hash":
        task["strategy"]["source_hash"] = "b" * 64
    elif binding == "random_seed":
        task["random_seed"] = 99
    else:
        manifest["snapshot_manifest_version"] = 2

    with pytest.raises(LearningBacktestError, match="不匹配"):
        _run_checkpoint_training(
            monkeypatch,
            task=task,
            manifest=manifest,
            checkpoint=paused["training"]["checkpoint"],
        )


def test_learning_derives_forward_return_from_factor_and_kline_snapshots(tmp_path) -> None:
    import pyarrow as pa
    from pyarrow import parquet

    factor_path = tmp_path / "factor.parquet"
    kline_path = tmp_path / "kline.parquet"
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    factor_rows = []
    kline_rows = []
    for index in range(14):
        day = (start + timedelta(days=index)).date().isoformat()
        factor_rows.append(
            {
                "event_time": start + timedelta(days=index, hours=15),
                "available_at": start + timedelta(days=index, hours=16),
                "tradable_from": start + timedelta(days=index, hours=17),
                "symbol": "AAA",
                "value_score": float(index),
            }
        )
        kline_rows.append({"date": day, "symbol": "AAA", "close": 100.0 + index})
    parquet.write_table(pa.Table.from_pylist(factor_rows), factor_path)
    parquet.write_table(pa.Table.from_pylist(kline_rows), kline_path)

    def record(path):
        return {
            "path": path.name,
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    task = {
        "engine_type": "ml_factor",
        "strategy": {
            "id": "temporal_ml_rank_v1",
            "source_hash": ML_STRATEGY_HASH,
            "entrypoint": "temporal_ml_rank_v1",
        },
        "universe": {"symbols": ["AAA"]},
        "period": {
            "start": "2026-01-01T00:00:00+00:00",
            "end": "2026-01-14T23:59:59+00:00",
        },
        "data": {"snapshot": {"snapshot_id": "btsnap_v1_" + "a" * 32}},
        "execution": {"capital": 100000, "rate": 0.0001},
        "parameters": {
            "feature_columns": ["value_score"],
            "label_column": "forward_return_2d",
            "train_days": 6,
            "test_days": 3,
            "step_days": 3,
            "top_k": 1,
        },
    }
    manifest = {
        "datasets": [
            {"name": "factor_matrix_daily", "files": [record(factor_path)]},
            {"name": "kline_daily", "files": [record(kline_path)]},
        ]
    }
    result = run_learning_backtest(
        task_id="learning-test",
        task=task,
        manifest=manifest,
        data_root=tmp_path,
    )
    assert result["metrics"]["n_days"] > 0
    assert result["diagnostics"]["label_column"] == "forward_return_2d"
    assert result["replay_audit"]["event_count"] > result["metrics"]["n_days"]
    assert len(result["replay_audit"]["chain_sha256"]) == 64
