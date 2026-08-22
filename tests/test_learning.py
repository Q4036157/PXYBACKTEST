from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib

import pytest
from pydantic import ValidationError

from app.learning import LearningBacktestError, _fit_linear_regression, _generate_folds
from app.learning import ML_STRATEGY_HASH, run_learning_backtest
from app.models import SubmitBacktestRequestV2


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
