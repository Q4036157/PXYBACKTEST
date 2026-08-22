from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.workflow import WorkflowSpec, validate_workflow


def test_workflow_returns_topological_order_and_signal_only_boundary() -> None:
    result = validate_workflow(
        {
            "workflow_id": "daily-ml",
            "name": "日线 ML",
            "mode": "live_signal",
            "nodes": [
                {"id": "source", "type": "data_source"},
                {"id": "features", "type": "feature_engineering", "depends_on": ["source"]},
                {"id": "model", "type": "model_training", "depends_on": ["features"]},
                {"id": "portfolio", "type": "portfolio", "depends_on": ["model"]},
                {"id": "risk", "type": "risk", "depends_on": ["portfolio"]},
                {"id": "backtest", "type": "backtest", "depends_on": ["risk"]},
                {"id": "signal", "type": "live_signal", "depends_on": ["backtest"]},
            ],
        }
    )
    assert result["valid"] is True
    assert result["topological_order"][0] == "source"
    assert result["topological_order"][-1] == "signal"
    assert result["execution_boundary"] == "signal_only_no_order_submission"


def test_workflow_rejects_cycle_and_missing_backtest_dependency() -> None:
    with pytest.raises(ValueError, match="循环"):
        WorkflowSpec.model_validate(
            {
                "workflow_id": "cycle",
                "name": "cycle",
                "nodes": [
                    {"id": "source", "type": "data_source", "depends_on": ["backtest"]},
                    {"id": "backtest", "type": "backtest", "depends_on": ["source"]},
                ],
            }
        )
    with pytest.raises(ValueError, match="必须依赖 data_source"):
        validate_workflow(
            {
                "workflow_id": "bad",
                "name": "bad",
                "nodes": [
                    {"id": "source", "type": "data_source"},
                    {"id": "backtest", "type": "backtest"},
                ],
            }
        )
