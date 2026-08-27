from __future__ import annotations

from app.reporting import build_report_projection, distribution, monthly_returns, rolling_sharpe


def test_monthly_returns_and_distribution_are_result_only() -> None:
    curve = [
        {"date": "2026-01-01", "value": 100},
        {"date": "2026-01-31", "value": 110},
        {"date": "2026-02-01", "value": 110},
        {"date": "2026-02-28", "value": 99},
    ]
    assert monthly_returns(curve) == [
        {"month": "2026-01", "return": 0.1},
        {"month": "2026-02", "return": -0.1},
    ]
    assert sum(item["count"] for item in distribution([1, 2, 3], bins=2)) == 3


def test_report_projection_contains_kpis_and_frontend_tables() -> None:
    curve = [{"date": f"2026-01-{index + 1:02d}", "value": 100 + index} for index in range(25)]
    result = build_report_projection(
        {"metrics": {"total_return": 42}, "curves": {"equity": curve}, "deals": [{"pnl": 1}]}
    )
    assert result["contract_version"] == "pxybacktest.report.v1"
    assert result["kpis"]["total_return"] == 42
    assert result["tables"]["rolling_sharpe_20"]
    assert result["tables"]["deals"] == [{"pnl": 1}]
    assert rolling_sharpe(curve, window=20)
