from __future__ import annotations

from pathlib import Path

from app.runner_registry import RunnerProbeConfig, build_runner_registry
from test_runner_registry import _package


def test_unimplemented_later_phase_platform_does_not_resolve(tmp_path: Path) -> None:
    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=tmp_path / "PXYBACKTEST",
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=None,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
        )
    )
    package = _package(
        platform="tradingview",
        adapter_id="pine-ir",
        mode="compat",
        semantics="tradingview_bar",
        language="pine",
    )

    result = registry.resolve(package)

    assert result["resolved"] is False
    assert result["runner"] is None
    assert "尚未接入" in result["reason"]
