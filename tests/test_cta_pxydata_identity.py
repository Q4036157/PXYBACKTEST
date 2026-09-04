from __future__ import annotations

import os

from app.config import pxylh_cta_worker_environment
from app.result_contract import build_result_v2
from app.worker_process import _configure_pxylh_cta_worker_environment


def _request_with_snapshot() -> dict:
    return {
        "_task_contract": {
            "engine_type": "vnpy_cta",
            "data": {
                "snapshot": {
                    "provider": "pxydata",
                    "snapshot_id": "btsnap_v1_" + "a" * 32,
                    "manifest_sha256": "b" * 64,
                    "quality_accepted": True,
                }
            },
        }
    }


def test_cta_worker_environment_maps_file_identity_without_secret_value() -> None:
    mapped = pxylh_cta_worker_environment(
        {
            "PXYBACKTEST_PXYDATA_API_KEY_FILE": r"C:\secrets\pxydata-api-key",
            "PXYBACKTEST_PXYDATA_BASE_URL": "http://127.0.0.1:3020/",
            "PXYBACKTEST_PXYDATA_DATA_ROOT": r"E:\pxy-runtime\PXYDATA\data",
            "PXYBACKTEST_PXYDATA_API_KEY": "must-not-cross-worker-boundary",
        }
    )

    assert mapped == {
        "PXYDATA_API_KEY_FILE": r"C:\secrets\pxydata-api-key",
        "PXYDATA_BASE_URL": "http://127.0.0.1:3020/",
        "PXYDATA_DATA_DIR": r"E:\pxy-runtime\PXYDATA\data",
    }
    assert "must-not-cross-worker-boundary" not in mapped.values()


def test_worker_applies_identity_mapping_before_pxylh_loader_import(
    monkeypatch,
) -> None:
    key_file = r"C:\ProgramData\PXY\secrets\pxydata-api-key"
    monkeypatch.setenv("PXYBACKTEST_PXYDATA_API_KEY_FILE", key_file)
    monkeypatch.delenv("PXYDATA_API_KEY_FILE", raising=False)
    monkeypatch.delenv("PXYDATA_API_KEY", raising=False)

    provenance = _configure_pxylh_cta_worker_environment()

    assert os.environ["PXYDATA_API_KEY_FILE"] == key_file
    assert provenance["credential_source"] == "pxybacktest_api_key_file"
    assert key_file not in str(provenance)
    assert provenance["actual_upstream"] == "not_reported_by_loader"
    assert provenance["immutable_snapshot_verified"] is False


def test_plain_cta_result_does_not_claim_requested_snapshot_as_execution_snapshot() -> None:
    result = build_result_v2(
        task_id="task-cta",
        request=_request_with_snapshot(),
        raw_result={
            "statistics": {"total_return": 0.01},
            "data_provenance": {
                "loader": "pxylh.services.backtest_service.kline_loader",
                "execution_source": "vnpy_database_compat_cache",
                "population_policy": "pxydata_preferred_with_vnpy_ccxt_fallback",
                "actual_upstream": "not_reported_by_loader",
                "credential_source": "pxybacktest_api_key_file",
                "immutable_snapshot_verified": False,
            },
        },
    )

    assert result["engine_type"] == "vnpy_cta"
    assert "data_snapshot" not in result
    assert result["reproducibility"]["manifest_sha256"] is None
    assert result["diagnostics"]["requested_data_snapshot"]["snapshot_id"].startswith(
        "btsnap_v1_"
    )
    assert (
        result["diagnostics"]["data_provenance"]["execution_source"]
        == "vnpy_database_compat_cache"
    )


def test_cta_result_emits_snapshot_only_with_execution_evidence() -> None:
    execution_snapshot = {
        "provider": "pxydata",
        "snapshot_id": "btsnap_v1_" + "c" * 32,
        "manifest_sha256": "d" * 64,
        "quality_accepted": True,
    }
    result = build_result_v2(
        task_id="task-cta-bound",
        request=_request_with_snapshot(),
        raw_result={
            "data_snapshot": execution_snapshot,
            "data_provenance": {
                "execution_source": "pxydata_immutable_snapshot",
                "actual_upstream": "pxydata_snapshot",
                "immutable_snapshot_verified": True,
            },
        },
    )

    assert result["data_snapshot"] == execution_snapshot
    assert result["reproducibility"]["manifest_sha256"] == "d" * 64
    assert result["diagnostics"]["quality_accepted"] is True


def test_cta_result_rejects_malformed_snapshot_evidence() -> None:
    result = build_result_v2(
        task_id="task-cta-invalid-snapshot",
        request=_request_with_snapshot(),
        raw_result={
            "data_snapshot": {
                "snapshot_id": "claimed-snapshot",
                "manifest_sha256": "not-a-sha256",
            },
            "data_provenance": {"immutable_snapshot_verified": True},
        },
    )

    assert "data_snapshot" not in result
    assert result["reproducibility"]["manifest_sha256"] is None
