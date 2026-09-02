from __future__ import annotations

import json
from pathlib import Path

from app.parity_acceptance import compare_acceptance_vector
from app.tqsdk_acceptance import (
    TQSDK_ACCEPTANCE_GATE_CONTRACT,
    TQSDK_ACCEPTANCE_VECTOR_ID,
    build_tqsdk_acceptance_gate,
    build_tqsdk_acceptance_vector,
    load_tqsdk_acceptance_gate,
)


def _actual() -> dict:
    return {
        "strategy": {"source_hash": "a" * 64},
        "data_snapshot": {"manifest_sha256": "b" * 64},
        "diagnostics": {
            "runtime_identity": "tqsdk-3.10.2",
            "sandbox": {
                "strength": "windows_restricted",
                "restricted_token": True,
                "job_object": True,
                "dedicated_identity": True,
                "task_directory_acl": True,
                "filesystem_isolated": True,
                "network_allowlist_enforced": True,
                "network_policy_sha256": "c" * 64,
                "submit_ready": True,
            },
        },
        "deals": [{"trade_id": "T1", "price": 500.2}],
        "account_curve": [{"trading_day": "20260818", "balance": 1_000_100}],
        "final_account": {"balance": 1_000_100},
        "execution_snapshot": {
            "bar_history": [{"symbol": "SHFE.au2612", "close": 500.2}]
        },
        "replay_audit": {"event_count": 4, "chain_sha256": "d" * 64},
    }


def test_tqsdk_vector_requires_second_execution_and_all_three_dimensions() -> None:
    actual = _actual()
    vector = build_tqsdk_acceptance_vector(actual)

    result = compare_acceptance_vector(vector, actual)

    assert result.all_passed is True
    assert result.trades.status == "passed"
    assert result.account.status == "passed"
    assert result.visual.status == "passed"


def test_tqsdk_gate_binds_network_policy_and_parity_evidence(tmp_path: Path) -> None:
    actual = _actual()
    vector = build_tqsdk_acceptance_vector(actual)
    result = compare_acceptance_vector(vector, actual)
    gate = build_tqsdk_acceptance_gate(vector=vector, actual=actual, result=result)
    path = tmp_path / "gate.json"
    path.write_text(gate.model_dump_json(indent=2), encoding="utf-8")

    loaded = load_tqsdk_acceptance_gate(
        path,
        network_policy_sha256="c" * 64,
        runtime_identity="tqsdk-3.10.2",
        strategy_source_sha256="a" * 64,
    )

    assert loaded is not None
    assert loaded.contract_version == TQSDK_ACCEPTANCE_GATE_CONTRACT
    assert loaded.vector_id == TQSDK_ACCEPTANCE_VECTOR_ID
    assert loaded.all_passed is True
    assert load_tqsdk_acceptance_gate(
        path, network_policy_sha256="e" * 64
    ) is None
    assert load_tqsdk_acceptance_gate(
        path,
        network_policy_sha256="c" * 64,
        runtime_identity="tqsdk-3.10.3",
    ) is None
    assert load_tqsdk_acceptance_gate(
        path,
        network_policy_sha256="c" * 64,
        strategy_source_sha256="f" * 64,
    ) is None


def test_tqsdk_visual_difference_blocks_gate() -> None:
    actual = _actual()
    vector = build_tqsdk_acceptance_vector(actual)
    changed = json.loads(json.dumps(actual))
    changed["execution_snapshot"]["bar_history"][0]["close"] = 501.0

    result = compare_acceptance_vector(vector, changed)

    assert result.all_passed is False
    assert result.visual.status == "failed"
