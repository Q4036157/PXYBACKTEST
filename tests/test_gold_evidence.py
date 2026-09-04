from __future__ import annotations

import json
from datetime import UTC, datetime

from app.gold_evidence import generate_vnpy_gold_evidence
from app.kernel import stable_hash


def test_generate_vnpy_gold_evidence_writes_complete_package(tmp_path):
    source_hash = "a" * 64
    data_hash = stable_hash([])
    actual = {
        "strategy": {"source_hash": source_hash},
        "data_snapshot": {"manifest_sha256": data_hash},
        "diagnostics": {"runtime_identity": "vnpy-test"},
        "deals": [],
        "execution_snapshot": {"account_curve": [], "bar_history": []},
        "replay_audit": {"event_count": 0, "chain_sha256": "b" * 64},
    }
    vector = {
        "contract_version": "pxybacktest.parity-acceptance.v1",
        "vector_id": "vnpy-test-vector",
        "platform": "vnpy",
        "strategy_source_sha256": source_hash,
        "data_manifest_sha256": data_hash,
        "runtime_identity": "vnpy-test",
        "identity_checks": [
            {"path": "strategy.source_hash", "expected": source_hash},
            {"path": "data_snapshot.manifest_sha256", "expected": data_hash},
            {"path": "diagnostics.runtime_identity", "expected": "vnpy-test"},
        ],
        "trades": {"checks": [{"path": "deals", "expected": []}]},
        "account": {
            "checks": [{"path": "execution_snapshot.account_curve", "expected": []}]
        },
        "visual": {
            "checks": [{"path": "execution_snapshot.bar_history", "expected": []}]
        },
    }
    vector_path = tmp_path / "vector-source.json"
    vector_path.write_text(json.dumps(vector), encoding="utf-8")
    output_dir = tmp_path / "evidence"
    instant = datetime(2026, 9, 4, 1, 2, 3, tzinfo=UTC)

    summary = generate_vnpy_gold_evidence(
        output_dir=output_dir,
        reviewer="自动复核",
        vector_path=vector_path,
        actual_factory=lambda: actual,
        repositories={
            "PXYBACKTEST": {
                "commit": "c" * 40,
                "tracked_worktree_dirty": False,
            }
        },
        now=lambda: instant,
    )

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    task_result = json.loads(
        (output_dir / "task-result.json").read_text(encoding="utf-8")
    )
    assert summary["all_passed"] is True
    assert manifest["golden_case_id"] == "GOLD-001"
    assert manifest["test"]["dimensions"] == {
        "trades": "passed",
        "account": "passed",
        "visual": "passed",
    }
    assert manifest["task_id"] == task_result["task_id"] == summary["task_id"]
    assert manifest["repository_matrix"]["PXYBACKTEST"]["commit"] == "c" * 40
    assert set(manifest["artifact_sha256"]) == {
        "request.json",
        "vector.json",
        "oracle-actual.json",
        "acceptance-result.json",
        "task-result.json",
    }
    assert (output_dir / "SHA256SUMS.txt").read_text(encoding="ascii").count("\n") == 6
