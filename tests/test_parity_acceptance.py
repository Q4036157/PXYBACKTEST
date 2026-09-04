from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.parity_acceptance import AcceptanceVector, compare_acceptance_vector
from app.vnpy_acceptance import run_vnpy_acceptance_vector


VECTOR_PATH = (
    Path(__file__).parents[1]
    / "acceptance"
    / "vectors"
    / "vnpy_cta_native_v1.json"
)


def _vector() -> AcceptanceVector:
    return AcceptanceVector.model_validate_json(VECTOR_PATH.read_text(encoding="utf-8"))


def test_pinned_vnpy_vector_passes_trade_account_and_visual_dimensions() -> None:
    pytest.importorskip("vnpy")
    actual = run_vnpy_acceptance_vector()

    result = compare_acceptance_vector(_vector(), actual)

    assert result.all_passed is True
    assert result.trades.status == "passed"
    assert result.account.status == "passed"
    assert result.visual.status == "passed"
    evidence = result.to_strategy_evidence()
    assert evidence["vector_id"] == "vnpy-cta-native-v1"
    assert len(evidence["trades"]["evidence_sha256"]) == 64


def test_vnpy_strategy_identity_is_independent_of_checkout_line_endings(
    tmp_path,
) -> None:
    from app import vnpy_acceptance

    source = (
        Path(vnpy_acceptance.__file__).parent
        / "acceptance_strategies"
        / "vnpy_cta_v1.py"
    )
    lf_source = source.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    expected = hashlib.sha256(lf_source).hexdigest()
    assert expected == "0748b90a5105a31c065fb405e610cde18bb4e34ca9e9953acf7730ae4831208f"

    crlf_source = tmp_path / "vnpy_cta_v1.py"
    crlf_source.write_bytes(lf_source.replace(b"\n", b"\r\n"))
    normalized = (
        crlf_source.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    )
    assert hashlib.sha256(normalized).hexdigest() == expected


def test_trade_difference_does_not_get_hidden_by_matching_account() -> None:
    pytest.importorskip("vnpy")
    actual = run_vnpy_acceptance_vector()
    changed = copy.deepcopy(actual)
    changed["deals"][0]["price"] = 101.2

    result = compare_acceptance_vector(_vector(), changed)

    assert result.all_passed is False
    assert result.trades.status == "failed"
    assert result.trades.first_mismatch_path == "deals"
    assert result.account.status == "passed"
    assert result.visual.status == "passed"


def test_visual_history_difference_fails_only_visual_dimension() -> None:
    pytest.importorskip("vnpy")
    actual = run_vnpy_acceptance_vector()
    changed = copy.deepcopy(actual)
    changed["execution_snapshot"]["bar_history"][0]["close"] = 999

    result = compare_acceptance_vector(_vector(), changed)

    assert result.all_passed is False
    assert result.trades.status == "passed"
    assert result.account.status == "passed"
    assert result.visual.status == "failed"
    assert result.visual.first_mismatch_path == "execution_snapshot.bar_history"


def test_runtime_identity_difference_fails_all_dimensions() -> None:
    pytest.importorskip("vnpy")
    actual = run_vnpy_acceptance_vector()
    actual["diagnostics"]["runtime_identity"] = "vnpy-other"

    result = compare_acceptance_vector(_vector(), actual)

    assert result.all_passed is False
    assert {result.trades.status, result.account.status, result.visual.status} == {
        "failed"
    }
    assert result.trades.first_mismatch_path == "diagnostics.runtime_identity"


def test_vector_rejects_identity_check_that_disagrees_with_declared_hash() -> None:
    payload = _vector().model_dump(mode="json")
    payload["identity_checks"][0]["expected"] = "c" * 64

    with pytest.raises(ValidationError, match="身份未绑定或不一致"):
        AcceptanceVector.model_validate(payload)
