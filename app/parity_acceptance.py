"""跨平台逐笔成交、账户和可视化三维一致性验收。"""

from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .kernel import stable_hash


PARITY_ACCEPTANCE_CONTRACT = "pxybacktest.parity-acceptance.v1"
DIMENSION_NAMES = ("trades", "account", "visual")
_MISSING = object()


class AcceptanceCheck(BaseModel):
    """从实际结果路径取值，并与小型期望值或大型数据哈希比较。"""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(min_length=1, max_length=300)
    expected: Any = None
    expected_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-fA-F]{64}$",
    )

    @model_validator(mode="after")
    def validate_expected(self) -> "AcceptanceCheck":
        has_expected = "expected" in self.model_fields_set
        if has_expected == bool(self.expected_sha256):
            raise ValueError("expected 与 expected_sha256 必须且只能提供一个")
        if self.expected_sha256:
            self.expected_sha256 = self.expected_sha256.lower()
        return self


class AcceptanceDimension(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checks: list[AcceptanceCheck] = Field(min_length=1, max_length=100)


class AcceptanceVector(BaseModel):
    """绑定策略、数据、运行时和三维 Oracle 的固定测试向量。"""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.parity-acceptance.v1"] = (
        PARITY_ACCEPTANCE_CONTRACT
    )
    vector_id: str = Field(min_length=1, max_length=200)
    platform: str = Field(min_length=1, max_length=50)
    strategy_source_sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    data_manifest_sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    runtime_identity: str = Field(min_length=1, max_length=300)
    identity_checks: list[AcceptanceCheck] = Field(min_length=1, max_length=20)
    trades: AcceptanceDimension
    account: AcceptanceDimension
    visual: AcceptanceDimension
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckComparison(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    matched: bool
    expected_sha256: str
    actual_sha256: str | None
    error: str | None = None


class DimensionComparison(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Literal["trades", "account", "visual"]
    status: Literal["passed", "failed"]
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    checks: list[CheckComparison]
    first_mismatch_path: str | None = None


class AcceptanceResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.parity-acceptance.v1"] = (
        PARITY_ACCEPTANCE_CONTRACT
    )
    vector_id: str
    platform: str
    all_passed: bool
    trades: DimensionComparison
    account: DimensionComparison
    visual: DimensionComparison

    def to_strategy_evidence(self) -> dict[str, Any]:
        """生成 StrategyPackage.parity_evidence 可直接校验的结构。"""

        return {
            "vector_id": self.vector_id,
            **{
                name: {
                    "status": getattr(self, name).status,
                    "evidence_sha256": getattr(self, name).evidence_sha256,
                    "summary": (
                        "全部检查通过"
                        if getattr(self, name).status == "passed"
                        else f"首个差异: {getattr(self, name).first_mismatch_path}"
                    ),
                }
                for name in DIMENSION_NAMES
            },
        }


def _resolve_path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _project_like(expected: Any, actual: Any) -> Any:
    """只比较 Oracle 声明字段，但数组长度和顺序必须完全一致。"""

    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return actual
        return {
            str(key): (
                _project_like(value, actual[key])
                if key in actual
                else {"__missing__": str(key)}
            )
            for key, value in expected.items()
        }
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return actual
        if len(expected) != len(actual):
            return {
                "__length__": len(actual),
                "__items__": [
                    _project_like(item, actual[index])
                    for index, item in enumerate(expected[: len(actual)])
                ],
            }
        return [
            _project_like(expected_item, actual_item)
            for expected_item, actual_item in zip(expected, actual)
        ]
    return actual


def _compare_check(result: Mapping[str, Any], check: AcceptanceCheck) -> CheckComparison:
    actual = _resolve_path(result, check.path)
    expected_hash = (
        str(check.expected_sha256)
        if check.expected_sha256
        else stable_hash(check.expected)
    )
    if actual is _MISSING:
        return CheckComparison(
            path=check.path,
            matched=False,
            expected_sha256=expected_hash,
            actual_sha256=None,
            error="实际结果缺少路径",
        )
    projected = actual if check.expected_sha256 else _project_like(check.expected, actual)
    actual_hash = stable_hash(projected)
    return CheckComparison(
        path=check.path,
        matched=actual_hash == expected_hash,
        expected_sha256=expected_hash,
        actual_sha256=actual_hash,
        error=None if actual_hash == expected_hash else "内容哈希不一致",
    )


def _compare_dimension(
    *,
    vector_id: str,
    name: Literal["trades", "account", "visual"],
    identity: list[CheckComparison],
    dimension: AcceptanceDimension,
    actual: Mapping[str, Any],
) -> DimensionComparison:
    checks = [*identity, *(_compare_check(actual, item) for item in dimension.checks)]
    mismatches = [item for item in checks if not item.matched]
    evidence_sha256 = stable_hash(
        {
            "contract_version": PARITY_ACCEPTANCE_CONTRACT,
            "vector_id": vector_id,
            "dimension": name,
            "checks": [item.model_dump(mode="json") for item in checks],
        }
    )
    return DimensionComparison(
        name=name,
        status="failed" if mismatches else "passed",
        evidence_sha256=evidence_sha256,
        checks=checks,
        first_mismatch_path=mismatches[0].path if mismatches else None,
    )


def compare_acceptance_vector(
    vector: AcceptanceVector,
    actual: Mapping[str, Any],
) -> AcceptanceResult:
    """严格比较三维结果，任一身份或维度不一致都不能通过。"""

    identity = [_compare_check(actual, check) for check in vector.identity_checks]
    dimensions = {
        name: _compare_dimension(
            vector_id=vector.vector_id,
            name=name,  # type: ignore[arg-type]
            identity=identity,
            dimension=getattr(vector, name),
            actual=actual,
        )
        for name in DIMENSION_NAMES
    }
    return AcceptanceResult(
        vector_id=vector.vector_id,
        platform=vector.platform,
        all_passed=all(item.status == "passed" for item in dimensions.values()),
        trades=dimensions["trades"],
        account=dimensions["account"],
        visual=dimensions["visual"],
    )


__all__ = [
    "AcceptanceCheck",
    "AcceptanceDimension",
    "AcceptanceResult",
    "AcceptanceVector",
    "CheckComparison",
    "DimensionComparison",
    "PARITY_ACCEPTANCE_CONTRACT",
    "compare_acceptance_vector",
]
