from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from app.strategy_package import (
    ArtifactIdentity,
    DataSubscription,
    EventEnvelope,
    EventKind,
    ExecutionProfile,
    RunnerDescriptor,
    RunnerMode,
    SourcePlatform,
    StrategyPackage,
    StrategySource,
    VerificationLevel,
)


SHA256 = "a" * 64


def _artifact(role: str = "source") -> ArtifactIdentity:
    return ArtifactIdentity(
        artifact_id=f"artifact-{role}",
        file_name={
            "source": "strategy.mq5",
            "binary": "strategy.ex5",
            "ir": "strategy.ir.json",
        }.get(role, "artifact.bin"),
        sha256=SHA256,
        media_type="text/plain",
        role=role,
        size_bytes=100,
    )


def _execution(semantics: str, matching_model: str) -> ExecutionProfile:
    return ExecutionProfile(
        semantics=semantics,
        initial_cash=100_000,
        leverage=100,
        matching_model=matching_model,
        position_mode="hedging",
    )


def test_mt5_native_strategy_package_preserves_native_semantics() -> None:
    package = StrategyPackage(
        strategy_id="123-knight",
        version="sha256:123",
        source=StrategySource(
            platform=SourcePlatform.MT5,
            language="MQL5",
            entrypoint="123骑士.ex5",
            artifacts=[_artifact(), _artifact("binary")],
            license_policy="user_supplied",
        ),
        runner=RunnerDescriptor(
            mode=RunnerMode.NATIVE_ORACLE,
            adapter_id="mt5-native",
            adapter_version="1",
            runtime_identity="mt5-build-6140",
            acceptance_vector_ids=["mt5-xau-20260818-20260821"],
        ),
        subscriptions=[
            DataSubscription(
                kind=EventKind.TICK,
                symbols=["xauusdm"],
            )
        ],
        execution=_execution("mt5_hedging", "native"),
        verification_level=VerificationLevel.NATIVE_VERIFIED,
    )

    assert package.source.language == "mql5"
    assert package.subscriptions[0].symbols == ["XAUUSDM"]
    assert package.execution.matching_model == "native"


def test_joinquant_package_can_require_bar_factor_financial_and_sentiment() -> None:
    package = StrategyPackage(
        strategy_id="jq-multi-factor",
        version="v1",
        source=StrategySource(
            platform=SourcePlatform.JOINQUANT,
            language="python",
            entrypoint="strategy.py:initialize",
            artifacts=[_artifact()],
            license_policy="user_supplied",
        ),
        runner=RunnerDescriptor(
            mode=RunnerMode.COMPAT,
            adapter_id="joinquant-compat",
            adapter_version="1",
            runtime_identity="python-3.12",
        ),
        subscriptions=[
            DataSubscription(
                kind=EventKind.BAR,
                universe_ref="csi300",
                interval="1d",
            ),
            DataSubscription(
                kind=EventKind.FUNDAMENTAL_PIT,
                universe_ref="csi300",
                fields=["pe_ratio", "roe"],
            ),
            DataSubscription(
                kind=EventKind.FACTOR,
                universe_ref="csi300",
                fields=["quality", "momentum"],
            ),
            DataSubscription(
                kind=EventKind.SENTIMENT,
                universe_ref="csi300",
                fields=["score"],
            ),
        ],
        execution=ExecutionProfile(
            semantics="joinquant_a_share",
            base_currency="CNY",
            initial_cash=1_000_000,
            matching_model="bar_ohlc_conservative",
            position_mode="portfolio",
            price_adjustment="provider",
        ),
    )

    assert {item.kind for item in package.subscriptions} == {
        EventKind.BAR,
        EventKind.FUNDAMENTAL_PIT,
        EventKind.FACTOR,
        EventKind.SENTIMENT,
    }


def test_portable_ir_requires_ir_artifact() -> None:
    with pytest.raises(ValidationError, match="role=ir"):
        StrategyPackage(
            strategy_id="pine-portable",
            version="v1",
            source=StrategySource(
                platform=SourcePlatform.TRADINGVIEW,
                language="pine",
                entrypoint="strategy.pine",
                artifacts=[_artifact()],
                license_policy="user_supplied",
            ),
            runner=RunnerDescriptor(
                mode=RunnerMode.PORTABLE_IR,
                adapter_id="pine-ir",
                adapter_version="1",
                runtime_identity="pxy-ir-1",
            ),
            subscriptions=[
                DataSubscription(
                    kind=EventKind.BAR,
                    symbols=["BTCUSDT"],
                    interval="1m",
                )
            ],
            execution=_execution("tradingview_bar", "bar_close"),
        )


def test_parity_verified_package_requires_acceptance_vector() -> None:
    with pytest.raises(ValidationError, match="acceptance_vector_ids"):
        StrategyPackage(
            strategy_id="vnpy-strategy",
            version="v1",
            source=StrategySource(
                platform=SourcePlatform.VNPY,
                language="python",
                entrypoint="strategy:DemoStrategy",
                artifacts=[_artifact()],
                license_policy="internal",
            ),
            runner=RunnerDescriptor(
                mode=RunnerMode.NATIVE_SANDBOX,
                adapter_id="vnpy-cta",
                adapter_version="1",
                runtime_identity="vnpy-pinned",
            ),
            subscriptions=[
                DataSubscription(
                    kind=EventKind.BAR,
                    symbols=["RB.LOCAL"],
                    interval="1m",
                )
            ],
            execution=_execution("vnpy_cta", "bar_ohlc_conservative"),
            verification_level=VerificationLevel.PARITY_VERIFIED,
        )


def _parity_verified_payload() -> dict:
    return {
        "strategy_id": "vnpy-parity-strategy",
        "version": "v1",
        "source": {
            "platform": "vnpy",
            "language": "python",
            "entrypoint": "strategy:ParityStrategy",
            "artifacts": [_artifact().model_dump(mode="json")],
            "license_policy": "internal",
        },
        "runner": {
            "mode": "native_sandbox",
            "adapter_id": "vnpy-cta",
            "adapter_version": "1",
            "runtime_identity": "vnpy-pinned",
            "acceptance_vector_ids": ["vnpy-vector-1"],
        },
        "subscriptions": [
            {"kind": "bar", "symbols": ["RB.LOCAL"], "interval": "1m"}
        ],
        "execution": {
            "semantics": "vnpy_cta",
            "initial_cash": 100_000,
            "matching_model": "bar_ohlc_conservative",
            "position_mode": "hedging",
        },
        "verification_level": "parity_verified",
    }


def test_parity_verified_requires_trade_account_and_visual_evidence() -> None:
    payload = _parity_verified_payload()
    payload["parity_evidence"] = [
        {
            "vector_id": "vnpy-vector-1",
            "trades": {"status": "passed", "evidence_sha256": "a" * 64},
            "account": {"status": "passed", "evidence_sha256": "b" * 64},
            "visual": {"status": "failed", "evidence_sha256": "c" * 64},
        }
    ]

    with pytest.raises(ValidationError, match="逐笔成交、账户和可视化必须全部通过"):
        StrategyPackage.model_validate(payload)


def test_parity_verified_accepts_only_complete_three_dimension_evidence() -> None:
    payload = _parity_verified_payload()
    payload["parity_evidence"] = [
        {
            "vector_id": "vnpy-vector-1",
            "trades": {"status": "passed", "evidence_sha256": "a" * 64},
            "account": {"status": "passed", "evidence_sha256": "b" * 64},
            "visual": {"status": "passed", "evidence_sha256": "c" * 64},
        }
    ]

    package = StrategyPackage.model_validate(payload)

    assert package.verification_level == VerificationLevel.PARITY_VERIFIED
    assert package.parity_evidence[0].all_passed is True


def test_event_envelope_keeps_event_and_available_time_separate() -> None:
    event_time = datetime(2026, 6, 30, 0, 0, tzinfo=UTC)
    available_at = event_time + timedelta(days=30)
    event = EventEnvelope(
        seq=1,
        event_id="financial-1",
        kind=EventKind.FUNDAMENTAL_PIT,
        event_time=event_time,
        available_at=available_at,
        ingested_at=available_at + timedelta(seconds=1),
        source="pxydata",
        snapshot_id="snapshot-1",
        revision_id="revision-1",
        symbol="600000.SH",
        payload={"roe": 0.12},
    )

    assert event.available_at > event.event_time


def test_event_envelope_rejects_naive_time_and_early_ingestion() -> None:
    aware = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValidationError, match="必须携带时区"):
        EventEnvelope(
            seq=1,
            event_id="bar-1",
            kind=EventKind.BAR,
            event_time=datetime(2026, 1, 1),
            available_at=aware,
            ingested_at=aware,
            source="pxydata",
            snapshot_id="snapshot-1",
            revision_id="revision-1",
            payload={},
        )

    with pytest.raises(ValidationError, match="不能早于"):
        EventEnvelope(
            seq=2,
            event_id="news-1",
            kind=EventKind.NEWS,
            event_time=aware,
            available_at=aware + timedelta(minutes=1),
            ingested_at=aware,
            source="pxydata",
            snapshot_id="snapshot-1",
            revision_id="revision-1",
            payload={},
        )
