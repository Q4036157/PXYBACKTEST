from __future__ import annotations

import asyncio
import json
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from .config import Settings

A_SHARE_ADAPTER_CONTRACT = "pxybacktest.engine-adapter.a-share.v1"
DAA_ENGINE_TYPES = {"a_share_portfolio", "factor_matrix", "event_sentiment"}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class DaaCapabilitiesError(RuntimeError):
    pass


class DaaCapabilitiesClient(Protocol):
    @property
    def configured(self) -> bool: ...

    async def get_capabilities(self) -> dict[str, Any]: ...


@dataclass
class DaaAdapterClient:
    settings: Settings
    cache_seconds: float = 60.0
    timeout_seconds: float = 12.0
    _cached_at: float = field(default=0.0, init=False)
    _cached: dict[str, Any] | None = field(default=None, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)

    @property
    def configured(self) -> bool:
        return self.settings.a_share_adapter_available

    async def get_capabilities(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._load_capabilities)

    def _load_capabilities(self) -> dict[str, Any]:
        if not self.configured:
            raise DaaCapabilitiesError("DAA A 股适配器未安装")
        with self._lock:
            now = time.monotonic()
            if self._cached is not None and now - self._cached_at < self.cache_seconds:
                return self._cached
            try:
                process = subprocess.run(
                    [
                        str(self.settings.daa_python),
                        "-m",
                        "app.backtest.pxy_adapter",
                        "--capabilities",
                    ],
                    cwd=self.settings.daa_backend_root,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=self.timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                raise DaaCapabilitiesError("DAA 策略目录读取超时") from exc
            except OSError as exc:
                raise DaaCapabilitiesError("DAA 策略目录进程启动失败") from exc
            if process.returncode != 0:
                raise DaaCapabilitiesError("DAA 策略目录读取失败")
            try:
                payload = json.loads(process.stdout)
            except json.JSONDecodeError as exc:
                raise DaaCapabilitiesError("DAA 策略目录格式无效") from exc
            normalized = _validate_capabilities(payload)
            self._cached = normalized
            self._cached_at = now
            return normalized


def _validate_capabilities(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise DaaCapabilitiesError("DAA 策略目录必须是对象")
    if payload.get("contract_version") != A_SHARE_ADAPTER_CONTRACT:
        raise DaaCapabilitiesError("DAA 策略目录契约版本不匹配")
    raw_strategies = payload.get("strategies")
    if not isinstance(raw_strategies, list):
        raise DaaCapabilitiesError("DAA 策略目录缺少 strategies")
    strategies: list[dict[str, Any]] = []
    for item in raw_strategies:
        if not isinstance(item, dict):
            raise DaaCapabilitiesError("DAA 策略条目格式无效")
        strategy_id = str(item.get("id") or "").strip()
        source_hash = str(item.get("source_hash") or "").strip().lower()
        if not strategy_id or _SHA256_RE.fullmatch(source_hash) is None:
            raise DaaCapabilitiesError("DAA 策略身份无效")
        raw_engines = item.get("engine_types") or ["a_share_portfolio"]
        if not isinstance(raw_engines, list):
            raise DaaCapabilitiesError("DAA 策略 engine_types 格式无效")
        engine_types = list(
            dict.fromkeys(
                str(value).strip()
                for value in raw_engines
                if str(value).strip() in DAA_ENGINE_TYPES
            )
        )
        if not engine_types:
            raise DaaCapabilitiesError("DAA 策略没有受支持的引擎类型")
        strategies.append(
            {
                "id": strategy_id,
                "name": str(item.get("name") or strategy_id),
                "description": str(item.get("description") or ""),
                "version": str(item.get("version") or "builtin"),
                "source_hash": source_hash,
                "entrypoint": str(item.get("entrypoint") or strategy_id),
                "tags": list(item.get("tags") or []),
                "asset_types": list(item.get("asset_types") or []),
                "timeframes": list(item.get("timeframes") or []),
                "parameters": list(item.get("parameters") or []),
                "engine_types": engine_types,
            }
        )
    return {
        "contract_version": A_SHARE_ADAPTER_CONTRACT,
        "worker_version": str(payload.get("worker_version") or "unknown"),
        "strategies": strategies,
    }
