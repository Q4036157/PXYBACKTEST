"""受控的实时 LLM 信号节点。

只调用工作站配置的 OpenAI-compatible provider；请求体不能指定任意 URL，避免
把回测服务变成 SSRF 代理。该节点只返回信号和审计摘要，不提供交易接口。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LLMSignalError(RuntimeError):
    """LLM provider 或输出格式错误。"""


class LLMRealtimeSignalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=80)
    model: str = Field(default="default", min_length=1, max_length=120)
    instruction: str = Field(min_length=1, max_length=8000)
    factors: dict[str, float] = Field(default_factory=dict, max_length=128)
    market_intelligence: list[str] = Field(default_factory=list, max_length=32)
    mode: str = Field(default="paper", pattern=r"^(paper|live_signal)$")

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


async def generate_realtime_signal(
    request: LLMRealtimeSignalRequest,
    *,
    base_url: str,
    api_key: str,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    if not base_url or not api_key:
        raise LLMSignalError("LLM provider 尚未配置")
    prompt = _build_prompt(request)
    payload = {
        "model": request.model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": '只输出 JSON: {"score": number, "reason": string}'},
            {"role": "user", "content": prompt},
        ],
    }
    response = await asyncio.to_thread(
        _request_json,
        base_url.rstrip("/") + "/chat/completions",
        api_key,
        payload,
        timeout_seconds,
    )
    content = _extract_content(response)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = {"score": float(content.strip()), "reason": "provider returned numeric score"}
    try:
        score = float(parsed.get("score"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise LLMSignalError("LLM 输出缺少 score") from exc
    if not -1 <= score <= 1:
        raise LLMSignalError("LLM score 必须在 [-1, 1]")
    generated_at = datetime.now(timezone.utc).isoformat()
    audit_hash = hashlib.sha256(
        json.dumps({"request": request.model_dump(mode="json"), "response": parsed}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return {
        "symbol": request.symbol,
        "score": score,
        "reason": str(parsed.get("reason") or "")[:1000],
        "mode": request.mode,
        "provider": base_url.rstrip("/"),
        "model": request.model,
        "generated_at": generated_at,
        "audit_hash": audit_hash,
        "historical_backtest": False,
        "execution_boundary": "signal_only_no_order_submission",
    }


def _build_prompt(request: LLMRealtimeSignalRequest) -> str:
    intelligence = "\n".join(f"- {item[:1000]}" for item in request.market_intelligence)
    factors = json.dumps(request.factors, ensure_ascii=False, sort_keys=True)
    return (
        f"symbol={request.symbol}\n"
        f"factors={factors}\n"
        f"market_intelligence:\n{intelligence}\n"
        f"instruction={request.instruction}\n"
        "Return a JSON object with score in [-1,1] and a concise reason."
    )


def _request_json(url: str, api_key: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise LLMSignalError(f"LLM provider 请求失败: {type(exc).__name__}") from exc


def _extract_content(response: dict[str, Any]) -> str:
    try:
        content = response["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise LLMSignalError("LLM provider 返回格式无效") from exc
    if isinstance(content, list):
        content = "".join(str(item.get("text") or "") if isinstance(item, dict) else str(item) for item in content)
    return str(content).strip()
