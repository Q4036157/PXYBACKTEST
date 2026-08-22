from __future__ import annotations

import hashlib
import asyncio

from app.custom_nodes import CustomDataNodeSpec, run_custom_data_node, validate_custom_data_node
from app.llm_signal import LLMRealtimeSignalRequest, generate_realtime_signal


def test_custom_node_is_hash_bound_and_context_is_restricted(tmp_path) -> None:
    module = tmp_path / "node.py"
    module.write_text("def main(ctx, datas):\n    return {'mode': ctx['mode'], 'rows': len(datas), 'exchange': ctx.get('exchange')}\n", encoding="utf-8")
    digest = hashlib.sha256(module.read_bytes()).hexdigest()
    spec = CustomDataNodeSpec(module="node.py", source_hash=digest)
    assert validate_custom_data_node(spec, root=tmp_path)["valid"] is True
    result = run_custom_data_node(spec, root=tmp_path, datas=[1, 2], context={"exchange": "hidden"})
    assert result == {"mode": "research", "rows": 2, "exchange": None}


def test_llm_signal_is_normalized_and_audited(monkeypatch) -> None:
    def fake_request(url, api_key, payload, timeout):
        assert url.endswith("/chat/completions")
        return {"choices": [{"message": {"content": '{"score": 0.65, "reason": "flow"}'}}]}

    monkeypatch.setattr("app.llm_signal._request_json", fake_request)
    result = asyncio.run(generate_realtime_signal(
        LLMRealtimeSignalRequest(symbol="litusdt_swap_lighter", instruction="判断主动买卖"),
        base_url="http://llm.local/v1",
        api_key="secret",
    ))
    assert result["symbol"] == "LITUSDT_SWAP_LIGHTER"
    assert result["score"] == 0.65
    assert result["historical_backtest"] is False
    assert len(result["audit_hash"]) == 64
