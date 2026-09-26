from app.models import SubmitBacktestRequestV2
from app.mt5_ohlc_options import mt5_task_options


def test_vnpy_mt5_ohlc_execution_settings_reach_worker() -> None:
    payload = {
        "schema_version": 2,
        "engine_type": "vnpy_cta",
        "strategy": {
            "id": "example", "version": "1.0.0",
            "source_hash": "12345678abcdef", "entrypoint": "ExampleStrategy",
        },
        "universe": {"symbols": ["LABUSDT_SWAP_BINANCE.GLOBAL"]},
        "period": {
            "start": "2026-04-01T00:00:00+08:00",
            "end": "2026-08-02T00:00:00+08:00",
            "interval": "1m", "timezone": "Asia/Shanghai",
        },
        "data": {"selection": {
            "datasets": ["kline_1m"],
            "decision_time": "2026-08-02T00:00:00+08:00",
            "quality_policy": "allow_unverified",
        }},
        "execution": {
            "capital": 1000, "mode": "TICK", "tick_model": "mt5_ohlc_1m",
            "tick_spread": 0.00002, "contract_size_override": 500,
            "pricetick_override": 0.00001, "min_volume_override": 0.01,
        },
        "parameters": {"回测MT5风险仓位": True},
        "random_seed": 7,
    }
    request = SubmitBacktestRequestV2.model_validate(payload).to_worker_request()
    options = mt5_task_options(request)
    assert options == {
        "tick_model": "mt5_ohlc_1m", "tick_spread": 0.00002,
        "contract_size_override": 500, "pricetick_override": 0.00001,
        "min_volume_override": 0.01,
    }
