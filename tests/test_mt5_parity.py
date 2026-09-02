from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from app.mt5_parity import (
    MT5_PARITY_CONTRACT,
    ParityTolerance,
    compare_deals,
    load_pxy_result,
    normalize_pxy_deals,
    parse_mt5_report,
)


def _write_report(path: Path) -> Path:
    path.write_text(
        """<!doctype html><html><body><table>
<tr><td>专家:</td><td><b>123骑士</b></td></tr>
<tr><td>交易品种:</td><td><b>XAUUSDm</b></td></tr>
<tr><td>总净盈利:</td><td><b>10.50</b></td><td>最大净值亏损:</td><td><b>2.00 (0.02%)</b></td></tr>
<tr><td>输入:</td><td><b>InpLotSize=0.02</b></td></tr>
<tr><th><b>订单</b></th></tr>
<tr><td>时间</td><td>订单</td></tr>
<tr><th><b>成交</b></th></tr>
<tr><td><b>时间</b></td><td>成交</td><td>交易品种</td><td>类型</td><td>趋势</td><td>交易量</td><td>价位</td><td>订单</td><td>手续费</td><td>库存费</td><td>盈利</td><td>结余</td><td>注释</td></tr>
<tr><td>2026.08.18 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td><td></td><td>0.00</td><td>0.00</td><td>100 000.00</td><td>100 000.00</td><td></td></tr>
<tr><td>2026.08.18 00:00:00</td><td>2</td><td>XAUUSDm</td><td>buy</td><td>in</td><td>0.02</td><td>4425.999</td><td>2</td><td>0.00</td><td>0.00</td><td>0.00</td><td>100 000.00</td><td>entry</td></tr>
<tr><td>2026.08.18 00:00:02</td><td>3</td><td>XAUUSDm</td><td>sell</td><td>out</td><td>0.02</td><td>4426.057</td><td>3</td><td>0.00</td><td>0.00</td><td>0.11</td><td>100 000.11</td><td></td></tr>
</table></body></html>""",
        encoding="utf-8-sig",
    )
    return path


def test_parse_mt5_report_extracts_metadata_inputs_and_execution_deals(tmp_path: Path) -> None:
    report = parse_mt5_report(_write_report(tmp_path / "report.html"))

    assert report.metadata["专家"] == "123骑士"
    assert report.metadata["交易品种"] == "XAUUSDm"
    assert report.metadata["总净盈利"] == "10.50"
    assert report.metadata["最大净值亏损"] == "2.00 (0.02%)"
    assert report.inputs == {"InpLotSize": "0.02"}
    assert len(report.deals) == 2
    assert report.deals[0].sequence == 1
    assert report.deals[0].side == "buy"
    assert report.deals[0].entry == "in"
    assert report.deals[0].price == Decimal("4425.999")
    assert report.deals[1].profit == Decimal("0.11")


def test_compare_deals_accepts_pxy_aliases_and_money_rounding(tmp_path: Path) -> None:
    report = parse_mt5_report(_write_report(tmp_path / "report.html"))
    pxy = normalize_pxy_deals(
        [
            {
                "datetime": "2026-08-18T00:00:00",
                "trade_id": "pxy-1",
                "order_id": "pxy-order-1",
                "vt_symbol": "XAUUSDm.LOCAL",
                "direction": "long",
                "offset": "open",
                "volume": 0.02,
                "price": 4425.999,
                "pnl": 0,
                "balance": 100000,
            },
            {
                "datetime": "2026-08-18T00:00:02",
                "trade_id": "pxy-2",
                "order_id": "pxy-order-2",
                "symbol": "XAUUSDm",
                "direction": "short",
                "offset": "close",
                "volume": 0.02,
                "price": 4426.057,
                "pnl": 0.114,
                "balance": 100000.114,
            },
        ]
    )

    result = compare_deals(report.deals, pxy)

    assert result.contract_version == MT5_PARITY_CONTRACT
    assert result.matched is True
    assert result.mismatch_count == 0


def test_compare_deals_reports_first_semantic_divergence(tmp_path: Path) -> None:
    report = parse_mt5_report(_write_report(tmp_path / "report.html"))
    wrong = replace(report.deals[1], side="buy", price=Decimal("4426.060"))

    result = compare_deals(
        report.deals,
        (report.deals[0], wrong),
        tolerance=ParityTolerance(price=Decimal("0.001")),
    )

    assert result.matched is False
    assert result.first_mismatch_sequence == 2
    assert [(item.field, item.delta) for item in result.mismatches] == [
        ("side", None),
        ("price", "0.003"),
    ]


def test_compare_deals_never_hides_missing_tail() -> None:
    pxy = normalize_pxy_deals(
        [
            {
                "time": "2026-08-18 00:00:00",
                "symbol": "XAUUSDm",
                "side": "buy",
                "entry": "in",
                "volume": "0.02",
                "price": "1",
            }
        ]
    )

    result = compare_deals(pxy, ())

    assert result.matched is False
    assert result.mismatches[0].field == "deal_count"
    assert result.mismatches[0].sequence == 1


def test_load_pxy_result_accepts_nested_unified_result(tmp_path: Path) -> None:
    path = tmp_path / "result.json"
    path.write_text(
        json.dumps(
            {
                "result": {
                    "deals": [
                        {
                            "time": "2026-08-18T00:00:00+08:00",
                            "symbol": "XAUUSDm",
                            "side": "buy",
                            "entry": "in",
                            "volume": "0.02",
                            "price": "4425.999",
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    deals = load_pxy_result(path)

    assert len(deals) == 1
    assert deals[0].time.tzinfo is None
    assert deals[0].time.hour == 0
