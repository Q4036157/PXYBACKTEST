# OSkhQuant 能力吸收记录

本次审阅覆盖 `OSkhQuant-main` 的全部 Python、策略、配置、文档和依赖文件。
由于其许可证为 CC BY-NC 4.0，本仓库没有原样复制受限源码；移植内容均为基于公开行为的独立实现，
并保留 PXYBACKTEST 的 snapshot、result v2 和 replay 审计边界。

## 已移植并接入结果契约

- `app/metrics.py`：总收益、年化收益、波动率、Sharpe、Sortino、Calmar、最大回撤、胜率、
  profit factor、连续盈亏、基准 beta/alpha 等纯函数指标。
- `app/indicators.py`：MA、EMA、RSI、MACD、BOLL、ATR、TR、CROSS；等长输出，预热区为 NaN。
- `app/reporting.py`：结果窗口所需的 KPI、净值/回撤/基准曲线、月度收益、滚动 Sharpe、收益分布，
  输出纯 JSON 投影，不引入 PyQt 或 matplotlib。
- `app/result_contract.py`：legacy vn.py 和 A 股结果现在会补齐标准指标、equity/drawdown/benchmark 曲线，
  并附带 `report` 投影；引擎已有指标优先保留，不覆盖其原始口径。
- `app/costs.py`：最低佣金、卖出印花税、沪市过户费、固定费用、比例/跳数滑点的独立成本计算器。
- `app/legacy_config.py` 与 `scripts/convert_kh_config.py`：将 `.kh` 的股票池、日期、资金、费用、
  滑点、复权和触发器配置转换为 PXY 任务公共部分；转换结果仍必须补齐可信 strategy identity 和 PXYDATA snapshot。

## 有意未移植

- PyQt 窗口、QSettings、Windows 自更新和桌面线程界面；PXYBACKTEST 使用 API/worker/JSON。
- MiniQMT/xtquant 数据下载、A 股目录浏览和硬编码 SH/SZ 文件格式；数据必须来自 PXYDATA snapshot。
- `KhRiskManager` 的恒真检查、立即全部成交、假设成交、随机示例行情和固定示例收益。
- 只适用于 A 股的交易日/午休/整百/T+1 代码；PXY 回测按资产类别和事件时间处理。

## 逐文件审阅清单

已审阅：`GUIkhQuant.py`、`khFrame.py`、`khTrade.py`、`khRisk.py`、`khConfig.py`、
`khQuantImport.py`、`GUI.py`、`GUIDataViewer.py`、`GUIplotLoadData.py`、`GUIScheduler.py`、
`miniQMT_data_parser.py`、`miniQMT_data_viewer.py`、`backtest_result_window.py`、`khQTTools.py`、
`SettingsDialog.py`、`update_manager.py`、`MyTT.py`、`version.py`、全部 `strategies/*.py` 与 `*.kh`、
`README.md`、`项目文件说明.md`、`requirements.txt`。

## 验证

```powershell
.venv\Scripts\python.exe -m pytest -q
```

当前结果：126 passed，`compileall` 通过，`scripts/verify.ps1` 通过。转换器、成本模型、指标和报告模块均有独立测试；没有把 PyQt/xtquant 加入 PXYBACKTEST worker 依赖。
