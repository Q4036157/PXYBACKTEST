# PXYBACKTEST

PXYLH 的独立工作站回测执行服务。109 和 204 只负责用户鉴权与请求代理，回测计算仅在 `app-win-01` 的隔离子进程中执行。

## 当前范围

- 所有已登录 PXYLH 用户均可提交任务，任务按用户隔离。
- 支持 Lighter、OKX、Binance、BitMart、MT4/MT5 标准 VNPY 品种格式。
- 规则、A 股因子、盘口微观结构和时间序列 ML 回测均走统一任务契约；Optuna/Walk-forward 作为独立优化层。
- 内置 `linear_regression` 学习基线无需额外 ML 包；LightGBM、LSTM、带位置编码的多时间步 Transformer、集成模型、QLib 和 RD-Agent 均为可选研究依赖。
- 初始状态与后续增量事件分离，浏览器视觉刷新受限，策略引擎不跳 Tick。
- 默认全局并发 1、每用户最多排队 3 个任务。
- 每用户队列上限在 SQLite 事务内完成检查和创建，避免并发提交绕过限制。
- `trade`、终态和显式 `reliable` 事件在队列满时失败闭环；普通 UI/回放事件允许降采样，
  但会写入明确的丢弃日志，不再静默丢失。

## 开源扩展边界

回测服务的 API、任务队列、事件协议和执行进程属于本仓库；策略和行情引擎通过
`PXYBACKTEST_PXYLH_ROOT` 接入，便于工作站部署时复用现有 vn.py 运行时。当前仓库
仍需要一个可用的 PXYLH 引擎目录才能执行真实回测，不能把这一点误认为已经完成了
完全脱离 PXYLH 源码的独立发行版。

后续扩展建议沿同一任务协议增加数据与策略适配器：

- A 股多因子：独立的数据源、交易日历、复权、因子快照和组合构建器；禁止把因子未来值注入当前 bar。
- 时空融合：将时间序列特征与空间关系图作为版本化输入快照，记录数据截止时间和特征版本，保证可复现。
- 参数优化：在单次可视化回测稳定后增加独立优化队列、资源配额和结果元数据，不与单次任务共用无限制并发。

## 学习回测与工作流

`/api/v2/capabilities` 会声明 `ml_factor`、`deep_learning` 和节点工作流能力。
学习任务必须绑定 PXYDATA 的不可变 `ml_features_daily`、`factor_matrix_daily`
或 `lighter_microstructure_factors` 快照，并在 `parameters` 中指定 `feature_columns`、
`label_column` 和训练/测试窗口。例如：

```json
{
  "engine_type": "ml_factor",
  "strategy": {
    "id": "temporal_ml_rank_v1",
    "version": "builtin-v1",
    "source_hash": "<内置策略 SHA256>",
    "entrypoint": "temporal_ml_rank_v1"
  },
  "parameters": {
    "model_type": "linear_regression",
    "feature_columns": ["value_score", "quality_score", "sentiment_5d"],
    "label_column": "forward_return_5d",
    "task_type": "ranking",
    "seq_len": 1,
    "train_days": 252,
    "test_days": 63,
    "purge_days": 5,
    "embargo_days": 1,
    "top_k": 10
  }
}
```

Lighter 微观结构面板可直接使用 `ofi_normalized`、`trade_imbalance`、
`depth_imbalance_5`、`microprice_gap_bps`、`absorption_score`、
`cancel_pressure`、`oi_flow_confirmation` 和 `future_mid_return_bps`。
资金费率、持仓量等上下文列如果已经被 PXYDATA 的面板保存，也可通过
`feature_columns` 选入。`task_type` 支持 `binary`、`ranking`、`regression`；
`model_type=lstm` 使用按品种分组的真实时间窗口；`model_type=transformer_seq` 使用
带可学习位置编码的多时间步 TransformerEncoder；`model_type=ensemble` 将
LightGBM、LSTM、Transformer 的 OOS 预测按 `ensemble_weights` 加权。没有安装对应
可选依赖时，服务会在能力接口中标记不可用并拒绝任务，不会悄悄降级。

Lighter 专用 `lighter_microstructure` 引擎会从同一份 manifest 回放主动买/卖、资金费
和多档盘口事件；若只有 `lighter_funding_history`，也可执行资金费研究回放。盘口重建
遇到 nonce 断档会丢弃断档后的状态，避免坏盘口进入结果。

数据切分按事件时间进行，`available_at > decision_time` 的数据会直接失败；
结果只生成研究信号，不提交真实订单。需要实盘时，应把 OOS 信号交给
PXYLH 的预览、风控和人工确认链路。QLib 负责离线数据集/模型研究，RD-Agent
负责提出因子和实验候选，二者都不能绕过 PXYDATA 快照和 PXYBACKTEST 回测。

可选依赖安装：

```powershell
python -m pip install -e ".[ml,research]"
```

`/api/v2/workflows/validate` 可校验“数据 → 特征 → 模型 → 组合 → 风控 →
回测 → 报告/信号”的 DAG；新增 `custom_data`、`model_ensemble` 和 `llm_signal`
节点。`/api/v2/workflows/editor` 提供一个轻量 JSON 编辑器；它是后端编辑器骨架，
不是 BeeQuant 的完整商业画布。`/api/v2/signals/llm` 只接受工作站配置的
OpenAI-compatible provider，返回带 `audit_hash` 的实时信号，明确禁止历史回测和下单。
`custom_data` 只加载工作站 `PXYBACKTEST_CUSTOM_NODES_ROOT` 下、SHA256 已登记的
本地受信任 Python；它不是公网任意代码沙箱。

开源体验应通过 PXYLH 登录后的代理入口访问。不要直接把工作站 `3024`、服务令牌、
账户信息或私有数据暴露到公网。

## 本机安装

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install.ps1
```

安装脚本优先复用 `PXYBACKTEST_PYTHON` 指定的 Python，其次使用相邻 PXYLH
仓库的 `venv312`。它验证 PXYLH 回测运行时，并单独补齐参数优化所需的 `optuna`，
不会因为只缺优化依赖而重新钉死或降级主平台已有依赖。PXYLH 运行时必须先独立安装完成。

服务令牌必须放在工作站本地密钥文件中，不提交到 Git。必要环境变量：

- `PXYBACKTEST_SERVICE_TOKEN_FILE`
- `PXYBACKTEST_PXYLH_ROOT`
- `PXYBACKTEST_RUNTIME_ROOT`
- `PXYBACKTEST_PXYDATA_DATA_ROOT`
- `PXYBACKTEST_LLM_BASE_URL`、`PXYBACKTEST_LLM_API_KEY_FILE`（仅启用实时 LLM 信号时）
- `PXYBACKTEST_CUSTOM_NODES_ROOT`（默认 `D:\x1\pxy-runtime\PXYBACKTEST\custom_nodes`）

本机验证：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

WinSW、端口、Caddy 和来源限制由 PXYOPS 管理。本仓库不保存生产令牌或节点地址。
