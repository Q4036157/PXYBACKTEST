# PXYBACKTEST 统一回测第一垂直切片

> 状态：开发完成，待集成负责人提交
> 基线提交：`e3048bd82c8725a1241a94086a2cc711670af953`
> 复核日期：2026-08-25
> 验证方式：`python -m pytest -q tests/test_kernel.py tests/test_contract_v2.py tests/test_store.py tests/test_manager_event_compaction.py`，40 passed；完整 `scripts/verify.ps1` 尚未运行。

## 本次交付

- `app/models.py`：V2 执行配置补充信号时间、入场/出场成交、撮合、复权、公司行动、T+1、涨跌停、部分成交和 bps 费用字段；保留旧 `rate/slippage` 兼容字段，并对 BAR/TICK 组合和费用别名做显式校验。
- `app/snapshot_verifier.py`：校验任务引用的 snapshot ID、manifest SHA256、数据集完整集合、固定文件集合、路径穿越和文件 size/SHA256 元数据；允许不同逻辑 dataset 复用同一物理文件。
- `app/kernel.py`：提供稳定 canonical JSON/hash、严格递增的可重放事件日志、Decimal 现金/费用结算的最小 PortfolioLedger 和 fill 回放。
- `app/result_contract.py`：在现有 v2 结果上追加输入契约、manifest、事件日志和最终结果哈希，不改变旧顶层结果字段。
- `tests/test_kernel.py`：加入账本、T+1、事件 fingerprint、manifest 路径和 replay golden vectors。

## 明确边界

这不是新回测引擎，也没有宣称所有现有引擎已经统一。legacy vn.py 仍保持 `provenance_only` 和 `strictly_reproducible=false`；DAA/其他适配器仍需后续把规范化 FillEvent 和 ledger 结果接入统一结果契约。PXYDATA 仍处于质量认证闭环，不能以 `/health` 代替快照质量认证。

下一阶段应在不改变本切片契约的前提下，把一个适配器的 trade 转为 FillEvent，写入完整 replay，再处理任务提交幂等和事件 WAL。
