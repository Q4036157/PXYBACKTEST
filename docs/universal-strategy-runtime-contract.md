# 跨平台统一策略运行契约

> 契约版本：`pxybacktest.strategy-package.v1`、`pxybacktest.event-envelope.v1`
>
> 代码基线：PXYBACKTEST `40d85b7ef1feee0eb31b8ba96beb949bb2cbe9fd`
>
> 复核日期：2026-09-02
>
> 验证方式：`python -m pytest -q tests/test_strategy_package.py`

## 目标

统一回测终端接受 MT4、MT5、TradeBlazer（TB）、天勤 TqSdk、vn.py、TradingView
Pine、聚宽和普通 Python 策略。这里的“支持”不是用一个解释器冒充所有平台，而是统一：

- 策略身份、源码/二进制/参数哈希；
- 数据订阅和 PIT/事件可得时间；
- 执行语义、订单意图、持仓、账户和结果；
- ReplayClock、EventCursor、暂停/继续/取消和可视化；
- 原生平台结果与兼容引擎的差异验收。

## 四类运行方式

| 方式 | 用途 | 示例 |
| --- | --- | --- |
| `native_oracle` | 调用平台官方/原生回测器，作为权威结果 | MT4/MT5 Strategy Tester、TB、TradingView/聚宽导出报告 |
| `native_sandbox` | 在隔离 Python 环境运行原框架策略 | 天勤 TqSdk、vn.py、普通 Python |
| `compat` | 通过兼容 API 和 PXY 数据运行原策略 | 聚宽 API shim、已移植 MQL/TB 策略 |
| `portable_ir` | 把受支持语法转换成稳定策略 IR | Pine 子集、规则表达式、AI 生成策略 |

任意源码导入后必须明确运行方式和验证等级，禁止静默降级。例如 Pine Script 没有官方
本地执行器时，只能使用 TradingView 原生导出作为 Oracle，或者声明为“受支持语法子集”的
portable IR；不能把转换结果标记成 TradingView 完全一致。

## 策略包

策略包（`StrategyPackage`）至少包含：

- 来源平台、语言、入口点；
- 源码、二进制、参数、依赖锁或 IR artifact 的 SHA256；
- runner 模式、adapter 版本和运行时身份；
- 参数 Schema；
- 数据订阅；
- 执行语义；
- 默认拒绝网络的沙箱权限；
- 验证等级和固定验收向量。

验证等级依次为 `imported`、`compiled`、`native_verified`、`parity_verified`、
`optimized`。`native_verified` 起必须绑定固定验收向量；原生报告和数据身份完整才可标记
`native_verified`。`parity_verified` 和 `optimized` 还必须为每个验收向量附带三份不可变
证据：逐笔成交（`trades`）、账户路径（`account`）和可视化回放（`visual`）。三项状态都为
`passed` 且各自包含证据 SHA256 后，契约才允许保存该等级；任何一项失败或未验证都不能
称为与原平台一致。

## 统一数据输入

统一事件信封（`EventEnvelope`）覆盖：

- Tick、Quote、逐笔成交、K 线；
- 盘口快照和增量；
- PIT 财务、公司行动；
- 新闻、舆情、宏观/行业/公司事件；
- 因子值、交易日历；
- 资金费率和借券数据。

每个事件必须具有单调序号、事件时间（`event_time`）、可得时间（`available_at`）、
入库时间（`ingested_at`）、快照 ID 和修订 ID。策略回放只能在 ReplayClock 到达
`available_at` 后看到事件，禁止使用财报期末日提前读取后来披露的数据。

## 执行语义不能强行合并

统一结果不等于统一撮合规则。策略包显式选择以下执行档案之一：MT4、MT5 对冲、MT5
净额、TB、天勤、vn.py CTA、TradingView bar、聚宽 A 股组合、portable 或 custom。

执行档案负责：

- Bid/Ask、bar 内成交假设和同 Tick 顺序；
- 净额/对冲/组合持仓；
- T+1、涨跌停、停牌、复权和公司行动；
- 手续费、滑点、库存费、资金费、保证金和强平；
- 订单生命周期、部分成交、撤单和拒单。

## 平台接入顺序

1. vn.py 与天勤：优先原生 Python sandbox，共享 PXYDATA 快照。
2. MT4/MT5：原生 Strategy Tester Oracle + 经逐笔验证的 compat adapter。
3. 聚宽：实现 `initialize/handle_data/before_trading_start`、数据查询和下单 API shim，
   同时使用聚宽报告作为 Oracle。
4. TradingView：导入 Pine、解析参数和订阅；先支持无未来函数的确定性策略子集，
   用导出交易列表验收。
5. TB：先确认具体版本、脚本语言和可自动化回测接口，再实现 native Oracle；无官方
   自动化接口时只做明确语法子集转换。
6. AI 生成：直接生成 StrategyPackage/portable IR 或目标平台源码，同时生成测试向量，
   不允许绕过沙箱、PIT 和验收门槛。

## 安全和可重复性

用户策略属于不可信代码。每个任务使用专属工作目录、CPU/内存/超时配额；网络默认拒绝，
环境变量显式白名单，数据快照只读。依赖不能在执行时临时联网安装，必须提前物化并记录
lock hash。源码、运行时、数据或账户规格任一身份变化都生成新的执行快照。

## 与 C++ 内核的关系

C++ 内核只服务 `compat` 和 `portable_ir` 的高频热点：Tick 解码、撮合、订单状态机、
账本和指标。`native_oracle` 仍由平台原生运行时执行。所有 C++ 路径必须与 Python 参考
实现及原生 Oracle 对固定向量一致，不能为了速度改变策略语义。
