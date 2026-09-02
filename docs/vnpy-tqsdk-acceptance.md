# vn.py / 天勤第一阶段适配与三维验收

> 契约版本：`pxybacktest.parity-acceptance.v1`、`pxybacktest.tqsdk-native-worker.v1`
>
> 代码基线：PXYBACKTEST `830dbff5ae34f7d94069c61b546c55af4716c3bd`
>
> 复核日期：2026-09-03
>
> 验证方式：`powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1`

## 一致性门禁

每个固定向量必须分别验证逐笔成交（`trades`）、账户路径（`account`）和可视化
回放（`visual`）。每项检查绑定策略源码、数据清单和运行时身份；任一身份变化或任一
维度失败，整体结果（`all_passed`）就是 `false`。维度证据 SHA256 可直接写入
`StrategyPackage.parity_evidence`，但不能把一个向量的通过结果外推到其他策略、参数、
品种或日期。

离线比较统一结果：

```powershell
python -m app.cli accept-result --vector <vector.json> --actual <result.json>
```

## vn.py 固定向量

内置向量 `acceptance/vectors/vnpy_cta_native_v1.json` 使用真实 vn.py 4.2.0 和
vnpy_ctastrategy 1.4.0，在 6 根确定性合成分钟 K 线上执行最小 CTA 策略。Oracle 包含：

- 两条按原始顺序保存的成交；
- 余额、净盈亏、手续费、滑点、成交额和成交数量；
- 六根完整历史 K 线、九个完整执行事件的回放哈希和终态投影。

运行固定向量：

```powershell
python -m app.cli accept-vnpy
```

它证明固定运行时对该向量没有回归，不代表所有 vn.py 策略已兼容。

## 天勤原生 worker

天勤适配器在专用 Python 子进程中运行原策略脚本，并强制把顶层 `tqsdk.TqApi`
替换为任务指定的 `TqSim + TqBacktest`。策略不能通过正常构造参数切换成实盘账户、
其他日期或 Web GUI。结果从原生 `trade_log`、订单、持仓和账户对象提取，不模拟天勤
成交逻辑。

当前已实现：

- 策略与结果必须位于独立任务目录；
- 子进程环境变量白名单、标准输入关闭、超时和无窗口运行；
- 天勤凭据只从 `PXYBACKTEST_TQSDK_USERNAME`、`PXYBACKTEST_TQSDK_PASSWORD`
  受控环境变量读取，不写入任务 JSON；
- 逐笔成交、每日账户、最终账户、订单、持仓和天勤指标物化；
- 失败结果使用 UTF-8 结构化文件，避免 Windows 控制台编码破坏中文原因。

当前没有完成，因此注册表保持 `submit_ready=false`：

- Windows 受限令牌与 Job Object；
- 网络只允许天勤必要目标；
- 与主任务队列、暂停/继续/取消协议连接；
- TqSdk 图表数据到统一 ReplayEvent 的转换；
- 天勤原生 Oracle 与 PXY 可视化的三维固定向量。

在这些项目完成前，worker 只能称为进程隔离（`process_isolation_only`），不能称为
安全 sandbox，也不能在前端开放任意用户代码提交。

## 运行时部署边界

TqSdk 是 PXYBACKTEST 可选依赖 `tqsdk>=3.10,<3.11`。专用 Python 通过
`PXYBACKTEST_TQSDK_PYTHON` 配置；不能引用或复制 PXYFUTURES 的虚拟环境。凭据文件、
WinSW 环境变量、E 盘发布路径和服务重启由 PXYOPS 管理，本仓库不保存密钥。

2026-09-03 工作站开发环境 `D:/x1/x2/PXYBACKTEST/.venv` 已离线导入验证 TqSdk
3.10.2；这只是开发运行时已安装，不代表 E 盘发布快照或 WinSW 服务已配置。服务环境
读取到专用 Python 且完成安全门禁前，能力状态仍必须保持 `submit_ready=false`。
