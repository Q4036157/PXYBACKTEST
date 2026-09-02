# vn.py / 天勤第一阶段适配与三维验收

> 契约版本：`pxybacktest.parity-acceptance.v1`、`pxybacktest.tqsdk-native-worker.v1`
>
> 代码基线：PXYBACKTEST `ec656d2a41d119a6f976d958bc9543a502502ea2`
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

当前源码已实现：

- 策略与结果必须位于独立任务目录；
- Windows 受限令牌（`Restricted Token`）和专用本地账户；
- Job Object 的进程树终止、内存、CPU、活动进程数和超时/取消限制；
- 子进程只继承 NUL 标准流和显式句柄白名单；
- 仅在任务执行期间授予专用账户任务目录 ACL，结束后立即撤销；
- PXYOPS 按专用账户的所有进程、TCP 443 和解析后的天勤 IPv4 建立出站补集阻断，
  同时阻断 UDP/ICMP，避免策略启动其他程序绕过 Python 程序规则；策略证明仍同时
  约束虚拟环境启动器与 `pyvenv.cfg` 指向的基础 Python，防止启动器转交进程后绕过；
- 天勤凭据只从受 ACL 保护的文件读入父进程，再以内存环境变量传给受限子进程；
- 天勤凭据只从 `PXYBACKTEST_TQSDK_USERNAME`、`PXYBACKTEST_TQSDK_PASSWORD`
  受控环境变量读取，不写入任务 JSON；
- 逐笔成交、每日账户、最终账户、订单、持仓和天勤指标物化；
- 策略实际订阅的 K 线/Tick/Quote 转换为完整 `ReplayEvent`，统一进入
  `EventCursor + ReplayClock + ReplayAudit`；
- `/api/v2/tqsdk/tasks` 接入现有用户隔离队列，支持超时、取消和结果快照；
- 原生计算期间不伪造暂停：暂停/继续/调速只作用于计算完成后的可视化回放；
- 失败结果使用 UTF-8 结构化文件，避免 Windows 控制台编码破坏中文原因。

固定向量使用 `SHFE.au2612`、2026-08-18 至 2026-08-20、1 分钟 K 线、
100 万初始资金和最小双均线策略。门禁要求：首次真实执行记录 Oracle，第二次独立执行
逐笔成交、账户、完整 K 线/ReplayAudit 全部一致，并且安全策略哈希、TqSdk 3.10.x
运行时身份和固定策略源码哈希都没有变化。只设置裸策略 ID/哈希环境变量不构成网络证明。

当前工作站尚未配置 PXYBACKTEST 天勤凭据，管理员网络策略也尚未执行，因此没有真实
Oracle 文件和门禁文件，注册表必须保持 `submit_ready=false`。这不是代码测试失败，而是
运行验收尚未发生。执行顺序：

```powershell
Set-PxyBacktestTqSdkCredentials.ps1
Set-PxyBacktestTqSdkNetworkPolicy.ps1
Run-PxyBacktestTqSdkAcceptance.ps1
Verify-PxyBacktestTqSdkSandbox.ps1
```

只有第三步生成真实向量且第二次三维复验通过，`PXYBACKTEST_TQSDK_ACCEPTANCE_FILE`
指向的门禁才有效。随后仍需按单独部署授权重启 WinSW，能力接口才会重新读取环境。

以下任一项缺失时仍保持 `submit_ready=false`：

- 专用账户、受限令牌、任务目录 ACL 或 Job Object；
- 网络白名单文件与当前虚拟环境启动器或基础 Python 的路径/SHA256 不一致；
- 当前 TqSdk 版本或固定验收策略源码与生成门禁时不一致；
- 真实天勤固定向量三维证据缺失或任一维度不一致。

单元测试中的伪 TqSdk 只证明转换和队列逻辑，不是原生 Oracle。真实门禁文件缺失时，
前端和 CLI 都不能提交任意天勤代码。

## 运行时部署边界

TqSdk 是 PXYBACKTEST 可选依赖 `tqsdk>=3.10,<3.11`。专用 Python 通过
`PXYBACKTEST_TQSDK_PYTHON` 配置；不能引用或复制 PXYFUTURES 的虚拟环境。凭据文件、
WinSW 环境变量、E 盘发布路径和服务重启由 PXYOPS 管理，本仓库不保存密钥。

2026-09-03 工作站开发环境 `D:/x1/x2/PXYBACKTEST/.venv` 已离线导入验证 TqSdk
3.10.2；这只是开发运行时已安装，不代表 E 盘发布快照或 WinSW 服务已配置。服务环境
读取到专用 Python 且完成安全门禁前，能力状态仍必须保持 `submit_ready=false`。
