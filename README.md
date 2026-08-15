# PXYBACKTEST

PXYLH 的独立工作站回测执行服务。109 和 204 只负责用户鉴权与请求代理，回测计算仅在 `app-win-01` 的隔离子进程中执行。

## 当前范围

- 所有已登录 PXYLH 用户均可提交任务，任务按用户隔离。
- 支持 Lighter、OKX、Binance、BitMart、MT4/MT5 标准 VNPY 品种格式。
- 单次 MT5 式可视化回测；参数优化不在当前阶段。
- 初始状态与后续增量事件分离，浏览器视觉刷新受限，策略引擎不跳 Tick。
- 默认全局并发 1、每用户最多排队 3 个任务。

## 开源扩展边界

回测服务的 API、任务队列、事件协议和执行进程属于本仓库；策略和行情引擎通过
`PXYBACKTEST_PXYLH_ROOT` 接入，便于工作站部署时复用现有 vn.py 运行时。当前仓库
仍需要一个可用的 PXYLH 引擎目录才能执行真实回测，不能把这一点误认为已经完成了
完全脱离 PXYLH 源码的独立发行版。

后续扩展建议沿同一任务协议增加数据与策略适配器：

- A 股多因子：独立的数据源、交易日历、复权、因子快照和组合构建器；禁止把因子未来值注入当前 bar。
- 时空融合：将时间序列特征与空间关系图作为版本化输入快照，记录数据截止时间和特征版本，保证可复现。
- 参数优化：在单次可视化回测稳定后增加独立优化队列、资源配额和结果元数据，不与单次任务共用无限制并发。

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

本机验证：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

WinSW、端口、Caddy 和来源限制由 PXYOPS 管理。本仓库不保存生产令牌或节点地址。
