# PXYBACKTEST 开发规范

- 所有交流、注释和运行说明使用中文。
- 修改前读取 `D:/x1/x2/AGENTS.md`、本文件以及 `D:/x1/x2/PXYOPS/manifests/repositories.yaml`。
- 本仓库只负责回测任务 API、用户隔离队列、事件协议和工作站隔离执行；PXYLH 负责登录鉴权，并提供策略与行情运行时。
- 不复制 PXYLH 的用户、账户或生产交易状态。`PXYBACKTEST_PXYLH_ROOT` 是运行时依赖，不代表本仓库拥有 PXYLH 源码。
- WinSW、端口、Caddy、来源限制和节点部署归 PXYOPS；本仓库只维护脱敏的 `deploy/app.yaml`。
- 不提交令牌、数据库、日志、回测运行目录或用户结果；运行数据必须位于 `PXYBACKTEST_RUNTIME_ROOT`。
- `app/main.py`、`app/manager.py`、`app/worker_process.py`、`app/store.py`、认证与模型文件属于共享热区，修改前确认独占工作包。
- 不部署、不重启服务，也不把健康检查结果当作部署授权。

## 验证

完整验证优先运行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

仅修改独立单元时可先运行对应的 `python -m pytest -q tests/<test_file>.py`，交付前仍应执行完整验证。
