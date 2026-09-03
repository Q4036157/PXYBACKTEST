"""天勤 TqSdk 原生回测子进程适配器。

父进程使用 Windows 受限令牌、专用本地账户、Job Object、任务目录 ACL 和
PXYOPS 网络白名单启动本模块。任一安全证明缺失时结果保持
``submit_ready=false``，任务提交接口不会开放。
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import runpy
import sys
import traceback
from collections.abc import Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .kernel import stable_hash
from .tqsdk_replay import build_tqsdk_replay


TQSDK_WORKER_CONTRACT = "pxybacktest.tqsdk-native-worker.v1"
_AUTH_USER_ENV = "PXYBACKTEST_TQSDK_USERNAME"
_AUTH_PASSWORD_ENV = "PXYBACKTEST_TQSDK_PASSWORD"
_AUTH_USER_FILE_ENV = "PXYBACKTEST_TQSDK_USERNAME_FILE"
_AUTH_PASSWORD_FILE_ENV = "PXYBACKTEST_TQSDK_PASSWORD_FILE"
_SANDBOX_USER_ENV = "PXYBACKTEST_TQSDK_SANDBOX_USER"
_SANDBOX_PASSWORD_FILE_ENV = "PXYBACKTEST_TQSDK_SANDBOX_PASSWORD_FILE"
_BOOTSTRAP_ERROR_ENV = "PXYBACKTEST_TQSDK_BOOTSTRAP_ERROR"
_IMPORT_TRACE_ENV = "PXYBACKTEST_TQSDK_IMPORT_TRACE"
_TQ_ENDPOINTS = {
    "TQ_AUTH_URL": "https://auth.shinnytech.com",
    "TQ_INS_URL": "https://openmd.shinnytech.com/t/md/symbols/latest.json",
    "TQ_MD_URL": "wss://backtest.shinnytech.com/t/md/front/mobile",
    "TQ_CHINESE_HOLIDAY_URL": "https://files.shinnytech.com/shinny_chinese_holiday.json",
    "TQ_CONT_TABLE_URL": "https://files.shinnytech.com/continuous_table.json",
}


def _secret_from_environment(value_name: str, file_name: str) -> str:
    direct = os.getenv(value_name, "").strip()
    if direct:
        return direct
    path_raw = os.getenv(file_name, "").strip()
    if not path_raw:
        return ""
    try:
        return Path(path_raw).read_text(encoding="utf-8").strip()
    except OSError:
        return ""


class TqSdkWorkerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.tqsdk-native-worker.v1"] = (
        TQSDK_WORKER_CONTRACT
    )
    task_id: str = Field(min_length=1, max_length=200)
    task_root: Path
    strategy_path: Path
    result_path: Path
    start_date: date
    end_date: date
    initial_balance: float = Field(gt=0)
    require_auth: bool = True
    memory_mb: int = Field(default=4096, ge=128, le=262144)
    cpu_cores: int = Field(default=1, ge=1, le=128)

    @model_validator(mode="after")
    def validate_paths_and_period(self) -> "TqSdkWorkerRequest":
        root = self.task_root.resolve()
        strategy = self.strategy_path.resolve()
        result = self.result_path.resolve()
        if not strategy.is_relative_to(root) or not result.is_relative_to(root):
            raise ValueError("策略和结果文件必须位于任务目录内")
        if not strategy.is_file() or strategy.suffix.lower() != ".py":
            raise ValueError("天勤策略必须是任务目录内的 Python 文件")
        if self.end_date < self.start_date:
            raise ValueError("end_date 不能早于 start_date")
        self.task_root = root
        self.strategy_path = strategy
        self.result_path = result
        return self


class TqSdkWorkerError(RuntimeError):
    pass


def _isolated_python_path(python_path: Path, system_root: Path) -> str:
    """只保留天勤子进程需要的 Python/Windows DLL 搜索路径。"""

    venv_root = python_path.parent.parent
    base_root = python_path.parent
    config_path = venv_root / "pyvenv.cfg"
    if config_path.is_file():
        try:
            for line in config_path.read_text(encoding="utf-8").splitlines():
                name, separator, value = line.partition("=")
                if separator and name.strip().lower() == "home" and value.strip():
                    base_root = Path(value.strip()).resolve()
                    break
        except OSError:
            pass
    candidates = (
        python_path.parent,
        base_root,
        base_root / "DLLs",
        system_root / "System32",
        system_root,
    )
    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = os.path.normcase(str(candidate.resolve()))
        if normalized not in seen and candidate.is_dir():
            seen.add(normalized)
            unique.append(str(candidate.resolve()))
    return os.pathsep.join(unique)


def _safe_child_error(value: Any, *secrets: str) -> str:
    message = str(value or "")
    for secret in secrets:
        if secret:
            message = message.replace(secret, "<redacted>")
    return " ".join(message.split())[-2000:]


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items() if not str(key).startswith("_")}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    try:
        return _plain(dict(value))
    except (TypeError, ValueError):
        return str(value)


def _normalize_trade(item: Mapping[str, Any], *, trading_day: str) -> dict[str, Any]:
    exchange = str(item.get("exchange_id") or "")
    instrument = str(item.get("instrument_id") or "")
    symbol = f"{exchange}.{instrument}".strip(".")
    return {
        "trading_day": trading_day,
        "trade_time_ns": int(item.get("trade_date_time") or 0),
        "trade_id": str(item.get("trade_id") or ""),
        "order_id": str(item.get("order_id") or ""),
        "symbol": symbol,
        "direction": str(item.get("direction") or "").lower(),
        "offset": str(item.get("offset") or "").lower(),
        "volume": int(item.get("volume") or 0),
        "price": float(item.get("price") or 0.0),
        "commission": float(item.get("commission") or 0.0),
    }


def _extract_simulation(sim: Any, api: Any | None) -> dict[str, Any]:
    trade_log = _plain(getattr(sim, "trade_log", {}) or {})
    deals: list[dict[str, Any]] = []
    account_curve: list[dict[str, Any]] = []
    if isinstance(trade_log, Mapping):
        for trading_day in sorted(str(key) for key in trade_log):
            daily = trade_log.get(trading_day) or trade_log.get(int(trading_day))
            if not isinstance(daily, Mapping):
                continue
            for trade in daily.get("trades") or []:
                if isinstance(trade, Mapping):
                    deals.append(_normalize_trade(trade, trading_day=trading_day))
            account = daily.get("account")
            if isinstance(account, Mapping):
                account_curve.append({"trading_day": trading_day, **_plain(account)})

    orders: Any = {}
    positions: Any = {}
    final_account: Any = account_curve[-1] if account_curve else {}
    if api is not None:
        for name, fallback in (
            ("get_order", orders),
            ("get_position", positions),
            ("get_account", final_account),
        ):
            try:
                value = _plain(getattr(api, name)())
            except Exception:  # noqa: BLE001 - 已关闭的原生 API 允许回退日志
                value = fallback
            if name == "get_order":
                orders = value
            elif name == "get_position":
                positions = value
            else:
                final_account = value
    return {
        "deals": deals,
        "orders": orders,
        "positions": positions,
        "account_curve": account_curve,
        "final_account": final_account,
        "native_metrics": _plain(getattr(sim, "tqsdk_stat", {}) or {}),
        "native_trade_log_sha256": stable_hash(trade_log),
    }


class _MarketCapture:
    """记录策略真实订阅到的 K 线、Tick 和 Quote，不参与撮合。"""

    def __init__(self, api: Any):
        self.api = api
        self._serials: list[tuple[str, str, Any]] = []
        self._quotes: list[tuple[str, Any]] = []
        self._seen: set[tuple[str, str, str]] = set()
        self.events: list[dict[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.api, name)

    def get_kline_serial(self, symbol: str, *args: Any, **kwargs: Any) -> Any:
        frame = self.api.get_kline_serial(symbol, *args, **kwargs)
        self._serials.append(("market_bar", str(symbol), frame))
        self.capture()
        return frame

    def get_tick_serial(self, symbol: str, *args: Any, **kwargs: Any) -> Any:
        frame = self.api.get_tick_serial(symbol, *args, **kwargs)
        self._serials.append(("market_tick", str(symbol), frame))
        self.capture()
        return frame

    def get_quote(self, symbol: str, *args: Any, **kwargs: Any) -> Any:
        quote = self.api.get_quote(symbol, *args, **kwargs)
        self._quotes.append((str(symbol), quote))
        self.capture()
        return quote

    def wait_update(self, *args: Any, **kwargs: Any) -> Any:
        updated = self.api.wait_update(*args, **kwargs)
        self.capture()
        return updated

    def close(self) -> Any:
        self.capture()
        return self.api.close()

    @staticmethod
    def _records(frame: Any) -> list[dict[str, Any]]:
        try:
            records = frame.to_dict("records")
        except (AttributeError, TypeError, ValueError):
            try:
                records = [vars(row) for row in frame.itertuples(index=False)]
            except (AttributeError, TypeError, ValueError):
                return []
        return [dict(item) for item in records if isinstance(item, Mapping)]

    def capture(self) -> None:
        for event_type, symbol, frame in self._serials:
            for raw in self._records(frame):
                payload = _plain(raw)
                if not isinstance(payload, dict):
                    continue
                timestamp = payload.get("datetime") or payload.get("datetime_ns")
                if not timestamp or not math.isfinite(float(timestamp)):
                    continue
                key = (event_type, symbol, str(int(float(timestamp))))
                if key in self._seen:
                    continue
                self._seen.add(key)
                self.events.append(
                    {
                        "event_type": event_type,
                        "symbol": symbol,
                        "datetime_ns": int(float(timestamp)),
                        **{
                            name: value
                            for name, value in payload.items()
                            if name not in {"datetime", "datetime_ns"}
                        },
                    }
                )
        for symbol, quote in self._quotes:
            payload = _plain(quote)
            if not isinstance(payload, dict):
                continue
            timestamp = payload.get("datetime") or payload.get("datetime_ns")
            if not timestamp:
                continue
            key = ("market_tick", symbol, str(timestamp))
            if key in self._seen:
                continue
            self._seen.add(key)
            self.events.append(
                {
                    "event_type": "market_tick",
                    "symbol": symbol,
                    "datetime": str(timestamp),
                    **{
                        name: value
                        for name, value in payload.items()
                        if name not in {"datetime", "datetime_ns"}
                    },
                }
            )


def run_tqsdk_strategy(request: TqSdkWorkerRequest) -> dict[str, Any]:
    """在当前子进程中以 TqBacktest/TqSim 强制覆盖策略的 TqApi。"""

    try:
        tqsdk = importlib.import_module("tqsdk")
        exceptions = importlib.import_module("tqsdk.exceptions")
    except ModuleNotFoundError as exc:
        raise TqSdkWorkerError("专用 Python 未安装 tqsdk") from exc

    username = os.getenv(_AUTH_USER_ENV, "").strip()
    password = os.getenv(_AUTH_PASSWORD_ENV, "").strip()
    if request.require_auth and (not username or not password):
        raise TqSdkWorkerError("天勤回测凭据未通过受控环境变量配置")

    original_api = tqsdk.TqApi
    sim = tqsdk.TqSim(init_balance=request.initial_balance)
    backtest = tqsdk.TqBacktest(request.start_date, request.end_date)
    auth = tqsdk.TqAuth(username, password) if username and password else None
    created: list[Any] = []
    captures: list[_MarketCapture] = []

    def controlled_api(*args: Any, **kwargs: Any) -> Any:
        if created:
            raise TqSdkWorkerError("单个策略任务只允许创建一个 TqApi")
        # 策略不得把回测任务切换成实盘账户、其他日期或 Web GUI。
        kwargs.pop("account", None)
        kwargs.pop("backtest", None)
        kwargs.pop("auth", None)
        kwargs["web_gui"] = False
        kwargs["disable_print"] = True
        if auth is not None:
            kwargs["auth"] = auth
        # 原策略传入的任何位置账户参数都不得进入回测子进程。
        del args
        api = original_api(sim, backtest=backtest, **kwargs)
        created.append(api)
        captured = _MarketCapture(api)
        captures.append(captured)
        return captured

    tqsdk.TqApi = controlled_api
    finished_error = getattr(exceptions, "BacktestFinished")
    execution_error: Exception | None = None
    try:
        runpy.run_path(str(request.strategy_path), run_name="__main__")
    except finished_error:
        pass
    except BaseException as exc:  # noqa: BLE001 - SystemExit 也必须结构化
        execution_error = exc
    finally:
        tqsdk.TqApi = original_api

    api = created[0] if created else None
    capture = captures[0] if captures else None
    if capture is not None:
        capture.capture()
    before_close = _extract_simulation(sim, api)
    if api is not None:
        try:
            api.close()
        except Exception:  # noqa: BLE001
            pass
    after_close = _extract_simulation(sim, None)
    extracted = after_close if after_close["account_curve"] else before_close
    if extracted is after_close:
        for name in ("orders", "positions", "final_account"):
            if not extracted.get(name) and before_close.get(name):
                extracted[name] = before_close[name]
    if execution_error is not None:
        raise TqSdkWorkerError(
            f"天勤策略执行失败: {type(execution_error).__name__}: {execution_error}"
        ) from execution_error
    if api is None:
        raise TqSdkWorkerError("策略没有创建 TqApi，无法执行原生回测")

    fallback_time = datetime.combine(
        request.end_date, datetime.max.time(), tzinfo=timezone.utc
    ).isoformat()
    replay = build_tqsdk_replay(
        run_id=request.task_id,
        market_events=capture.events if capture is not None else [],
        deals=extracted["deals"],
        orders=extracted["orders"],
        positions=extracted["positions"],
        account_curve=extracted["account_curve"],
        final_account=extracted["final_account"],
        fallback_time=fallback_time,
    )
    return {
        "contract_version": TQSDK_WORKER_CONTRACT,
        "ok": True,
        "task_id": request.task_id,
        "platform": "tqsdk",
        "runtime_identity": f"tqsdk-{getattr(tqsdk, '__version__', 'unknown')}",
        "sandbox": {
            "strength": "child_execution",
            "restricted_token": False,
            "job_object": False,
            "network_allowlist_enforced": False,
            "submit_ready": False,
        },
        "period": {
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
        },
        **extracted,
        **replay,
    }


def _write_result(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def launch_tqsdk_worker(
    request: TqSdkWorkerRequest,
    *,
    python_executable: Path,
    project_root: Path,
    timeout_seconds: int,
    runtime_pythonpaths: Sequence[Path] = (),
    memory_mb: int | None = None,
    cpu_cores: int | None = None,
    cancel_check: Callable[[], bool] | None = None,
    identity_mode: Literal["configured", "current_process"] = "configured",
) -> dict[str, Any]:
    """使用专用 Python 启动子进程，并限制继承环境与输出大小。

    ``current_process`` 只允许内置可信固定向量使用；其安全状态始终不能
    满足提交门禁。用户策略必须继续使用 ``configured``。
    """

    # 仅父进程需要 ctypes/Win32 沙箱 API。延迟导入可避免已经受限的策略
    # 子进程再次加载 _ctypes.pyd；安全令牌和 Job Object 在此调用之前已建立。
    from .windows_sandbox import (
        SandboxCancelledError,
        SandboxIdentity,
        SandboxLimits,
        SandboxTimeoutError,
        launch_sandboxed_process,
    )

    python_path = python_executable.resolve()
    if not python_path.is_file():
        raise TqSdkWorkerError(f"天勤 Python 不存在: {python_path}")
    request_path = request.task_root / "tqsdk-worker-request.json"
    bootstrap_error_path = request.task_root / "tqsdk-worker-bootstrap-error.log"
    import_trace_path = request.task_root / "tqsdk-worker-import-trace.log"
    _write_result(request_path, request.model_dump(mode="json"))

    inherited_names = (
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "TEMP",
        "TMP",
        _AUTH_USER_ENV,
        _AUTH_PASSWORD_ENV,
    )
    child_env = {
        name: os.environ[name]
        for name in inherited_names
        if os.environ.get(name)
    }
    username = _secret_from_environment(_AUTH_USER_ENV, _AUTH_USER_FILE_ENV)
    password = _secret_from_environment(_AUTH_PASSWORD_ENV, _AUTH_PASSWORD_FILE_ENV)
    if username:
        child_env[_AUTH_USER_ENV] = username
    if password:
        child_env[_AUTH_PASSWORD_ENV] = password
    child_env.update(_TQ_ENDPOINTS)
    system_root = Path(
        child_env.get("SYSTEMROOT") or child_env.get("WINDIR") or r"C:\Windows"
    )
    child_env["PATH"] = _isolated_python_path(python_path, system_root)
    child_env["PYTHONUTF8"] = "1"
    child_env["PYTHONIOENCODING"] = "utf-8"
    child_env["PYTHONNOUSERSITE"] = "1"
    child_env[_BOOTSTRAP_ERROR_ENV] = str(bootstrap_error_path)
    child_env[_IMPORT_TRACE_ENV] = str(import_trace_path)
    # TqSdk 在导入阶段会调用 Path.home() 初始化 .tqsdk/otg_logs。
    # 受限环境不能继承真实用户 Profile；为每个任务提供独立 Profile，
    # 既保证原生 SDK 可导入，也把配置、缓存和临时文件限制在任务目录内。
    sandbox_profile = request.task_root / ".sandbox-profile"
    sandbox_temp = sandbox_profile / "Temp"
    sandbox_appdata = sandbox_profile / "AppData" / "Roaming"
    sandbox_local_appdata = sandbox_profile / "AppData" / "Local"
    for directory in (
        sandbox_profile,
        sandbox_temp,
        sandbox_appdata,
        sandbox_local_appdata,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    child_env.update(
        {
            "HOME": str(sandbox_profile),
            "USERPROFILE": str(sandbox_profile),
            "APPDATA": str(sandbox_appdata),
            "LOCALAPPDATA": str(sandbox_local_appdata),
            "TEMP": str(sandbox_temp),
            "TMP": str(sandbox_temp),
        }
    )
    if identity_mode not in {"configured", "current_process"}:
        raise ValueError(f"未知天勤执行身份模式: {identity_mode}")
    sandbox_user = (
        os.getenv(_SANDBOX_USER_ENV, "").strip()
        if identity_mode == "configured"
        else ""
    )
    sandbox_password = (
        _secret_from_environment(
            "PXYBACKTEST_TQSDK_SANDBOX_PASSWORD", _SANDBOX_PASSWORD_FILE_ENV
        )
        if identity_mode == "configured"
        else ""
    )
    sandbox_identity = (
        SandboxIdentity(username=sandbox_user, password=sandbox_password)
        if sandbox_user and sandbox_password
        else None
    )
    child_env["PYTHONPATH"] = os.pathsep.join(
        str(path.resolve()) for path in (project_root, *runtime_pythonpaths)
    )
    command = [
        str(python_path),
        "-X",
        "utf8",
        "-m",
        "app.tqsdk_worker_bootstrap",
        "--request",
        str(request_path),
    ]
    try:
        completed = launch_sandboxed_process(
            command,
            cwd=request.task_root,
            environment=child_env,
            limits=SandboxLimits(
                timeout_seconds=max(1, int(timeout_seconds)),
                memory_mb=memory_mb or request.memory_mb,
                cpu_cores=cpu_cores or request.cpu_cores,
            ),
            cancel_check=cancel_check,
            identity=sandbox_identity,
        )
    except SandboxCancelledError as exc:
        raise TqSdkWorkerError("天勤策略任务已取消") from exc
    except SandboxTimeoutError as exc:
        raise TqSdkWorkerError("天勤策略子进程执行超时") from exc
    if completed.exit_code != 0:
        try:
            failed_payload = json.loads(
                request.result_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            failed_payload = {}
        structured_error = _safe_child_error(
            str(failed_payload.get("error") or "").strip()
            if isinstance(failed_payload, dict)
            else "",
            username,
            password,
        )
        if structured_error:
            raise TqSdkWorkerError(structured_error)
        try:
            diagnostic = bootstrap_error_path.read_text(encoding="utf-8")
        except OSError:
            diagnostic = ""
        diagnostic = _safe_child_error(diagnostic, username, password)
        if diagnostic:
            raise TqSdkWorkerError(
                f"天勤策略子进程退出码 {completed.exit_code}: {diagnostic}"
            )
        try:
            imported = [
                line.strip()
                for line in import_trace_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except OSError:
            imported = []
        import_tail = " -> ".join(imported[-12:])
        import_suffix = f"，崩溃前导入: {import_tail}" if import_tail else ""
        raise TqSdkWorkerError(
            f"天勤策略子进程退出码 {completed.exit_code}，"
            f"且没有结构化错误{import_suffix}"
        )
    try:
        payload = json.loads(request.result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TqSdkWorkerError("天勤策略子进程未生成有效结果") from exc
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise TqSdkWorkerError(str(payload.get("error") or "天勤 worker 返回失败"))
    payload["sandbox"] = completed.security_state()
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行天勤 TqSdk 原生回测子进程")
    parser.add_argument("--request", required=True)
    args = parser.parse_args(argv)
    try:
        request = TqSdkWorkerRequest.model_validate_json(
            Path(args.request).read_text(encoding="utf-8")
        )
        result = run_tqsdk_strategy(request)
        _write_result(request.result_path, result)
        return 0
    except BaseException as exc:  # noqa: BLE001 - SystemExit 也必须落盘
        # 先保留第一现场。结果 JSON 若因 ACL、路径或磁盘问题写入失败，
        # 外层 bootstrap 依然能读到真实异常，而不是只看到 SystemExit: 1。
        diagnostic_raw = os.getenv(_BOOTSTRAP_ERROR_ENV, "").strip()
        if diagnostic_raw:
            try:
                Path(diagnostic_raw).write_text(
                    traceback.format_exc(), encoding="utf-8"
                )
            except OSError:
                pass
        error = {
            "contract_version": TQSDK_WORKER_CONTRACT,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            raw = json.loads(Path(args.request).read_text(encoding="utf-8"))
            raw_result_path = str(raw.get("result_path") or "").strip()
            if raw_result_path:
                _write_result(Path(raw_result_path), error)
        except Exception:  # noqa: BLE001
            pass
        print(json.dumps(error, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "TQSDK_WORKER_CONTRACT",
    "TqSdkWorkerError",
    "TqSdkWorkerRequest",
    "launch_tqsdk_worker",
    "run_tqsdk_strategy",
]
