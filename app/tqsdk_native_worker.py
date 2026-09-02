"""天勤 TqSdk 原生回测子进程适配器。

该模块只提供受控任务目录、清理后的环境变量和超时隔离。当前尚未使用 Windows
受限令牌、Job Object 和网络目标白名单，因此结果明确标记为
``process_isolation_only``，不能宣称已经达到不可信代码安全沙箱等级。
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import runpy
import subprocess
import sys
from collections.abc import Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .kernel import stable_hash


TQSDK_WORKER_CONTRACT = "pxybacktest.tqsdk-native-worker.v1"
_AUTH_USER_ENV = "PXYBACKTEST_TQSDK_USERNAME"
_AUTH_PASSWORD_ENV = "PXYBACKTEST_TQSDK_PASSWORD"


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
        return api

    tqsdk.TqApi = controlled_api
    finished_error = getattr(exceptions, "BacktestFinished")
    execution_error: Exception | None = None
    try:
        runpy.run_path(str(request.strategy_path), run_name="__main__")
    except finished_error:
        pass
    except Exception as exc:  # noqa: BLE001 - 错误需要写入任务结果
        execution_error = exc
    finally:
        tqsdk.TqApi = original_api

    api = created[0] if created else None
    before_close = _extract_simulation(sim, api)
    if api is not None:
        try:
            api.close()
        except Exception:  # noqa: BLE001
            pass
    after_close = _extract_simulation(sim, None)
    extracted = after_close if after_close["account_curve"] else before_close
    if execution_error is not None:
        raise TqSdkWorkerError(
            f"天勤策略执行失败: {type(execution_error).__name__}: {execution_error}"
        ) from execution_error
    if api is None:
        raise TqSdkWorkerError("策略没有创建 TqApi，无法执行原生回测")

    return {
        "contract_version": TQSDK_WORKER_CONTRACT,
        "ok": True,
        "task_id": request.task_id,
        "platform": "tqsdk",
        "runtime_identity": f"tqsdk-{getattr(tqsdk, '__version__', 'unknown')}",
        "sandbox": {
            "strength": "process_isolation_only",
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
        "visual": {
            "available": False,
            "reason": "原生 TqSdk 图表事件尚未转换为统一 ReplayEvent",
        },
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
) -> dict[str, Any]:
    """使用专用 Python 启动子进程，并限制继承环境与输出大小。"""

    python_path = python_executable.resolve()
    if not python_path.is_file():
        raise TqSdkWorkerError(f"天勤 Python 不存在: {python_path}")
    request_path = request.task_root / "tqsdk-worker-request.json"
    _write_result(request_path, request.model_dump(mode="json"))

    inherited_names = (
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "TEMP",
        "TMP",
        "PATH",
        _AUTH_USER_ENV,
        _AUTH_PASSWORD_ENV,
    )
    child_env = {
        name: os.environ[name]
        for name in inherited_names
        if os.environ.get(name)
    }
    child_env["PYTHONPATH"] = os.pathsep.join(
        str(path.resolve()) for path in (project_root, *runtime_pythonpaths)
    )
    command = [
        str(python_path),
        "-m",
        "app.tqsdk_native_worker",
        "--request",
        str(request_path),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=str(request.task_root),
            env=child_env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(1, int(timeout_seconds)),
            creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TqSdkWorkerError("天勤策略子进程执行超时") from exc
    if completed.returncode != 0:
        try:
            failed_payload = json.loads(
                request.result_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            failed_payload = {}
        structured_error = (
            str(failed_payload.get("error") or "").strip()
            if isinstance(failed_payload, dict)
            else ""
        )
        if structured_error:
            raise TqSdkWorkerError(structured_error)
        message = " ".join((completed.stderr or completed.stdout).split())[-2000:]
        raise TqSdkWorkerError(
            f"天勤策略子进程退出码 {completed.returncode}: {message}"
        )
    try:
        payload = json.loads(request.result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TqSdkWorkerError("天勤策略子进程未生成有效结果") from exc
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise TqSdkWorkerError(str(payload.get("error") or "天勤 worker 返回失败"))
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
    except Exception as exc:  # noqa: BLE001
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
