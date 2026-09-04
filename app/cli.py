"""PXYBACKTEST 统一命令行客户端。

CLI 只调用回测服务公开 API，不直接读 SQLite、Parquet 或运行目录，保证与
前端使用同一份任务/数据快照契约。所有命令输出 JSON，便于脚本自动编排。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, TextIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "http://127.0.0.1:3024"
DEFAULT_TOKEN_FILE = Path(r"C:\ProgramData\PXY\secrets\pxy-backtest-service-token")
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


class CliError(RuntimeError):
    """可向用户展示的 CLI 错误。"""


class BacktestApiClient:
    def __init__(self, base_url: str, token: str, user_id: str, source_node: str, timeout: float):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.user_id = user_id
        self.source_node = source_node
        self.timeout = timeout

    def request(self, method: str, path: str, payload: Any | None = None, *, auth: bool = True) -> Any:
        headers = {"Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if auth:
            headers.update(
                {
                    "X-PXY-Service-Token": self.token,
                    "X-PXY-User-Id": self.user_id,
                    "X-PXY-Source-Node": self.source_node,
                }
            )
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        request = Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            try:
                detail = str(json.loads(detail).get("detail") or detail)
            except json.JSONDecodeError:
                pass
            raise CliError(f"HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise CliError(f"无法连接回测服务 {self.base_url}: {exc.reason}") from exc
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise CliError("回测服务返回了非 JSON 响应") from exc


def _load_token(args: argparse.Namespace) -> str:
    token = str(args.token or os.getenv("PXYBACKTEST_SERVICE_TOKEN", "")).strip()
    token_file = Path(args.token_file or os.getenv("PXYBACKTEST_SERVICE_TOKEN_FILE", str(DEFAULT_TOKEN_FILE)))
    if not token and token_file.is_file():
        token = token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise CliError("未配置服务令牌；请设置 --token 或 PXYBACKTEST_SERVICE_TOKEN_FILE")
    return token


def _read_json(path: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CliError(f"请求文件不是有效 JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CliError("请求 JSON 顶层必须是对象")
    return value


def _write_json(value: Any, output: TextIO) -> None:
    output.write(json.dumps(value, ensure_ascii=False, indent=2))
    output.write("\n")


def _client(args: argparse.Namespace) -> BacktestApiClient:
    return BacktestApiClient(
        base_url=args.base_url or os.getenv("PXYBACKTEST_BASE_URL", DEFAULT_BASE_URL),
        token=_load_token(args),
        user_id=(args.user_id or os.getenv("PXYBACKTEST_USER_ID", "cli-user")).strip(),
        source_node=(args.source_node or os.getenv("PXYBACKTEST_SOURCE_NODE", "cli")).strip(),
        timeout=float(args.timeout),
    )


def _submit(client: BacktestApiClient, request_payload: dict[str, Any]) -> dict[str, Any]:
    if (
        request_payload.get("contract_version")
        == "pxybacktest.tqsdk-task-submission.v1"
    ):
        path = "/api/v2/tqsdk/tasks"
    else:
        path = "/api/v2/tasks" if request_payload.get("schema_version") == 2 else "/api/v1/tasks"
    result = client.request("POST", path, request_payload)
    if not isinstance(result, dict) or not result.get("task_id"):
        raise CliError("提交成功响应缺少 task_id")
    return result


def _wait(client: BacktestApiClient, task_id: str, poll_seconds: float, timeout_seconds: float, output: TextIO) -> dict[str, Any]:
    started = time.monotonic()
    last = None
    while True:
        task = client.request("GET", f"/api/v1/tasks/{task_id}")
        status = str(task.get("status") or "unknown") if isinstance(task, dict) else "unknown"
        progress = task.get("progress") if isinstance(task, dict) else None
        marker = (status, progress)
        if marker != last:
            print(f"[回测] {task_id} 状态={status} 进度={progress}", file=output)
            last = marker
        if status in TERMINAL_STATUSES:
            return task
        if time.monotonic() - started >= timeout_seconds:
            raise CliError(f"等待任务超时: {task_id}")
        time.sleep(poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PXYBACKTEST 自动回测 CLI")
    parser.add_argument("--base-url", default=None, help="回测服务地址，默认 http://127.0.0.1:3024")
    parser.add_argument("--token", default=None, help="服务令牌（不建议写入命令历史）")
    parser.add_argument("--token-file", default=None, help="服务令牌文件")
    parser.add_argument("--user-id", default=None, help="用户标识，默认 cli-user")
    parser.add_argument("--source-node", default=None, help="来源节点，默认 cli")
    parser.add_argument("--timeout", type=float, default=30.0, help="单次 HTTP 超时秒数")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="读取服务健康状态")
    sub.add_parser("capabilities", help="读取引擎能力目录")

    accept_result = sub.add_parser(
        "accept-result", help="离线执行成交、账户、可视化三维一致性验收"
    )
    accept_result.add_argument("--vector", required=True, help="固定验收向量 JSON")
    accept_result.add_argument("--actual", required=True, help="待验收统一结果 JSON")
    accept_vnpy = sub.add_parser(
        "accept-vnpy", help="运行内置 vn.py 原生固定向量并执行三维验收"
    )
    accept_vnpy.add_argument(
        "--vector",
        default=str(
            Path(__file__).parents[1]
            / "acceptance"
            / "vectors"
            / "vnpy_cta_native_v1.json"
        ),
    )
    evidence_vnpy = sub.add_parser(
        "evidence-vnpy", help="运行 GOLD-001 并生成完整、不可覆盖的证据包"
    )
    evidence_vnpy.add_argument("--output-dir", required=True)
    evidence_vnpy.add_argument("--reviewer", required=True)
    evidence_vnpy.add_argument(
        "--vector",
        default=str(
            Path(__file__).parents[1]
            / "acceptance"
            / "vectors"
            / "vnpy_cta_native_v1.json"
        ),
    )
    record_tqsdk = sub.add_parser(
        "record-tqsdk", help="首次真实执行并记录天勤固定 Oracle 向量"
    )
    record_tqsdk.add_argument("--vector-output", required=True)
    record_tqsdk.add_argument("--actual-output", default=None)
    accept_tqsdk = sub.add_parser(
        "accept-tqsdk", help="第二次独立执行天勤固定向量并生成三维门禁"
    )
    accept_tqsdk.add_argument("--vector", required=True)
    accept_tqsdk.add_argument("--gate-output", required=True)
    accept_tqsdk.add_argument("--actual-output", default=None)
    trusted_tqsdk = sub.add_parser(
        "verify-tqsdk-trusted",
        help="连续执行两次内置天勤固定向量；只验证功能，不生成提交门禁",
    )
    trusted_tqsdk.add_argument("--report-output", required=True)

    submit = sub.add_parser("submit", help="提交一个 JSON 回测任务")
    submit.add_argument("--request-file", required=True, help="SubmitBacktestRequest(V2) JSON 文件")

    run = sub.add_parser("run", help="提交任务并自动等待到终态")
    run.add_argument("--request-file", required=True, help="SubmitBacktestRequest(V2) JSON 文件")
    run.add_argument("--poll-seconds", type=float, default=1.0, help="轮询间隔")
    run.add_argument("--wait-timeout", type=float, default=86400.0, help="等待终态的最长秒数")
    run.add_argument("--save", default=None, help="将终态 JSON 另存为文件")

    batch = sub.add_parser("batch", help="按文件名顺序自动提交多个 JSON 任务")
    batch.add_argument("--request-dir", required=True, help="请求 JSON 目录")
    batch.add_argument("--poll-seconds", type=float, default=1.0)
    batch.add_argument("--wait-timeout", type=float, default=86400.0)
    batch.add_argument("--continue-on-error", action="store_true")

    status = sub.add_parser("status", help="读取任务状态；不指定 task-id 时列出任务")
    status.add_argument("task_id", nargs="?")
    result = sub.add_parser("result", help="读取任务结果/执行快照")
    result.add_argument("task_id")
    result.add_argument("--save", default=None)
    for name in ("pause", "resume", "cancel"):
        action = sub.add_parser(name, help=f"{name} 任务")
        action.add_argument("task_id")
    speed = sub.add_parser("speed", help="调整可视化回放速度")
    speed.add_argument("task_id")
    speed.add_argument("speed", type=float)
    return parser


def main(argv: list[str] | None = None, *, output: TextIO | None = None) -> int:
    out = output or sys.stdout
    args = build_parser().parse_args(argv)
    try:
        if args.command == "accept-result":
            from .parity_acceptance import (
                AcceptanceVector,
                compare_acceptance_vector,
            )

            vector = AcceptanceVector.model_validate(_read_json(args.vector))
            result = compare_acceptance_vector(vector, _read_json(args.actual))
            _write_json(result.model_dump(mode="json"), out)
            return 0 if result.all_passed else 2
        if args.command == "accept-vnpy":
            from .parity_acceptance import (
                AcceptanceVector,
                compare_acceptance_vector,
            )
            from .vnpy_acceptance import run_vnpy_acceptance_vector

            vector = AcceptanceVector.model_validate(_read_json(args.vector))
            result = compare_acceptance_vector(
                vector,
                run_vnpy_acceptance_vector(),
            )
            _write_json(result.model_dump(mode="json"), out)
            return 0 if result.all_passed else 2
        if args.command == "evidence-vnpy":
            from .gold_evidence import generate_vnpy_gold_evidence

            try:
                evidence = generate_vnpy_gold_evidence(
                    output_dir=Path(args.output_dir),
                    reviewer=args.reviewer,
                    vector_path=Path(args.vector),
                )
            except (OSError, ValueError) as exc:
                raise CliError(str(exc)) from exc
            _write_json(evidence, out)
            return 0
        if args.command == "record-tqsdk":
            from .tqsdk_acceptance import (
                build_tqsdk_acceptance_vector,
                run_tqsdk_acceptance_candidate,
                write_json,
            )

            actual = run_tqsdk_acceptance_candidate()
            vector = build_tqsdk_acceptance_vector(actual)
            write_json(Path(args.vector_output), vector)
            if args.actual_output:
                write_json(Path(args.actual_output), actual)
            _write_json(
                {
                    "recorded": True,
                    "vector": str(Path(args.vector_output).resolve()),
                    "vector_id": vector.vector_id,
                    "next_step": "必须再次独立运行 accept-tqsdk，首次记录不能直接放行",
                },
                out,
            )
            return 0
        if args.command == "accept-tqsdk":
            from .parity_acceptance import AcceptanceVector, compare_acceptance_vector
            from .tqsdk_acceptance import (
                build_tqsdk_acceptance_gate,
                run_tqsdk_acceptance_candidate,
                write_json,
            )

            vector = AcceptanceVector.model_validate(_read_json(args.vector))
            actual = run_tqsdk_acceptance_candidate()
            result = compare_acceptance_vector(vector, actual)
            if args.actual_output:
                write_json(Path(args.actual_output), actual)
            if not result.all_passed:
                _write_json(result.model_dump(mode="json"), out)
                return 2
            try:
                gate = build_tqsdk_acceptance_gate(
                    vector=vector,
                    actual=actual,
                    result=result,
                )
            except ValueError as exc:
                raise CliError(str(exc)) from exc
            write_json(Path(args.gate_output), gate)
            _write_json(
                {
                    "all_passed": True,
                    "gate": str(Path(args.gate_output).resolve()),
                    "acceptance": result.model_dump(mode="json"),
                },
                out,
            )
            return 0
        if args.command == "verify-tqsdk-trusted":
            from .tqsdk_acceptance import (
                run_tqsdk_trusted_acceptance,
                write_json,
            )

            report = run_tqsdk_trusted_acceptance()
            write_json(Path(args.report_output), report)
            _write_json(
                {
                    "all_passed": report["all_passed"],
                    "submit_ready": False,
                    "execution_lane": report["execution_lane"],
                    "report": str(Path(args.report_output).resolve()),
                },
                out,
            )
            return 0 if report["all_passed"] else 2
        if args.command == "health":
            # 健康接口不需要服务令牌。
            client = BacktestApiClient(
                args.base_url or os.getenv("PXYBACKTEST_BASE_URL", DEFAULT_BASE_URL),
                "", "", "", args.timeout
            )
            _write_json(client.request("GET", "/health", auth=False), out)
            return 0
        client = _client(args)
        if args.command == "capabilities":
            _write_json(client.request("GET", "/api/v2/capabilities"), out)
        elif args.command == "submit":
            _write_json(_submit(client, _read_json(args.request_file)), out)
        elif args.command == "run":
            submitted = _submit(client, _read_json(args.request_file))
            task = _wait(client, str(submitted["task_id"]), args.poll_seconds, args.wait_timeout, sys.stderr)
            if args.save:
                Path(args.save).write_text(json.dumps(task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            _write_json(task, out)
            return 0 if task.get("status") == "completed" else 2
        elif args.command == "batch":
            files = sorted(Path(args.request_dir).glob("*.json"))
            if not files:
                raise CliError(f"请求目录没有 JSON 文件: {args.request_dir}")
            results = []
            all_completed = True
            for path in files:
                try:
                    submitted = _submit(client, _read_json(str(path)))
                    task = _wait(client, str(submitted["task_id"]), args.poll_seconds, args.wait_timeout, sys.stderr)
                    results.append({"file": str(path), "task": task})
                    if task.get("status") != "completed":
                        all_completed = False
                    if task.get("status") != "completed" and not args.continue_on_error:
                        break
                except CliError as exc:
                    results.append({"file": str(path), "error": str(exc)})
                    all_completed = False
                    if not args.continue_on_error:
                        break
            _write_json({"total": len(files), "results": results}, out)
            return 0 if all_completed else 2
        elif args.command == "status":
            path = f"/api/v1/tasks/{args.task_id}" if args.task_id else "/api/v1/tasks"
            _write_json(client.request("GET", path), out)
        elif args.command == "result":
            task = client.request("GET", f"/api/v1/tasks/{args.task_id}")
            if args.save:
                Path(args.save).write_text(json.dumps(task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            _write_json(task, out)
        elif args.command in {"pause", "resume"}:
            _write_json(client.request("POST", f"/api/v1/tasks/{args.task_id}/{args.command}"), out)
        elif args.command == "cancel":
            _write_json(client.request("DELETE", f"/api/v1/tasks/{args.task_id}"), out)
        elif args.command == "speed":
            _write_json(client.request("POST", f"/api/v1/tasks/{args.task_id}/speed", {"speed": args.speed}), out)
        return 0
    except CliError as exc:
        print(f"[错误] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
