"""Windows 不可信 Python 子进程的受限启动器。

受限令牌和 Job Object 在本进程内强制执行；网络白名单由 PXYOPS 使用专用
服务账号和 Windows 防火墙实施，本模块只接受不可为空的 SHA256 策略证明，
不会因为调用方请求了 allowlisted 就宣称已经实施。
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import locale
import os
import re
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path


NETWORK_POLICY_ID_ENV = "PXYBACKTEST_TQSDK_NETWORK_POLICY_ID"
NETWORK_POLICY_SHA256_ENV = "PXYBACKTEST_TQSDK_NETWORK_POLICY_SHA256"
NETWORK_POLICY_FILE_ENV = "PXYBACKTEST_TQSDK_NETWORK_POLICY_FILE"


def _normalized_path(path: Path) -> str:
    """Windows 路径按大小写不敏感方式比较，其他平台保持原语义。"""

    return os.path.normcase(str(path.resolve()))


class SandboxLaunchError(RuntimeError):
    """Windows 沙箱进程无法安全启动。"""


class SandboxTimeoutError(SandboxLaunchError):
    """沙箱进程达到任务超时并已终止。"""


class SandboxCancelledError(SandboxLaunchError):
    """沙箱进程收到取消请求并已终止。"""


@dataclass(frozen=True)
class SandboxLimits:
    timeout_seconds: int
    memory_mb: int = 4096
    cpu_cores: int = 1
    # Windows venv 启动器会短暂创建基础解释器子进程，因此至少允许两个进程。
    active_process_limit: int = 2

    def __post_init__(self) -> None:
        if self.timeout_seconds < 1:
            raise ValueError("沙箱超时必须大于 0 秒")
        if self.memory_mb < 128:
            raise ValueError("沙箱内存限制不能小于 128 MB")
        if self.cpu_cores < 1:
            raise ValueError("沙箱 CPU 核数必须大于 0")
        if self.active_process_limit < 1:
            raise ValueError("沙箱活动进程限制必须大于 0")


@dataclass(frozen=True)
class SandboxIdentity:
    username: str
    password: str
    domain: str = "."

    def __post_init__(self) -> None:
        if not self.username.strip() or not self.password:
            raise ValueError("沙箱专用账户和密码不能为空")


@dataclass(frozen=True)
class SandboxProcessResult:
    exit_code: int
    restricted_token: bool
    job_object: bool
    dedicated_identity: bool
    task_directory_acl: bool
    network_allowlist_enforced: bool
    network_policy_id: str | None
    network_policy_sha256: str | None
    process_creation_api: str
    limits: SandboxLimits

    def security_state(self) -> dict[str, object]:
        complete = bool(
            self.restricted_token
            and self.job_object
            and self.dedicated_identity
            and self.task_directory_acl
            and self.network_allowlist_enforced
        )
        return {
            "strength": "windows_restricted" if complete else "windows_partial",
            "restricted_token": self.restricted_token,
            "job_object": self.job_object,
            "dedicated_identity": self.dedicated_identity,
            "task_directory_acl": self.task_directory_acl,
            "filesystem_isolated": self.dedicated_identity
            and self.task_directory_acl,
            "network_allowlist_enforced": self.network_allowlist_enforced,
            "network_policy_id": self.network_policy_id,
            "network_policy_sha256": self.network_policy_sha256,
            "process_creation_api": self.process_creation_api,
            "limits": {
                "timeout_seconds": self.limits.timeout_seconds,
                "memory_mb": self.limits.memory_mb,
                "cpu_cores": self.limits.cpu_cores,
                "active_process_limit": self.limits.active_process_limit,
            },
            "submit_ready": complete,
        }


def network_policy_attestation(
    environ: Mapping[str, str] | None = None,
) -> tuple[bool, str | None, str | None]:
    """读取部署层白名单文件；环境变量中的裸 ID/哈希不能充当安全证明。"""

    values = environ if environ is not None else os.environ
    policy_id = ""
    policy_sha256 = ""
    policy_file_raw = str(values.get(NETWORK_POLICY_FILE_ENV, "")).strip()
    if not policy_file_raw:
        return False, None, None
    try:
        payload = json.loads(
            Path(policy_file_raw).read_text(encoding="utf-8-sig")
        )
        if not isinstance(payload, dict):
            raise ValueError("天勤网络策略证明必须是 JSON 对象")
        configured_python = Path(
            str(values.get("PXYBACKTEST_TQSDK_PYTHON", ""))
        )
        configured_hash = (
            hashlib.sha256(configured_python.read_bytes()).hexdigest()
            if configured_python.is_file()
            else ""
        )
        effective_python = Path(
            str(payload.get("effective_python_path") or "")
        )
        effective_hash = (
            hashlib.sha256(effective_python.read_bytes()).hexdigest()
            if effective_python.is_file()
            else ""
        )
        protected_programs: dict[str, str] = {}
        for item in payload.get("programs") or []:
            if not isinstance(item, dict):
                continue
            program = Path(str(item.get("path") or ""))
            declared_hash = str(item.get("sha256") or "").lower()
            actual_hash = (
                hashlib.sha256(program.read_bytes()).hexdigest()
                if program.is_file()
                else ""
            )
            if actual_hash and declared_hash == actual_hash:
                protected_programs[_normalized_path(program)] = actual_hash
        expected_programs = {
            _normalized_path(configured_python),
            _normalized_path(effective_python),
        }
        if (
            payload.get("contract_version")
            == "pxyops.tqsdk-network-policy.v1"
            and payload.get("enforced") is True
            and payload.get("remote_port") == 443
            and payload.get("rule_scope") == "sandbox_account_all_programs"
            and _normalized_path(Path(str(payload.get("python_path") or "")))
            == _normalized_path(configured_python)
            and payload.get("python_sha256") == configured_hash
            and payload.get("effective_python_sha256") == effective_hash
            and len(expected_programs) == 2
            and expected_programs.issubset(protected_programs)
        ):
            policy_id = str(payload.get("policy_id") or "").strip()
            policy_sha256 = str(
                payload.get("policy_sha256") or ""
            ).strip().lower()
    except (OSError, ValueError, TypeError):
        policy_id = ""
        policy_sha256 = ""
    valid = bool(policy_id and re.fullmatch(r"[0-9a-f]{64}", policy_sha256))
    return valid, policy_id or None, policy_sha256 or None


if os.name == "nt":
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)

    LPBYTE = ctypes.POINTER(wintypes.BYTE)
    SIZE_T = ctypes.c_size_t
    ULONG_PTR = ctypes.c_size_t

    class STARTUPINFOW(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("lpReserved", wintypes.LPWSTR),
            ("lpDesktop", wintypes.LPWSTR),
            ("lpTitle", wintypes.LPWSTR),
            ("dwX", wintypes.DWORD),
            ("dwY", wintypes.DWORD),
            ("dwXSize", wintypes.DWORD),
            ("dwYSize", wintypes.DWORD),
            ("dwXCountChars", wintypes.DWORD),
            ("dwYCountChars", wintypes.DWORD),
            ("dwFillAttribute", wintypes.DWORD),
            ("dwFlags", wintypes.DWORD),
            ("wShowWindow", wintypes.WORD),
            ("cbReserved2", wintypes.WORD),
            ("lpReserved2", LPBYTE),
            ("hStdInput", wintypes.HANDLE),
            ("hStdOutput", wintypes.HANDLE),
            ("hStdError", wintypes.HANDLE),
        ]

    class PROCESS_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("hProcess", wintypes.HANDLE),
            ("hThread", wintypes.HANDLE),
            ("dwProcessId", wintypes.DWORD),
            ("dwThreadId", wintypes.DWORD),
        ]

    class STARTUPINFOEXW(ctypes.Structure):
        _fields_ = [
            ("StartupInfo", STARTUPINFOW),
            ("lpAttributeList", ctypes.c_void_p),
        ]

    class SECURITY_ATTRIBUTES(ctypes.Structure):
        _fields_ = [
            ("nLength", wintypes.DWORD),
            ("lpSecurityDescriptor", ctypes.c_void_p),
            ("bInheritHandle", wintypes.BOOL),
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", SIZE_T),
            ("MaximumWorkingSetSize", SIZE_T),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ULONG_PTR),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", SIZE_T),
            ("JobMemoryLimit", SIZE_T),
            ("PeakProcessMemoryUsed", SIZE_T),
            ("PeakJobMemoryUsed", SIZE_T),
        ]

    class JOBOBJECT_CPU_RATE_CONTROL_INFORMATION(ctypes.Structure):
        _fields_ = [("ControlFlags", wintypes.DWORD), ("CpuRate", wintypes.DWORD)]

    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.GetExitCodeProcess.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.InitializeProcThreadAttributeList.restype = wintypes.BOOL
    kernel32.InitializeProcThreadAttributeList.argtypes = [
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(SIZE_T),
    ]
    kernel32.UpdateProcThreadAttribute.restype = wintypes.BOOL
    kernel32.UpdateProcThreadAttribute.argtypes = [
        ctypes.c_void_p,
        wintypes.DWORD,
        SIZE_T,
        ctypes.c_void_p,
        SIZE_T,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    kernel32.DeleteProcThreadAttributeList.argtypes = [ctypes.c_void_p]
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.CreateRestrictedToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.LogonUserW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.CreateProcessAsUserW.argtypes = [
        wintypes.HANDLE,
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.BOOL,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.LPCWSTR,
        ctypes.POINTER(STARTUPINFOW),
        ctypes.POINTER(PROCESS_INFORMATION),
    ]
    advapi32.CreateProcessWithTokenW.restype = wintypes.BOOL
    advapi32.CreateProcessWithTokenW.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.LPCWSTR,
        ctypes.POINTER(STARTUPINFOW),
        ctypes.POINTER(PROCESS_INFORMATION),
    ]


def _win_error_from_code(action: str, code: int) -> SandboxLaunchError:
    normalized = int(code) & 0xFFFFFFFF
    format_code = normalized if normalized < 0x80000000 else normalized - 0x100000000
    return SandboxLaunchError(
        f"{action}失败，Win32错误={normalized} (0x{normalized:08X}): "
        f"{ctypes.FormatError(format_code)}"
    )


def _win_error(action: str) -> SandboxLaunchError:
    return _win_error_from_code(action, ctypes.get_last_error())


def _environment_block(environment: Mapping[str, str]) -> ctypes.Array:
    entries = [
        f"{key}={value}"
        for key, value in sorted(environment.items(), key=lambda item: item[0].upper())
        if key and not key.startswith("=") and "\x00" not in key and "\x00" not in value
    ]
    return ctypes.create_unicode_buffer("\x00".join(entries) + "\x00\x00")


def _create_restricted_token(
    identity: SandboxIdentity | None,
) -> wintypes.HANDLE:
    TOKEN_ASSIGN_PRIMARY = 0x0001
    TOKEN_DUPLICATE = 0x0002
    TOKEN_QUERY = 0x0008
    TOKEN_ADJUST_DEFAULT = 0x0080
    TOKEN_ADJUST_SESSIONID = 0x0100
    DISABLE_MAX_PRIVILEGE = 0x1
    LUA_TOKEN = 0x4
    source = wintypes.HANDLE()
    if identity is not None:
        LOGON32_LOGON_INTERACTIVE = 2
        LOGON32_PROVIDER_DEFAULT = 0
        if not advapi32.LogonUserW(
            identity.username,
            identity.domain,
            identity.password,
            LOGON32_LOGON_INTERACTIVE,
            LOGON32_PROVIDER_DEFAULT,
            ctypes.byref(source),
        ):
            raise _win_error("登录天勤沙箱专用账户")
    else:
        desired = (
            TOKEN_ASSIGN_PRIMARY
            | TOKEN_DUPLICATE
            | TOKEN_QUERY
            | TOKEN_ADJUST_DEFAULT
            | TOKEN_ADJUST_SESSIONID
        )
        if not advapi32.OpenProcessToken(
            kernel32.GetCurrentProcess(), desired, ctypes.byref(source)
        ):
            raise _win_error("打开当前进程令牌")
    restricted = wintypes.HANDLE()
    try:
        if not advapi32.CreateRestrictedToken(
            source,
            DISABLE_MAX_PRIVILEGE | LUA_TOKEN,
            0,
            None,
            0,
            None,
            0,
            None,
            ctypes.byref(restricted),
        ):
            raise _win_error("创建受限令牌")
        return restricted
    finally:
        kernel32.CloseHandle(source)


def _configure_job(limits: SandboxLimits) -> wintypes.HANDLE:
    JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
    JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 0x00000008
    JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION = 0x00000400
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE = 0x1
    JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP = 0x4
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    JOB_OBJECT_CPU_RATE_CONTROL_INFORMATION_CLASS = 15

    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise _win_error("创建 Job Object")
    try:
        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = (
            JOB_OBJECT_LIMIT_PROCESS_MEMORY
            | JOB_OBJECT_LIMIT_ACTIVE_PROCESS
            | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION
            | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        info.BasicLimitInformation.ActiveProcessLimit = limits.active_process_limit
        info.ProcessMemoryLimit = limits.memory_mb * 1024 * 1024
        if not kernel32.SetInformationJobObject(
            job,
            JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            ctypes.byref(info),
            ctypes.sizeof(info),
        ):
            raise _win_error("设置 Job Object 内存/进程限制")

        host_cores = max(1, os.cpu_count() or 1)
        cpu_rate = max(1, min(10_000, int(limits.cpu_cores * 10_000 / host_cores)))
        cpu = JOBOBJECT_CPU_RATE_CONTROL_INFORMATION(
            JOB_OBJECT_CPU_RATE_CONTROL_ENABLE | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
            cpu_rate,
        )
        if not kernel32.SetInformationJobObject(
            job,
            JOB_OBJECT_CPU_RATE_CONTROL_INFORMATION_CLASS,
            ctypes.byref(cpu),
            ctypes.sizeof(cpu),
        ):
            raise _win_error("设置 Job Object CPU 限制")
        return job
    except Exception:
        kernel32.CloseHandle(job)
        raise


def _set_task_directory_access(
    path: Path, identity: SandboxIdentity, *, grant: bool
) -> None:
    """只在策略运行期间授予专用账户任务目录访问权。"""

    account = (
        f"{identity.domain}\\{identity.username}"
        if identity.domain not in {"", "."}
        else identity.username
    )
    # icacls /T /C 会在单个子项失败时继续处理，总退出码不能
    # 作为每个已有文件获得 ACE 的证明。逐项执行并检查，确保
    # request/strategy 可读，后续结果文件则继承任务目录的 ACE。
    targets = [path, *sorted(path.rglob("*"), key=lambda item: str(item).lower())]
    for target in targets:
        inherited_rights = "(OI)(CI)F" if target.is_dir() else "F"
        if grant:
            sandbox_rights = "(OI)(CI)M" if target.is_dir() else "M"
            command = [
                "icacls.exe",
                str(target),
                "/inheritance:r",
                "/grant:r",
                f"{account}:{sandbox_rights}",
                f"SYSTEM:{inherited_rights}",
                f"Administrators:{inherited_rights}",
            ]
        else:
            command = [
                "icacls.exe",
                str(target),
                "/inheritance:r",
                "/remove:g",
                account,
                "/grant:r",
                f"SYSTEM:{inherited_rights}",
                f"Administrators:{inherited_rights}",
            ]
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding=locale.getpreferredencoding(False),
            errors="replace",
            creationflags=subprocess.CREATE_NO_WINDOW,
            check=False,
        )
        if completed.returncode != 0:
            action = "授予" if grant else "撤销"
            detail = " ".join(
                f"{completed.stdout or ''} {completed.stderr or ''}".split()
            )[-500:]
            suffix = f"：{detail}" if detail else ""
            raise SandboxLaunchError(
                f"{action}天勤沙箱路径 ACL 失败，exit_code="
                f"{completed.returncode}，path={target}{suffix}"
            )


def _launch_windows(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    limits: SandboxLimits,
    cancel_check: Callable[[], bool] | None,
    identity: SandboxIdentity | None,
) -> SandboxProcessResult:
    CREATE_SUSPENDED = 0x00000004
    CREATE_UNICODE_ENVIRONMENT = 0x00000400
    CREATE_NO_WINDOW = 0x08000000
    EXTENDED_STARTUPINFO_PRESENT = 0x00080000
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST = 0x00020002
    STARTF_USESTDHANDLES = 0x00000100
    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x00000080
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    WAIT_TIMEOUT = 0x00000102
    WAIT_FAILED = 0xFFFFFFFF
    INFINITE_SLICE_MS = 50

    task_acl_applied = False
    if identity is not None:
        _set_task_directory_access(cwd, identity, grant=True)
        task_acl_applied = True
    try:
        token = _create_restricted_token(identity)
        try:
            job = _configure_job(limits)
        except Exception:
            kernel32.CloseHandle(token)
            raise
    except Exception:
        if task_acl_applied and identity is not None:
            _set_task_directory_access(cwd, identity, grant=False)
        raise
    startup = STARTUPINFOEXW()
    startup.StartupInfo.cb = ctypes.sizeof(startup)
    process = PROCESS_INFORMATION()
    command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(list(command)))
    env_block = _environment_block(environment)
    security = SECURITY_ATTRIBUTES(
        ctypes.sizeof(SECURITY_ATTRIBUTES), None, True
    )
    null_in = kernel32.CreateFileW(
        "NUL",
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        ctypes.byref(security),
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )
    null_out = kernel32.CreateFileW(
        "NUL",
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        ctypes.byref(security),
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )
    if null_in in (None, INVALID_HANDLE_VALUE) or null_out in (
        None,
        INVALID_HANDLE_VALUE,
    ):
        if null_in not in (None, INVALID_HANDLE_VALUE):
            kernel32.CloseHandle(null_in)
        if null_out not in (None, INVALID_HANDLE_VALUE):
            kernel32.CloseHandle(null_out)
        kernel32.CloseHandle(job)
        kernel32.CloseHandle(token)
        if task_acl_applied and identity is not None:
            _set_task_directory_access(cwd, identity, grant=False)
        raise _win_error("打开沙箱 NUL 标准流")
    startup.StartupInfo.dwFlags |= STARTF_USESTDHANDLES
    startup.StartupInfo.hStdInput = null_in
    startup.StartupInfo.hStdOutput = null_out
    startup.StartupInfo.hStdError = null_out
    attribute_size = SIZE_T()
    kernel32.InitializeProcThreadAttributeList(
        None, 1, 0, ctypes.byref(attribute_size)
    )
    attribute_buffer = ctypes.create_string_buffer(attribute_size.value)
    startup.lpAttributeList = ctypes.cast(attribute_buffer, ctypes.c_void_p)
    if not kernel32.InitializeProcThreadAttributeList(
        startup.lpAttributeList, 1, 0, ctypes.byref(attribute_size)
    ):
        kernel32.CloseHandle(null_out)
        kernel32.CloseHandle(null_in)
        kernel32.CloseHandle(job)
        kernel32.CloseHandle(token)
        if task_acl_applied and identity is not None:
            _set_task_directory_access(cwd, identity, grant=False)
        raise _win_error("初始化沙箱句柄白名单")
    inherited_handles = (wintypes.HANDLE * 2)(null_in, null_out)
    if not kernel32.UpdateProcThreadAttribute(
        startup.lpAttributeList,
        0,
        PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
        ctypes.cast(inherited_handles, ctypes.c_void_p),
        ctypes.sizeof(inherited_handles),
        None,
        None,
    ):
        kernel32.DeleteProcThreadAttributeList(startup.lpAttributeList)
        kernel32.CloseHandle(null_out)
        kernel32.CloseHandle(null_in)
        kernel32.CloseHandle(job)
        kernel32.CloseHandle(token)
        if task_acl_applied and identity is not None:
            _set_task_directory_access(cwd, identity, grant=False)
        raise _win_error("设置沙箱继承句柄白名单")
    try:
        creation_flags = (
            CREATE_SUSPENDED
            | CREATE_UNICODE_ENVIRONMENT
            | CREATE_NO_WINDOW
            | EXTENDED_STARTUPINFO_PRESENT
        )
        process_creation_api = "CreateProcessAsUserW"
        created = advapi32.CreateProcessAsUserW(
            token,
            None,
            command_line,
            None,
            None,
            True,
            creation_flags,
            ctypes.cast(env_block, ctypes.c_void_p),
            str(cwd),
            ctypes.cast(ctypes.byref(startup), ctypes.POINTER(STARTUPINFOW)),
            ctypes.byref(process),
        )
        create_error = ctypes.get_last_error() if not created else 0
        process_creation_error = create_error
        if not created and create_error == 1314:
            # 提权管理员通常有 SeImpersonatePrivilege，但默认没有
            # SeAssignPrimaryToken/SeIncreaseQuota。保持同一受限主令牌，
            # 改用 CreateProcessWithTokenW，避免要求扩大本机账户特权。
            process_creation_api = "CreateProcessWithTokenW"
            command_line = ctypes.create_unicode_buffer(
                subprocess.list2cmdline(list(command))
            )
            fallback_startup = STARTUPINFOW()
            fallback_startup.cb = ctypes.sizeof(fallback_startup)
            ctypes.set_last_error(0)
            created = advapi32.CreateProcessWithTokenW(
                token,
                0,
                None,
                command_line,
                creation_flags & ~EXTENDED_STARTUPINFO_PRESENT,
                ctypes.cast(env_block, ctypes.c_void_p),
                str(cwd),
                ctypes.byref(fallback_startup),
                ctypes.byref(process),
            )
            process_creation_error = (
                ctypes.get_last_error() if not created else 0
            )
        if not created:
            primary_suffix = (
                f"，CreateProcessAsUserW首次错误={create_error}"
                if process_creation_api == "CreateProcessWithTokenW"
                else ""
            )
            raise _win_error_from_code(
                f"使用受限令牌创建策略进程"
                f"（API={process_creation_api}{primary_suffix}）",
                process_creation_error,
            )
        try:
            if not kernel32.AssignProcessToJobObject(job, process.hProcess):
                kernel32.TerminateProcess(process.hProcess, 1)
                raise _win_error("将策略进程加入 Job Object")
            if kernel32.ResumeThread(process.hThread) == 0xFFFFFFFF:
                kernel32.TerminateJobObject(job, 1)
                raise _win_error("恢复策略进程")

            deadline = time.monotonic() + limits.timeout_seconds
            while True:
                wait_result = kernel32.WaitForSingleObject(
                    process.hProcess, INFINITE_SLICE_MS
                )
                if wait_result == 0:
                    break
                if wait_result == WAIT_FAILED:
                    kernel32.TerminateJobObject(job, 1)
                    raise _win_error("等待策略进程")
                if wait_result != WAIT_TIMEOUT:
                    kernel32.TerminateJobObject(job, 1)
                    raise SandboxLaunchError(f"等待策略进程返回未知状态: {wait_result}")
                if cancel_check is not None and cancel_check():
                    kernel32.TerminateJobObject(job, 2)
                    raise SandboxCancelledError("天勤策略任务已取消")
                if time.monotonic() >= deadline:
                    kernel32.TerminateJobObject(job, 3)
                    raise SandboxTimeoutError("天勤策略子进程执行超时")

            exit_code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(process.hProcess, ctypes.byref(exit_code)):
                raise _win_error("读取策略进程退出码")
            enforced, policy_id, policy_sha256 = network_policy_attestation()
            return SandboxProcessResult(
                exit_code=int(exit_code.value),
                restricted_token=True,
                job_object=True,
                dedicated_identity=identity is not None,
                task_directory_acl=task_acl_applied,
                network_allowlist_enforced=enforced,
                network_policy_id=policy_id,
                network_policy_sha256=policy_sha256,
                process_creation_api=process_creation_api,
                limits=limits,
            )
        finally:
            if process.hThread:
                kernel32.CloseHandle(process.hThread)
            if process.hProcess:
                kernel32.CloseHandle(process.hProcess)
    finally:
        kernel32.DeleteProcThreadAttributeList(startup.lpAttributeList)
        kernel32.CloseHandle(null_out)
        kernel32.CloseHandle(null_in)
        kernel32.CloseHandle(job)
        kernel32.CloseHandle(token)
        if task_acl_applied and identity is not None:
            _set_task_directory_access(cwd, identity, grant=False)


def _launch_portable(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    limits: SandboxLimits,
    cancel_check: Callable[[], bool] | None,
    identity: SandboxIdentity | None,
) -> SandboxProcessResult:
    process = subprocess.Popen(
        list(command),
        cwd=str(cwd),
        env=dict(environment),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + limits.timeout_seconds
    try:
        while process.poll() is None:
            if cancel_check is not None and cancel_check():
                process.kill()
                raise SandboxCancelledError("天勤策略任务已取消")
            if time.monotonic() >= deadline:
                process.kill()
                raise SandboxTimeoutError("天勤策略子进程执行超时")
            time.sleep(0.05)
        enforced, policy_id, policy_sha256 = network_policy_attestation()
        return SandboxProcessResult(
            exit_code=int(process.returncode or 0),
            restricted_token=False,
            job_object=False,
            dedicated_identity=False,
            task_directory_acl=False,
            network_allowlist_enforced=enforced,
            network_policy_id=policy_id,
            network_policy_sha256=policy_sha256,
            process_creation_api="subprocess.Popen",
            limits=limits,
        )
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=1)


def launch_sandboxed_process(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    limits: SandboxLimits,
    cancel_check: Callable[[], bool] | None = None,
    identity: SandboxIdentity | None = None,
) -> SandboxProcessResult:
    """启动受限进程；Windows 强制受限令牌和 Job Object。"""

    if not command:
        raise ValueError("沙箱命令不能为空")
    if not cwd.is_dir():
        raise ValueError(f"沙箱工作目录不存在: {cwd}")
    if os.name == "nt":
        return _launch_windows(
            command,
            cwd=cwd,
            environment=environment,
            limits=limits,
            cancel_check=cancel_check,
            identity=identity,
        )
    return _launch_portable(
        command,
        cwd=cwd,
        environment=environment,
        limits=limits,
        cancel_check=cancel_check,
        identity=identity,
    )


__all__ = [
    "NETWORK_POLICY_ID_ENV",
    "NETWORK_POLICY_FILE_ENV",
    "NETWORK_POLICY_SHA256_ENV",
    "SandboxCancelledError",
    "SandboxLaunchError",
    "SandboxIdentity",
    "SandboxLimits",
    "SandboxProcessResult",
    "SandboxTimeoutError",
    "launch_sandboxed_process",
    "network_policy_attestation",
]
