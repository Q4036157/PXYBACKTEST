from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


def _read_secret(path: Path | None) -> str:
    if path is None or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").strip()


PXYLH_CTA_WORKER_ENV_MAP = {
    "PXYBACKTEST_PXYDATA_API_KEY_FILE": "PXYDATA_API_KEY_FILE",
    "PXYBACKTEST_PXYDATA_BASE_URL": "PXYDATA_BASE_URL",
    "PXYBACKTEST_PXYDATA_DATA_ROOT": "PXYDATA_DATA_DIR",
}


def pxylh_cta_worker_environment(
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """把 PXYBACKTEST 配置名映射为复用 CTA loader 所需的环境名。"""

    source = os.environ if environ is None else environ
    mapped: dict[str, str] = {}
    for source_name, worker_name in PXYLH_CTA_WORKER_ENV_MAP.items():
        value = str(source.get(source_name) or "").strip()
        if value:
            mapped[worker_name] = value
    return mapped


@dataclass(frozen=True)
class Settings:
    runtime_root: Path
    pxylh_root: Path
    service_token: str
    # 工作站实际 DAA 仓库目录；可通过 PXYBACKTEST_DAA_ROOT 覆盖。
    daa_root: Path = Path(r"D:\x1\x2\DAA")
    daa_python_override: Path | None = None
    pxydata_data_root: Path = Path(r"E:\pxy-runtime\PXYDATA\data")
    pxydata_base_url: str = "http://127.0.0.1:3020"
    pxydata_api_key: str = ""
    llm_base_url: str = ""
    llm_api_key: str = ""
    custom_nodes_root: Path = Path(r"E:\pxy-runtime\PXYBACKTEST\custom_nodes")
    max_concurrent_tasks: int = 1
    max_queued_per_user: int = 3
    render_interval_ms: int = 50

    @property
    def data_dir(self) -> Path:
        return self.runtime_root / "data"

    @property
    def jobs_dir(self) -> Path:
        return self.runtime_root / "jobs"

    @property
    def results_dir(self) -> Path:
        return self.runtime_root / "results"

    @property
    def database_path(self) -> Path:
        return self.data_dir / "backtest.sqlite3"

    @property
    def daa_backend_root(self) -> Path:
        return self.daa_root / "backend"

    @property
    def daa_python(self) -> Path:
        if self.daa_python_override is not None:
            return self.daa_python_override
        windows_python = self.daa_backend_root / ".venv" / "Scripts" / "python.exe"
        if windows_python.is_file():
            return windows_python
        return self.daa_backend_root / ".venv" / "bin" / "python"

    @property
    def a_share_adapter_available(self) -> bool:
        return (
            self.daa_python.is_file()
            and (
                self.daa_backend_root / "app" / "backtest" / "pxy_adapter.py"
            ).is_file()
            and self.pxydata_data_root.is_dir()
        )

    def ensure_directories(self) -> None:
        for path in (self.runtime_root, self.data_dir, self.jobs_dir, self.results_dir):
            path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Settings":
        runtime_root = Path(
            os.getenv("PXYBACKTEST_RUNTIME_ROOT", r"E:\pxy-runtime\PXYBACKTEST")
        )
        pxylh_root = Path(os.getenv("PXYBACKTEST_PXYLH_ROOT", r"D:\x1\x2\PXYLH"))
        daa_root = Path(os.getenv("PXYBACKTEST_DAA_ROOT", r"D:\x1\x2\DAA"))
        daa_python_raw = os.getenv("PXYBACKTEST_DAA_PYTHON", "").strip()
        if not daa_python_raw:
            workstation_daa_python = Path(
                r"D:\x1\x2\DAA\backend\.venv\Scripts\python.exe"
            )
            if workstation_daa_python.is_file():
                daa_python_raw = str(workstation_daa_python)
        pxydata_data_root = Path(
            os.getenv(
                "PXYBACKTEST_PXYDATA_DATA_ROOT",
                r"E:\pxy-runtime\PXYDATA\data",
            )
        )
        secret_path_raw = os.getenv("PXYBACKTEST_SERVICE_TOKEN_FILE", "").strip()
        secret_path = Path(secret_path_raw) if secret_path_raw else None
        service_token = os.getenv(
            "PXYBACKTEST_SERVICE_TOKEN", ""
        ).strip() or _read_secret(secret_path)
        pxydata_secret_path_raw = os.getenv(
            "PXYBACKTEST_PXYDATA_API_KEY_FILE", ""
        ).strip()
        pxydata_secret_path = (
            Path(pxydata_secret_path_raw) if pxydata_secret_path_raw else None
        )
        pxydata_api_key = os.getenv(
            "PXYBACKTEST_PXYDATA_API_KEY", ""
        ).strip() or _read_secret(pxydata_secret_path)
        llm_secret_path_raw = os.getenv("PXYBACKTEST_LLM_API_KEY_FILE", "").strip()
        llm_secret_path = Path(llm_secret_path_raw) if llm_secret_path_raw else None
        llm_api_key = os.getenv("PXYBACKTEST_LLM_API_KEY", "").strip() or _read_secret(llm_secret_path)
        return cls(
            runtime_root=runtime_root,
            pxylh_root=pxylh_root,
            service_token=service_token,
            daa_root=daa_root,
            daa_python_override=Path(daa_python_raw) if daa_python_raw else None,
            pxydata_data_root=pxydata_data_root,
            pxydata_base_url=os.getenv(
                "PXYBACKTEST_PXYDATA_BASE_URL", "http://127.0.0.1:3020"
            )
            .strip()
            .rstrip("/"),
            pxydata_api_key=pxydata_api_key,
            llm_base_url=os.getenv("PXYBACKTEST_LLM_BASE_URL", "").strip().rstrip("/"),
            llm_api_key=llm_api_key,
            custom_nodes_root=Path(os.getenv(
                "PXYBACKTEST_CUSTOM_NODES_ROOT",
                r"E:\pxy-runtime\PXYBACKTEST\custom_nodes",
            )),
            max_concurrent_tasks=max(
                1, int(os.getenv("PXYBACKTEST_MAX_CONCURRENT_TASKS", "1"))
            ),
            max_queued_per_user=max(
                1, int(os.getenv("PXYBACKTEST_MAX_QUEUED_PER_USER", "3"))
            ),
            render_interval_ms=max(
                33, int(os.getenv("PXYBACKTEST_RENDER_INTERVAL_MS", "50"))
            ),
        )
