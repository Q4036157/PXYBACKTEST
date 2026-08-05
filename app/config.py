from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _read_secret(path: Path | None) -> str:
    if path is None or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").strip()


@dataclass(frozen=True)
class Settings:
    runtime_root: Path
    pxylh_root: Path
    service_token: str
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

    def ensure_directories(self) -> None:
        for path in (self.runtime_root, self.data_dir, self.jobs_dir, self.results_dir):
            path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Settings":
        runtime_root = Path(
            os.getenv("PXYBACKTEST_RUNTIME_ROOT", r"D:\x1\pxy-runtime\PXYBACKTEST")
        )
        pxylh_root = Path(os.getenv("PXYBACKTEST_PXYLH_ROOT", r"D:\x1\x2\PXYLH"))
        secret_path_raw = os.getenv("PXYBACKTEST_SERVICE_TOKEN_FILE", "").strip()
        secret_path = Path(secret_path_raw) if secret_path_raw else None
        service_token = os.getenv("PXYBACKTEST_SERVICE_TOKEN", "").strip() or _read_secret(
            secret_path
        )
        return cls(
            runtime_root=runtime_root,
            pxylh_root=pxylh_root,
            service_token=service_token,
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

