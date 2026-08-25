"""PXYDATA 回测快照清单的统一信任边界校验。"""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SnapshotManifestError(ValueError):
    pass


def _safe_relative_path(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/")
    path = PurePosixPath(text)
    if (
        not text
        or path.is_absolute()
        or ".." in path.parts
        or ":" in path.parts[0]
        or text.startswith("//")
    ):
        raise SnapshotManifestError(f"快照文件路径不安全: {text or '<empty>'}")
    return path.as_posix()


def validate_snapshot_manifest(
    manifest: dict[str, Any],
    *,
    snapshot_id: str,
    manifest_sha256: str,
    expected_datasets: set[str],
) -> dict[str, Any]:
    """校验 provider manifest 与任务中公开快照引用完全一致。

    文件内容 SHA256 由各 engine adapter 在读取前校验；此处统一校验身份、
    数据集集合、路径和文件元数据，禁止适配器绕过任务绑定的文件集合。
    """
    if str(manifest.get("snapshot_id") or "") != snapshot_id:
        raise SnapshotManifestError("快照清单 snapshot_id 与任务引用不一致")
    if str(manifest.get("manifest_sha256") or "") != manifest_sha256:
        raise SnapshotManifestError("快照清单 manifest_sha256 与任务引用不一致")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise SnapshotManifestError("快照清单缺少 datasets")

    normalized: list[dict[str, Any]] = []
    names: set[str] = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise SnapshotManifestError("快照清单 dataset 必须是对象")
        name = str(dataset.get("name") or "").strip()
        if not name or name in names:
            raise SnapshotManifestError(f"快照清单数据集名称无效或重复: {name}")
        names.add(name)
        files = dataset.get("files")
        if not isinstance(files, list) or not files:
            raise SnapshotManifestError(f"快照数据集 {name} 缺少固定文件集合")
        normalized_files: list[dict[str, Any]] = []
        for item in files:
            if not isinstance(item, dict):
                raise SnapshotManifestError(f"快照数据集 {name} 文件项必须是对象")
            relative_path = _safe_relative_path(item.get("path"))
            digest = str(item.get("sha256") or "").lower()
            try:
                size_bytes = int(item.get("size_bytes"))
            except (TypeError, ValueError) as exc:
                raise SnapshotManifestError(f"快照文件大小无效: {relative_path}") from exc
            if _SHA256_RE.fullmatch(digest) is None or size_bytes < 0:
                raise SnapshotManifestError(f"快照文件校验元数据无效: {relative_path}")
            normalized_files.append(
                {**item, "path": relative_path, "sha256": digest, "size_bytes": size_bytes}
            )
        normalized.append({**dataset, "name": name, "files": normalized_files})

    if names != expected_datasets:
        missing = sorted(expected_datasets - names)
        extra = sorted(names - expected_datasets)
        raise SnapshotManifestError(
            f"快照数据集集合不一致: missing={missing}, extra={extra}"
        )
    return {**manifest, "datasets": normalized}


__all__ = ["SnapshotManifestError", "validate_snapshot_manifest"]
