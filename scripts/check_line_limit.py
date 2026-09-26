"""Guard source and test file sizes while legacy modules are being split."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

LIMIT = 500
ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "scripts" / "line_limit_legacy.json"
SOURCE_SUFFIXES = {
    ".py", ".ps1", ".bat", ".cmd", ".cpp", ".cc", ".c", ".h", ".hpp",
    ".js", ".ts", ".tsx", ".vue", ".css",
}


def tracked_sources(root: Path) -> list[Path]:
    output = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "app", "tests", "scripts", "BTBAT", "deploy"], cwd=root
    )
    return [
        root / name.decode("utf-8")
        for name in output.split(b"\0")
        if name and Path(name.decode("utf-8")).suffix.lower() in SOURCE_SUFFIXES
    ]


def violations(root: Path, paths: list[Path], legacy: dict[str, int], strict: bool) -> list[str]:
    errors: list[str] = []
    for path in paths:
        relative = path.relative_to(root).as_posix()
        line_count = len(path.read_text(encoding="utf-8-sig").splitlines())
        if line_count <= LIMIT:
            continue
        allowance = LIMIT if strict else int(legacy.get(relative, LIMIT))
        if line_count > allowance:
            errors.append(f"{relative}: {line_count} lines (allowed {allowance})")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Check 500-line source and test limit")
    parser.add_argument("--strict", action="store_true", help="Reject all legacy exceptions")
    args = parser.parse_args()
    legacy = json.loads(BASELINE.read_text(encoding="utf-8"))
    errors = violations(ROOT, tracked_sources(ROOT), legacy, args.strict)
    if errors:
        print("File size gate failed:\n" + "\n".join(errors))
        return 1
    print(f"File size gate passed (limit={LIMIT}, strict={args.strict}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
