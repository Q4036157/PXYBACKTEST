from __future__ import annotations

from pathlib import Path

from scripts.check_line_limit import violations


def test_file_size_gate_rejects_new_oversized_source_and_test(tmp_path: Path) -> None:
    source = tmp_path / "app" / "new_module.py"
    test = tmp_path / "tests" / "test_new_module.py"
    source.parent.mkdir()
    test.parent.mkdir()
    source.write_text("x\n" * 501, encoding="utf-8")
    test.write_text("x\n" * 501, encoding="utf-8")

    errors = violations(tmp_path, [source, test], {}, strict=False)

    assert len(errors) == 2
    assert "app/new_module.py" in errors[0]
    assert "tests/test_new_module.py" in errors[1]


def test_legacy_allowance_cannot_grow_and_strict_mode_rejects_it(tmp_path: Path) -> None:
    path = tmp_path / "app" / "legacy.py"
    path.parent.mkdir()
    path.write_text("x\n" * 600, encoding="utf-8")

    assert violations(tmp_path, [path], {"app/legacy.py": 600}, strict=False) == []
    assert violations(tmp_path, [path], {"app/legacy.py": 600}, strict=True)
    path.write_text("x\n" * 601, encoding="utf-8")
    assert violations(tmp_path, [path], {"app/legacy.py": 600}, strict=False)
