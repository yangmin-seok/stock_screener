from __future__ import annotations

import sys
from pathlib import Path


def pytest_addoption(parser):
    parser.addoption(
        "--debug-sys-path",
        action="store_true",
        default=False,
        help="Print sys.path[0] at session start for import-path debugging.",
    )


def pytest_configure(config):
    src_path = Path(__file__).resolve().parents[1] / "src"
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)

    if config.getoption("--debug-sys-path"):
        first_entry = sys.path[0] if sys.path else "<empty>"
        print(f"[debug-sys-path] sys.path[0]={first_entry}")
