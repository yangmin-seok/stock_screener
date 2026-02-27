from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class BacktestConfig:
    run: dict[str, Any]
    universe: dict[str, Any]
    filters: dict[str, Any]
    selection: dict[str, Any]
    portfolio: dict[str, Any]
    costs: dict[str, Any]
    output: dict[str, Any]


DEFAULT_CONFIG: dict[str, Any] = {
    "run": {
        "name": "example_run",
        "start_date": "2021-01-01",
        "end_date": "2021-12-31",
        "rebalance": "monthly",  # weekly/monthly/yearly: W|M|Y aliases supported
    },
    "universe": {
        "market": ["KOSPI", "KOSDAQ"],
        "min_price": None,
    },
    "filters": {
        "descriptive": {},
        "fundamental": {},
        "technical": {},
    },
    "selection": {
        "top_n": 20,
        "sort_by": "relative_value",
        "ascending": False,
    },
    "portfolio": {
        "weighting": "equal",
        "max_positions": 20,
    },
    "costs": {
        "commission_bps": 5,
        "slippage_bps": 5,
    },
    "output": {
        "dir": "outputs/backtest",
        "save_trades": True,
        "save_daily_nav": True,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
            continue
        merged[key] = value
    return merged


def _parse_scalar(raw: str) -> Any:
    text = raw.strip()
    if text in {"", "null", "None", "~"}:
        return None
    if text.lower() == "true":
        return True
    if text.lower() == "false":
        return False
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    if text.startswith("[") and text.endswith("]"):
        body = text[1:-1].strip()
        if not body:
            return []
        return [_parse_scalar(item.strip()) for item in body.split(",")]
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


def _strip_inline_comment(value: str) -> str:
    in_single = False
    in_double = False

    for idx, ch in enumerate(value):
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if ch == "#" and not in_single and not in_double:
            return value[:idx].rstrip()
    return value.rstrip()


def _load_simple_yaml(path: Path) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        if ":" not in line:
            raise ValueError(f"Unsupported YAML line: {raw_line}")

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        while stack and indent <= stack[-1][0]:
            stack.pop()
        current = stack[-1][1]

        if value == "":
            node: dict[str, Any] = {}
            current[key] = node
            stack.append((indent, node))
            continue

        current[key] = _parse_scalar(_strip_inline_comment(value))

    return root


def load_backtest_config(path: str | Path) -> BacktestConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Backtest config not found: {config_path}")

    loaded = _load_simple_yaml(config_path)
    merged = _deep_merge(DEFAULT_CONFIG, loaded)

    return BacktestConfig(
        run=merged["run"],
        universe=merged["universe"],
        filters=merged["filters"],
        selection=merged["selection"],
        portfolio=merged["portfolio"],
        costs=merged["costs"],
        output=merged["output"],
    )
