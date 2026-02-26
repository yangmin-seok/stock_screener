from __future__ import annotations

from dataclasses import dataclass

import stock_screener.cli as cli


@dataclass
class _SnapshotResult:
    asof_date: str
    snapshot: int


class _FakePipeline:
    foreign_flow_called = False

    def __init__(self, db_path):
        self.db_path = db_path

    def update_reserve_ratio_only(self, asof_date=None):
        return "2025-01-15", 7

    def rebuild_snapshot_only(self, asof_date=None, lookback_days=3650):
        return _SnapshotResult(asof_date=asof_date or "2025-01-15", snapshot=11)

    def run(self, *args, **kwargs):
        raise AssertionError("run() must not be called for --update-reserve-only branch")


def test_cli_update_reserve_only_does_not_collect_investor_flow(monkeypatch, capsys):
    monkeypatch.setattr(cli, "DailyBatchPipeline", _FakePipeline)
    monkeypatch.setattr(
        "sys.argv",
        [
            "stock_screener.cli",
            "--db-path",
            "data/test.db",
            "--update-reserve-only",
            "--rebuild-snapshot",
        ],
    )

    cli.main()

    out = capsys.readouterr().out
    assert "reserve_ratio updated: asof=2025-01-15, rows=7" in out
    assert "does not collect investor flow" in out
    assert "investor_flow_daily remains unchanged" in out
    assert "snapshot_metrics rebuilt: asof=2025-01-15, rows=11" in out
