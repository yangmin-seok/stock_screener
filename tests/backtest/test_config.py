from stock_screener.backtest.config import load_backtest_config


def test_load_backtest_config_strips_inline_comment_from_scalar(tmp_path):
    cfg = tmp_path / "backtest.yaml"
    cfg.write_text(
        "\n".join(
            [
                "run:",
                "  rebalance: M  # supported: W(weekly), M(monthly), Y(yearly)",
            ]
        ),
        encoding="utf-8",
    )

    loaded = load_backtest_config(cfg)

    assert loaded.run["rebalance"] == "M"
