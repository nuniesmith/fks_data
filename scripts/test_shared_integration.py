"""Quick integration smoke test for shared_python within fks_data service.

Run with: `python scripts/test_shared_integration.py`
Fails (non-zero exit) if a core shared primitive is unusable.
"""
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    try:  # first attempt (installed package)
        from shared_python import (  # type: ignore
            get_settings,
            RiskParams,
            composite_position,
            TradeSignal,
            win_rate_with_costs,
            Trade,
        )
    except Exception:
        # Attempt to add relative path: ../../shared/python
        root = Path(__file__).resolve().parents[2]  # .../repos/fks
        candidate = root.parent / "shared" / "shared_python"
        if (candidate / "pyproject.toml").exists():
            sys.path.insert(0, str(candidate / "src"))
            try:
                from shared_python import (  # type: ignore
                    get_settings,
                    RiskParams,
                    composite_position,
                    TradeSignal,
                    win_rate_with_costs,
                    Trade,
                )
            except Exception as e:  # noqa: BLE001
                print(f"IMPORT_FAIL: {e}")
                return 2
        else:
            print("IMPORT_FAIL: shared_python path not found; install with pip -e ../shared/python")
            return 2

    # Settings load
    s = get_settings()
    if not getattr(s, "APP_ENV", None):
        print("SETTINGS_FAIL: APP_ENV missing")
        return 3

    # Position sizing smoke
    params = RiskParams()
    sized = composite_position(
        equity=100_000,
        price=25_000,
        side=1,
        win_prob=0.55,
        win_loss_ratio=1.2,
        recent_vol=0.4,
        avg_correlation=0.5,
        confidence=0.7,
        params=params,
    )
    if sized.position_size == 0:
        print("SIZING_WARN: got zero position size (may be fine, but flagging)")

    # Metrics smoke
    wr = win_rate_with_costs([Trade(pnl=10), Trade(pnl=-5), Trade(pnl=2)])
    if wr <= 0:
        print("METRICS_FAIL: win rate <= 0")
        return 4

    # Type instantiation
    import datetime as _dt
    _ = TradeSignal(
        symbol="BTCUSDT",
        side="LONG",
        strength=0.9,
        timestamp=_dt.datetime.utcnow(),
        strategy="demo",
        meta={"src": "integration"},
    )

    print("OK: shared integration succeeded")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
