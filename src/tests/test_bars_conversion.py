import sys
from pathlib import Path

# Ensure src root is on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
	sys.path.insert(0, str(ROOT))

from adapters import get_adapter  # type: ignore
from bars import to_market_bars  # type: ignore


def test_to_market_bars_basic(monkeypatch):
	sample = [
		[1700000000000, "50000", "50500", "49500", "50200", "123.4"],
		[1700000006000, "50200", "50600", "50100", "50400", "56.7"],
	]

	def stub_http(url, params=None, headers=None, timeout=None):
		return sample

	adapter = get_adapter("binance", http=stub_http)
	out = adapter.fetch(symbol="BTCUSDT", interval="1m", limit=2)
	bars = to_market_bars(out)
	assert len(bars) == 2
	first = bars[0]
	assert first.open == 50000.0
	assert first.provider == "binance"
	assert first.ts == 1700000000
	assert first.ohlc_tuple[1] == 50500.0
	# ensure volume parsed
	assert bars[1].volume == 56.7
