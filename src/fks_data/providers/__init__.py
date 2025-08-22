"""Provider submodules for data service.

Each provider module exposes functions returning normalized data structures.
This layer will help decouple the large monolithic service file.
"""

from .alpha import alpha_daily as alpha_daily_fn, alpha_intraday as alpha_intraday_fn, alpha_news as alpha_news_fn  # noqa
from .binance import binance_klines as binance_klines_fn  # noqa
from .polygon import polygon_aggs as polygon_aggs_fn  # noqa
from .cmc import cmc_quotes as cmc_quotes_fn  # noqa
from .yfin import sample_prices as yfin_sample_prices_fn, daily_ohlcv as yfin_daily_ohlcv_fn, crypto_ohlcv as yfin_crypto_ohlcv_fn  # noqa
