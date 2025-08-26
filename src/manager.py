"""
Refactored data management system for trading applications.
Implements a modular, extensible architecture for asset data handling.
"""

import json
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple, Type, TypeVar, Union

import numpy as np
import pandas as pd
import requests
from loguru import logger

# Setup logging
logger = logger.opt(colors=True)


class Timeframe(Enum):
    """Enum for standard timeframes with their seconds values."""

    M1 = 60  # 1 minute
    M5 = 300  # 5 minutes
    M15 = 900  # 15 minutes
    M30 = 1800  # 30 minutes
    H1 = 3600  # 1 hour
    H4 = 14400  # 4 hours
    D1 = 86400  # 1 day
    W1 = 604800  # 1 week
    MN1 = 2592000  # ~1 month (30 days)

    @classmethod
    def get_name(cls, seconds: int) -> str:
        """Get timeframe name from seconds value."""
        # Allow for some tolerance in the seconds value (±5% of the interval)
        for tf in cls:
            if abs(tf.value - seconds) <= 0.05 * tf.value:
                return tf.name
        return f"Custom ({seconds} seconds)"

    @classmethod
    def get_pandas_freq(cls, timeframe: str) -> str:
        """Convert timeframe to pandas frequency string."""
        mapping = {
            "M1": "1T",
            "M5": "5T",
            "M15": "15T",
            "M30": "30T",
            "H1": "1H",
            "H4": "4H",
            "D1": "1D",
            "W1": "1W",
            "MN1": "1M",
        }
        return mapping.get(timeframe, "1D")  # Default to daily if unknown


# Type variable for generics
T = TypeVar("T")


class DataSource(ABC):
    """Abstract base class for data sources."""

    @abstractmethod
    def fetch_data(self, symbol: str, **kwargs) -> Optional[pd.DataFrame]:
        """Fetch data for the given symbol."""
        pass


class APIDataSource(DataSource):
    """Base class for API data sources."""

    def __init__(self, api_key: Optional[str] = None, rate_limit_delay: float = 0.5):
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay  # Delay between API calls in seconds

    def _handle_rate_limiting(self):
        """Handle rate limiting by adding a delay between requests."""
        time.sleep(self.rate_limit_delay)


class GoldAPIDataSource(APIDataSource):
    """Data source for GoldAPI.io."""

    def __init__(self, api_key: str, symbol: str = "XAU", currency: str = "USD"):
        super().__init__(api_key)
        self.symbol = symbol
        self.currency = currency

    def fetch_data(
        self, symbol: str = "", lookback_days: int = 30, **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        Fetch gold price data from GoldAPI.io

        Args:
            symbol: Override default symbol if provided
            lookback_days: Number of historical days to fetch

        Returns:
            DataFrame with gold price data or None if failed
        """
        try:
            logger.info("Fetching gold data from GoldAPI.io...")

            # Use provided symbol or default
            actual_symbol = symbol or self.symbol

            # First, get today's data
            current_data = self._get_gold_api_data()
            if not current_data:
                logger.error("Failed to fetch current gold price data from API")
                return None

            # Create a list to store daily data
            all_data = []

            # Add today's data
            today_date = datetime.now().strftime("%Y-%m-%d")

            # Extract price data, with fallbacks for missing fields
            all_data.append(self._extract_price_data(current_data, today_date))

            # Now try to get some historical data
            successful_days = 1  # We already have today
            current_date = datetime.now()

            # Try to get a reasonable amount of history
            for i in range(1, min(lookback_days, 90)):
                try:
                    past_date = current_date - timedelta(days=i)
                    date_str = past_date.strftime("%Y%m%d")

                    # Only attempt to get data for trading days (approximate)
                    if past_date.weekday() < 5:  # Monday-Friday
                        past_data = self._get_gold_api_data(date_str)

                        if past_data:
                            all_data.append(
                                self._extract_price_data(
                                    past_data, past_date.strftime("%Y-%m-%d")
                                )
                            )
                            successful_days += 1

                except Exception as e:
                    logger.warning(
                        f"Error fetching historical data for {date_str}: {e}"
                    )
                    continue

            # If we didn't get enough data, generate synthetic data
            if successful_days < 5:
                logger.info(
                    f"Only got {successful_days} days of data from API, generating additional synthetic data"
                )

                # Get the last real price we have
                last_real_price = all_data[0]["close"]
                last_real_date = datetime.strptime(all_data[0]["datetime"], "%Y-%m-%d")

                # Generate synthetic historical prices
                for i in range(successful_days, lookback_days):
                    past_date = current_date - timedelta(days=i)

                    # Skip weekends in synthetic data too
                    if past_date.weekday() >= 5:  # Saturday or Sunday
                        continue

                    # Generate a plausible price change
                    days_back = (last_real_date - past_date).days
                    volatility = 0.005 * min(
                        days_back, 10
                    )  # Increase volatility for older dates

                    # Random walk based on previous price
                    change_pct = np.random.normal(0, volatility)
                    close_price = last_real_price * (1 - change_pct * days_back / 10)

                    # Generate plausible intraday range
                    daily_volatility = 0.005
                    high_price = close_price * (
                        1 + abs(np.random.normal(0, daily_volatility))
                    )
                    low_price = close_price * (
                        1 - abs(np.random.normal(0, daily_volatility))
                    )
                    open_price = close_price * (
                        1 + np.random.normal(0, daily_volatility / 2)
                    )

                    # Ensure high is highest and low is lowest
                    high_price = max(high_price, open_price, close_price)
                    low_price = min(low_price, open_price, close_price)

                    all_data.append(
                        {
                            "datetime": past_date.strftime("%Y-%m-%d"),
                            "open": round(open_price, 2),
                            "high": round(high_price, 2),
                            "low": round(low_price, 2),
                            "close": round(close_price, 2),
                            "volume": 0,
                        }
                    )

            # Create DataFrame and sort by date
            df = pd.DataFrame(all_data)
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime")

            # Clean the data - ensure no NaN values
            df = DataProcessor.clean_dataframe(df)

            # Return the DataFrame
            return df

        except Exception as e:
            logger.error(f"Error fetching gold data from API: {e}")
            return None

    def _extract_price_data(self, data_dict: Dict, date_str: str) -> Dict:
        """Extract price data from API response with fallbacks."""
        open_price = self._safe_extract(
            data_dict, "price_gram_24k", data_dict.get("price", 0)
        )
        high_price = self._safe_extract(
            data_dict, "high_price", data_dict.get("price", 0)
        )
        low_price = self._safe_extract(
            data_dict, "low_price", data_dict.get("price", 0)
        )
        close_price = self._safe_extract(data_dict, "price", 0)

        return {
            "datetime": date_str,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": 0,  # API doesn't provide volume data
        }

    def _safe_extract(self, data_dict: Dict, key: str, default: Any = None) -> Any:
        """Safely extract a value from a dictionary, with type checking."""
        value = data_dict.get(key, default)

        # Ensure we don't have strings where we need numbers
        if (
            isinstance(value, str)
            and default is not None
            and isinstance(default, (int, float))
        ):
            try:
                return float(value)
            except ValueError:
                return default

        return value

    def _get_gold_api_data(self, date_str: str = "") -> Optional[Dict]:
        """
        Internal method to make a request to GoldAPI.io

        Args:
            date_str: Optional date string in format YYYYMMDD

        Returns:
            Dictionary with gold price data or None if failed
        """
        # Build the URL - for date requests, add a date parameter
        date_param = f"/{date_str}" if date_str else ""
        url = f"https://www.goldapi.io/api/{self.symbol}/{self.currency}{date_param}"

        headers = {"x-access-token": self.api_key, "Content-Type": "application/json"}

        try:
            # Add delay between requests to avoid rate limiting
            self._handle_rate_limiting()

            response = requests.get(url, headers=headers)

            # Check if we hit rate limits
            if response.status_code == 429:
                logger.error(f"API request error: 429 Too Many Requests for url: {url}")
                return None

            response.raise_for_status()

            # Parse JSON response
            result = json.loads(response.text)
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"API request error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            return None


class YFinanceDataSource(DataSource):
    """Data source for Yahoo Finance."""

    def __init__(self):
        # Check if yfinance is available
        try:
            import yfinance as yf  # type: ignore

            self.yf = yf
            self.available = True
        except ImportError:
            logger.warning("YFinance package not available")
            self.available = False

    def fetch_data(
        self,
        symbol: str,
        period: str = "1y",
        interval: str = "1d",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch data from Yahoo Finance for the given symbol.

        Args:
            symbol: The ticker symbol to fetch
            period: Time period to fetch (e.g., "1d", "5d", "1mo", "1y")
            interval: Data interval (e.g., "1m", "5m", "1h", "1d")

        Returns:
            DataFrame with price data or None if failed
        """
        _ = kwargs  # Explicitly access kwargs to avoid unused variable warning
        if not self.available:
            logger.error("YFinance package not available")
            return None

        try:
            # Map common symbols to YFinance tickers
            yf_symbol_map = {
                "GOLD": "GC=F",
                "SILVER": "SI=F",
                "BTCUSD": "BTC-USD",
                "ETHUSD": "ETH-USD",
                "USOIL": "CL=F",
            }

            # Use the mapped symbol if available, otherwise use as-is
            yf_symbol = yf_symbol_map.get(symbol.upper(), symbol)

            logger.info(f"Downloading {symbol} data from YFinance as {yf_symbol}...")

            # Download the data
            if start_date is not None or end_date is not None:
                # Use explicit date window when provided
                data = self.yf.download(
                    yf_symbol,
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    auto_adjust=True,
                    progress=False,
                )
            else:
                data = self.yf.download(
                    yf_symbol, period=period, interval=interval, auto_adjust=True, progress=False
                )

            if data is None:
                logger.warning(
                    f"No data returned from YFinance for {yf_symbol} (None returned)"
                )
                return None

            if hasattr(data, "empty") and data.empty:
                logger.warning(f"No data returned from YFinance for {yf_symbol}")
                return None

            # Process the returned data
            # Log the actual columns returned by yfinance for debugging
            if hasattr(data, "columns"):
                logger.info(f"YFinance columns: {data.columns.tolist()}")

            # Handle potential multi-level columns
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                logger.info("Detected multi-level columns, flattening...")
                # Flatten multi-level columns
                data.columns = [
                    "_".join(col).strip() if isinstance(col, tuple) else col
                    for col in data.columns
                ]

            # Reset index to convert Date to column
            if hasattr(data, "reset_index"):
                data = data.reset_index()
            else:
                logger.error(
                    "Data returned from YFinance does not have reset_index method"
                )
                return None

            # Convert all column names to lowercase for consistency
            if hasattr(data, "columns"):
                data.columns = [str(col).lower() for col in data.columns]

            # Rename the date column if needed
            if (
                hasattr(data, "columns")
                and "date" in data.columns
                and "datetime" not in data.columns
            ):
                data.rename(columns={"date": "datetime"}, inplace=True)

            # Check if we have the required column names (now all lowercase)
            required_columns = ["open", "high", "low", "close"]
            missing_columns = [
                col
                for col in required_columns
                if hasattr(data, "columns") and col not in data.columns
            ]

            if missing_columns:
                missing_cols_str = ", ".join(missing_columns)
                logger.error(
                    f"Missing required columns in YFinance data: {missing_cols_str}"
                )
                return None

            # Make sure datetime is in datetime format
            if hasattr(data, "columns") and "datetime" in data.columns:
                data["datetime"] = pd.to_datetime(data["datetime"])

            # Clean the data before returning
            return DataProcessor.clean_dataframe(data)

        except Exception as e:
            logger.error(f"Error fetching data from YFinance: {e}")
            return None


class DataProcessor:
    """Utility class for data processing operations."""

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Clean a dataframe of NaN values and ensure proper types."""
        if df is None or df.empty:
            return df

        # Replace any NaN values in numeric columns
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                # Use ffill() and bfill() directly
                df[col] = df[col].ffill().bfill()

                # If still have NaN, use median or default
                if df[col].isna().any():
                    if df[col].notna().any():
                        median_value = df[col].median()
                        df[col] = df[col].fillna(median_value)
                    else:
                        # If all NaN, use a default
                        df[col] = df[col].fillna(1000.0)  # Default price if all missing

        # Ensure volume is an integer and has no NaN
        if "volume" in df.columns:
            df["volume"] = df["volume"].fillna(0).astype("int64")

        return df

    @staticmethod
    def analyze_timeframe(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze the timeframe of a DataFrame with DatetimeIndex.

        Args:
            df: DataFrame with datetime index or column

        Returns:
            Dictionary with timeframe information
        """
        result = {"timeframe": "Unknown", "irregularities": 0.0, "gaps_detected": False}

        # If DataFrame doesn't have a DatetimeIndex, try to use a datetime column
        if not isinstance(df.index, pd.DatetimeIndex):
            datetime_cols = [
                col
                for col in df.columns
                if "date" in col.lower() or "time" in col.lower()
            ]
            if datetime_cols:
                # Use the first datetime column
                df = df.set_index(datetime_cols[0])
            else:
                return result

        # Determine timeframe from data
        if len(df) > 1:
            # Calculate all time differences between consecutive points
            time_diffs = df.index.to_series().diff().dropna()

            if not time_diffs.empty:
                # Find the most common time difference (mode)
                # Convert to seconds for easier handling
                time_diff_seconds = time_diffs.dt.total_seconds()

                # Get the most common interval
                most_common_interval = time_diff_seconds.mode().iloc[0]

                # Count how many intervals differ from the mode
                total_intervals = len(time_diff_seconds)
                regular_intervals = (
                    abs(time_diff_seconds - most_common_interval)
                    <= 0.05 * most_common_interval
                ).sum()

                # Calculate percentage of irregularity
                irregularity_pct = 100 * (1 - regular_intervals / total_intervals)
                result["irregularities"] = round(irregularity_pct, 2)

                # Set the timeframe based on the most common interval
                result["timeframe"] = Timeframe.get_name(int(most_common_interval))

                # Detect possible gaps
                # A gap is when the difference is significantly more than the regular interval
                gap_threshold = 2.5 * most_common_interval  # 2.5x normal interval
                gaps = (time_diff_seconds > gap_threshold).sum()
                result["gaps_detected"] = gaps > 0
                if gaps > 0:
                    result["gap_count"] = gaps
                    logger.info(
                        f"Detected {gaps} potential gaps in the time series data"
                    )

        return result

    @staticmethod
    def resample_data(df: pd.DataFrame, target_timeframe: str) -> pd.DataFrame:
        """
        Resample data to a target timeframe.

        Args:
            df: DataFrame with DatetimeIndex
            target_timeframe: Target timeframe (e.g., 'M1', 'H1', 'D1')

        Returns:
            Resampled DataFrame
        """
        # Check if we have a valid DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            datetime_cols = [
                col
                for col in df.columns
                if "date" in col.lower() or "time" in col.lower()
            ]
            if datetime_cols:
                df = df.set_index(datetime_cols[0])
            else:
                logger.error("Cannot resample data without a DatetimeIndex")
                return df

        # Get pandas frequency string for target timeframe
        target_freq = Timeframe.get_pandas_freq(target_timeframe)

        # Resample the data
        try:
            # For upsampling to a higher timeframe (e.g., M1 -> H1)
            aggregation = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
            }

            # Only include volume if it exists
            if "volume" in df.columns:
                aggregation["volume"] = "sum"

            resampled = df.resample(target_freq).agg(**aggregation).dropna()

            # Get current timeframe info
            current_tf_info = DataProcessor.analyze_timeframe(df)
            current_timeframe = current_tf_info["timeframe"]

            logger.info(
                f"Resampled data from {current_timeframe} to {target_timeframe}, resulting in {len(resampled)} rows"
            )

            return resampled

        except Exception as e:
            logger.error(f"Error resampling data: {e}")
            return df

    @staticmethod
    def detect_missing_data(df: pd.DataFrame) -> Dict:
        """
        Detect missing data points in a time series.

        Args:
            df: DataFrame with DatetimeIndex

        Returns:
            Dictionary with missing data information
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            return {"error": "Not a time series with DatetimeIndex"}

        # Get the typical time interval
        time_diffs = df.index.to_series().diff().dropna()
        if time_diffs.empty:
            return {"error": "Not enough data points to analyze"}

        # Get most common interval in seconds
        most_common_interval = time_diffs.dt.total_seconds().mode().iloc[0]

        # Create a complete time series with the expected frequency
        if most_common_interval < 86400:  # Less than a day
            # For intraday data, we need to be careful about market hours
            # This is simplified and might need adjustment for specific markets
            freq = f"{int(most_common_interval)}S"
        else:
            # For daily or higher, we can use calendar days
            freq = f"{int(most_common_interval / 86400)}D"

        # Create an ideal index
        ideal_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)

        # Check for missing points
        missing_points = ideal_index.difference(df.index)

        return {
            "total_expected": len(ideal_index),
            "actual_points": len(df),
            "missing_points": len(missing_points),
            "missing_percentage": round(
                100 * len(missing_points) / len(ideal_index), 2
            ),
            "first_missing": missing_points[0] if len(missing_points) > 0 else None,
            "sample_missing": (
                missing_points[:5].tolist() if len(missing_points) > 0 else []
            ),
        }

    @staticmethod
    def fill_missing_data(df: pd.DataFrame, method: str = "ffill") -> pd.DataFrame:
        """
        Fill missing data points in a time series.

        Args:
            df: DataFrame with DatetimeIndex
            method: Method to fill missing data ('ffill', 'bfill', 'interpolate')

        Returns:
            DataFrame with filled missing data
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            logger.error("Cannot fill missing data without a DatetimeIndex")
            return df

        # Get the most common time interval
        time_diffs = df.index.to_series().diff().dropna()
        if time_diffs.empty:
            return df

        most_common_interval = time_diffs.dt.total_seconds().mode().iloc[0]

        # Convert to a frequency string
        if most_common_interval < 60:
            freq = f"{int(most_common_interval)}S"  # Seconds
        elif most_common_interval < 3600:
            freq = f"{int(most_common_interval / 60)}T"  # Minutes
        elif most_common_interval < 86400:
            freq = f"{int(most_common_interval / 3600)}H"  # Hours
        else:
            freq = f"{int(most_common_interval / 86400)}D"  # Days

        # Create a complete time series
        full_idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)

        # Reindex the DataFrame
        df_reindexed = df.reindex(full_idx)

        # Fill missing values
        if method == "ffill":
            df_filled = df_reindexed.ffill()
        elif method == "bfill":
            df_filled = df_reindexed.bfill()
        elif method == "interpolate":
            df_filled = df_reindexed.interpolate(method="time")
        else:
            logger.warning(f"Unknown fill method: {method}, using forward fill")
            df_filled = df_reindexed.ffill()

        # Make sure we don't have any remaining NaNs
        df_filled = df_filled.ffill().bfill()

        logger.info(
            f"Filled {len(full_idx) - len(df)} missing data points using {method}"
        )

        return df_filled


class AssetDataFactory:
    """Factory for creating asset-specific data managers."""

    @staticmethod
    def get_manager(asset_type: str, config: Dict[str, Any]) -> "AssetDataManager":
        """
        Get an asset-specific data manager based on asset type.

        Args:
            asset_type: Type of asset (e.g., 'GOLD', 'CRYPTO')
            config: Configuration for the asset manager

        Returns:
            Instance of AssetDataManager
        """
        asset_type = asset_type.upper()

        if asset_type == "GOLD":
            return GoldDataManager(config)
        elif asset_type in ["BTC", "BTCUSD", "ETH", "ETHUSD"]:
            return CryptoDataManager(config)
        else:
            return GenericAssetDataManager(config)


class AssetDataManager:
    """Base class for asset-specific data managers."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_cache = {}

    def get_data_info(self, data_path: str) -> Dict[str, Any]:
        """
        Get detailed information about a data file.

        Args:
            data_path: Path to the data file

        Returns:
            Dictionary with data information
        """
        if not os.path.exists(data_path):
            return {
                "filename": os.path.basename(data_path),
                "exists": False,
                "error": "File not found",
            }

        try:
            df = self.load_data(data_path)

            info = {
                "filename": os.path.basename(data_path),
                "exists": True,
                "rows": len(df),
            }

            # Analyze timeframe if we have a DatetimeIndex
            if isinstance(df.index, pd.DatetimeIndex):
                timeframe_info = DataProcessor.analyze_timeframe(df)
                info.update(timeframe_info)

                # Add date range information
                start_date = df.index[0]
                end_date = df.index[-1]
                info["start_date"] = start_date
                info["end_date"] = end_date
                info["date_range"] = (
                    f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
                )

                # Add timezone information
                info["timezone"] = str(df.index.tz) if df.index.tz else "None"
            else:
                info["timeframe"] = "Unknown"
                info["date_range"] = f"Rows: {len(df)}"

            return info

        except Exception as e:
            logger.error(f"Error analyzing data file: {e}")
            return {
                "filename": os.path.basename(data_path),
                "exists": True,
                "error": str(e),
            }

    def load_data(
        self, data_path: str, date_range: Optional[Tuple[datetime, datetime]] = None
    ) -> pd.DataFrame:
        """
        Load data from a file with optional date filtering.

        Args:
            data_path: Path to the data file
            date_range: Optional (start_date, end_date) tuple

        Returns:
            DataFrame with the loaded data
        """
        try:
            # Determine file type from extension
            if data_path.endswith(".csv"):
                df = pd.read_csv(data_path)
            elif data_path.endswith(".xlsx"):
                df = pd.read_excel(data_path)
            else:
                raise ValueError(f"Unsupported file format: {data_path}")

            # Clean the dataframe
            df = DataProcessor.clean_dataframe(df)

            # Check for datetime column
            datetime_columns = [
                col
                for col in df.columns
                if "date" in col.lower() or "time" in col.lower()
            ]

            # If we found a datetime column, convert it to datetime and set as index
            if datetime_columns:
                datetime_col = datetime_columns[0]
                # Convert to datetime, handle various formats
                df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")
                # Drop rows with invalid datetimes
                df = df.dropna(subset=[datetime_col])
                # Set datetime as index
                df = df.set_index(datetime_col)
            else:
                logger.warning(f"No datetime column found in {data_path}")

            # Convert column names to lowercase for consistency
            df.columns = [col.lower() for col in df.columns]

            # Filter by date range if provided
            if date_range and isinstance(df.index, pd.DatetimeIndex):
                start_date, end_date = date_range
                if start_date and end_date:
                    # Using .loc for inclusive range
                    df = df.loc[start_date:end_date]
                    logger.info(
                        f"Filtered data to date range: {start_date} to {end_date}, {len(df)} rows remaining"
                    )

            return df

        except Exception as e:
            logger.error(f"Error loading data file: {e}")
            raise

    def get_data_for_timeframe(
        self,
        data_path: str,
        target_timeframe: str,
        date_range: Optional[Tuple[datetime, datetime]] = None,
    ) -> pd.DataFrame:
        """
        Get data resampled to the specified timeframe.

        Args:
            data_path: Path to the data file
            target_timeframe: Target timeframe (e.g., 'M1', 'H1', 'D1')
            date_range: Optional date range filter

        Returns:
            DataFrame with resampled data
        """
        # Create a cache key
        cache_key = f"{data_path}_{target_timeframe}"
        if date_range:
            cache_key += f"_{date_range[0].strftime('%Y%m%d')}_{date_range[1].strftime('%Y%m%d')}"

        # Check if we have this in cache
        if cache_key in self.data_cache:
            logger.info(f"Using cached data for {cache_key}")
            return self.data_cache[cache_key]

        # Load the data
        df = self.load_data(data_path, date_range)

        # Get current timeframe
        timeframe_info = DataProcessor.analyze_timeframe(df)
        current_timeframe = timeframe_info["timeframe"]

        # If current and target timeframes are the same, return as-is
        if current_timeframe == target_timeframe:
            self.data_cache[cache_key] = df
            return df

        # Resample to target timeframe
        resampled = DataProcessor.resample_data(df, target_timeframe)

        # Cache the result
        self.data_cache[cache_key] = resampled

        return resampled

    @abstractmethod
    def create_sample_data(self) -> pd.DataFrame:
        """Create sample data for this asset type."""
        pass

    @abstractmethod
    def try_fetch_data(self) -> bool:
        """Try to fetch real data for this asset type."""
        pass


class GoldDataManager(AssetDataManager):
    """Asset data manager for gold price data."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        # Initialize data sources for gold
        self.data_sources = []

        # Add GoldAPI source if configured
        if "goldapi_key" in config:
            self.data_sources.append(
                GoldAPIDataSource(
                    api_key=config["goldapi_key"],
                    symbol=config.get("symbol", "XAU"),
                    currency=config.get("currency", "USD"),
                )
            )

        # Add YFinance source as fallback
        self.data_sources.append(YFinanceDataSource())

    def create_sample_data(self) -> pd.DataFrame:
        """Create sample gold price data."""
        # Get the target file path
        data_path = Path(self.config.get("data_path", "/app/data/gold_data.csv"))
        data_path.parent.mkdir(exist_ok=True)

        # Create sample date range (last 100 days)
        end_date = datetime.now()
        dates = [end_date - timedelta(days=i) for i in range(100)]
        dates.reverse()

        # Create random price data (simulated gold prices)
        start_price = 1800.0
        close_prices = [start_price]

        # Generate somewhat realistic price movements
        for i in range(1, len(dates)):
            change_pct = np.random.normal(
                0, 0.01
            )  # Normal distribution with 1% std dev
            new_price = close_prices[-1] * (1 + change_pct)
            close_prices.append(new_price)

        # Create OHLC data
        data = []
        for i, date in enumerate(dates):
            close = close_prices[i]
            # Random but realistic high, low, open based on close
            high = close * (1 + abs(np.random.normal(0, 0.005)))
            low = close * (1 - abs(np.random.normal(0, 0.005)))

            if i > 0:
                # Open based on previous close with small gap
                open_price = close_prices[i - 1] * (1 + np.random.normal(0, 0.002))
            else:
                open_price = close * (1 - np.random.normal(0, 0.003))

            data.append(
                {
                    "datetime": date.strftime("%Y-%m-%d"),
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "close": round(close, 2),
                    "volume": int(np.random.normal(10000, 2000)),
                }
            )

        # Create a DataFrame and save to CSV
        df = pd.DataFrame(data)

        # Convert datetime to proper datetime objects
        df["datetime"] = pd.to_datetime(df["datetime"])

        # Save with datetime as a column (not index)
        df.to_csv(data_path, index=False)

        logger.info(f"Created sample gold data file at {data_path}")
        return df

    def try_fetch_data(self) -> bool:
        """Try to fetch real gold data from available sources."""
        data_path = Path(self.config.get("data_path", "/app/data/gold_data.csv"))

        # Try each data source in sequence
        for source in self.data_sources:
            try:
                logger.info(
                    f"Attempting to fetch gold data using {source.__class__.__name__}..."
                )
                gold_df = source.fetch_data(symbol="GOLD")

                if gold_df is not None and not gold_df.empty:
                    # We got data, save it
                    gold_df.to_csv(data_path, index=False)
                    logger.info(f"Successfully fetched gold data to {data_path}")
                    return True
            except Exception as e:
                logger.error(
                    f"Error fetching gold data from {source.__class__.__name__}: {e}"
                )

        # If all sources failed, create sample data
        logger.warning("All data sources failed, creating sample gold data")
        self.create_sample_data()
        return False


class CryptoDataManager(AssetDataManager):
    """Asset data manager for cryptocurrency data."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.symbol = config.get("symbol", "BTCUSD")

        # Initialize data sources
        self.data_sources = [YFinanceDataSource()]

    def create_sample_data(self) -> pd.DataFrame:
        """Create sample cryptocurrency price data."""
        # Get the target file path
        data_path = Path(
            self.config.get("data_path", f"/app/data/{self.symbol.lower()}_data.csv")
        )
        data_path.parent.mkdir(exist_ok=True)

        # Create sample date range (last 365 days for crypto)
        end_date = datetime.now()
        dates = [end_date - timedelta(days=i) for i in range(365)]
        dates.reverse()

        # Set parameters based on crypto type
        if "BTC" in self.symbol:
            start_price = 30000.0
            daily_volatility = 0.03  # 3%
            price_formatter = lambda x: round(x, 1)
        elif "ETH" in self.symbol:
            start_price = 2000.0
            daily_volatility = 0.04  # 4%
            price_formatter = lambda x: round(x, 1)
        else:
            # Default for unknown crypto
            start_price = 100.0
            daily_volatility = 0.05  # 5%
            price_formatter = lambda x: round(x, 2)

        # Generate price data with realistic movements
        close_prices = [start_price]

        for i in range(1, len(dates)):
            # Crypto trades 24/7 so no weekend effect
            # Generate a random price change with some trend component
            trend_component = 0.0002 * (np.random.random() - 0.5)  # Small trend
            random_component = np.random.normal(0, daily_volatility)
            change_pct = random_component + trend_component

            # Apply the change to the previous price
            new_price = close_prices[-1] * (1 + change_pct)

            # Ensure price stays positive and reasonable
            new_price = max(new_price, start_price * 0.1)  # Don't go below 10% of start
            close_prices.append(new_price)

        # Create OHLC data
        data = []
        for i, date in enumerate(dates):
            close = close_prices[i]

            # Crypto often has higher intraday volatility
            intraday_volatility = daily_volatility / 1.5

            high = close * (1 + abs(np.random.normal(0, intraday_volatility)))
            low = close * (1 - abs(np.random.normal(0, intraday_volatility)))

            if i > 0:
                # Open based on previous close with small gap
                open_price = close_prices[i - 1] * (
                    1 + np.random.normal(0, intraday_volatility / 2)
                )
            else:
                open_price = close * (1 - np.random.normal(0, intraday_volatility / 3))

            # Make sure high is highest and low is lowest
            high = max(high, open_price, close)
            low = min(low, open_price, close)

            # Add hourly timestamps for more granular data
            base_date = date.replace(hour=0, minute=0, second=0, microsecond=0)

            # For demo purposes, just create one daily record
            data.append(
                {
                    "datetime": date.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": price_formatter(open_price),
                    "high": price_formatter(high),
                    "low": price_formatter(low),
                    "close": price_formatter(close),
                    "volume": int(
                        np.random.normal(10000, 2000) * 10
                    ),  # Higher volume for crypto
                }
            )

        # Create a DataFrame and save to CSV
        df = pd.DataFrame(data)

        # Convert datetime to proper datetime objects
        df["datetime"] = pd.to_datetime(df["datetime"])

        # Save with datetime as a column (not index)
        df.to_csv(data_path, index=False)

        logger.info(f"Created sample {self.symbol} data file at {data_path}")
        return df

    def try_fetch_data(self) -> bool:
        """Try to fetch real cryptocurrency data."""
        data_path = Path(
            self.config.get("data_path", f"/app/data/{self.symbol.lower()}_data.csv")
        )

        # Try each data source
        for source in self.data_sources:
            try:
                logger.info(
                    f"Attempting to fetch {self.symbol} data using {source.__class__.__name__}..."
                )

                # For crypto, use hourly data for more granularity
                crypto_df = source.fetch_data(
                    symbol=self.symbol, period="1y", interval="1h"
                )

                if crypto_df is not None and not crypto_df.empty:
                    # We got data, save it
                    crypto_df.to_csv(data_path, index=False)
                    logger.info(
                        f"Successfully fetched {self.symbol} data to {data_path}"
                    )
                    return True
            except Exception as e:
                logger.error(
                    f"Error fetching {self.symbol} data from {source.__class__.__name__}: {e}"
                )

        # If all sources failed, create sample data
        logger.warning(f"All data sources failed, creating sample {self.symbol} data")
        self.create_sample_data()
        return False


class GenericAssetDataManager(AssetDataManager):
    """Generic asset data manager for other asset types."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.symbol = config.get("symbol", "UNKNOWN")

        # Initialize with YFinance as the default source
        self.data_sources = [YFinanceDataSource()]

    def create_sample_data(self) -> pd.DataFrame:
        """Create sample data for a generic asset."""
        # Get the target file path
        data_path = Path(
            self.config.get("data_path", f"/app/data/{self.symbol.lower()}_data.csv")
        )
        data_path.parent.mkdir(exist_ok=True)

        # Create sample date range (last 100 days)
        end_date = datetime.now()
        dates = [end_date - timedelta(days=i) for i in range(100)]
        dates.reverse()

        # Set some generic parameters
        start_price = 100.0
        daily_volatility = 0.02  # 2%

        # Generate price data
        close_prices = [start_price]

        for i in range(1, len(dates)):
            is_weekend = dates[i].weekday() >= 5

            # Different behavior for weekends
            volatility_modifier = 0.3 if is_weekend else 1.0

            change_pct = np.random.normal(0, daily_volatility * volatility_modifier)
            new_price = close_prices[-1] * (1 + change_pct)
            close_prices.append(new_price)

        # Create OHLC data
        data = []
        for i, date in enumerate(dates):
            close = close_prices[i]

            # Random but realistic high, low, open based on close
            high = close * (1 + abs(np.random.normal(0, 0.005)))
            low = close * (1 - abs(np.random.normal(0, 0.005)))

            if i > 0:
                # Open based on previous close with small gap
                open_price = close_prices[i - 1] * (1 + np.random.normal(0, 0.002))
            else:
                open_price = close * (1 - np.random.normal(0, 0.003))

            data.append(
                {
                    "datetime": date.strftime("%Y-%m-%d"),
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "close": round(close, 2),
                    "volume": int(np.random.normal(10000, 2000)),
                }
            )

        # Create a DataFrame and save to CSV
        df = pd.DataFrame(data)

        # Convert datetime to proper datetime objects
        df["datetime"] = pd.to_datetime(df["datetime"])

        # Save with datetime as a column (not index)
        df.to_csv(data_path, index=False)

        logger.info(f"Created sample {self.symbol} data file at {data_path}")
        return df

    def try_fetch_data(self) -> bool:
        """Try to fetch real data for this asset."""
        data_path = Path(
            self.config.get("data_path", f"/app/data/{self.symbol.lower()}_data.csv")
        )

        # Try each data source
        for source in self.data_sources:
            try:
                logger.info(
                    f"Attempting to fetch {self.symbol} data using {source.__class__.__name__}..."
                )

                asset_df = source.fetch_data(
                    symbol=self.symbol, period="1y", interval="1d"
                )

                if asset_df is not None and not asset_df.empty:
                    # We got data, save it
                    asset_df.to_csv(data_path, index=False)
                    logger.info(
                        f"Successfully fetched {self.symbol} data to {data_path}"
                    )
                    return True
            except Exception as e:
                logger.error(
                    f"Error fetching {self.symbol} data from {source.__class__.__name__}: {e}"
                )

        # If all sources failed, create sample data
        logger.warning(f"All data sources failed, creating sample {self.symbol} data")
        self.create_sample_data()
        return False


class DataManager:
    """
    Main data manager class that acts as a facade to the different asset managers.
    """

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataManager with configuration.

        Args:
            config_dict: Optional configuration dictionary
        """
        # Load configuration
        self.config = config_dict or {}

        # Initialize asset managers dict
        self.asset_managers = {}

        # Initialize data cache
        self.data_cache = {}

        # Current data info - for backward compatibility
        self.current_data_info = {
            "filename": "",
            "rows": 0,
            "timeframe": "",
            "date_range": "",
            "start_date": None,
            "end_date": None,
            "gaps_detected": False,
            "irregularities": 0.0,
        }

        # Lazy import holder for adapter factory (Week 2 integration)
        self._adapter_factory = None

    # ---------------- Adapter Layer Integration -----------------
    def fetch_market_data(self, provider: str, **kwargs) -> Dict[str, Any]:
        """Fetch normalized market data via unified adapter layer.

        Args:
            provider: Adapter name (e.g. 'binance', 'polygon').
            **kwargs: Provider-specific request params.

        Returns:
            Canonical dict: { provider: str, data: list[dict], request: dict }

        Notes:
            - Wraps import in-method to avoid hard dependency if older
              workflows use DataManager without adapters.
            - Raises ValueError if adapter unknown (mirrors factory behavior).
        """
        if self._adapter_factory is None:
            try:  # defer import so legacy usages of DataManager remain unaffected if path differs
                from fks_data.adapters import get_adapter  # type: ignore
                self._adapter_factory = get_adapter
            except Exception as e:  # pragma: no cover
                raise RuntimeError(f"Adapter layer unavailable: {e}")
        adapter = self._adapter_factory(provider)
        return adapter.fetch(**kwargs)

    def get_asset_manager(self, asset_symbol: str) -> AssetDataManager:
        """
        Get or create an asset manager for the specified symbol.

        Args:
            asset_symbol: Asset symbol (e.g., 'GOLD', 'BTCUSD')

        Returns:
            AssetDataManager instance
        """
        asset_symbol = asset_symbol.upper()

        # Check if we already have a manager for this asset
        if asset_symbol in self.asset_managers:
            return self.asset_managers[asset_symbol]

        # Get asset-specific configuration
        asset_config = self.config.get("assets", {}).get(asset_symbol, {})

        # Add default data path if not specified
        if "data_path" not in asset_config:
            asset_config["data_path"] = f"/app/data/{asset_symbol.lower()}_data.csv"

        # Determine asset type
        if asset_symbol == "GOLD":
            asset_type = "GOLD"
        elif asset_symbol in ["BTCUSD", "BTC", "ETHUSD", "ETH"]:
            asset_type = "CRYPTO"
        else:
            asset_type = "GENERIC"

        # Create the appropriate asset manager
        manager = AssetDataFactory.get_manager(asset_type, asset_config)

        # Store for future use
        self.asset_managers[asset_symbol] = manager

        return manager

    def get_available_data_files(self) -> List[str]:
        """Get list of available data files in the data directory."""
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)

        # Find all CSV files in the data directory
        data_files = list(data_dir.glob("*.csv"))
        data_files.extend(list(data_dir.glob("*.xlsx")))

        # Convert to relative paths
        try:
            return [str(file.relative_to(Path.cwd())) for file in data_files]
        except ValueError:
            # Fallback if relative_to fails
            return [str(file) for file in data_files]

    def create_sample_gold_data(self) -> pd.DataFrame:
        """Create sample gold data (compatibility method)."""
        gold_manager = self.get_asset_manager("GOLD")
        return gold_manager.create_sample_data()

    def try_fetch_gold_data(self) -> bool:
        """Try to fetch gold data (compatibility method)."""
        gold_manager = self.get_asset_manager("GOLD")
        return gold_manager.try_fetch_data()

    def load_data_info(self, data_path: str) -> bool:
        """Load and analyze information about a data file (compatibility method)."""
        # Extract asset symbol from filename
        filename = os.path.basename(data_path).lower()

        # Try to determine asset type from filename
        if "gold" in filename:
            asset_symbol = "GOLD"
        elif "btc" in filename or "bitcoin" in filename:
            asset_symbol = "BTCUSD"
        elif "eth" in filename or "ethereum" in filename:
            asset_symbol = "ETHUSD"
        else:
            # Default to generic if we can't determine
            asset_symbol = "GENERIC"

        # Get the appropriate asset manager
        asset_manager = self.get_asset_manager(asset_symbol)

        try:
            # Get data info from the asset manager
            info = asset_manager.get_data_info(data_path)

            # Update current_data_info for backward compatibility
            self.current_data_info = {
                "filename": info.get("filename", ""),
                "rows": info.get("rows", 0),
                "timeframe": info.get("timeframe", "Unknown"),
                "date_range": info.get("date_range", "Unknown"),
                "start_date": info.get("start_date", None),
                "end_date": info.get("end_date", None),
                "gaps_detected": info.get("gaps_detected", False),
                "irregularities": info.get("irregularities", 0.0),
            }

            return True
        except Exception as e:
            logger.error(f"Error loading data info: {e}")
            self.current_data_info = {
                "filename": os.path.basename(data_path),
                "rows": 0,
                "timeframe": "Unknown",
                "date_range": "Error loading data",
                "start_date": None,
                "end_date": None,
                "gaps_detected": False,
                "irregularities": 0.0,
            }
            return False

    def load_data(
        self, data_path: str, date_range: Optional[Tuple[datetime, datetime]] = None
    ) -> pd.DataFrame:
        """Load data from file (compatibility method)."""
        # Create a cache key
        cache_key = f"load_{data_path}"
        if date_range:
            cache_key += f"_{date_range[0].strftime('%Y%m%d')}_{date_range[1].strftime('%Y%m%d')}"

        # Check cache first
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]

        # Extract asset symbol from filename
        filename = os.path.basename(data_path).lower()

        # Try to determine asset type from filename
        if "gold" in filename:
            asset_symbol = "GOLD"
        elif "btc" in filename or "bitcoin" in filename:
            asset_symbol = "BTCUSD"
        elif "eth" in filename or "ethereum" in filename:
            asset_symbol = "ETHUSD"
        else:
            # Default to generic if we can't determine
            asset_symbol = "GENERIC"

        # Get the appropriate asset manager
        asset_manager = self.get_asset_manager(asset_symbol)

        # Load the data
        df = asset_manager.load_data(data_path, date_range)

        # Cache the result
        self.data_cache[cache_key] = df

        return df

    def get_data_for_timeframe(
        self,
        data_path: str,
        target_timeframe: str,
        date_range: Optional[Tuple[datetime, datetime]] = None,
    ) -> pd.DataFrame:
        """Get data resampled to target timeframe (compatibility method)."""
        # Extract asset symbol from filename
        filename = os.path.basename(data_path).lower()

        # Try to determine asset type from filename
        if "gold" in filename:
            asset_symbol = "GOLD"
        elif "btc" in filename or "bitcoin" in filename:
            asset_symbol = "BTCUSD"
        elif "eth" in filename or "ethereum" in filename:
            asset_symbol = "ETHUSD"
        else:
            # Default to generic if we can't determine
            asset_symbol = "GENERIC"

        # Get the appropriate asset manager
        asset_manager = self.get_asset_manager(asset_symbol)

        # Get the data
        return asset_manager.get_data_for_timeframe(
            data_path, target_timeframe, date_range
        )

    def get_data_in_range(
        self,
        data_path: str,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
    ) -> pd.DataFrame:
        """
        Load data filtered to a specific date range (compatibility method).

        Args:
            data_path: Path to the data file
            start_date: Start date (string or datetime)
            end_date: End date (string or datetime)

        Returns:
            DataFrame with data in the specified range
        """
        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        return self.load_data(data_path, date_range=(start_date, end_date))

    def save_file_upload(self, file: Any, target_dir: str = "data") -> Optional[str]:
        """Save an uploaded file to the target directory."""
        try:
            # Create target directory if it doesn't exist
            data_dir = Path(target_dir)
            data_dir.mkdir(exist_ok=True)

            # Define target path
            target_path = data_dir / file.name

            # Save the file
            file.save(target_path)

            logger.info(f"File uploaded: {file.name} to {target_path}")
            return str(target_path)

        except Exception as e:
            logger.error(f"Error handling file upload: {e}")
            return None

    def detect_missing_data_points(self, df: pd.DataFrame) -> Dict:
        """Detect missing data points (compatibility method)."""
        return DataProcessor.detect_missing_data(df)

    def fill_missing_data(
        self, df: pd.DataFrame, method: str = "ffill"
    ) -> pd.DataFrame:
        """Fill missing data points (compatibility method)."""
        return DataProcessor.fill_missing_data(df, method)

    def create_sample_asset_data(self, asset_symbol: str) -> pd.DataFrame:
        """Create sample data for a specific asset."""
        asset_manager = self.get_asset_manager(asset_symbol)
        return asset_manager.create_sample_data()

    def try_fetch_asset_data(self, asset_symbol: str) -> bool:
        """Try to fetch data for a specific asset."""
        asset_manager = self.get_asset_manager(asset_symbol)
        return asset_manager.try_fetch_data()
