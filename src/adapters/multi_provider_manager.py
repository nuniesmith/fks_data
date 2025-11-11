"""Multi-provider manager for dynamic provider switching with failover and verification.

This manager handles:
- Provider priority ordering
- Automatic failover on errors/rate limits
- Data verification by cross-checking multiple sources
- Health checks and circuit breaker patterns
"""
from __future__ import annotations

import random
import time
from typing import Any, Callable, Dict, List, Optional

try:
    from shared_python.exceptions import DataFetchError  # type: ignore
    from shared_python.logging import get_logger  # type: ignore
except (ImportError, ModuleNotFoundError):
    import logging
    
    class DataFetchError(Exception):
        """Fallback exception when shared_python is not available."""
        pass
    
    def get_logger(name: str) -> logging.Logger:
        """Fallback logger when shared_python is not available."""
        return logging.getLogger(name)

from .base import APIAdapter
from . import get_adapter


class ProviderHealth:
    """Tracks health status of a provider."""
    
    def __init__(self, name: str):
        self.name = name
        self.failures = 0
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.is_circuit_open = False
        self.circuit_open_time: Optional[float] = None
    
    def record_failure(self):
        """Record a failure and update circuit breaker state."""
        self.failures += 1
        self.last_failure_time = time.time()
        # Open circuit after 3 consecutive failures
        if self.failures >= 3:
            self.is_circuit_open = True
            self.circuit_open_time = time.time()
    
    def record_success(self):
        """Record a success and reset failure count."""
        self.failures = 0
        self.last_success_time = time.time()
        self.is_circuit_open = False
        self.circuit_open_time = None
    
    def should_retry(self, cooldown_seconds: float = 30.0) -> bool:
        """Check if provider should be retried after cooldown."""
        if not self.is_circuit_open:
            return True
        if self.circuit_open_time and (time.time() - self.circuit_open_time) >= cooldown_seconds:
            # Half-open state: allow one attempt
            return True
        return False


class MultiProviderManager:
    """Manages multiple data providers with failover and verification."""
    
    def __init__(
        self,
        providers: Optional[List[str]] = None,
        verify_data: bool = True,
        verification_threshold: float = 0.01,  # 1% price variance allowed
        cooldown_seconds: float = 30.0,
    ):
        """
        Initialize multi-provider manager.
        
        Args:
            providers: List of provider names in priority order. If None, uses default order.
            verify_data: Whether to cross-check data from multiple sources.
            verification_threshold: Maximum price variance (as fraction) for verification.
            cooldown_seconds: Time to wait before retrying failed providers.
        """
        self._log = get_logger("fks_data.adapters.multi_provider_manager")
        self.verify_data = verify_data
        self.verification_threshold = verification_threshold
        self.cooldown_seconds = cooldown_seconds
        
        # Default provider priority by asset type
        self.default_providers = {
            "crypto": ["binance", "cmc", "polygon"],
            "stock": ["polygon", "eodhd"],
            "etf": ["polygon", "eodhd"],
        }
        
        # Use provided providers or default
        self.providers = providers or self.default_providers.get("crypto", ["binance", "cmc"])
        
        # Track health of each provider
        self.provider_health: Dict[str, ProviderHealth] = {
            name: ProviderHealth(name) for name in self.providers
        }
        
        self._log.info(f"Initialized MultiProviderManager with providers: {self.providers}")
    
    def get_data(
        self,
        asset: str,
        granularity: str = "1m",
        start_date: Optional[float] = None,
        end_date: Optional[float] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch data from providers with automatic failover.
        
        Args:
            asset: Asset symbol (e.g., "BTC", "AAPL")
            granularity: Data granularity (e.g., "1m", "1h", "1d")
            start_date: Start timestamp (optional)
            end_date: End timestamp (optional)
            **kwargs: Additional parameters for adapter fetch
        
        Returns:
            Normalized data dict with provider info
        """
        # Try providers in priority order
        last_error: Optional[Exception] = None
        
        for provider_name in self.providers:
            health = self.provider_health[provider_name]
            
            # Skip if circuit is open and cooldown hasn't passed
            if not health.should_retry(self.cooldown_seconds):
                self._log.debug(f"Skipping {provider_name} (circuit open)")
                continue
            
            try:
                adapter = get_adapter(provider_name)
                
                # Build adapter-specific kwargs
                adapter_kwargs = self._build_adapter_kwargs(
                    provider_name, asset, granularity, start_date, end_date, **kwargs
                )
                
                data = adapter.fetch(**adapter_kwargs)
                
                # Verify data if enabled and we have multiple providers
                if self.verify_data and len(self.providers) > 1:
                    verified = self._verify_data(data, asset, provider_name)
                    if not verified:
                        self._log.warning(f"Data verification failed for {provider_name}, trying next provider")
                        health.record_failure()
                        continue
                
                # Success - record and return
                health.record_success()
                self._log.info(f"Successfully fetched data from {provider_name} for {asset}")
                return data
                
            except Exception as e:
                last_error = e
                health.record_failure()
                self._log.warning(f"Provider {provider_name} failed: {e}, trying next provider")
                continue
        
        # All providers failed
        raise DataFetchError(
            "multi_provider",
            f"All providers failed for {asset}. Last error: {last_error}"
        )
    
    def _build_adapter_kwargs(
        self,
        provider_name: str,
        asset: str,
        granularity: str,
        start_date: Optional[float],
        end_date: Optional[float],
        **kwargs
    ) -> Dict[str, Any]:
        """Build provider-specific kwargs for adapter fetch."""
        adapter_kwargs = kwargs.copy()
        
        if provider_name == "binance":
            adapter_kwargs["symbol"] = asset.upper() + "USDT" if not asset.endswith("USDT") else asset.upper()
            adapter_kwargs["interval"] = granularity
            if start_date:
                adapter_kwargs["start_time"] = int(start_date * 1000)  # Binance uses milliseconds
            if end_date:
                adapter_kwargs["end_time"] = int(end_date * 1000)
        elif provider_name == "cmc":
            adapter_kwargs["symbol"] = asset.upper()
            adapter_kwargs["endpoint"] = "quotes_latest"  # or "listings_latest" for bulk
        elif provider_name == "polygon":
            # Polygon uses ticker format like "X:BTCUSD" for crypto or "AAPL" for stocks
            if asset.upper() in ["BTC", "ETH", "SOL"]:  # Common crypto
                adapter_kwargs["ticker"] = f"X:{asset.upper()}USD"
            else:
                adapter_kwargs["ticker"] = asset.upper()
            # Map granularity to Polygon timespan
            timespan_map = {"1m": "minute", "1h": "hour", "1d": "day"}
            adapter_kwargs["timespan"] = timespan_map.get(granularity, "day")
            adapter_kwargs["range"] = 1
            if start_date:
                adapter_kwargs["fro"] = int(start_date)
            if end_date:
                adapter_kwargs["to"] = int(end_date)
        elif provider_name == "eodhd":
            adapter_kwargs["symbol"] = asset.upper()
            adapter_kwargs["interval"] = granularity
        
        return adapter_kwargs
    
    def _verify_data(
        self,
        data: Dict[str, Any],
        asset: str,
        primary_provider: str
    ) -> bool:
        """
        Verify data by cross-checking with a secondary provider.
        
        Returns True if data is verified or verification is skipped.
        """
        if not data.get("data"):
            return False
        
        # Get latest price from primary data
        primary_data = data["data"]
        if not primary_data:
            return False
        
        latest = primary_data[-1] if isinstance(primary_data, list) else primary_data
        primary_price = latest.get("close") or latest.get("price", 0)
        
        if primary_price == 0:
            return False
        
        # Try to get spot check from secondary provider
        secondary_providers = [p for p in self.providers if p != primary_provider]
        if not secondary_providers:
            # No secondary provider to verify against
            return True
        
        secondary_name = random.choice(secondary_providers)
        secondary_health = self.provider_health[secondary_name]
        
        # Skip verification if secondary is unhealthy
        if secondary_health.is_circuit_open and not secondary_health.should_retry(self.cooldown_seconds):
            self._log.debug(f"Skipping verification with {secondary_name} (unhealthy)")
            return True
        
        try:
            adapter = get_adapter(secondary_name)
            adapter_kwargs = self._build_adapter_kwargs(
                secondary_name, asset, "1m", None, None
            )
            secondary_data = adapter.fetch(**adapter_kwargs)
            
            secondary_data_list = secondary_data.get("data", [])
            if not secondary_data_list:
                self._log.warning(f"Secondary provider {secondary_name} returned no data")
                return True  # Don't fail verification if secondary has no data
            
            secondary_latest = secondary_data_list[-1] if isinstance(secondary_data_list, list) else secondary_data_list
            secondary_price = secondary_latest.get("close") or secondary_latest.get("price", 0)
            
            if secondary_price == 0:
                return True  # Can't verify if secondary has no price
            
            # Check variance
            variance = abs(primary_price - secondary_price) / primary_price
            if variance > self.verification_threshold:
                self._log.warning(
                    f"Price variance too high: {primary_provider}={primary_price}, "
                    f"{secondary_name}={secondary_price}, variance={variance:.2%}"
                )
                return False
            
            self._log.debug(f"Verification passed: variance={variance:.2%}")
            return True
            
        except Exception as e:
            self._log.debug(f"Verification check failed (non-critical): {e}")
            return True  # Don't fail primary fetch if verification check fails
    
    def get_provider_status(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all providers."""
        status = {}
        for name, health in self.provider_health.items():
            status[name] = {
                "failures": health.failures,
                "is_circuit_open": health.is_circuit_open,
                "last_failure_time": health.last_failure_time,
                "last_success_time": health.last_success_time,
            }
        return status


__all__ = ["MultiProviderManager", "ProviderHealth"]

