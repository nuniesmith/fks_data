from dataclasses import dataclass
import os

@dataclass
class _Settings:
    alpha_api_key: str | None = None
    polygon_api_key: str | None = None
    binance_api_key: str | None = None

def get_settings() -> _Settings:  # very small subset
    return _Settings(
        alpha_api_key=os.getenv("ALPHA_API_KEY"),
        polygon_api_key=os.getenv("POLYGON_API_KEY"),
        binance_api_key=os.getenv("BINANCE_API_KEY"),
    )

__all__ = ["get_settings", "_Settings"]
