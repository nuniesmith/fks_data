"""NewsAPI Adapter for fetching financial news.

Provides access to NewsAPI for financial news articles and sentiment analysis.
Uses the unified adapter pattern for consistent error handling and rate limiting.

Migrated from fks_data_ingestion service for consolidation.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .base import APIAdapter, DataFetchError


class NewsAPIAdapter(APIAdapter):
    """Adapter for NewsAPI news articles.
    
    Provides:
    - Historical news search via /everything endpoint
    - Top headlines via /top-headlines endpoint
    - Symbol-specific financial news
    
    Rate Limits (Free tier):
    - 100 requests per day
    - Max 100 articles per request
    
    Configuration:
        NEWSAPI_KEY: API key for NewsAPI
        FKS_NEWSAPI_TIMEOUT: Request timeout (default: 30s)
        FKS_NEWSAPI_RPS: Rate limit per second (default: 1)
    """

    name = "newsapi"
    base_url = "https://newsapi.org/v2"
    rate_limit_per_sec = 1.0  # Conservative to stay within daily limits

    EVERYTHING_ENDPOINT = "/everything"
    TOP_HEADLINES_ENDPOINT = "/top-headlines"
    SOURCES_ENDPOINT = "/sources"

    def __init__(self, http=None, *, timeout: float | None = None, api_key: str | None = None):
        super().__init__(http=http, timeout=timeout or 30.0)
        self._api_key = api_key or os.getenv("NEWSAPI_KEY") or os.getenv("MASSIVE_API_KEY")
        if not self._api_key:
            self._log.warning("NewsAPI key not configured - API calls will fail")

    def _build_request(
        self,
        *,
        endpoint: str = "everything",
        symbol: str | None = None,
        query: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        language: str = "en",
        sort_by: str = "publishedAt",
        page_size: int = 100,
        page: int = 1,
        category: str = "business",
        country: str = "us",
        **_kwargs,
    ) -> tuple[str, dict[str, Any], dict[str, str]]:
        """Build request for NewsAPI endpoints.
        
        Args:
            endpoint: "everything" or "top-headlines"
            symbol: Stock ticker to search for
            query: Custom search query
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            language: Language code (default: "en")
            sort_by: Sort order (publishedAt, relevancy, popularity)
            page_size: Results per page (max 100)
            page: Page number
            category: For top-headlines (business, technology, etc.)
            country: For top-headlines (us, gb, etc.)
        
        Returns:
            Tuple of (url, params, headers)
        """
        if not self._api_key:
            raise DataFetchError("NewsAPI key not configured")

        headers = {"Authorization": f"Bearer {self._api_key}"}

        if endpoint == "top-headlines":
            url = f"{self.base_url}{self.TOP_HEADLINES_ENDPOINT}"
            params = {
                "category": category,
                "country": country,
                "pageSize": min(page_size, 100),
                "apiKey": self._api_key,
            }
        else:  # everything (default)
            url = f"{self.base_url}{self.EVERYTHING_ENDPOINT}"
            
            # Build query from symbol if provided
            search_query = query
            if not search_query and symbol:
                search_query = f"{symbol} OR ${symbol}"
            if not search_query:
                search_query = "finance OR stock OR market OR trading"
            
            # Default dates
            if not from_date:
                from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            if not to_date:
                to_date = datetime.now().strftime("%Y-%m-%d")
            
            params = {
                "q": search_query,
                "from": from_date,
                "to": to_date,
                "language": language,
                "sortBy": sort_by,
                "pageSize": min(page_size, 100),
                "page": page,
                "apiKey": self._api_key,
            }

        return url, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: dict | None = None) -> dict[str, Any]:
        """Normalize NewsAPI response to standard format.
        
        Args:
            raw: Raw API response
            request_kwargs: Original request parameters
        
        Returns:
            Normalized response with standardized article format
        """
        request_kwargs = request_kwargs or {}
        
        if not isinstance(raw, dict):
            raise DataFetchError(f"Unexpected response type: {type(raw)}")

        status = raw.get("status")
        if status != "ok":
            error_code = raw.get("code", "unknown")
            error_message = raw.get("message", "Unknown NewsAPI error")
            raise DataFetchError(f"NewsAPI error [{error_code}]: {error_message}")

        articles = raw.get("articles", [])
        total_results = raw.get("totalResults", len(articles))
        symbol = request_kwargs.get("symbol")
        query = request_kwargs.get("query")

        # Normalize article format
        normalized_articles: List[Dict[str, Any]] = []
        for article in articles:
            source_info = article.get("source", {})
            normalized_articles.append({
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "content": article.get("content", ""),
                "url": article.get("url", ""),
                "image_url": article.get("urlToImage", ""),
                "source": source_info.get("name", ""),
                "source_id": source_info.get("id", ""),
                "author": article.get("author", ""),
                "published_at": article.get("publishedAt", ""),
                "symbol": symbol,
                "query": query,
                # Metadata for feature engineering
                "provider": self.name,
                "fetched_at": datetime.utcnow().isoformat(),
            })

        return {
            "status": "ok",
            "total_results": total_results,
            "data": normalized_articles,
            "metadata": {
                "provider": self.name,
                "endpoint": request_kwargs.get("endpoint", "everything"),
                "symbol": symbol,
                "query": query,
                "count": len(normalized_articles),
            },
        }

    # High-level convenience methods
    
    def fetch_news(
        self,
        symbol: str | None = None,
        query: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        language: str = "en",
        sort_by: str = "publishedAt",
        page_size: int = 100,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        """Fetch financial news with pagination.
        
        Args:
            symbol: Stock ticker symbol
            query: Custom search query
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            language: Language code
            sort_by: Sort order
            page_size: Results per page
            max_pages: Maximum pages to fetch (default: 5 = 500 articles max)
        
        Returns:
            List of normalized article dictionaries
        """
        all_articles: List[Dict[str, Any]] = []
        
        for page in range(1, max_pages + 1):
            try:
                result = self.fetch(
                    endpoint="everything",
                    symbol=symbol,
                    query=query,
                    from_date=from_date,
                    to_date=to_date,
                    language=language,
                    sort_by=sort_by,
                    page_size=page_size,
                    page=page,
                )
                
                articles = result.get("data", [])
                if not articles:
                    break
                
                all_articles.extend(articles)
                
                # Check if we've fetched all available
                total = result.get("total_results", 0)
                if len(all_articles) >= total:
                    break
                    
            except DataFetchError as e:
                self._log.error(f"Failed to fetch news page {page}: {e}")
                break
        
        self._log.info(f"Fetched {len(all_articles)} news articles for symbol={symbol}, query={query}")
        return all_articles

    def fetch_top_headlines(
        self,
        category: str = "business",
        country: str = "us",
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch top headlines.
        
        Args:
            category: News category (business, technology, general, etc.)
            country: Country code (us, gb, etc.)
            page_size: Number of results
        
        Returns:
            List of normalized headline articles
        """
        try:
            result = self.fetch(
                endpoint="top-headlines",
                category=category,
                country=country,
                page_size=page_size,
            )
            return result.get("data", [])
        except DataFetchError as e:
            self._log.error(f"Failed to fetch top headlines: {e}")
            return []

    def fetch_news_for_symbols(
        self,
        symbols: List[str],
        from_date: str | None = None,
        to_date: str | None = None,
        page_size: int = 20,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch news for multiple symbols.
        
        Args:
            symbols: List of stock ticker symbols
            from_date: Start date
            to_date: End date
            page_size: Results per symbol
        
        Returns:
            Dictionary mapping symbol to list of articles
        """
        results: Dict[str, List[Dict[str, Any]]] = {}
        
        for symbol in symbols:
            articles = self.fetch_news(
                symbol=symbol,
                from_date=from_date,
                to_date=to_date,
                page_size=page_size,
                max_pages=1,  # Limit for batch operations
            )
            results[symbol] = articles
        
        return results
