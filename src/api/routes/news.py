"""
News API Routes for FKS Data Service.

Provides endpoints for fetching financial news from NewsAPI.
Migrated from fks_data_ingestion service.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/news", tags=["news"])


# Request/Response Models

class NewsSearchRequest(BaseModel):
    """Request model for news search."""
    symbol: Optional[str] = Field(None, description="Stock ticker symbol")
    query: Optional[str] = Field(None, description="Custom search query")
    from_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    to_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    language: str = Field("en", description="Language code")
    sort_by: str = Field("publishedAt", description="Sort order (publishedAt, relevancy, popularity)")
    page_size: int = Field(100, ge=1, le=100, description="Results per page")
    max_pages: int = Field(1, ge=1, le=5, description="Maximum pages to fetch")


class BulkNewsRequest(BaseModel):
    """Request model for bulk news fetch."""
    symbols: List[str] = Field(..., min_length=1, max_length=20, description="List of stock symbols")
    from_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    to_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    page_size: int = Field(20, ge=1, le=50, description="Results per symbol")


class NewsArticle(BaseModel):
    """News article response model."""
    title: str
    description: Optional[str]
    content: Optional[str]
    url: str
    image_url: Optional[str]
    source: str
    source_id: Optional[str]
    author: Optional[str]
    published_at: str
    symbol: Optional[str]
    query: Optional[str]
    provider: str = "newsapi"
    fetched_at: str


class NewsResponse(BaseModel):
    """News search response model."""
    status: str = "ok"
    total_results: int
    articles: List[NewsArticle]
    metadata: Dict[str, Any]


# Lazy adapter initialization
_news_adapter = None


def get_news_adapter():
    """Get or create NewsAPI adapter instance."""
    global _news_adapter
    if _news_adapter is None:
        from adapters.newsapi import NewsAPIAdapter
        _news_adapter = NewsAPIAdapter()
    return _news_adapter


# API Endpoints

@router.get("/search", response_model=NewsResponse)
async def search_news(
    symbol: Optional[str] = Query(None, description="Stock ticker symbol"),
    query: Optional[str] = Query(None, description="Custom search query"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    language: str = Query("en", description="Language code"),
    sort_by: str = Query("publishedAt", description="Sort order"),
    page_size: int = Query(100, ge=1, le=100, description="Results per page"),
    max_pages: int = Query(1, ge=1, le=5, description="Maximum pages"),
) -> NewsResponse:
    """
    Search for financial news articles.
    
    Can search by stock symbol or custom query. If neither provided,
    returns general financial market news.
    """
    try:
        adapter = get_news_adapter()
        articles = adapter.fetch_news(
            symbol=symbol,
            query=query,
            from_date=from_date,
            to_date=to_date,
            language=language,
            sort_by=sort_by,
            page_size=page_size,
            max_pages=max_pages,
        )
        
        return NewsResponse(
            status="ok",
            total_results=len(articles),
            articles=[NewsArticle(**a) for a in articles],
            metadata={
                "symbol": symbol,
                "query": query,
                "from_date": from_date,
                "to_date": to_date,
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/headlines", response_model=NewsResponse)
async def get_top_headlines(
    category: str = Query("business", description="News category"),
    country: str = Query("us", description="Country code"),
    page_size: int = Query(100, ge=1, le=100, description="Results"),
) -> NewsResponse:
    """
    Get top business/financial headlines.
    
    Fetches current top headlines from NewsAPI filtered by category and country.
    """
    try:
        adapter = get_news_adapter()
        articles = adapter.fetch_top_headlines(
            category=category,
            country=country,
            page_size=page_size,
        )
        
        return NewsResponse(
            status="ok",
            total_results=len(articles),
            articles=[NewsArticle(**a) for a in articles],
            metadata={
                "category": category,
                "country": country,
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"Error fetching headlines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search", response_model=NewsResponse)
async def search_news_post(request: NewsSearchRequest) -> NewsResponse:
    """
    Search for financial news articles (POST version).
    
    Accepts complex search parameters in request body.
    """
    try:
        adapter = get_news_adapter()
        articles = adapter.fetch_news(
            symbol=request.symbol,
            query=request.query,
            from_date=request.from_date,
            to_date=request.to_date,
            language=request.language,
            sort_by=request.sort_by,
            page_size=request.page_size,
            max_pages=request.max_pages,
        )
        
        return NewsResponse(
            status="ok",
            total_results=len(articles),
            articles=[NewsArticle(**a) for a in articles],
            metadata={
                "symbol": request.symbol,
                "query": request.query,
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk", response_model=Dict[str, NewsResponse])
async def fetch_bulk_news(request: BulkNewsRequest) -> Dict[str, NewsResponse]:
    """
    Fetch news for multiple stock symbols.
    
    Efficiently fetches news for multiple symbols in a single request.
    Limited to 20 symbols per request.
    """
    try:
        adapter = get_news_adapter()
        results = adapter.fetch_news_for_symbols(
            symbols=request.symbols,
            from_date=request.from_date,
            to_date=request.to_date,
            page_size=request.page_size,
        )
        
        response: Dict[str, NewsResponse] = {}
        for symbol, articles in results.items():
            response[symbol] = NewsResponse(
                status="ok",
                total_results=len(articles),
                articles=[NewsArticle(**a) for a in articles],
                metadata={
                    "symbol": symbol,
                    "fetched_at": datetime.utcnow().isoformat(),
                }
            )
        
        return response
    except Exception as e:
        logger.error(f"Error fetching bulk news: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/symbol/{symbol}", response_model=NewsResponse)
async def get_news_for_symbol(
    symbol: str,
    from_date: Optional[str] = Query(None, description="Start date"),
    to_date: Optional[str] = Query(None, description="End date"),
    page_size: int = Query(50, ge=1, le=100),
) -> NewsResponse:
    """
    Get news articles for a specific stock symbol.
    
    Convenience endpoint for fetching news about a single stock.
    """
    try:
        adapter = get_news_adapter()
        articles = adapter.fetch_news(
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
            page_size=page_size,
            max_pages=2,  # Up to 200 articles for single symbol
        )
        
        return NewsResponse(
            status="ok",
            total_results=len(articles),
            articles=[NewsArticle(**a) for a in articles],
            metadata={
                "symbol": symbol,
                "from_date": from_date,
                "to_date": to_date,
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"Error fetching news for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
