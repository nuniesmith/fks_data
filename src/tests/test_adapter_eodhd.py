"""Tests for EODHD adapter."""

from unittest.mock import Mock, patch

import pytest

from src.adapters.eodhd import EODHDAdapter


# Local exception for testing
class DataFetchError(Exception):
    def __init__(self, provider: str, message: str):
        self.provider = provider
        super().__init__(f"{provider}: {message}")


class TestEODHDAdapter:
    """Test EODHD API adapter functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Mock API key environment variable
        self.api_key_patch = patch.dict('os.environ', {'EODHD_API_KEY': 'test_api_key'})
        self.api_key_patch.start()

        # Mock HTTP client
        self.mock_http = Mock()
        self.adapter = EODHDAdapter(http=self.mock_http)

    def teardown_method(self):
        """Clean up test fixtures."""
        self.api_key_patch.stop()

    def test_init_requires_api_key(self):
        """Test that adapter requires API key."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(DataFetchError, match="EODHD_API_KEY environment variable not set"):
                EODHDAdapter()

    def test_build_request_fundamentals(self):
        """Test building fundamentals request."""
        url, params, headers = self.adapter._build_request(
            data_type="fundamentals",
            symbol="AAPL.US"
        )

        assert url == "https://eodhistoricaldata.com/api/fundamentals/AAPL.US"
        assert params["api_token"] == "test_api_key"
        assert params["fmt"] == "json"
        assert headers["User-Agent"] == "FKS-Trading/1.0"

    def test_build_request_earnings(self):
        """Test building earnings calendar request."""
        url, params, headers = self.adapter._build_request(
            data_type="earnings",
            symbol="AAPL.US",
            from_date="2025-01-01",
            to_date="2025-12-31"
        )

        assert url == "https://eodhistoricaldata.com/api/calendar/earnings"
        assert params["symbols"] == "AAPL.US"
        assert params["from"] == "2025-01-01"
        assert params["to"] == "2025-12-31"

    def test_build_request_economic(self):
        """Test building economic indicators request."""
        url, params, headers = self.adapter._build_request(
            data_type="economic",
            country="US",
            from_date="2025-01-01"
        )

        assert url == "https://eodhistoricaldata.com/api/economic-events"
        assert params["country"] == "US"
        assert params["from"] == "2025-01-01"
        assert "symbols" not in params  # Should be removed for economic data

    def test_build_request_insider_transactions(self):
        """Test building insider transactions request."""
        url, params, headers = self.adapter._build_request(
            data_type="insider_transactions",
            symbol="AAPL.US",
            limit=100
        )

        assert url == "https://eodhistoricaldata.com/api/insider-transactions"
        assert params["code"] == "AAPL.US"
        assert params["limit"] == 100

    def test_build_request_invalid_data_type(self):
        """Test error handling for invalid data type."""
        with pytest.raises(DataFetchError, match="Unsupported data_type: invalid"):
            self.adapter._build_request(data_type="invalid")

    def test_normalize_fundamentals(self):
        """Test normalization of fundamentals data."""
        mock_response = {
            "General": {"Code": "AAPL", "Name": "Apple Inc."},
            "Highlights": {"MarketCapitalization": 3000000000000},
            "Valuation": {"PERatio": 25.5},
            "Financials": {
                "Balance_Sheet": {
                    "yearly": {
                        "2023-09-30": {"totalAssets": 352755000000}
                    }
                },
                "Income_Statement": {
                    "yearly": {
                        "2023-09-30": {"totalRevenue": 383285000000}
                    }
                }
            }
        }

        result = self.adapter._normalize(
            mock_response,
            request_kwargs={"data_type": "fundamentals", "symbol": "AAPL.US"}
        )

        assert result["provider"] == "eodhd"
        assert result["data_type"] == "fundamentals"
        assert len(result["data"]) == 1

        data = result["data"][0]
        assert data["symbol"] == "AAPL.US"
        assert data["general"]["Code"] == "AAPL"
        assert data["highlights"]["MarketCapitalization"] == 3000000000000
        assert "timestamp" in data

    def test_normalize_earnings(self):
        """Test normalization of earnings data."""
        mock_response = [
            {
                "code": "AAPL.US",
                "name": "Apple Inc.",
                "report_date": "2025-01-30",
                "estimate": 2.25,
                "actual": 2.35,
                "difference": 0.10,
                "surprise_percent": 4.44
            }
        ]

        result = self.adapter._normalize(
            mock_response,
            request_kwargs={"data_type": "earnings"}
        )

        assert result["provider"] == "eodhd"
        assert result["data_type"] == "earnings"
        assert len(result["data"]) == 1

        data = result["data"][0]
        assert data["symbol"] == "AAPL.US"
        assert data["company_name"] == "Apple Inc."
        assert data["earnings_date"] == "2025-01-30"
        assert data["estimate"] == 2.25
        assert data["actual"] == 2.35

    def test_normalize_economic(self):
        """Test normalization of economic indicators."""
        mock_response = [
            {
                "country": "US",
                "event": "Non-Farm Payrolls",
                "date": "2025-02-07",
                "time": "13:30:00",
                "currency": "USD",
                "importance": "High",
                "actual": 200000,
                "estimate": 185000,
                "previous": 175000,
                "change_percent": 5.7
            }
        ]

        result = self.adapter._normalize(
            mock_response,
            request_kwargs={"data_type": "economic"}
        )

        assert result["provider"] == "eodhd"
        assert result["data_type"] == "economic"
        assert len(result["data"]) == 1

        data = result["data"][0]
        assert data["country"] == "US"
        assert data["event_name"] == "Non-Farm Payrolls"
        assert data["importance"] == "High"
        assert data["actual"] == 200000

    def test_normalize_insider_transactions(self):
        """Test normalization of insider transactions."""
        mock_response = [
            {
                "code": "AAPL.US",
                "fullName": "Tim Cook",
                "position": "CEO",
                "transactionDate": "2025-01-15",
                "transactionType": "Sale",
                "shares": 50000,
                "price": 180.50,
                "value": 9025000
            }
        ]

        result = self.adapter._normalize(
            mock_response,
            request_kwargs={"data_type": "insider_transactions"}
        )

        assert result["provider"] == "eodhd"
        assert result["data_type"] == "insider_transactions"
        assert len(result["data"]) == 1

        data = result["data"][0]
        assert data["symbol"] == "AAPL.US"
        assert data["insider_name"] == "Tim Cook"
        assert data["position"] == "CEO"
        assert data["transaction_type"] == "Sale"
        assert data["shares"] == 50000

    def test_normalize_invalid_payload(self):
        """Test error handling for invalid payload."""
        with pytest.raises(DataFetchError, match="Unexpected payload type"):
            self.adapter._normalize(
                "invalid_string",
                request_kwargs={"data_type": "fundamentals"}
            )

    @patch('time.sleep')  # Mock sleep for rate limiting tests
    def test_fetch_with_rate_limiting(self, mock_sleep):
        """Test that adapter respects rate limiting."""
        # Mock successful response
        self.mock_http.return_value = {
            "General": {"Code": "AAPL", "Name": "Apple Inc."}
        }

        # Make two requests quickly
        result1 = self.adapter.fetch(data_type="fundamentals", symbol="AAPL.US")
        result2 = self.adapter.fetch(data_type="fundamentals", symbol="MSFT.US")

        # Should have slept between requests (rate limiting)
        assert mock_sleep.called
        assert result1["provider"] == "eodhd"
        assert result2["provider"] == "eodhd"


if __name__ == "__main__":
    pytest.main([__file__])
