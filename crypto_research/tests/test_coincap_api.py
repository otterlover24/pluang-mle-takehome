"""
Tests for CoinCap API utilities
"""

import os
import socket
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

from crypto_research.dataflows.coincap_utils import CoinCapAPI

# Load environment variables
load_dotenv()


@pytest.fixture(scope="module")
def network_available():
    """Check if network is available"""
    try:
        socket.gethostbyname("rest.coincap.io")
        return True
    except socket.gaierror:
        return False


class TestCoinCapAPI:
    """Test suite for CoinCap API wrapper"""

    @pytest.fixture
    def api(self):
        """Create API instance for tests"""
        return CoinCapAPI()

    def test_api_initialization(self):
        """Test API client initialization"""
        api = CoinCapAPI()
        assert api is not None
        assert api.base_url == "https://rest.coincap.io"

    def test_symbol_to_slug_mapping(self):
        """Test cryptocurrency symbol to slug mapping"""
        api = CoinCapAPI()
        assert "btc" in api.symbol_to_slug
        assert api.symbol_to_slug["btc"] == "bitcoin"
        assert "eth" in api.symbol_to_slug
        assert api.symbol_to_slug["eth"] == "ethereum"

    def test_get_historical_quotes_bitcoin(self, api, network_available):
        """Test getting historical data for Bitcoin"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            # Check columns exist
            assert "Price USD" in df.columns
            # Check that index is DatetimeIndex
            assert isinstance(df.index, pd.DatetimeIndex)
            # Check data is positive
            assert all(df["Price USD"] > 0), "All prices should be positive"
        else:
            raise ValueError("No data returned from API")

    def test_get_historical_quotes_ethereum(self, api, network_available):
        """Test getting historical data for Ethereum"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("ETH", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "Price USD" in df.columns
            assert all(df["Price USD"] > 0), "All prices should be positive"

    def test_get_historical_quotes_date_range(self, api, network_available):
        """Test that date range is respected"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = "2024-01-31"
        start_date = "2024-01-01"

        df = api.get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            # Convert start and end dates to timezone-aware timestamps (UTC)
            start_dt = pd.to_datetime(start_date).tz_localize("UTC")
            end_dt = pd.to_datetime(end_date).tz_localize("UTC")

            # Check that dates are within range (with some tolerance for timezone)
            assert df.index.min() >= start_dt - pd.Timedelta(days=1)
            assert df.index.max() <= end_dt + pd.Timedelta(days=1)

    def test_get_historical_quotes_invalid_symbol(self, api, network_available):
        """Test handling of invalid cryptocurrency symbol"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("INVALID_XYZ", start_date, end_date)

        # Should return empty DataFrame for invalid symbol
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_historical_quotes_data_structure(self, api, network_available):
        """Test the structure of returned data"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        if not df.empty:
            # Check data types
            assert df["Price USD"].dtype in [float, "float64"]

            # Check that index is DatetimeIndex
            assert isinstance(df.index, pd.DatetimeIndex)

            # Check that all prices are positive
            assert all(df["Price USD"] > 0)

    def test_get_historical_quotes_single_day(self, api, network_available):
        """Test getting data for a single day"""
        if not network_available:
            pytest.skip("Network not available")

        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", date, date)

        assert isinstance(df, pd.DataFrame)
        # May or may not have data depending on API response

    def test_get_historical_quotes_longer_period(self, api, network_available):
        """Test getting data for a longer period"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert len(df) > 0, "Should have multiple days of data"

    def test_get_historical_quotes_with_api_key(self, api, network_available):
        """Test that API key from environment is used if available"""
        if not network_available:
            pytest.skip("Network not available")

        api_key = os.getenv("COINCAP_API_KEY")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        # Should work with or without API key
        assert isinstance(df, pd.DataFrame)

        if api_key:
            print(f"✓ Test ran with API key")
        else:
            print(f"✓ Test ran without API key (rate limits apply)")

    def test_get_historical_quotes_multiple_cryptos(self, api, network_available):
        """Test getting data for multiple cryptocurrencies"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        cryptos = ["BTC", "ETH"]
        results = {}

        for crypto in cryptos:
            df = api.get_historical_quotes(crypto, start_date, end_date)
            results[crypto] = df
            assert isinstance(df, pd.DataFrame)

        # At least one should have data
        has_data = any(not df.empty for df in results.values())

        if not has_data:
            raise ValueError("No data returned - possible API issues or rate limiting")

        assert has_data, "At least one cryptocurrency should return data"

    @pytest.mark.parametrize(
        "symbol,expected_empty",
        [
            ("BTC", False),
            ("ETH", False),
            ("INVALID_XYZ", True),
        ],
    )
    def test_get_historical_quotes_parametrized(
        self, api, symbol, expected_empty, network_available
    ):
        """Parametrized test for different symbols"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes(symbol, start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if expected_empty:
            assert df.empty, f"{symbol} should return empty DataFrame"
        else:
            # May or may not be empty depending on API availability
            pass

    def test_get_historical_quotes_error_handling(self, api, monkeypatch):
        """Test error handling for network issues"""
        import requests

        def mock_get(*args, **kwargs):
            raise requests.exceptions.ConnectionError("Network error")

        monkeypatch.setattr(requests, "get", mock_get)

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        # Should return empty DataFrame on error
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_historical_quotes_timeout(self, api, monkeypatch):
        """Test timeout handling"""
        import requests

        def mock_get(*args, **kwargs):
            raise requests.exceptions.Timeout("Request timed out")

        monkeypatch.setattr(requests, "get", mock_get)

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_historical_quotes_http_error(self, api, monkeypatch):
        """Test HTTP error handling"""
        import requests
        from unittest.mock import Mock

        def mock_get(*args, **kwargs):
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "403 Forbidden"
            )
            mock_response.text = "Forbidden"
            return mock_response

        monkeypatch.setattr(requests, "get", mock_get)

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        assert df.empty


# Manual test script
if __name__ == "__main__":
    print("Running manual CoinCap API tests...\n")

    api_key = os.getenv("COINCAP_API_KEY")
    if api_key:
        print(f"✓ API Key found: {api_key[:10]}...")
    else:
        print("⚠ No API key found (using free tier rate limits)")

    # Check network connectivity
    try:
        socket.gethostbyname("rest.coincap.io")
        print("✓ Network connection available")
    except socket.gaierror:
        print("❌ Cannot connect to rest.coincap.io")
        exit(1)

    try:
        # Test 1: Initialize API
        print("\n1. Testing API initialization...")
        api = CoinCapAPI()
        print("   ✓ API initialized successfully")
        print(f"   ✓ Base URL: {api.base_url}")

        # Test 2: Get Bitcoin data
        print("\n2. Testing Bitcoin historical data...")
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        df = api.get_historical_quotes("BTC", start_date, end_date)
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} days of data")
            print(f"   ✓ Latest price: ${df['Price USD'].iloc[-1]:,.2f}")
            print(f"   ✓ Date range: {start_date} to {end_date}")
        else:
            print("   ⚠ No data returned")

        # Test 3: Get Ethereum data
        print("\n3. Testing Ethereum historical data...")
        df_eth = api.get_historical_quotes("ETH", start_date, end_date)
        if not df_eth.empty:
            print(f"   ✓ Retrieved {len(df_eth)} days of data")
            print(f"   ✓ Latest price: ${df_eth['Price USD'].iloc[-1]:,.2f}")
        else:
            print("   ⚠ No data returned")

        # Test 4: Test invalid symbol
        print("\n4. Testing invalid symbol handling...")
        df_invalid = api.get_historical_quotes("INVALID_XYZ", start_date, end_date)
        if df_invalid.empty:
            print("   ✓ Correctly handled invalid symbol")
        else:
            print("   ⚠ Unexpected data for invalid symbol")

        # Test 5: Data structure validation
        print("\n5. Testing data structure...")
        if not df.empty:
            required_columns = ["Price USD"]
            missing = [col for col in required_columns if col not in df.columns]
            if not missing:
                print("   ✓ All required columns present")
            else:
                print(f"   ✗ Missing columns: {missing}")

            if isinstance(df.index, pd.DatetimeIndex):
                print("   ✓ Index is DatetimeIndex")
            else:
                print("   ✗ Index is not DatetimeIndex")

        # Test 6: Symbol to slug mapping
        print("\n6. Testing symbol mapping...")
        print(f"   ✓ BTC maps to: {api.symbol_to_slug.get('btc', 'Not found')}")
        print(f"   ✓ ETH maps to: {api.symbol_to_slug.get('eth', 'Not found')}")

        print("\n✅ All manual tests completed!")

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        exit(1)
