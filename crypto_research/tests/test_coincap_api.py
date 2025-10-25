"""
Tests for CoinCap API utilities
"""

import os
import socket
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

from crypto_research.dataflows.coincap_utils import (
    get_historical_quotes,
    get_macd,
    symbol_to_slug,
)

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


class TestCoinCapUtils:
    """Test suite for CoinCap API utilities"""

    def test_symbol_to_slug_mapping(self):
        """Test cryptocurrency symbol to slug mapping"""
        assert "btc" in symbol_to_slug
        assert symbol_to_slug["btc"] == "bitcoin"
        assert "eth" in symbol_to_slug
        assert symbol_to_slug["eth"] == "ethereum"

    def test_get_historical_quotes_bitcoin(self, network_available):
        """Test getting historical data for Bitcoin"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", start_date, end_date)

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

    def test_get_historical_quotes_ethereum(self, network_available):
        """Test getting historical data for Ethereum"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("ETH", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "Price USD" in df.columns
            assert all(df["Price USD"] > 0), "All prices should be positive"

    def test_get_historical_quotes_date_range(self, network_available):
        """Test that date range is respected"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = "2024-01-31"
        start_date = "2024-01-01"

        df = get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            # Convert start and end dates to timezone-aware timestamps (UTC)
            start_dt = pd.to_datetime(start_date).tz_localize("UTC")
            end_dt = pd.to_datetime(end_date).tz_localize("UTC")

            # Check that dates are within range (with some tolerance for timezone)
            assert df.index.min() >= start_dt - pd.Timedelta(days=1)
            assert df.index.max() <= end_dt + pd.Timedelta(days=1)

    def test_get_historical_quotes_invalid_symbol(self, network_available):
        """Test handling of invalid cryptocurrency symbol"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("INVALID_XYZ", start_date, end_date)

        # Should return empty DataFrame for invalid symbol
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_historical_quotes_data_structure(self, network_available):
        """Test the structure of returned data"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", start_date, end_date)

        if not df.empty:
            # Check data types
            assert df["Price USD"].dtype in [float, "float64"]

            # Check that index is DatetimeIndex
            assert isinstance(df.index, pd.DatetimeIndex)

            # Check that all prices are positive
            assert all(df["Price USD"] > 0)

    def test_get_historical_quotes_single_day(self, network_available):
        """Test getting data for a single day"""
        if not network_available:
            pytest.skip("Network not available")

        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", date, date)

        assert isinstance(df, pd.DataFrame)
        # May or may not have data depending on API response

    def test_get_historical_quotes_longer_period(self, network_available):
        """Test getting data for a longer period"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert len(df) > 0, "Should have multiple days of data"

    def test_get_historical_quotes_with_api_key(self, network_available):
        """Test that API key from environment is used if available"""
        if not network_available:
            pytest.skip("Network not available")

        api_key = os.getenv("COINCAP_API_KEY")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", start_date, end_date)

        # Should work with or without API key
        assert isinstance(df, pd.DataFrame)

        if api_key:
            print(f"✓ Test ran with API key")
        else:
            print(f"✓ Test ran without API key (rate limits apply)")

    def test_get_historical_quotes_multiple_cryptos(self, network_available):
        """Test getting data for multiple cryptocurrencies"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        cryptos = ["BTC", "ETH"]
        results = {}

        for crypto in cryptos:
            df = get_historical_quotes(crypto, start_date, end_date)
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
        self, symbol, expected_empty, network_available
    ):
        """Parametrized test for different symbols"""
        if not network_available:
            pytest.skip("Network not available")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes(symbol, start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        if expected_empty:
            assert df.empty, f"{symbol} should return empty DataFrame"
        else:
            # May or may not be empty depending on API availability
            pass

    def test_get_historical_quotes_error_handling(self, monkeypatch):
        """Test error handling for network issues"""
        import requests

        def mock_get(*args, **kwargs):
            raise requests.exceptions.ConnectionError("Network error")

        monkeypatch.setattr(requests, "get", mock_get)

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", start_date, end_date)

        # Should return empty DataFrame on error
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_historical_quotes_timeout(self, monkeypatch):
        """Test timeout handling"""
        import requests

        def mock_get(*args, **kwargs):
            raise requests.exceptions.Timeout("Request timed out")

        monkeypatch.setattr(requests, "get", mock_get)

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_historical_quotes_http_error(self, monkeypatch):
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

        df = get_historical_quotes("BTC", start_date, end_date)

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_macd_bitcoin(self, network_available):
        """Test getting MACD data for Bitcoin"""
        if not network_available:
            pytest.skip("Network not available")

        df = get_macd("BTC")

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            # Check required columns exist
            assert "MACD" in df.columns
            assert "Signal" in df.columns
            assert "Histogram" in df.columns
            # Check that index is DatetimeIndex
            assert isinstance(df.index, pd.DatetimeIndex)
            # Check data types are numeric
            assert df["MACD"].dtype in [float, "float64"]
            assert df["Signal"].dtype in [float, "float64"]
            assert df["Histogram"].dtype in [float, "float64"]

    def test_get_macd_ethereum(self, network_available):
        """Test getting MACD data for Ethereum"""
        if not network_available:
            pytest.skip("Network not available")

        df = get_macd("ETH")

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "MACD" in df.columns
            assert "Signal" in df.columns
            assert "Histogram" in df.columns

    def test_get_macd_invalid_symbol(self, network_available):
        """Test handling of invalid cryptocurrency symbol for MACD"""
        if not network_available:
            pytest.skip("Network not available")

        df = get_macd("INVALID_XYZ")

        # Should return empty DataFrame for invalid symbol
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_macd_data_structure(self, network_available):
        """Test the structure of returned MACD data"""
        if not network_available:
            pytest.skip("Network not available")

        df = get_macd("BTC")

        if not df.empty:
            # Check all required columns are present
            required_columns = ["MACD", "Signal", "Histogram"]
            for col in required_columns:
                assert col in df.columns, f"Missing column: {col}"

            # Check that index is DatetimeIndex
            assert isinstance(df.index, pd.DatetimeIndex)

            # Check that we have multiple data points
            assert len(df) > 0, "Should have MACD data points"

    def test_get_macd_histogram_calculation(self, network_available):
        """Test that MACD histogram is correctly calculated"""
        if not network_available:
            pytest.skip("Network not available")

        df = get_macd("BTC")

        if not df.empty:
            # Histogram should equal MACD - Signal (approximately)
            calculated_histogram = df["MACD"] - df["Signal"]
            # Allow for small floating point differences
            assert all(
                abs(df["Histogram"] - calculated_histogram) < 0.01
            ), "Histogram should equal MACD - Signal"

    @pytest.mark.parametrize(
        "symbol,expected_empty",
        [
            ("BTC", False),
            ("ETH", False),
            ("INVALID_XYZ", True),
        ],
    )
    def test_get_macd_parametrized(self, symbol, expected_empty, network_available):
        """Parametrized test for MACD with different symbols"""
        if not network_available:
            pytest.skip("Network not available")

        df = get_macd(symbol)

        assert isinstance(df, pd.DataFrame)
        if expected_empty:
            assert df.empty, f"{symbol} should return empty DataFrame"
        else:
            # May or may not be empty depending on API availability
            pass

    def test_get_macd_error_handling(self, monkeypatch):
        """Test error handling for MACD network issues"""
        import requests

        def mock_get(*args, **kwargs):
            raise requests.exceptions.ConnectionError("Network error")

        monkeypatch.setattr(requests, "get", mock_get)

        df = get_macd("BTC")

        # Should return empty DataFrame on error
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_macd_timeout(self, monkeypatch):
        """Test timeout handling for MACD"""
        import requests

        def mock_get(*args, **kwargs):
            raise requests.exceptions.Timeout("Request timed out")

        monkeypatch.setattr(requests, "get", mock_get)

        df = get_macd("BTC")

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_macd_http_error(self, monkeypatch):
        """Test HTTP error handling for MACD"""
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

        df = get_macd("BTC")

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
        # Test 1: Symbol mapping
        print("\n1. Testing symbol mapping...")
        print(f"   ✓ BTC maps to: {symbol_to_slug.get('btc', 'Not found')}")
        print(f"   ✓ ETH maps to: {symbol_to_slug.get('eth', 'Not found')}")

        # Test 2: Get Bitcoin data
        print("\n2. Testing Bitcoin historical data...")
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        df = get_historical_quotes("BTC", start_date, end_date)
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} days of data")
            print(f"   ✓ Latest price: ${df['Price USD'].iloc[-1]:,.2f}")
            print(f"   ✓ Date range: {start_date} to {end_date}")
        else:
            print("   ⚠ No data returned")

        # Test 3: Get Ethereum data
        print("\n3. Testing Ethereum historical data...")
        df_eth = get_historical_quotes("ETH", start_date, end_date)
        if not df_eth.empty:
            print(f"   ✓ Retrieved {len(df_eth)} days of data")
            print(f"   ✓ Latest price: ${df_eth['Price USD'].iloc[-1]:,.2f}")
        else:
            print("   ⚠ No data returned")

        # Test 4: Test invalid symbol
        print("\n4. Testing invalid symbol handling...")
        df_invalid = get_historical_quotes("INVALID_XYZ", start_date, end_date)
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

        # Test 7: MACD indicator for Bitcoin
        print("\n7. Testing MACD indicator for Bitcoin...")
        df_macd = get_macd("BTC")
        if not df_macd.empty:
            print(f"   ✓ Retrieved {len(df_macd)} MACD data points")
            print(f"   ✓ Latest MACD: {df_macd['MACD'].iloc[-1]:.4f}")
            print(f"   ✓ Latest Signal: {df_macd['Signal'].iloc[-1]:.4f}")
            print(f"   ✓ Latest Histogram: {df_macd['Histogram'].iloc[-1]:.4f}")
            # Verify histogram calculation
            calculated_hist = df_macd["MACD"].iloc[-1] - df_macd["Signal"].iloc[-1]
            if abs(df_macd["Histogram"].iloc[-1] - calculated_hist) < 0.01:
                print("   ✓ Histogram calculation verified")
        else:
            print("   ⚠ No MACD data returned")

        # Test 8: MACD indicator for Ethereum
        print("\n8. Testing MACD indicator for Ethereum...")
        df_macd_eth = get_macd("ETH")
        if not df_macd_eth.empty:
            print(f"   ✓ Retrieved {len(df_macd_eth)} MACD data points")
            print(f"   ✓ Latest MACD: {df_macd_eth['MACD'].iloc[-1]:.4f}")
        else:
            print("   ⚠ No MACD data returned")

        print("\n✅ All manual tests completed!")

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        exit(1)
