"""
Tests for CoinMarketCap API utilities
"""

import os
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv

from crypto_research.dataflows.coinmarketcap_utils import (
    CoinMarketCapAPI,
    get_crypto_price_data,
    get_crypto_fundamentals,
    get_market_metrics,
    format_crypto_data_for_agents,
)

# Load environment variables
load_dotenv()


class TestCoinMarketCapAPI:
    """Test suite for CoinMarketCap API wrapper"""

    @pytest.fixture
    def api(self):
        """Create API instance for tests"""
        api_key = os.getenv("COINMARKETCAP_API_KEY")
        if not api_key:
            pytest.skip("COINMARKETCAP_API_KEY not set")
        return CoinMarketCapAPI(api_key)

    def test_api_initialization(self):
        """Test API client initialization"""
        api_key = os.getenv("COINMARKETCAP_API_KEY")
        if not api_key:
            pytest.skip("COINMARKETCAP_API_KEY not set")

        api = CoinMarketCapAPI(api_key)
        assert api.api_key == api_key
        assert api.base_url == CoinMarketCapAPI.BASE_URL

    def test_api_initialization_without_key(self):
        """Test that API raises error without key"""
        # Temporarily remove env var
        old_key = os.environ.pop("COINMARKETCAP_API_KEY", None)

        try:
            with pytest.raises(ValueError, match="API key not provided"):
                CoinMarketCapAPI()
        finally:
            # Restore env var
            if old_key:
                os.environ["COINMARKETCAP_API_KEY"] = old_key

    def test_get_crypto_map(self, api):
        """Test cryptocurrency symbol to ID mapping"""
        crypto_map = api.get_crypto_map()

        assert isinstance(crypto_map, dict)
        assert len(crypto_map) > 0
        assert "BTC" in crypto_map
        assert "ETH" in crypto_map
        assert isinstance(crypto_map["BTC"], int)

    def test_get_crypto_id(self, api):
        """Test getting crypto ID by symbol"""
        btc_id = api.get_crypto_id("BTC")
        assert isinstance(btc_id, int)
        assert btc_id == 1  # Bitcoin is always ID 1

        eth_id = api.get_crypto_id("ETH")
        assert isinstance(eth_id, int)
        assert eth_id == 1027  # Ethereum is always ID 1027

    def test_get_crypto_id_invalid_symbol(self, api):
        """Test that invalid symbol raises error"""
        with pytest.raises(ValueError, match="not found"):
            api.get_crypto_id("INVALID_SYMBOL_XYZ")

    def test_get_latest_quote(self, api):
        """Test getting latest price quotes"""
        quotes = api.get_latest_quote(["BTC", "ETH"])

        assert "data" in quotes
        assert len(quotes["data"]) > 0

        # Check BTC data structure
        btc_id = str(api.get_crypto_id("BTC"))
        assert btc_id in quotes["data"]

        btc_data = quotes["data"][btc_id]
        assert "quote" in btc_data
        assert "USD" in btc_data["quote"]

        usd_quote = btc_data["quote"]["USD"]
        assert "price" in usd_quote
        assert "volume_24h" in usd_quote
        assert "market_cap" in usd_quote
        assert isinstance(usd_quote["price"], (int, float))

    def test_get_historical_quotes(self, api):
        """Test getting historical OHLCV data"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        df = api.get_historical_quotes("BTC", start_date, end_date)

        assert not df.empty
        assert "Open" in df.columns
        assert "High" in df.columns
        assert "Low" in df.columns
        assert "Close" in df.columns
        assert "Volume" in df.columns
        assert len(df) > 0

    def test_get_crypto_info(self, api):
        """Test getting cryptocurrency information"""
        info = api.get_crypto_info(["BTC"])

        assert "data" in info
        btc_id = str(api.get_crypto_id("BTC"))
        assert btc_id in info["data"]

        btc_info = info["data"][btc_id]
        assert "name" in btc_info
        assert btc_info["name"] == "Bitcoin"
        assert "symbol" in btc_info
        assert "description" in btc_info

    def test_get_global_metrics(self, api):
        """Test getting global market metrics"""
        metrics = api.get_global_metrics()

        assert "data" in metrics
        data = metrics["data"]

        assert "quote" in data
        assert "USD" in data["quote"]

        usd_quote = data["quote"]["USD"]
        assert "total_market_cap" in usd_quote
        assert "total_volume_24h" in usd_quote
        assert isinstance(usd_quote["total_market_cap"], (int, float))


class TestHelperFunctions:
    """Test suite for helper functions"""

    @pytest.fixture(autouse=True)
    def check_api_key(self):
        """Check if API key is available"""
        if not os.getenv("COINMARKETCAP_API_KEY"):
            pytest.skip("COINMARKETCAP_API_KEY not set")

    def test_get_crypto_price_data(self):
        """Test price data retrieval helper"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        df = get_crypto_price_data("BTC", start_date, end_date)

        assert not df.empty
        assert all(
            col in df.columns for col in ["Open", "High", "Low", "Close", "Volume"]
        )

    def test_get_crypto_fundamentals(self):
        """Test fundamentals retrieval helper"""
        fundamentals = get_crypto_fundamentals("BTC")

        assert "symbol" in fundamentals
        assert fundamentals["symbol"] == "BTC"
        assert "name" in fundamentals
        assert "market_cap" in fundamentals
        assert "price" in fundamentals
        assert "volume_24h" in fundamentals
        assert isinstance(fundamentals["market_cap"], (int, float))

    def test_get_market_metrics(self):
        """Test market metrics helper"""
        metrics = get_market_metrics()

        assert "total_market_cap" in metrics
        assert "total_volume_24h" in metrics
        assert "bitcoin_dominance" in metrics
        assert "ethereum_dominance" in metrics
        assert isinstance(metrics["total_market_cap"], (int, float))

    def test_format_crypto_data_for_agents(self):
        """Test data formatting for agents"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        data = format_crypto_data_for_agents("BTC", start_date, end_date)

        assert "ticker" in data
        assert data["ticker"] == "BTC"
        assert "price_data" in data
        assert "fundamentals" in data
        assert "market_metrics" in data
        assert "data_period" in data

        # Check data period
        assert data["data_period"]["start"] == start_date
        assert data["data_period"]["end"] == end_date


# Manual test script (run with: python -m crypto_research.tests.test_coinmarketcap_api)
if __name__ == "__main__":
    print("Running manual CoinMarketCap API tests...\n")

    api_key = os.getenv("COINMARKETCAP_API_KEY")
    if not api_key:
        print("❌ COINMARKETCAP_API_KEY not set in environment")
        exit(1)

    print("✓ API Key found")

    try:
        # Test 1: Initialize API
        print("\n1. Testing API initialization...")
        api = CoinMarketCapAPI(api_key)
        print("   ✓ API initialized successfully")

        # Test 2: Get crypto ID
        print("\n2. Testing crypto ID lookup...")
        btc_id = api.get_crypto_id("BTC")
        print(f"   ✓ BTC ID: {btc_id}")

        # Test 3: Get latest quote
        print("\n3. Testing latest quote...")
        quotes = api.get_latest_quote(["BTC"])
        btc_quote = quotes["data"][str(btc_id)]["quote"]["USD"]
        print(f"   ✓ BTC Price: ${btc_quote['price']:,.2f}")
        print(f"   ✓ 24h Volume: ${btc_quote['volume_24h']:,.0f}")

        # Test 4: Get historical data
        print("\n4. Testing historical data...")
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        df = api.get_historical_quotes("BTC", start_date, end_date)
        print(f"   ✓ Retrieved {len(df)} days of data")
        print(f"   ✓ Date range: {start_date} to {end_date}")

        # Test 5: Get fundamentals
        print("\n5. Testing fundamentals...")
        fundamentals = get_crypto_fundamentals("BTC")
        print(f"   ✓ Name: {fundamentals['name']}")
        print(f"   ✓ Market Cap: ${fundamentals['market_cap']:,.0f}")
        print(f"   ✓ 24h Change: {fundamentals['percent_change_24h']:.2f}%")

        # Test 6: Get market metrics
        print("\n6. Testing global metrics...")
        metrics = get_market_metrics()
        print(f"   ✓ Total Market Cap: ${metrics['total_market_cap']:,.0f}")
        print(f"   ✓ BTC Dominance: {metrics['bitcoin_dominance']:.2f}%")

        print("\n✅ All tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        exit(1)
