"""Tests for Twitter ingestion module."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from narrativealpha.ingestion.twitter import TwitterClient, TwitterRateLimitError
from narrativealpha.models import Tweet


class TestTwitterClient:
    """Test suite for TwitterClient."""

    @pytest.fixture
    def mock_client(self):
        """Create a TwitterClient with mocked HTTP client."""
        with patch("narrativealpha.ingestion.twitter.settings") as mock_settings:
            mock_settings.twitter_bearer_token = "test_token"
            client = TwitterClient()
            client.client = AsyncMock()
            yield client

    @pytest.mark.asyncio
    async def test_initialization_requires_token(self):
        """Test that client requires bearer token."""
        with patch("narrativealpha.ingestion.twitter.settings") as mock_settings:
            mock_settings.twitter_bearer_token = ""
            with pytest.raises(ValueError, match="bearer token required"):
                TwitterClient()

    @pytest.mark.asyncio
    async def test_parse_tweet(self, mock_client):
        """Test tweet parsing from API response."""
        tweet_data = {
            "id": "123456789",
            "text": "Bitcoin is pumping! $BTC to the moon 🚀",
            "author_id": "987654321",
            "created_at": "2024-01-15T10:30:00.000Z",
            "public_metrics": {
                "like_count": 150,
                "retweet_count": 45,
                "reply_count": 12,
                "quote_count": 8,
                "bookmark_count": 23,
                "impression_count": 5000,
            },
            "lang": "en",
            "entities": {
                "cashtags": [{"tag": "BTC"}],
                "hashtags": [],
                "urls": [],
            },
        }

        users = {"987654321": {"username": "cryptotrader"}}

        tweet = mock_client._parse_tweet(tweet_data, users)

        assert tweet.id == "123456789"
        assert tweet.author_username == "cryptotrader"
        assert tweet.likes == 150
        assert tweet.reposts == 45
        assert tweet.cashtags == ["BTC"]
        assert tweet.engagement_rate == pytest.approx(0.043, abs=0.001)

    @pytest.mark.asyncio
    async def test_engagement_filter(self, mock_client):
        """Test engagement filtering logic."""
        tweet = Tweet(
            id="1",
            author_id="a1",
            author_username="user",
            text="Test",
            created_at=datetime.now(timezone.utc),
            likes=50,
            reposts=20,
            replies=5,
        )

        # Should pass
        assert mock_client._passes_engagement_filter(tweet, {"likes": 10}) is True
        assert mock_client._passes_engagement_filter(tweet, {"likes": 50}) is True

        # Should fail
        assert mock_client._passes_engagement_filter(tweet, {"likes": 100}) is False
        assert mock_client._passes_engagement_filter(tweet, {"retweets": 50}) is False

    @pytest.mark.asyncio
    async def test_rate_limit_handling(self, mock_client):
        """Test rate limit header parsing."""
        headers = MagicMock()
        headers.get = MagicMock(
            side_effect=lambda x: {
                "x-rate-limit-remaining": "100",
                "x-rate-limit-reset": "1705312800",
            }.get(x)
        )

        mock_client._update_rate_limits(headers)

        assert mock_client._rate_limit_remaining == 100
        assert mock_client._rate_limit_reset is not None


class TestTweetStore:
    """Test suite for TweetStore."""

    @pytest.fixture
    def temp_store(self, tmp_path):
        """Create a temporary TweetStore."""
        from narrativealpha.ingestion.storage import TweetStore

        db_path = tmp_path / "test.db"
        return TweetStore(str(db_path))

    def test_store_and_retrieve(self, temp_store):
        """Test storing and retrieving a tweet."""
        tweet = Tweet(
            id="123",
            author_id="a1",
            author_username="testuser",
            text="Test tweet about $ETH",
            created_at=datetime.now(timezone.utc),
            likes=100,
            cashtags=["ETH"],
        )

        # Store
        inserted = temp_store.store_tweet(tweet)
        assert inserted is True

        # Duplicate should return False
        inserted = temp_store.store_tweet(tweet)
        assert inserted is False

        # Retrieve
        retrieved = temp_store.get_tweet("123")
        assert retrieved is not None
        assert retrieved.text == "Test tweet about $ETH"
        assert retrieved.cashtags == ["ETH"]

    def test_get_unprocessed(self, temp_store):
        """Test retrieving unprocessed tweets."""
        for i in range(5):
            tweet = Tweet(
                id=f"tweet_{i}",
                author_id="a1",
                author_username="user",
                text=f"Tweet {i}",
                created_at=datetime.now(timezone.utc),
                processed=(i >= 3),  # First 3 unprocessed
            )
            temp_store.store_tweet(tweet)

        unprocessed = temp_store.get_unprocessed(limit=10)
        assert len(unprocessed) == 3
        assert all(not t.processed for t in unprocessed)

    def test_mark_processed(self, temp_store):
        """Test marking tweets as processed."""
        for i in range(3):
            tweet = Tweet(
                id=f"tweet_{i}",
                author_id="a1",
                author_username="user",
                text=f"Tweet {i}",
                created_at=datetime.now(timezone.utc),
            )
            temp_store.store_tweet(tweet)

        updated = temp_store.mark_processed(["tweet_0", "tweet_1"])
        assert updated == 2

        stats = temp_store.get_stats()
        assert stats["processed"] == 2
        assert stats["unprocessed"] == 1

    def test_stats(self, temp_store):
        """Test storage statistics."""
        for i in range(10):
            tweet = Tweet(
                id=f"tweet_{i}",
                author_id="a1",
                author_username="user",
                text=f"Tweet {i}",
                created_at=datetime.now(timezone.utc),
            )
            temp_store.store_tweet(tweet)

        stats = temp_store.get_stats()
        assert stats["total_tweets"] == 10
        assert stats["unprocessed"] == 10
        assert stats["processed"] == 0
