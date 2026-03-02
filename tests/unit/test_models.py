"""Tests for data models."""
import pytest
from datetime import datetime, timezone

from narrativealpha.models import Tweet, RedditPost, Narrative


class TestTweet:
    """Test suite for Tweet model."""
    
    def test_create_minimal(self):
        """Test creating a tweet with minimal fields."""
        tweet = Tweet(
            id="123",
            author_id="a1",
            author_username="testuser",
            text="Hello world",
            created_at=datetime.now(timezone.utc),
        )
        
        assert tweet.id == "123"
        assert tweet.platform == "twitter"
        assert tweet.likes == 0
        assert tweet.cashtags == []
    
    def test_create_full(self):
        """Test creating a tweet with all fields."""
        now = datetime.now(timezone.utc)
        
        tweet = Tweet(
            id="123",
            author_id="a1",
            author_username="crypto_guru",
            text="$BTC is going to $100k! 🚀",
            created_at=now,
            likes=1000,
            replies=150,
            reposts=500,
            quote_tweets=200,
            bookmarks=50,
            impressions=50000,
            language="en",
            cashtags=["BTC"],
            hashtags=["crypto", "bitcoin"],
            urls=["https://example.com"],
            engagement_rate=0.036,
            is_reply=False,
            is_retweet=False,
        )
        
        assert tweet.cashtags == ["BTC"]
        assert tweet.hashtags == ["crypto", "bitcoin"]
        assert tweet.engagement_rate == 0.036


class TestNarrative:
    """Test suite for Narrative model."""
    
    def test_create_narrative(self):
        """Test creating a narrative."""
        narrative = Narrative(
            id="nar_123",
            name="Bitcoin Halving Bull Run",
            description="Narrative around Bitcoin's upcoming halving event",
            cashtags=["BTC", "ETH"],
            keywords=["halving", "bull run", "moon"],
            sentiment_score=0.75,
            velocity_score=0.85,
            overall_score=0.80,
        )
        
        assert narrative.is_active is True
        assert narrative.confidence == 0.0
        assert "BTC" in narrative.cashtags
