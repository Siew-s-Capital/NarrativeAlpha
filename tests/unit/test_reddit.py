"""Tests for Reddit ingestion."""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from narrativealpha.models import RedditPost
from narrativealpha.ingestion.reddit import RedditClient


class TestRedditPost:
    """Test suite for RedditPost model."""
    
    def test_create_minimal(self):
        """Test creating a Reddit post with minimal fields."""
        post = RedditPost(
            id="reddit_123",
            author_id="u_testuser",
            author_username="testuser",
            text="Check out $BTC!",
            created_at=datetime.now(timezone.utc),
            subreddit="Cryptocurrency",
        )
        
        assert post.id == "reddit_123"
        assert post.platform == "reddit"
        assert post.subreddit == "Cryptocurrency"
        assert post.upvotes == 0
        assert post.cashtags == []
    
    def test_create_full(self):
        """Test creating a Reddit post with all fields."""
        now = datetime.now(timezone.utc)
        
        post = RedditPost(
            id="reddit_456",
            author_id="u_cryptoguru",
            author_username="cryptoguru",
            text="$ETH is the future! #crypto #ethereum",
            created_at=now,
            likes=500,
            replies=100,
            subreddit="ethereum",
            title="Why ETH will flip BTC",
            upvotes=500,
            downvotes=50,
            awards=3,
            is_self=True,
            permalink="/r/ethereum/comments/456/test",
            cashtags=["ETH"],
            hashtags=["crypto", "ethereum"],
        )
        
        assert post.platform == "reddit"
        assert post.subreddit == "ethereum"
        assert post.title == "Why ETH will flip BTC"
        assert post.upvotes == 500
        assert post.awards == 3
        assert "ETH" in post.cashtags
    
    def test_cashtag_extraction_in_text(self):
        """Test that cashtags are extracted from text."""
        post = RedditPost(
            id="reddit_789",
            author_id="u_trader",
            author_username="trader",
            text="Buying $BTC and $ETH today! Also watching $SOL.",
            created_at=datetime.now(timezone.utc),
            subreddit="CryptoMarkets",
            cashtags=["BTC", "ETH", "SOL"],
        )
        
        assert len(post.cashtags) == 3
        assert "BTC" in post.cashtags
        assert "ETH" in post.cashtags
        assert "SOL" in post.cashtags


class TestRedditClient:
    """Test suite for RedditClient."""
    
    @pytest.fixture
    def mock_reddit_credentials(self):
        """Mock Reddit credentials."""
        with patch("narrativealpha.ingestion.reddit.settings") as mock_settings:
            mock_settings.reddit_client_id = "test_client_id"
            mock_settings.reddit_client_secret = "test_client_secret"
            mock_settings.reddit_user_agent = "TestBot/1.0"
            yield mock_settings
    
    @pytest.mark.asyncio
    async def test_client_initialization(self, mock_reddit_credentials):
        """Test Reddit client initializes correctly."""
        with patch("narrativealpha.ingestion.reddit.asyncpraw") as mock_asyncpraw:
            mock_reddit = AsyncMock()
            mock_asyncpraw.Reddit.return_value = mock_reddit
            
            client = RedditClient()
            
            assert client.client_id == "test_client_id"
            assert client.client_secret == "test_client_secret"
            assert client.user_agent == "TestBot/1.0"
    
    @pytest.mark.asyncio
    async def test_client_connect(self, mock_reddit_credentials):
        """Test Reddit client connection."""
        with patch("narrativealpha.ingestion.reddit.asyncpraw") as mock_asyncpraw:
            mock_reddit = AsyncMock()
            mock_asyncpraw.Reddit.return_value = mock_reddit
            
            client = RedditClient()
            await client.connect()
            
            mock_asyncpraw.Reddit.assert_called_once_with(
                client_id="test_client_id",
                client_secret="test_client_secret",
                user_agent="TestBot/1.0",
            )
            assert client._reddit is not None
            
            await client.close()
            mock_reddit.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_parse_submission(self, mock_reddit_credentials):
        """Test parsing Reddit submission into RedditPost."""
        with patch("narrativealpha.ingestion.reddit.asyncpraw"):
            client = RedditClient()
            
            # Create mock submission
            mock_submission = MagicMock()
            mock_submission.id = "abc123"
            mock_submission.author = "testuser"
            mock_submission.title = "$BTC to the moon!"
            mock_submission.selftext = "I believe $BTC will hit $100k soon."
            mock_submission.created_utc = 1700000000
            mock_submission.score = 1000
            mock_submission.num_comments = 200
            mock_submission.subreddit = "Bitcoin"
            mock_submission.is_self = True
            mock_submission.url = "https://self.post"
            mock_submission.downs = 50
            mock_submission.total_awards_received = 5
            mock_submission.permalink = "/r/Bitcoin/comments/abc123/test"
            
            post = client._parse_submission(mock_submission)
            
            assert post.id == "reddit_abc123"
            assert post.author_username == "testuser"
            assert post.subreddit == "Bitcoin"
            assert post.upvotes == 1000
            assert post.likes == 1000  # Normalized to likes
            assert post.replies == 200
            assert "BTC" in post.cashtags
            assert post.permalink == "https://reddit.com/r/Bitcoin/comments/abc123/test"
    
    def test_client_missing_credentials(self):
        """Test client raises error with missing credentials."""
        with patch("narrativealpha.ingestion.reddit.settings") as mock_settings:
            mock_settings.reddit_client_id = None
            mock_settings.reddit_client_secret = None
            
            with pytest.raises(ValueError, match="Reddit client ID and secret required"):
                RedditClient()
    
    def test_client_custom_credentials(self):
        """Test client with explicitly provided credentials."""
        with patch("narrativealpha.ingestion.reddit.asyncpraw"):
            client = RedditClient(
                client_id="custom_id",
                client_secret="custom_secret",
                user_agent="CustomBot/1.0",
            )
            
            assert client.client_id == "custom_id"
            assert client.client_secret == "custom_secret"
            assert client.user_agent == "CustomBot/1.0"
