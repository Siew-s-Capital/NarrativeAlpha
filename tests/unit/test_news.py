"""Tests for News ingestion."""

from datetime import datetime, timezone

import pytest

from narrativealpha.ingestion.news import NewsClient
from narrativealpha.models import NewsArticle


class TestNewsClient:
    @pytest.fixture
    def client(self):
        return NewsClient(api_key="test_api_key", base_url="https://example.com/v2")

    def test_parse_article(self, client):
        article = {
            "source": {"id": "coindesk", "name": "CoinDesk"},
            "title": "Bitcoin ETF sees record inflows",
            "description": "Spot ETF products continue momentum.",
            "content": "Analysts say $BTC narrative is strengthening #crypto.",
            "url": "https://www.coindesk.com/test-article",
            "urlToImage": "https://www.coindesk.com/image.png",
            "publishedAt": "2026-03-04T12:00:00Z",
        }

        parsed = client._parse_article(article)

        assert isinstance(parsed, NewsArticle)
        assert parsed.platform == "news"
        assert parsed.source_name == "CoinDesk"
        assert parsed.title == "Bitcoin ETF sees record inflows"
        assert "BTC" in parsed.cashtags
        assert "crypto" in parsed.hashtags
        assert parsed.article_url == "https://www.coindesk.com/test-article"

    def test_parse_article_missing_url_returns_none(self, client):
        article = {
            "source": {"id": "coindesk", "name": "CoinDesk"},
            "title": "No URL",
            "publishedAt": "2026-03-04T12:00:00Z",
        }
        article.pop("url", None)
        assert client._parse_article(article) is None


class TestNewsStorage:
    def test_store_and_retrieve_news(self, tmp_path):
        from narrativealpha.ingestion.storage import SocialPostStore

        store = SocialPostStore(str(tmp_path / "test.db"))
        article = NewsArticle(
            id="news_abc123",
            author_id="coindesk",
            author_username="CoinDesk",
            text="Bitcoin ETF sees record inflows",
            created_at=datetime.now(timezone.utc),
            source_name="CoinDesk",
            source_id="coindesk",
            title="Bitcoin ETF sees record inflows",
            description="desc",
            article_url="https://www.coindesk.com/test-article",
            image_url=None,
        )

        assert store.store_news_article(article) is True
        assert store.store_news_article(article) is False

        retrieved = store.get_post("news_abc123")
        assert retrieved is not None
        assert retrieved.platform == "news"
        assert retrieved.title == "Bitcoin ETF sees record inflows"
