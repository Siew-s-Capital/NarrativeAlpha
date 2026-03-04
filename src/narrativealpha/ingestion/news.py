"""News API client for ingesting market-relevant articles."""

from datetime import datetime, timedelta
from typing import AsyncIterator, Optional

import httpx
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from narrativealpha.config.settings import settings
from narrativealpha.models import NewsArticle

logger = structlog.get_logger()


class NewsRateLimitError(Exception):
    """Raised when News API rate limit is hit."""


class NewsClient:
    """Async News API client with retry + filtering support."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        self.api_key = api_key or settings.news_api_key
        self.base_url = (base_url or settings.news_api_base_url).rstrip("/")

        if not self.api_key:
            raise ValueError("News API key required")

        self.client = httpx.AsyncClient(
            headers={"X-Api-Key": self.api_key},
            timeout=30.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

        logger.info("news_client.initialized", base_url=self.base_url)

    async def close(self) -> None:
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, NewsRateLimitError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    async def _request(self, endpoint: str, params: dict) -> dict:
        response = await self.client.get(f"{self.base_url}{endpoint}", params=params)

        if response.status_code == 429:
            raise NewsRateLimitError("News API rate limited")

        response.raise_for_status()
        return response.json()

    async def search_articles(
        self,
        query: str,
        max_results: int = 100,
        hours_back: int = 24,
        language: str = "en",
        sort_by: str = "publishedAt",
        domains: Optional[list[str]] = None,
    ) -> AsyncIterator[NewsArticle]:
        """Search market-relevant news using News API /everything endpoint."""
        from_time = (datetime.utcnow() - timedelta(hours=hours_back)).isoformat()
        page = 1
        page_size = min(max(max_results, 1), 100)
        fetched = 0

        while fetched < max_results:
            params = {
                "q": query,
                "from": from_time,
                "language": language,
                "sortBy": sort_by,
                "pageSize": min(page_size, max_results - fetched),
                "page": page,
            }
            if domains:
                params["domains"] = ",".join(domains)

            data = await self._request("/everything", params=params)
            articles = data.get("articles", [])
            if not articles:
                break

            for article in articles:
                parsed = self._parse_article(article)
                if parsed is None:
                    continue
                yield parsed
                fetched += 1
                if fetched >= max_results:
                    break

            if len(articles) < params["pageSize"]:
                break
            page += 1

    def _parse_article(self, article: dict) -> Optional[NewsArticle]:
        url = article.get("url")
        published_at = article.get("publishedAt")
        if not url or not published_at:
            return None

        try:
            created_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except ValueError:
            return None

        title = article.get("title") or ""
        description = article.get("description")
        content = article.get("content")

        text_parts = [part.strip() for part in [title, description, content] if part]
        text = "\n\n".join(text_parts)[:4000]

        source = article.get("source") or {}
        source_name = source.get("name") or "unknown"
        source_id = source.get("id")

        # lightweight cashtag/hashtag extraction
        import re

        cashtags = list(set(re.findall(r"\$([A-Za-z]{1,10})", text)))
        hashtags = list(set(re.findall(r"#(\w+)", text)))

        import hashlib

        article_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

        return NewsArticle(
            id=f"news_{article_id}",
            author_id=source_id or source_name,
            author_username=source_name,
            text=text or title,
            created_at=created_at,
            likes=0,
            replies=0,
            reposts=0,
            language=None,
            cashtags=cashtags,
            hashtags=hashtags,
            urls=[url],
            source_name=source_name,
            source_id=source_id,
            title=title or url,
            description=description,
            article_url=url,
            image_url=article.get("urlToImage"),
        )
