"""Reddit API client for ingesting posts."""

import asyncio
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional

import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

try:
    import asyncpraw
    from asyncpraw import Reddit
    from asyncprawcore.exceptions import ResponseException, RequestException

    ASYNCPRAW_AVAILABLE = True
except ImportError:
    ASYNCPRAW_AVAILABLE = False

from narrativealpha.config.settings import settings
from narrativealpha.models import RedditPost

logger = structlog.get_logger()


class RedditRateLimitError(Exception):
    """Raised when Reddit rate limit is hit."""

    pass


class RedditClient:
    """Async Reddit API client with rate limit handling."""

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        if not ASYNCPRAW_AVAILABLE:
            raise ImportError(
                "asyncpraw is required for Reddit ingestion. "
                "Install with: pip install asyncpraw>=7.7"
            )

        self.client_id = client_id or settings.reddit_client_id
        self.client_secret = client_secret or settings.reddit_client_secret
        self.user_agent = user_agent or settings.reddit_user_agent

        if not self.client_id or not self.client_secret:
            raise ValueError("Reddit client ID and secret required")

        self._reddit: Optional[Reddit] = None
        self._rate_limit_remaining: Optional[int] = None
        self._rate_limit_reset: Optional[datetime] = None

        logger.info("reddit_client.initialized")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def connect(self) -> None:
        """Initialize Reddit connection."""
        self._reddit = asyncpraw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
        )
        logger.debug("reddit_client.connected")

    async def close(self) -> None:
        """Close Reddit connection."""
        if self._reddit:
            await self._reddit.close()
            logger.debug("reddit_client.closed")

    @retry(
        retry=retry_if_exception_type((ResponseException, RequestException, RedditRateLimitError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    async def _get_subreddit_posts(
        self,
        subreddit_name: str,
        sort: str = "hot",
        time_filter: Optional[str] = None,
        limit: int = 100,
    ) -> AsyncIterator[asyncpraw.models.Submission]:
        """Get posts from a subreddit with retry logic."""
        if not self._reddit:
            raise RuntimeError("Reddit client not connected. Use async context manager.")

        try:
            subreddit = await self._reddit.subreddit(subreddit_name)

            if sort == "hot":
                submissions = subreddit.hot(limit=limit)
            elif sort == "new":
                submissions = subreddit.new(limit=limit)
            elif sort == "top":
                submissions = subreddit.top(time_filter=time_filter or "day", limit=limit)
            elif sort == "rising":
                submissions = subreddit.rising(limit=limit)
            else:
                raise ValueError(f"Invalid sort: {sort}. Use hot/new/top/rising")

            async for submission in submissions:
                yield submission

        except ResponseException as e:
            if e.response.status_code == 429:
                logger.warning("reddit.rate_limited", subreddit=subreddit_name)
                raise RedditRateLimitError("Rate limited by Reddit API")
            logger.error(
                "reddit_api.error",
                subreddit=subreddit_name,
                status_code=e.response.status_code,
            )
            raise

    async def search_subreddit(
        self,
        subreddit: str,
        keywords: list[str],
        sort: str = "hot",
        time_filter: Optional[str] = None,
        max_results: int = 100,
        min_upvotes: int = 0,
        min_comments: int = 0,
        hours_back: Optional[int] = None,
    ) -> AsyncIterator[RedditPost]:
        """
        Search posts in a subreddit matching keywords.

        Args:
            subreddit: Subreddit name (without r/)
            keywords: List of keywords to search for (OR logic)
            sort: Sort method (hot/new/top/rising)
            time_filter: Time filter for top posts (hour/day/week/month/year/all)
            max_results: Maximum posts to return
            min_upvotes: Minimum upvotes filter
            min_comments: Minimum comments filter
            hours_back: Only return posts from last N hours

        Yields:
            RedditPost objects
        """
        cutoff_time = None
        if hours_back:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)

        logger.info(
            "reddit_search.starting",
            subreddit=subreddit,
            keywords=keywords,
            sort=sort,
            max_results=max_results,
        )

        posts_fetched = 0

        async for submission in self._get_subreddit_posts(
            subreddit, sort=sort, time_filter=time_filter, limit=max_results * 2
        ):
            # Time filter check
            if cutoff_time:
                post_time = datetime.utcfromtimestamp(submission.created_utc)
                if post_time < cutoff_time:
                    continue

            # Keyword filter (OR logic - match any keyword)
            if keywords:
                title_text = (submission.title or "").lower()
                body_text = (submission.selftext or "").lower()
                combined_text = f"{title_text} {body_text}"
                if not any(kw.lower() in combined_text for kw in keywords):
                    continue

            # Engagement filter
            if submission.score < min_upvotes:
                continue
            if submission.num_comments < min_comments:
                continue

            post = self._parse_submission(submission)
            yield post
            posts_fetched += 1

            if posts_fetched >= max_results:
                break

        logger.info(
            "reddit_search.completed",
            subreddit=subreddit,
            posts_fetched=posts_fetched,
        )

    async def search_multiple_subreddits(
        self,
        subreddits: list[str],
        keywords: list[str],
        sort: str = "hot",
        time_filter: Optional[str] = None,
        max_results_per_sub: int = 100,
        min_upvotes: int = 0,
        min_comments: int = 0,
        hours_back: Optional[int] = None,
    ) -> AsyncIterator[RedditPost]:
        """
        Search multiple subreddits in parallel.

        Args:
            subreddits: List of subreddit names
            keywords: List of keywords to search for
            sort: Sort method (hot/new/top/rising)
            time_filter: Time filter for top posts
            max_results_per_sub: Maximum posts per subreddit
            min_upvotes: Minimum upvotes filter
            min_comments: Minimum comments filter
            hours_back: Only return posts from last N hours

        Yields:
            RedditPost objects from all subreddits
        """
        logger.info(
            "reddit_multi_search.starting",
            subreddits=subreddits,
            keywords=keywords,
        )

        # Create search tasks for each subreddit
        async def search_one(sub: str) -> list[RedditPost]:
            posts = []
            try:
                async for post in self.search_subreddit(
                    subreddit=sub,
                    keywords=keywords,
                    sort=sort,
                    time_filter=time_filter,
                    max_results=max_results_per_sub,
                    min_upvotes=min_upvotes,
                    min_comments=min_comments,
                    hours_back=hours_back,
                ):
                    posts.append(post)
            except Exception as e:
                logger.error("reddit.subreddit_failed", subreddit=sub, error=str(e))
            return posts

        # Run searches concurrently
        results = await asyncio.gather(*[search_one(sub) for sub in subreddits])

        # Yield all posts (flattened)
        for posts in results:
            for post in posts:
                yield post

        total = sum(len(r) for r in results)
        logger.info("reddit_multi_search.completed", total_posts=total)

    def _parse_submission(self, submission: asyncpraw.models.Submission) -> RedditPost:
        """Parse Reddit submission into RedditPost model."""
        # Combine title and selftext for text field
        text_parts = [submission.title]
        if submission.selftext:
            text_parts.append(submission.selftext)
        text = "\n\n".join(text_parts)

        # Extract cashtags (crypto tickers like $BTC)
        import re

        cashtags = re.findall(r"\$([A-Za-z]{1,10})", text)

        # Extract hashtags
        hashtags = re.findall(r"#(\w+)", text)

        # Extract URLs
        urls = []
        if submission.url and not submission.is_self:
            urls.append(submission.url)
        if submission.selftext:
            url_pattern = r"https?://[^\s\)]+"
            urls.extend(re.findall(url_pattern, submission.selftext))

        created_at = datetime.utcfromtimestamp(submission.created_utc)

        return RedditPost(
            id=f"reddit_{submission.id}",
            author_id=str(submission.author) if submission.author else "deleted",
            author_username=str(submission.author) if submission.author else "deleted",
            text=text[:4000],  # Limit text length
            created_at=created_at,
            # Base engagement - use upvotes for likes to normalize across platforms
            likes=submission.score,
            replies=submission.num_comments,
            reposts=0,  # Reddit doesn't track reposts directly
            # Reddit-specific fields
            subreddit=str(submission.subreddit),
            title=submission.title,
            upvotes=submission.score,
            downvotes=getattr(submission, "downs", 0),
            awards=getattr(submission, "total_awards_received", 0),
            is_self=submission.is_self,
            permalink=f"https://reddit.com{submission.permalink}",
            # Content metadata
            language=None,  # Reddit doesn't expose language directly
            cashtags=list(set(cashtags)),
            hashtags=list(set(hashtags)),
            urls=list(set(urls)),
        )
