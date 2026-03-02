"""Twitter/X API client for ingesting tweets."""
import asyncio
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from narrativealpha.config.settings import settings
from narrativealpha.models import Tweet

logger = structlog.get_logger()


class TwitterRateLimitError(Exception):
    """Raised when Twitter rate limit is hit."""
    pass


class TwitterClient:
    """Async Twitter API v2 client with rate limit handling."""
    
    BASE_URL = "https://api.twitter.com/2"
    
    def __init__(self, bearer_token: Optional[str] = None):
        self.bearer_token = bearer_token or settings.twitter_bearer_token
        if not self.bearer_token:
            raise ValueError("Twitter bearer token required")
        
        self.client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self.bearer_token}"},
            timeout=30.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        
        # Rate limit tracking
        self._rate_limit_remaining: Optional[int] = None
        self._rate_limit_reset: Optional[datetime] = None
        
        logger.info("twitter_client.initialized")
    
    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()
        logger.debug("twitter_client.closed")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, *args):
        await self.close()
    
    def _update_rate_limits(self, headers: httpx.Headers) -> None:
        """Update rate limit tracking from response headers."""
        remaining = headers.get("x-rate-limit-remaining")
        reset_ts = headers.get("x-rate-limit-reset")
        
        if remaining:
            self._rate_limit_remaining = int(remaining)
        if reset_ts:
            self._rate_limit_reset = datetime.fromtimestamp(int(reset_ts))
        
        logger.debug(
            "rate_limits.updated",
            remaining=self._rate_limit_remaining,
            reset_at=self._rate_limit_reset,
        )
    
    async def _wait_for_rate_limit_reset(self) -> None:
        """Wait until rate limit resets if necessary."""
        if self._rate_limit_remaining == 0 and self._rate_limit_reset:
            wait_seconds = (self._rate_limit_reset - datetime.utcnow()).total_seconds()
            if wait_seconds > 0:
                logger.warning(
                    "rate_limit.waiting",
                    wait_seconds=wait_seconds,
                    reset_at=self._rate_limit_reset.isoformat(),
                )
                await asyncio.sleep(wait_seconds + 1)  # Buffer second
    
    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, TwitterRateLimitError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    async def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make an authenticated request to Twitter API."""
        await self._wait_for_rate_limit_reset()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = await self.client.request(method, url, **kwargs)
            self._update_rate_limits(response.headers)
            
            if response.status_code == 429:
                reset_ts = response.headers.get("x-rate-limit-reset")
                if reset_ts:
                    self._rate_limit_reset = datetime.fromtimestamp(int(reset_ts))
                raise TwitterRateLimitError(f"Rate limited. Reset at {self._rate_limit_reset}")
            
            response.raise_for_status()
            return response.json()
            
        except httpx.HTTPStatusError as e:
            logger.error(
                "twitter_api.error",
                status_code=e.response.status_code,
                endpoint=endpoint,
                error=str(e),
            )
            raise
    
    async def search_recent_tweets(
        self,
        query: str,
        max_results: int = 100,
        min_engagement: Optional[dict] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> AsyncIterator[Tweet]:
        """
        Search recent tweets matching query.
        
        Args:
            query: Search query (supports cashtags like $BTC)
            max_results: Maximum tweets to return (10-100 per page)
            min_engagement: Filter by engagement {"likes": 10, "retweets": 5}
            start_time: Start of search window
            end_time: End of search window
        
        Yields:
            Tweet objects
        """
        params = {
            "query": query,
            "max_results": min(max(max_results, 10), 100),
            "tweet.fields": "created_at,public_metrics,lang,entities,author_id",
            "expansions": "author_id",
            "user.fields": "username,public_metrics",
        }
        
        if start_time:
            params["start_time"] = start_time.isoformat()
        if end_time:
            params["end_time"] = end_time.isoformat()
        
        next_token = None
        tweets_fetched = 0
        
        logger.info(
            "twitter_search.starting",
            query=query,
            max_results=max_results,
        )
        
        while tweets_fetched < max_results:
            if next_token:
                params["next_token"] = next_token
            
            try:
                data = await self._request("GET", "/tweets/search/recent", params=params)
            except TwitterRateLimitError:
                logger.warning("twitter_search.rate_limited", fetched=tweets_fetched)
                break
            
            tweets = data.get("data", [])
            users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
            
            for tweet_data in tweets:
                tweet = self._parse_tweet(tweet_data, users)
                
                # Apply engagement filter
                if min_engagement and not self._passes_engagement_filter(tweet, min_engagement):
                    continue
                
                yield tweet
                tweets_fetched += 1
                
                if tweets_fetched >= max_results:
                    break
            
            next_token = data.get("meta", {}).get("next_token")
            if not next_token:
                break
        
        logger.info(
            "twitter_search.completed",
            query=query,
            tweets_fetched=tweets_fetched,
        )
    
    def _parse_tweet(self, data: dict, users: dict) -> Tweet:
        """Parse Twitter API response into Tweet model."""
        metrics = data.get("public_metrics", {})
        author_id = data.get("author_id", "")
        author = users.get(author_id, {})
        
        # Extract cashtags and hashtags
        entities = data.get("entities", {})
        cashtags = [c["tag"] for c in entities.get("cashtags", [])]
        hashtags = [h["tag"] for h in entities.get("hashtags", [])]
        urls = [u["expanded_url"] for u in entities.get("urls", [])]
        
        # Calculate engagement rate
        impressions = metrics.get("impression_count", 0)
        total_engagement = (
            metrics.get("like_count", 0) +
            metrics.get("retweet_count", 0) +
            metrics.get("reply_count", 0) +
            metrics.get("quote_count", 0)
        )
        engagement_rate = total_engagement / impressions if impressions > 0 else None
        
        return Tweet(
            id=data["id"],
            author_id=author_id,
            author_username=author.get("username", "unknown"),
            text=data["text"],
            created_at=datetime.fromisoformat(data["created_at"].replace("Z", "+00:00")),
            likes=metrics.get("like_count", 0),
            replies=metrics.get("reply_count", 0),
            reposts=metrics.get("retweet_count", 0),
            quote_tweets=metrics.get("quote_count", 0),
            bookmarks=metrics.get("bookmark_count", 0),
            impressions=impressions,
            language=data.get("lang"),
            cashtags=cashtags,
            hashtags=hashtags,
            urls=urls,
            engagement_rate=engagement_rate,
            is_reply=data.get("in_reply_to_user_id") is not None,
            is_retweet=data.get("referenced_tweets", [{}])[0].get("type") == "retweeted" if data.get("referenced_tweets") else False,
        )
    
    def _passes_engagement_filter(self, tweet: Tweet, min_engagement: dict) -> bool:
        """Check if tweet meets minimum engagement thresholds."""
        if "likes" in min_engagement and tweet.likes < min_engagement["likes"]:
            return False
        if "retweets" in min_engagement and tweet.reposts < min_engagement["retweets"]:
            return False
        if "replies" in min_engagement and tweet.replies < min_engagement["replies"]:
            return False
        return True
