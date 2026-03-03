"""Unified ingestion pipeline orchestrator for multi-platform data collection."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional

import structlog

from narrativealpha.config.settings import settings
from narrativealpha.ingestion.twitter import TwitterClient
from narrativealpha.ingestion.reddit import RedditClient
from narrativealpha.ingestion.storage import SocialPostStore
from narrativealpha.models import SocialPost, Tweet, RedditPost

logger = structlog.get_logger()


@dataclass
class IngestionConfig:
    """Configuration for ingestion sources."""
    
    # Twitter settings
    twitter_enabled: bool = True
    twitter_queries: list[str] = field(default_factory=list)
    twitter_max_results: int = 100
    twitter_min_likes: int = 10
    
    # Reddit settings
    reddit_enabled: bool = True
    reddit_subreddits: list[str] = field(default_factory=list)
    reddit_keywords: list[str] = field(default_factory=list)
    reddit_max_results: int = 50
    reddit_min_upvotes: int = 5
    reddit_sort: str = "hot"
    
    # General settings
    hours_back: int = 24
    store_path: Optional[str] = None


@dataclass
class IngestionResult:
    """Result of an ingestion run."""
    
    source: str
    started_at: datetime
    completed_at: datetime
    posts_stored: int = 0
    duplicates_skipped: int = 0
    errors: list[str] = field(default_factory=list)
    success: bool = True


class PipelineOrchestrator:
    """
    Orchestrates multi-platform social media ingestion.
    
    Handles Twitter, Reddit, and future sources with unified storage,
    error handling, and statistics aggregation.
    """
    
    def __init__(self, config: Optional[IngestionConfig] = None):
        self.config = config or IngestionConfig()
        self.store = SocialPostStore(self.config.store_path)
        self._results: list[IngestionResult] = []
        
        logger.info("orchestrator.initialized")
    
    async def ingest_twitter(
        self,
        queries: Optional[list[str]] = None,
        max_results: Optional[int] = None,
        min_likes: Optional[int] = None,
        hours_back: Optional[int] = None,
    ) -> IngestionResult:
        """
        Run Twitter ingestion.
        
        Args:
            queries: Search queries (uses config if not provided)
            max_results: Max tweets per query
            min_likes: Minimum likes filter
            hours_back: Hours back to search
        
        Returns:
            IngestionResult with stats
        """
        queries = queries or self.config.twitter_queries
        max_results = max_results or self.config.twitter_max_results
        min_likes = min_likes or self.config.twitter_min_likes
        hours_back = hours_back or self.config.hours_back
        
        started_at = datetime.utcnow()
        result = IngestionResult(
            source="twitter",
            started_at=started_at,
            completed_at=started_at,
        )
        
        if not queries:
            result.errors.append("No Twitter queries configured")
            result.success = False
            return result
        
        logger.info(
            "twitter_ingestion.starting",
            queries=queries,
            max_results=max_results,
        )
        
        try:
            async with TwitterClient() as client:
                start_time = datetime.utcnow() - timedelta(hours=hours_back)
                min_engagement = {"likes": min_likes} if min_likes > 0 else None
                
                for query in queries:
                    try:
                        async for tweet in client.search_recent_tweets(
                            query=query,
                            max_results=max_results,
                            min_engagement=min_engagement,
                            start_time=start_time,
                        ):
                            inserted = self.store.store_tweet(tweet)
                            if inserted:
                                result.posts_stored += 1
                            else:
                                result.duplicates_skipped += 1
                    except Exception as e:
                        error_msg = f"Query '{query}' failed: {str(e)}"
                        logger.error("twitter.query_failed", query=query, error=str(e))
                        result.errors.append(error_msg)
            
            result.completed_at = datetime.utcnow()
            logger.info(
                "twitter_ingestion.completed",
                stored=result.posts_stored,
                duplicates=result.duplicates_skipped,
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Twitter ingestion failed: {str(e)}")
            logger.error("twitter_ingestion.failed", error=str(e))
        
        self._results.append(result)
        return result
    
    async def ingest_reddit(
        self,
        subreddits: Optional[list[str]] = None,
        keywords: Optional[list[str]] = None,
        max_results: Optional[int] = None,
        min_upvotes: Optional[int] = None,
        hours_back: Optional[int] = None,
        sort: Optional[str] = None,
    ) -> IngestionResult:
        """
        Run Reddit ingestion.
        
        Args:
            subreddits: Subreddits to search
            keywords: Keywords to filter
            max_results: Max posts per subreddit
            min_upvotes: Minimum upvotes filter
            hours_back: Hours back to search
            sort: Sort method (hot/new/top/rising)
        
        Returns:
            IngestionResult with stats
        """
        subreddits = subreddits or self.config.reddit_subreddits
        keywords = keywords or self.config.reddit_keywords
        max_results = max_results or self.config.reddit_max_results
        min_upvotes = min_upvotes or self.config.reddit_min_upvotes
        hours_back = hours_back or self.config.hours_back
        sort = sort or self.config.reddit_sort
        
        started_at = datetime.utcnow()
        result = IngestionResult(
            source="reddit",
            started_at=started_at,
            completed_at=started_at,
        )
        
        if not subreddits:
            result.errors.append("No Reddit subreddits configured")
            result.success = False
            return result
        
        logger.info(
            "reddit_ingestion.starting",
            subreddits=subreddits,
            keywords=keywords,
        )
        
        try:
            async with RedditClient() as client:
                async for post in client.search_multiple_subreddits(
                    subreddits=subreddits,
                    keywords=keywords,
                    sort=sort,
                    max_results_per_sub=max_results,
                    min_upvotes=min_upvotes,
                    hours_back=hours_back,
                ):
                    inserted = self.store.store_reddit_post(post)
                    if inserted:
                        result.posts_stored += 1
                    else:
                        result.duplicates_skipped += 1
            
            result.completed_at = datetime.utcnow()
            logger.info(
                "reddit_ingestion.completed",
                stored=result.posts_stored,
                duplicates=result.duplicates_skipped,
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Reddit ingestion failed: {str(e)}")
            logger.error("reddit_ingestion.failed", error=str(e))
        
        self._results.append(result)
        return result
    
    async def run_all(self) -> list[IngestionResult]:
        """
        Run all configured ingestion sources.
        
        Returns:
            List of IngestionResult for each source
        """
        self._results = []
        
        logger.info("orchestrator.run_all.starting")
        
        tasks = []
        
        if self.config.twitter_enabled and self.config.twitter_queries:
            tasks.append(self.ingest_twitter())
        
        if self.config.reddit_enabled and self.config.reddit_subreddits:
            tasks.append(self.ingest_reddit())
        
        if not tasks:
            logger.warning("orchestrator.no_sources_configured")
            return []
        
        # Run all ingestions concurrently
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(
            "orchestrator.run_all.completed",
            results_count=len(self._results),
        )
        
        return self._results
    
    def get_stats(self) -> dict:
        """Get current storage stats."""
        return self.store.get_stats()
    
    def get_results(self) -> list[IngestionResult]:
        """Get results from last run."""
        return self._results.copy()
    
    def print_summary(self) -> None:
        """Print formatted summary of ingestion results."""
        print("\n" + "=" * 50)
        print("INGESTION SUMMARY")
        print("=" * 50)
        
        for result in self._results:
            status = "✅" if result.success else "❌"
            duration = result.completed_at - result.started_at
            
            print(f"\n{status} {result.source.upper()}")
            print(f"   Duration: {duration.total_seconds():.1f}s")
            print(f"   Posts stored: {result.posts_stored}")
            print(f"   Duplicates skipped: {result.duplicates_skipped}")
            
            if result.errors:
                print(f"   Errors: {len(result.errors)}")
                for error in result.errors[:3]:  # Show first 3 errors
                    print(f"      - {error}")
        
        stats = self.get_stats()
        print(f"\n📊 STORAGE STATS")
        print(f"   Total posts: {stats['total_posts']}")
        print(f"   Twitter: {stats.get('twitter', 0)}")
        print(f"   Reddit: {stats.get('reddit', 0)}")
        print(f"   Unprocessed: {stats['unprocessed']}")
        print("=" * 50)
