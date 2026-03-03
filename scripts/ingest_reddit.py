#!/usr/bin/env python3
"""CLI for running Reddit ingestion."""
import asyncio
import argparse
import sys
from datetime import datetime

import structlog

from narrativealpha.ingestion.reddit import RedditClient
from narrativealpha.ingestion.storage import SocialPostStore

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()


async def ingest_reddit(
    subreddits: list[str],
    keywords: list[str],
    max_results: int = 50,
    min_upvotes: int = 5,
    min_comments: int = 0,
    hours_back: int = 24,
    sort: str = "hot",
) -> None:
    """
    Ingest Reddit posts from specified subreddits.
    
    Args:
        subreddits: List of subreddit names (without r/)
        keywords: List of keywords to filter posts (OR logic)
        max_results: Max posts per subreddit
        min_upvotes: Minimum upvotes filter
        min_comments: Minimum comments filter
        hours_back: How many hours back to search
        sort: Sort method (hot/new/top/rising)
    """
    store = SocialPostStore()
    
    # Show current stats
    stats = store.get_stats()
    logger.info(
        "reddit_ingestion.starting",
        current_stats=stats,
        subreddits=subreddits,
        keywords=keywords,
    )
    
    total_stored = 0
    total_duplicates = 0
    
    async with RedditClient() as client:
        async for post in client.search_multiple_subreddits(
            subreddits=subreddits,
            keywords=keywords,
            sort=sort,
            max_results_per_sub=max_results,
            min_upvotes=min_upvotes,
            min_comments=min_comments,
            hours_back=hours_back,
        ):
            inserted = store.store_reddit_post(post)
            if inserted:
                total_stored += 1
            else:
                total_duplicates += 1
    
    # Final stats
    final_stats = store.get_stats()
    logger.info(
        "reddit_ingestion.completed",
        final_stats=final_stats,
        stored_this_run=total_stored,
        duplicates_this_run=total_duplicates,
    )
    
    print(f"\nReddit ingestion complete!")
    print(f"Posts stored this run: {total_stored}")
    print(f"Duplicates skipped: {total_duplicates}")
    print(f"Total posts in DB: {final_stats['total_posts']}")
    print(f"  - Twitter: {final_stats.get('twitter', 0)}")
    print(f"  - Reddit: {final_stats.get('reddit', 0)}")
    print(f"Unprocessed: {final_stats['unprocessed']}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Reddit posts for narrative analysis"
    )
    parser.add_argument(
        "--subreddits",
        nargs="+",
        required=True,
        help="Subreddit names to search (e.g., Cryptocurrency Bitcoin)",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=[],
        help="Keywords to filter posts (e.g., $BTC $ETH crypto)",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=50,
        help="Maximum posts to fetch per subreddit (default: 50)",
    )
    parser.add_argument(
        "--min-upvotes",
        type=int,
        default=5,
        help="Minimum upvotes threshold (default: 5)",
    )
    parser.add_argument(
        "--min-comments",
        type=int,
        default=0,
        help="Minimum comments threshold (default: 0)",
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=24,
        help="Only fetch posts from last N hours (default: 24)",
    )
    parser.add_argument(
        "--sort",
        choices=["hot", "new", "top", "rising"],
        default="hot",
        help="Sort method for posts (default: hot)",
    )
    
    args = parser.parse_args()
    
    try:
        asyncio.run(ingest_reddit(
            subreddits=args.subreddits,
            keywords=args.keywords,
            max_results=args.max_results,
            min_upvotes=args.min_upvotes,
            min_comments=args.min_comments,
            hours_back=args.hours_back,
            sort=args.sort,
        ))
    except KeyboardInterrupt:
        logger.info("reddit_ingestion.interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error("reddit_ingestion.failed", error=str(e))
        raise


if __name__ == "__main__":
    main()
