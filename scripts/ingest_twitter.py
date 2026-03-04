#!/usr/bin/env python3
"""CLI for running Twitter ingestion."""

import asyncio
import argparse
import sys
from datetime import datetime, timedelta

import structlog

from narrativealpha.ingestion.twitter import TwitterClient
from narrativealpha.ingestion.storage import TweetStore

structlog.configure(
    processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()]
)
logger = structlog.get_logger()


async def ingest_tweets(
    queries: list[str],
    max_results: int = 100,
    min_likes: int = 0,
    hours_back: int = 24,
) -> None:
    """
    Ingest tweets for given queries.

    Args:
        queries: List of search queries (supports cashtags like $BTC)
        max_results: Max tweets per query
        min_likes: Minimum likes filter
        hours_back: How many hours back to search
    """
    store = TweetStore()

    # Show current stats
    stats = store.get_stats()
    logger.info("ingestion.starting", current_stats=stats, queries=queries)

    async with TwitterClient() as client:
        for query in queries:
            logger.info("query.processing", query=query)

            start_time = datetime.utcnow() - timedelta(hours=hours_back)

            min_engagement = {"likes": min_likes} if min_likes > 0 else None

            stored_count = 0
            duplicate_count = 0

            async for tweet in client.search_recent_tweets(
                query=query,
                max_results=max_results,
                min_engagement=min_engagement,
                start_time=start_time,
            ):
                inserted = store.store_tweet(tweet)
                if inserted:
                    stored_count += 1
                else:
                    duplicate_count += 1

            logger.info(
                "query.completed",
                query=query,
                stored=stored_count,
                duplicates=duplicate_count,
            )

    # Final stats
    final_stats = store.get_stats()
    logger.info("ingestion.completed", final_stats=final_stats)
    print(f"\nIngestion complete!")
    print(f"Total tweets in DB: {final_stats['total_tweets']}")
    print(f"Unprocessed: {final_stats['unprocessed']}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest tweets from Twitter/X API for narrative analysis"
    )
    parser.add_argument("queries", nargs="+", help="Search queries (e.g., '$BTC' '$ETH crypto')")
    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Maximum tweets to fetch per query (default: 100)",
    )
    parser.add_argument(
        "--min-likes", type=int, default=10, help="Minimum likes threshold (default: 10)"
    )
    parser.add_argument(
        "--hours-back", type=int, default=24, help="Hours back to search (default: 24)"
    )

    args = parser.parse_args()

    try:
        asyncio.run(
            ingest_tweets(
                queries=args.queries,
                max_results=args.max_results,
                min_likes=args.min_likes,
                hours_back=args.hours_back,
            )
        )
    except KeyboardInterrupt:
        logger.info("ingestion.interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error("ingestion.failed", error=str(e))
        raise


if __name__ == "__main__":
    main()
