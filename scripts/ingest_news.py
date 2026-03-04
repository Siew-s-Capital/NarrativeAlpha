#!/usr/bin/env python3
"""CLI for running News API ingestion."""

import argparse
import asyncio
import sys

import structlog

from narrativealpha.ingestion.news import NewsClient
from narrativealpha.ingestion.storage import SocialPostStore

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()


async def ingest_news(
    queries: list[str],
    max_results: int = 50,
    hours_back: int = 24,
    language: str = "en",
    domains: list[str] | None = None,
) -> None:
    store = SocialPostStore()
    stats = store.get_stats()
    logger.info("news_ingestion.starting", current_stats=stats, queries=queries)

    stored = 0
    duplicates = 0

    async with NewsClient() as client:
        for query in queries:
            async for article in client.search_articles(
                query=query,
                max_results=max_results,
                hours_back=hours_back,
                language=language,
                domains=domains,
            ):
                inserted = store.store_news_article(article)
                if inserted:
                    stored += 1
                else:
                    duplicates += 1

    final_stats = store.get_stats()
    logger.info(
        "news_ingestion.completed",
        final_stats=final_stats,
        stored_this_run=stored,
        duplicates_this_run=duplicates,
    )

    print("\nNews ingestion complete!")
    print(f"Posts stored this run: {stored}")
    print(f"Duplicates skipped: {duplicates}")
    print(f"Total posts in DB: {final_stats['total_posts']}")
    print(f"  - Twitter: {final_stats.get('twitter', 0)}")
    print(f"  - Reddit: {final_stats.get('reddit', 0)}")
    print(f"  - News: {final_stats.get('news', 0)}")
    print(f"Unprocessed: {final_stats['unprocessed']}")


def main():
    parser = argparse.ArgumentParser(description="Ingest news articles for narrative analysis")
    parser.add_argument("queries", nargs="+", help='Search query terms (e.g. "$BTC ETF")')
    parser.add_argument("--max-results", type=int, default=50)
    parser.add_argument("--hours-back", type=int, default=24)
    parser.add_argument("--language", default="en")
    parser.add_argument("--domains", nargs="+", default=None)

    args = parser.parse_args()

    try:
        asyncio.run(
            ingest_news(
                queries=args.queries,
                max_results=args.max_results,
                hours_back=args.hours_back,
                language=args.language,
                domains=args.domains,
            )
        )
    except KeyboardInterrupt:
        logger.info("news_ingestion.interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error("news_ingestion.failed", error=str(e))
        raise


if __name__ == "__main__":
    main()
