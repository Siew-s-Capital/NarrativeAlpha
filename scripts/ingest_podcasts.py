#!/usr/bin/env python3
"""CLI for podcast transcript ingestion from RSS feeds."""

import argparse
import asyncio
import sys

import structlog

from narrativealpha.ingestion.podcast import PodcastClient
from narrativealpha.ingestion.storage import SocialPostStore

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()


async def ingest_podcasts(
    feeds: list[str],
    max_episodes: int = 20,
    keywords: list[str] | None = None,
) -> None:
    store = SocialPostStore()
    stats = store.get_stats()
    logger.info("podcast_ingestion.starting", current_stats=stats, feeds=feeds)

    stored = 0
    duplicates = 0

    client = PodcastClient()
    for feed_url in feeds:
        async for transcript in client.ingest_feed(
            feed_url=feed_url,
            max_episodes=max_episodes,
            keywords=keywords,
        ):
            inserted = store.store_podcast_transcript(transcript)
            if inserted:
                stored += 1
            else:
                duplicates += 1

    final_stats = store.get_stats()
    logger.info(
        "podcast_ingestion.completed",
        final_stats=final_stats,
        stored_this_run=stored,
        duplicates_this_run=duplicates,
    )

    print("\nPodcast ingestion complete!")
    print(f"Posts stored this run: {stored}")
    print(f"Duplicates skipped: {duplicates}")
    print(f"Total posts in DB: {final_stats['total_posts']}")
    print(f"  - Twitter: {final_stats.get('twitter', 0)}")
    print(f"  - Reddit: {final_stats.get('reddit', 0)}")
    print(f"  - News: {final_stats.get('news', 0)}")
    print(f"  - Podcast: {final_stats.get('podcast', 0)}")
    print(f"Unprocessed: {final_stats['unprocessed']}")


def main():
    parser = argparse.ArgumentParser(description="Ingest podcast transcripts from RSS feeds")
    parser.add_argument("feeds", nargs="+", help="RSS feed URLs")
    parser.add_argument("--max-episodes", type=int, default=20)
    parser.add_argument("--keywords", nargs="+", default=None)

    args = parser.parse_args()

    try:
        asyncio.run(
            ingest_podcasts(
                feeds=args.feeds,
                max_episodes=args.max_episodes,
                keywords=args.keywords,
            )
        )
    except KeyboardInterrupt:
        logger.info("podcast_ingestion.interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error("podcast_ingestion.failed", error=str(e))
        raise


if __name__ == "__main__":
    main()
