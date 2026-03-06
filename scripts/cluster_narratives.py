"""Cluster unprocessed posts into narratives."""

from __future__ import annotations

import argparse
import json

from narrativealpha.analysis import NarrativeClusteringEngine
from narrativealpha.ingestion.storage import SocialPostStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Cluster unprocessed posts into narratives")
    parser.add_argument("--db-path", type=str, default=None, help="SQLite DB path")
    parser.add_argument("--limit", type=int, default=200, help="Max posts to analyze")
    parser.add_argument("--min-cluster-size", type=int, default=2, help="Minimum cluster size")
    parser.add_argument(
        "--mark-processed",
        action="store_true",
        help="Mark analyzed posts as processed after clustering",
    )
    args = parser.parse_args()

    store = SocialPostStore(db_path=args.db_path)
    posts = store.get_unprocessed(limit=args.limit)

    engine = NarrativeClusteringEngine(min_cluster_size=args.min_cluster_size)
    narratives = engine.cluster_posts(posts)

    print(json.dumps([n.model_dump(mode="json") for n in narratives], indent=2))

    if args.mark_processed and posts:
        updated = store.mark_processed([p.id for p in posts])
        print(f"\nMarked {updated} posts as processed")


if __name__ == "__main__":
    main()
