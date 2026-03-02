"""Tweet storage and deduplication."""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import structlog

from narrativealpha.models import Tweet

logger = structlog.get_logger()


class TweetStore:
    """SQLite-backed storage for tweets with deduplication."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or "data/narrativealpha.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._init_db()
        logger.info("tweet_store.initialized", db_path=str(self.db_path))
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tweets (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    author_id TEXT NOT NULL,
                    author_username TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    likes INTEGER DEFAULT 0,
                    replies INTEGER DEFAULT 0,
                    reposts INTEGER DEFAULT 0,
                    quote_tweets INTEGER DEFAULT 0,
                    bookmarks INTEGER DEFAULT 0,
                    impressions INTEGER DEFAULT 0,
                    language TEXT,
                    cashtags TEXT,
                    hashtags TEXT,
                    urls TEXT,
                    engagement_rate REAL,
                    is_reply BOOLEAN DEFAULT 0,
                    is_retweet BOOLEAN DEFAULT 0,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT 0
                )
            """)
            
            # Indexes for common queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tweets_cashtags ON tweets(cashtags)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tweets_processed ON tweets(processed)")
            
            conn.commit()
    
    def store_tweet(self, tweet: Tweet) -> bool:
        """
        Store a tweet. Returns True if inserted, False if duplicate.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO tweets (
                        id, platform, author_id, author_username, text, created_at,
                        likes, replies, reposts, quote_tweets, bookmarks, impressions,
                        language, cashtags, hashtags, urls, engagement_rate,
                        is_reply, is_retweet, collected_at, processed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tweet.id,
                        tweet.platform,
                        tweet.author_id,
                        tweet.author_username,
                        tweet.text,
                        tweet.created_at.isoformat(),
                        tweet.likes,
                        tweet.replies,
                        tweet.reposts,
                        tweet.quote_tweets,
                        tweet.bookmarks,
                        tweet.impressions,
                        tweet.language,
                        json.dumps(tweet.cashtags),
                        json.dumps(tweet.hashtags),
                        json.dumps(tweet.urls),
                        tweet.engagement_rate,
                        tweet.is_reply,
                        tweet.is_retweet,
                        tweet.collected_at.isoformat(),
                        tweet.processed,
                    )
                )
                conn.commit()
                logger.debug("tweet.stored", tweet_id=tweet.id)
                return True
                
        except sqlite3.IntegrityError:
            logger.debug("tweet.duplicate", tweet_id=tweet.id)
            return False
        except Exception as e:
            logger.error("tweet.store_failed", tweet_id=tweet.id, error=str(e))
            raise
    
    def get_tweet(self, tweet_id: str) -> Optional[Tweet]:
        """Retrieve a tweet by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM tweets WHERE id = ?", (tweet_id,)
            ).fetchone()
            
            if row:
                return self._row_to_tweet(row)
            return None
    
    def get_unprocessed(self, limit: int = 100) -> list[Tweet]:
        """Get unprocessed tweets for analysis."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM tweets WHERE processed = 0 ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            
            return [self._row_to_tweet(row) for row in rows]
    
    def mark_processed(self, tweet_ids: list[str]) -> int:
        """Mark tweets as processed. Returns count updated."""
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ",".join("?" * len(tweet_ids))
            cursor = conn.execute(
                f"UPDATE tweets SET processed = 1 WHERE id IN ({placeholders})",
                tweet_ids
            )
            conn.commit()
            return cursor.rowcount
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
            unprocessed = conn.execute(
                "SELECT COUNT(*) FROM tweets WHERE processed = 0"
            ).fetchone()[0]
            
            return {
                "total_tweets": total,
                "unprocessed": unprocessed,
                "processed": total - unprocessed,
            }
    
    def _row_to_tweet(self, row: sqlite3.Row) -> Tweet:
        """Convert database row to Tweet model."""
        return Tweet(
            id=row["id"],
            platform=row["platform"],
            author_id=row["author_id"],
            author_username=row["author_username"],
            text=row["text"],
            created_at=datetime.fromisoformat(row["created_at"]),
            likes=row["likes"],
            replies=row["replies"],
            reposts=row["reposts"],
            quote_tweets=row["quote_tweets"],
            bookmarks=row["bookmarks"],
            impressions=row["impressions"],
            language=row["language"],
            cashtags=json.loads(row["cashtags"] or "[]"),
            hashtags=json.loads(row["hashtags"] or "[]"),
            urls=json.loads(row["urls"] or "[]"),
            engagement_rate=row["engagement_rate"],
            is_reply=bool(row["is_reply"]),
            is_retweet=bool(row["is_retweet"]),
            collected_at=datetime.fromisoformat(row["collected_at"]),
            processed=bool(row["processed"]),
        )
