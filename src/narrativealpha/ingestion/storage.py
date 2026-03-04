"""Unified storage for social media posts with deduplication."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import structlog

from narrativealpha.models import Tweet, RedditPost, NewsArticle, SocialPost

logger = structlog.get_logger()


class SocialPostStore:
    """SQLite-backed unified storage for social posts with deduplication."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or "data/narrativealpha.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()
        logger.info("post_store.initialized", db_path=str(self.db_path))

    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            # Unified posts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    author_id TEXT NOT NULL,
                    author_username TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    likes INTEGER DEFAULT 0,
                    replies INTEGER DEFAULT 0,
                    reposts INTEGER DEFAULT 0,
                    language TEXT,
                    cashtags TEXT,
                    hashtags TEXT,
                    urls TEXT,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT 0
                )
            """)

            # Platform-specific tables for extended data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tweets_extended (
                    post_id TEXT PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
                    impressions INTEGER DEFAULT 0,
                    quote_tweets INTEGER DEFAULT 0,
                    bookmarks INTEGER DEFAULT 0,
                    engagement_rate REAL,
                    is_reply BOOLEAN DEFAULT 0,
                    is_retweet BOOLEAN DEFAULT 0
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS reddit_posts_extended (
                    post_id TEXT PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
                    subreddit TEXT NOT NULL,
                    title TEXT,
                    upvotes INTEGER DEFAULT 0,
                    downvotes INTEGER DEFAULT 0,
                    awards INTEGER DEFAULT 0,
                    is_self BOOLEAN DEFAULT 1,
                    permalink TEXT
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS news_articles_extended (
                    post_id TEXT PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
                    source_name TEXT NOT NULL,
                    source_id TEXT,
                    title TEXT NOT NULL,
                    description TEXT,
                    article_url TEXT NOT NULL,
                    image_url TEXT
                )
            """)

            # Indexes for common queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_platform ON posts(platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_cashtags ON posts(cashtags)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_processed ON posts(processed)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reddit_subreddit ON reddit_posts_extended(subreddit)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_source ON news_articles_extended(source_name)"
            )

            conn.commit()

    def store_tweet(self, tweet: Tweet) -> bool:
        """Store a tweet. Returns True if inserted, False if duplicate."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Insert base post
                conn.execute(
                    """
                    INSERT INTO posts (
                        id, platform, author_id, author_username, text, created_at,
                        likes, replies, reposts, language, cashtags, hashtags, urls,
                        collected_at, processed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        tweet.language,
                        json.dumps(tweet.cashtags),
                        json.dumps(tweet.hashtags),
                        json.dumps(tweet.urls),
                        tweet.collected_at.isoformat(),
                        tweet.processed,
                    ),
                )

                # Insert extended data
                conn.execute(
                    """
                    INSERT INTO tweets_extended (
                        post_id, impressions, quote_tweets, bookmarks,
                        engagement_rate, is_reply, is_retweet
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tweet.id,
                        tweet.impressions,
                        tweet.quote_tweets,
                        tweet.bookmarks,
                        tweet.engagement_rate,
                        tweet.is_reply,
                        tweet.is_retweet,
                    ),
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

    def store_reddit_post(self, post: RedditPost) -> bool:
        """Store a Reddit post. Returns True if inserted, False if duplicate."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Insert base post
                conn.execute(
                    """
                    INSERT INTO posts (
                        id, platform, author_id, author_username, text, created_at,
                        likes, replies, reposts, language, cashtags, hashtags, urls,
                        collected_at, processed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        post.id,
                        post.platform,
                        post.author_id,
                        post.author_username,
                        post.text,
                        post.created_at.isoformat(),
                        post.likes,
                        post.replies,
                        post.reposts,
                        post.language,
                        json.dumps(post.cashtags),
                        json.dumps(post.hashtags),
                        json.dumps(post.urls),
                        post.collected_at.isoformat(),
                        post.processed,
                    ),
                )

                # Insert extended data
                conn.execute(
                    """
                    INSERT INTO reddit_posts_extended (
                        post_id, subreddit, title, upvotes, downvotes,
                        awards, is_self, permalink
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        post.id,
                        post.subreddit,
                        post.title,
                        post.upvotes,
                        post.downvotes,
                        post.awards,
                        post.is_self,
                        post.permalink,
                    ),
                )

                conn.commit()
                logger.debug("reddit_post.stored", post_id=post.id)
                return True

        except sqlite3.IntegrityError:
            logger.debug("reddit_post.duplicate", post_id=post.id)
            return False
        except Exception as e:
            logger.error("reddit_post.store_failed", post_id=post.id, error=str(e))
            raise

    def store_news_article(self, article: NewsArticle) -> bool:
        """Store a NewsArticle. Returns True if inserted, False if duplicate."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO posts (
                        id, platform, author_id, author_username, text, created_at,
                        likes, replies, reposts, language, cashtags, hashtags, urls,
                        collected_at, processed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        article.id,
                        article.platform,
                        article.author_id,
                        article.author_username,
                        article.text,
                        article.created_at.isoformat(),
                        article.likes,
                        article.replies,
                        article.reposts,
                        article.language,
                        json.dumps(article.cashtags),
                        json.dumps(article.hashtags),
                        json.dumps(article.urls),
                        article.collected_at.isoformat(),
                        article.processed,
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO news_articles_extended (
                        post_id, source_name, source_id, title,
                        description, article_url, image_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        article.id,
                        article.source_name,
                        article.source_id,
                        article.title,
                        article.description,
                        article.article_url,
                        article.image_url,
                    ),
                )

                conn.commit()
                logger.debug("news_article.stored", post_id=article.id)
                return True

        except sqlite3.IntegrityError:
            logger.debug("news_article.duplicate", post_id=article.id)
            return False
        except Exception as e:
            logger.error("news_article.store_failed", post_id=article.id, error=str(e))
            raise

    def get_post(self, post_id: str) -> Optional[Union[Tweet, RedditPost, NewsArticle]]:
        """Retrieve a post by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()

            if not row:
                return None

            platform = row["platform"]
            if platform == "twitter":
                return self._row_to_tweet(conn, row)
            if platform == "reddit":
                return self._row_to_reddit_post(conn, row)
            if platform == "news":
                return self._row_to_news_article(conn, row)
            return None

    def get_tweet(self, tweet_id: str) -> Optional[Tweet]:
        """Backward-compatible tweet getter."""
        post = self.get_post(tweet_id)
        return post if isinstance(post, Tweet) else None

    def get_unprocessed(self, limit: int = 100, platform: Optional[str] = None) -> list[SocialPost]:
        """Get unprocessed posts for analysis."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if platform:
                rows = conn.execute(
                    """SELECT * FROM posts 
                       WHERE processed = 0 AND platform = ? 
                       ORDER BY created_at DESC LIMIT ?""",
                    (platform, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM posts 
                       WHERE processed = 0 
                       ORDER BY created_at DESC LIMIT ?""",
                    (limit,),
                ).fetchall()

            posts = []
            for row in rows:
                platform = row["platform"]
                if platform == "twitter":
                    posts.append(self._row_to_tweet(conn, row))
                elif platform == "reddit":
                    posts.append(self._row_to_reddit_post(conn, row))
                elif platform == "news":
                    posts.append(self._row_to_news_article(conn, row))

            return posts

    def mark_processed(self, post_ids: list[str]) -> int:
        """Mark posts as processed. Returns count updated."""
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ",".join("?" * len(post_ids))
            cursor = conn.execute(
                f"UPDATE posts SET processed = 1 WHERE id IN ({placeholders})", post_ids
            )
            conn.commit()
            return cursor.rowcount

    def get_stats(self) -> dict:
        """Get storage statistics."""
        with sqlite3.connect(self.db_path) as conn:
            # Total counts
            total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            unprocessed = conn.execute("SELECT COUNT(*) FROM posts WHERE processed = 0").fetchone()[
                0
            ]

            # Platform breakdown
            twitter_count = conn.execute(
                "SELECT COUNT(*) FROM posts WHERE platform = 'twitter'"
            ).fetchone()[0]
            reddit_count = conn.execute(
                "SELECT COUNT(*) FROM posts WHERE platform = 'reddit'"
            ).fetchone()[0]
            news_count = conn.execute(
                "SELECT COUNT(*) FROM posts WHERE platform = 'news'"
            ).fetchone()[0]

            return {
                "total_posts": total,
                "total_tweets": twitter_count,
                "unprocessed": unprocessed,
                "processed": total - unprocessed,
                "twitter": twitter_count,
                "reddit": reddit_count,
                "news": news_count,
            }

    def _row_to_tweet(self, conn: sqlite3.Connection, row: sqlite3.Row) -> Tweet:
        """Convert database row to Tweet model."""
        ext_row = conn.execute(
            "SELECT * FROM tweets_extended WHERE post_id = ?", (row["id"],)
        ).fetchone()

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
            language=row["language"],
            cashtags=json.loads(row["cashtags"] or "[]"),
            hashtags=json.loads(row["hashtags"] or "[]"),
            urls=json.loads(row["urls"] or "[]"),
            collected_at=datetime.fromisoformat(row["collected_at"]),
            processed=bool(row["processed"]),
            impressions=ext_row["impressions"] if ext_row else 0,
            quote_tweets=ext_row["quote_tweets"] if ext_row else 0,
            bookmarks=ext_row["bookmarks"] if ext_row else 0,
            engagement_rate=ext_row["engagement_rate"] if ext_row else None,
            is_reply=bool(ext_row["is_reply"]) if ext_row else False,
            is_retweet=bool(ext_row["is_retweet"]) if ext_row else False,
        )

    def _row_to_reddit_post(self, conn: sqlite3.Connection, row: sqlite3.Row) -> RedditPost:
        """Convert database row to RedditPost model."""
        ext_row = conn.execute(
            "SELECT * FROM reddit_posts_extended WHERE post_id = ?", (row["id"],)
        ).fetchone()

        return RedditPost(
            id=row["id"],
            platform=row["platform"],
            author_id=row["author_id"],
            author_username=row["author_username"],
            text=row["text"],
            created_at=datetime.fromisoformat(row["created_at"]),
            likes=row["likes"],
            replies=row["replies"],
            reposts=row["reposts"],
            language=row["language"],
            cashtags=json.loads(row["cashtags"] or "[]"),
            hashtags=json.loads(row["hashtags"] or "[]"),
            urls=json.loads(row["urls"] or "[]"),
            collected_at=datetime.fromisoformat(row["collected_at"]),
            processed=bool(row["processed"]),
            subreddit=ext_row["subreddit"] if ext_row else "",
            title=ext_row["title"] if ext_row else None,
            upvotes=ext_row["upvotes"] if ext_row else 0,
            downvotes=ext_row["downvotes"] if ext_row else 0,
            awards=ext_row["awards"] if ext_row else 0,
            is_self=bool(ext_row["is_self"]) if ext_row else True,
            permalink=ext_row["permalink"] if ext_row else None,
        )

    def _row_to_news_article(self, conn: sqlite3.Connection, row: sqlite3.Row) -> NewsArticle:
        """Convert database row to NewsArticle model."""
        ext_row = conn.execute(
            "SELECT * FROM news_articles_extended WHERE post_id = ?", (row["id"],)
        ).fetchone()

        return NewsArticle(
            id=row["id"],
            platform=row["platform"],
            author_id=row["author_id"],
            author_username=row["author_username"],
            text=row["text"],
            created_at=datetime.fromisoformat(row["created_at"]),
            likes=row["likes"],
            replies=row["replies"],
            reposts=row["reposts"],
            language=row["language"],
            cashtags=json.loads(row["cashtags"] or "[]"),
            hashtags=json.loads(row["hashtags"] or "[]"),
            urls=json.loads(row["urls"] or "[]"),
            collected_at=datetime.fromisoformat(row["collected_at"]),
            processed=bool(row["processed"]),
            source_name=ext_row["source_name"] if ext_row else "unknown",
            source_id=ext_row["source_id"] if ext_row else None,
            title=ext_row["title"] if ext_row else row["text"][:120],
            description=ext_row["description"] if ext_row else None,
            article_url=(
                ext_row["article_url"]
                if ext_row
                else (json.loads(row["urls"] or "[]")[:1] or [""])[0]
            ),
            image_url=ext_row["image_url"] if ext_row else None,
        )


# Backward compatibility
TweetStore = SocialPostStore
