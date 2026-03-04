"""Data models for NarrativeAlpha."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class SocialPost(BaseModel):
    """Base model for social media posts across platforms."""

    id: str = Field(..., description="Unique post ID from source platform")
    platform: str = Field(..., description="Source platform (twitter, reddit, etc.)")
    author_id: str = Field(..., description="Author's platform ID")
    author_username: str = Field(..., description="Author's username")
    text: str = Field(..., description="Post content")
    created_at: datetime = Field(..., description="Post creation timestamp")

    # Engagement metrics
    likes: int = Field(default=0)
    replies: int = Field(default=0)
    reposts: int = Field(default=0)

    # Content metadata
    language: Optional[str] = Field(default=None)
    cashtags: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    urls: list[str] = Field(default_factory=list)

    # Processing metadata
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    processed: bool = Field(default=False)

    class Config:
        frozen = True


class Tweet(SocialPost):
    """Twitter/X specific post model."""

    platform: str = "twitter"

    # Twitter-specific fields
    impressions: int = Field(default=0)
    bookmarks: int = Field(default=0)
    quote_tweets: int = Field(default=0)
    is_reply: bool = Field(default=False)
    is_retweet: bool = Field(default=False)

    # Twitter metrics
    engagement_rate: Optional[float] = Field(default=None)


class RedditPost(SocialPost):
    """Reddit specific post model."""

    platform: str = "reddit"

    # Reddit-specific fields
    subreddit: str = Field(...)
    title: Optional[str] = Field(default=None)
    upvotes: int = Field(default=0)
    downvotes: int = Field(default=0)
    awards: int = Field(default=0)
    is_self: bool = Field(default=True)
    permalink: Optional[str] = Field(default=None)


class NewsArticle(SocialPost):
    """News article model normalized to the social post schema."""

    platform: str = "news"

    source_name: str = Field(...)
    source_id: Optional[str] = Field(default=None)
    title: str = Field(...)
    description: Optional[str] = Field(default=None)
    article_url: str = Field(...)
    image_url: Optional[str] = Field(default=None)


class Narrative(BaseModel):
    """Detected narrative/cluster model."""

    id: str = Field(..., description="Unique narrative ID")
    name: str = Field(..., description="Human-readable narrative name")
    description: str = Field(..., description="Narrative description/summary")

    # Temporal tracking
    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_seen: datetime = Field(default_factory=datetime.utcnow)

    # Associated content
    post_ids: list[str] = Field(default_factory=list)
    cashtags: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    # Scoring
    sentiment_score: float = Field(default=0.0)
    velocity_score: float = Field(default=0.0)
    saturation_score: float = Field(default=0.0)
    overall_score: float = Field(default=0.0)

    # Status
    is_active: bool = Field(default=True)
    confidence: float = Field(default=0.0)
