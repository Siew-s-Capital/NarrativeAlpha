"""NarrativeAlpha ingestion module."""

from narrativealpha.ingestion.twitter import TwitterClient
from narrativealpha.ingestion.reddit import RedditClient
from narrativealpha.ingestion.news import NewsClient
from narrativealpha.ingestion.podcast import PodcastClient
from narrativealpha.ingestion.storage import SocialPostStore, TweetStore

__all__ = [
    "TwitterClient",
    "RedditClient",
    "NewsClient",
    "PodcastClient",
    "SocialPostStore",
    "TweetStore",
]
