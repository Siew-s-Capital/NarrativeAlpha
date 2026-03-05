"""Podcast feed ingestion with transcript extraction."""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import struct_time
from typing import AsyncIterator, Optional

import feedparser
import structlog

from narrativealpha.models import PodcastTranscript

logger = structlog.get_logger()


class PodcastClient:
    """Podcast RSS ingestion client with lightweight transcript extraction."""

    def __init__(self, user_agent: str = "NarrativeAlpha/0.1.0"):
        self.user_agent = user_agent

    async def ingest_feed(
        self,
        feed_url: str,
        max_episodes: int = 20,
        keywords: Optional[list[str]] = None,
    ) -> AsyncIterator[PodcastTranscript]:
        """Ingest podcast episodes from an RSS feed and yield normalized transcript docs."""
        parsed = await asyncio.to_thread(
            feedparser.parse,
            feed_url,
            request_headers={"User-Agent": self.user_agent},
        )

        if getattr(parsed, "bozo", False):
            logger.warning("podcast_feed.parse_warning", feed_url=feed_url, bozo=bool(parsed.bozo))

        show_name = getattr(parsed.feed, "title", None) or "unknown"
        entries = list(getattr(parsed, "entries", []))[: max(max_episodes, 1)]

        for entry in entries:
            transcript = self._parse_episode(entry=entry, show_name=show_name, feed_url=feed_url)
            if transcript is None:
                continue

            if keywords:
                haystack = f"{transcript.episode_title}\n{transcript.text}".lower()
                if not any(keyword.lower() in haystack for keyword in keywords):
                    continue

            yield transcript

    def _parse_episode(self, entry: dict, show_name: str, feed_url: str) -> Optional[PodcastTranscript]:
        episode_url = entry.get("link")
        guid = entry.get("id") or entry.get("guid") or episode_url
        episode_title = (entry.get("title") or "Untitled Episode").strip()
        if not guid:
            return None

        transcript_text = self._extract_transcript_text(entry)
        if not transcript_text:
            return None

        created_at = self._parse_datetime(entry)
        audio_url = self._extract_audio_url(entry)
        transcript_source = self._extract_transcript_source(entry) or "rss:description"

        cashtags = sorted(set(re.findall(r"\$([A-Za-z]{1,10})", transcript_text)))
        hashtags = sorted(set(re.findall(r"#(\w+)", transcript_text)))

        stable = f"{feed_url}:{guid}".encode("utf-8")
        episode_id = hashlib.sha1(stable).hexdigest()[:16]

        return PodcastTranscript(
            id=f"podcast_{episode_id}",
            author_id=show_name,
            author_username=show_name,
            text=transcript_text[:8000],
            created_at=created_at,
            cashtags=cashtags,
            hashtags=hashtags,
            urls=[u for u in [episode_url, audio_url] if u],
            show_name=show_name,
            episode_title=episode_title,
            episode_url=episode_url,
            audio_url=audio_url,
            transcript_source=transcript_source,
        )

    def _extract_transcript_text(self, entry: dict) -> str:
        chunks: list[str] = []

        for content in entry.get("content", []) or []:
            value = content.get("value") if isinstance(content, dict) else None
            if value:
                chunks.append(value)

        for key in ("summary", "description"):
            value = entry.get(key)
            if value:
                chunks.append(value)

        cleaned = []
        for chunk in chunks:
            text = re.sub(r"<[^>]+>", " ", chunk)
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                cleaned.append(text)

        # de-duplicate while preserving order
        deduped = list(dict.fromkeys(cleaned))
        return "\n\n".join(deduped)

    def _extract_audio_url(self, entry: dict) -> Optional[str]:
        for link in entry.get("links", []) or []:
            if not isinstance(link, dict):
                continue
            href = link.get("href")
            link_type = (link.get("type") or "").lower()
            rel = (link.get("rel") or "").lower()
            if not href:
                continue
            if rel == "enclosure" or link_type.startswith("audio/"):
                return href
        return None

    def _extract_transcript_source(self, entry: dict) -> Optional[str]:
        for link in entry.get("links", []) or []:
            if not isinstance(link, dict):
                continue
            href = link.get("href")
            rel = (link.get("rel") or "").lower()
            link_type = (link.get("type") or "").lower()
            if not href:
                continue
            if "transcript" in rel or "transcript" in link_type:
                return href
        return None

    def _parse_datetime(self, entry: dict) -> datetime:
        if entry.get("published"):
            try:
                return parsedate_to_datetime(entry["published"]).astimezone(timezone.utc)
            except (TypeError, ValueError):
                pass

        for key in ("published_parsed", "updated_parsed"):
            value = entry.get(key)
            if isinstance(value, struct_time):
                return datetime(*value[:6], tzinfo=timezone.utc)

        return datetime.now(timezone.utc)
