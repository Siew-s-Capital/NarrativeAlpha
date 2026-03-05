"""Tests for podcast ingestion."""

from datetime import datetime, timezone

from narrativealpha.ingestion.podcast import PodcastClient
from narrativealpha.models import PodcastTranscript


class TestPodcastClient:
    def test_parse_episode(self):
        client = PodcastClient()
        entry = {
            "id": "ep-1",
            "title": "Bitcoin narratives this week",
            "link": "https://example.com/podcast/ep1",
            "published": "Thu, 05 Mar 2026 15:00:00 GMT",
            "summary": "We discuss $BTC and #crypto momentum.",
            "description": "ETF flows and macro context.",
            "links": [
                {"href": "https://cdn.example.com/ep1.mp3", "rel": "enclosure", "type": "audio/mpeg"}
            ],
        }

        parsed = client._parse_episode(entry, show_name="Macro Pod", feed_url="https://example.com/rss")

        assert isinstance(parsed, PodcastTranscript)
        assert parsed.platform == "podcast"
        assert parsed.show_name == "Macro Pod"
        assert parsed.episode_title == "Bitcoin narratives this week"
        assert parsed.audio_url == "https://cdn.example.com/ep1.mp3"
        assert "BTC" in parsed.cashtags
        assert "crypto" in parsed.hashtags

    def test_parse_episode_missing_guid_returns_none(self):
        client = PodcastClient()
        entry = {"title": "No ID", "summary": "text"}

        parsed = client._parse_episode(entry, show_name="Macro Pod", feed_url="https://example.com/rss")
        assert parsed is None


class TestPodcastStorage:
    def test_store_and_retrieve_podcast(self, tmp_path):
        from narrativealpha.ingestion.storage import SocialPostStore

        store = SocialPostStore(str(tmp_path / "test.db"))
        transcript = PodcastTranscript(
            id="podcast_abc123",
            author_id="Macro Pod",
            author_username="Macro Pod",
            text="Discussion of $ETH themes",
            created_at=datetime.now(timezone.utc),
            show_name="Macro Pod",
            episode_title="Episode 12",
            episode_url="https://example.com/ep12",
            audio_url="https://example.com/ep12.mp3",
            transcript_source="rss:description",
        )

        assert store.store_podcast_transcript(transcript) is True
        assert store.store_podcast_transcript(transcript) is False

        retrieved = store.get_post("podcast_abc123")
        assert retrieved is not None
        assert retrieved.platform == "podcast"
        assert retrieved.episode_title == "Episode 12"
