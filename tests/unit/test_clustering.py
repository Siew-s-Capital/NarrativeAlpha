"""Tests for narrative clustering engine."""

from datetime import datetime, timedelta, timezone

from narrativealpha.analysis import NarrativeClusteringEngine
from narrativealpha.models import SocialPost


def _post(post_id: str, text: str, cashtags: list[str], platform: str = "twitter") -> SocialPost:
    return SocialPost(
        id=post_id,
        platform=platform,
        author_id=f"author-{post_id}",
        author_username=f"user-{post_id}",
        text=text,
        created_at=datetime.now(timezone.utc),
        cashtags=cashtags,
    )


class TestNarrativeClusteringEngine:
    def test_clusters_by_cashtag(self):
        engine = NarrativeClusteringEngine(min_cluster_size=2)
        posts = [
            _post("1", "$BTC breakout above key level", ["BTC"]),
            _post("2", "Institutions buying more $BTC this week", ["BTC"]),
            _post("3", "$ETH ecosystem growth accelerates", ["ETH"]),
        ]

        narratives = engine.cluster_posts(posts)

        assert len(narratives) == 1
        assert narratives[0].cashtags == ["BTC"]
        assert set(narratives[0].post_ids) == {"1", "2"}
        assert narratives[0].confidence >= 0.65

    def test_clusters_by_keyword_when_no_cashtags(self):
        engine = NarrativeClusteringEngine(min_cluster_size=2)
        posts = [
            _post("a", "ETF approval momentum is building fast", []),
            _post("b", "ETF inflows are surprising even bulls", [], platform="news"),
            _post("c", "Macro volatility remains high", []),
        ]

        narratives = engine.cluster_posts(posts)

        assert len(narratives) == 1
        assert narratives[0].cashtags == []
        assert "etf" in narratives[0].keywords

    def test_respects_time_bounds_and_deterministic_id(self):
        now = datetime.now(timezone.utc)
        older = now - timedelta(hours=2)
        newer = now - timedelta(minutes=30)
        posts = [
            SocialPost(
                id="x",
                platform="reddit",
                author_id="a",
                author_username="u1",
                text="$SOL dev activity spikes",
                created_at=older,
                cashtags=["SOL"],
            ),
            SocialPost(
                id="y",
                platform="twitter",
                author_id="b",
                author_username="u2",
                text="$SOL memecoin volume is exploding",
                created_at=newer,
                cashtags=["SOL"],
            ),
        ]

        engine = NarrativeClusteringEngine(min_cluster_size=2)
        narratives = engine.cluster_posts(posts, now=now)

        assert len(narratives) == 1
        n = narratives[0]
        assert n.id.startswith("nar_")
        assert n.first_seen == older
        assert n.last_seen == now
