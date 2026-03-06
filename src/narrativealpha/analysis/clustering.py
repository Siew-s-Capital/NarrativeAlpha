"""Narrative clustering engine for grouping social posts into market narratives."""

from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from narrativealpha.models import Narrative, SocialPost

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,}")
_STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "been",
    "being",
    "from",
    "have",
    "into",
    "just",
    "more",
    "most",
    "over",
    "that",
    "their",
    "there",
    "they",
    "this",
    "were",
    "what",
    "when",
    "with",
    "will",
    "would",
    "your",
    "bitcoin",
    "crypto",
}


@dataclass(frozen=True)
class NarrativeDraft:
    """Draft narrative metadata created from clustered posts."""

    name: str
    description: str
    keywords: list[str]


class NarrativeLabeler(Protocol):
    """Optional labeler protocol for LLM-based narrative naming/summarization."""

    def label(self, posts: list[SocialPost], keywords: list[str], cashtags: list[str]) -> NarrativeDraft:
        """Return narrative naming metadata for a cluster."""


class NarrativeClusteringEngine:
    """Clusters social posts into candidate market narratives."""

    def __init__(self, min_cluster_size: int = 2, max_keywords: int = 8):
        self.min_cluster_size = min_cluster_size
        self.max_keywords = max_keywords

    def cluster_posts(
        self,
        posts: list[SocialPost],
        labeler: NarrativeLabeler | None = None,
        now: datetime | None = None,
    ) -> list[Narrative]:
        """Cluster posts and return ranked narrative objects."""
        if not posts:
            return []

        groups: dict[str, list[SocialPost]] = self._group_posts(posts)
        narratives: list[Narrative] = []

        for cluster_posts in groups.values():
            if len(cluster_posts) < self.min_cluster_size:
                continue

            cashtags = self._top_cashtags(cluster_posts)
            keywords = self._extract_keywords(cluster_posts)[: self.max_keywords]
            draft = self._build_draft(cluster_posts, cashtags, keywords, labeler)
            narratives.append(self._to_narrative(cluster_posts, draft, cashtags, now=now))

        return sorted(narratives, key=lambda n: (n.confidence, len(n.post_ids)), reverse=True)

    def _group_posts(self, posts: list[SocialPost]) -> dict[str, list[SocialPost]]:
        grouped: dict[str, list[SocialPost]] = defaultdict(list)

        for post in posts:
            cashtags = sorted({tag.upper() for tag in post.cashtags if tag})
            if cashtags:
                key = "tag:" + "+".join(cashtags)
            else:
                keywords = self._extract_keywords([post])
                top_keyword = keywords[0] if keywords else "misc"
                key = f"kw:{top_keyword}"
            grouped[key].append(post)

        return grouped

    def _extract_keywords(self, posts: list[SocialPost]) -> list[str]:
        token_counter: Counter[str] = Counter()
        for post in posts:
            for token in _WORD_RE.findall(post.text.lower()):
                if token in _STOPWORDS:
                    continue
                token_counter[token] += 1

        return [token for token, _ in token_counter.most_common(20)]

    def _top_cashtags(self, posts: list[SocialPost]) -> list[str]:
        tag_counter: Counter[str] = Counter()
        for post in posts:
            tag_counter.update(tag.upper() for tag in post.cashtags if tag)
        return [tag for tag, _ in tag_counter.most_common(5)]

    def _build_draft(
        self,
        posts: list[SocialPost],
        cashtags: list[str],
        keywords: list[str],
        labeler: NarrativeLabeler | None,
    ) -> NarrativeDraft:
        if labeler:
            try:
                return labeler.label(posts, keywords, cashtags)
            except Exception:
                pass

        if cashtags:
            label_target = "$" + ", $".join(cashtags[:2])
            name = f"{label_target} narrative momentum"
        else:
            label_target = keywords[0] if keywords else "market"
            name = f"{label_target.title()} discussion cluster"

        platform_mix = ", ".join(sorted({post.platform for post in posts}))
        top_keywords = ", ".join(keywords[:4]) if keywords else "broad market chatter"
        description = (
            f"Cluster of {len(posts)} posts across {platform_mix} focused on {top_keywords}."
        )
        return NarrativeDraft(name=name, description=description, keywords=keywords[: self.max_keywords])

    def _to_narrative(
        self,
        cluster_posts: list[SocialPost],
        draft: NarrativeDraft,
        cashtags: list[str],
        now: datetime | None,
    ) -> Narrative:
        post_ids = sorted(post.id for post in cluster_posts)
        id_seed = "|".join(post_ids)
        narrative_id = "nar_" + hashlib.sha1(id_seed.encode("utf-8")).hexdigest()[:12]

        created_times = [post.created_at for post in cluster_posts]
        first_seen = min(created_times)
        last_seen = max(created_times)
        confidence = min(1.0, 0.45 + 0.1 * len(cluster_posts))

        return Narrative(
            id=narrative_id,
            name=draft.name,
            description=draft.description,
            first_seen=first_seen,
            last_seen=last_seen if now is None else max(last_seen, now),
            post_ids=post_ids,
            cashtags=cashtags,
            keywords=draft.keywords,
            confidence=round(confidence, 3),
        )
