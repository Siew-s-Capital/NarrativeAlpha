"""Pipeline summary utilities.

Provides a function to generate a concise summary of a list of narratives.
"""
from typing import List, Dict

def summarize_narratives(narratives: List[Dict[str, str]]) -> str:
    """Return a plain‑text summary of the given narratives.

    Each narrative dict is expected to have a ``title`` and ``content`` key.
    The function concatenates titles and counts total words.
    """
    if not narratives:
        return "No narratives provided."
    titles = [n.get("title", "<untitled>") for n in narratives]
    total_words = sum(len(n.get("content", "").split()) for n in narratives)
    summary = f"Narratives ({len(narratives)} items): " + ", ".join(titles) + ". "
    summary += f"Total word count: {total_words}."
    return summary

__all__ = ["summarize_narratives"]
