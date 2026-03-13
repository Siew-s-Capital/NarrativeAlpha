import pytest
from narrativealpha.pipeline.summary import summarize_narratives

def test_summarize_empty():
    assert summarize_narratives([]) == "No narratives provided."

def test_summarize_basic():
    data = [
        {"title": "Story A", "content": "Hello world"},
        {"title": "Story B", "content": "Another story content"},
    ]
    result = summarize_narratives(data)
    assert "Narratives (2 items)" in result
    assert "Story A" in result and "Story B" in result
    # word count: 2 + 3 = 5
    assert "Total word count: 5" in result
