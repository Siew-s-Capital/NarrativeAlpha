# NarrativeAlpha

**Predict market narratives before they explode.**

AI-powered narrative detection and analysis for alpha generation.

## The Problem

Markets move on **narratives**, not just fundamentals.
Current tools track price/volume. Nobody tracks narrative formation in real-time.

## What It Does

1. **Multi-Source Ingestion** — Twitter, Reddit, News, Podcasts
2. **Narrative Detection** — LLM clusters discussions into themes
3. **Predictive Scoring** — Velocity, saturation, sentiment analysis
4. **Autonomous Reports** — Daily narrative intelligence

## Status

🚧 **Under Active Development** — Built nightly at Siew's Capital

### Progress Log

| Date | Feature | Status |
|------|---------|--------|
| 2026-03-02 | Project structure, config system, Twitter ingestion | ✅ Complete |
| 2026-03-03 | Reddit ingestion, data pipeline | 🔄 Next |

## Quick Start

```bash
# Clone and setup
git clone https://github.com/Siew-s-Capital/NarrativeAlpha.git
cd NarrativeAlpha
pip install -e ".[dev]"

# Configure
cp .env.example .env
# Edit .env with your API keys

# Run Twitter ingestion
python scripts/ingest_twitter.py "$BTC" "$ETH" --max-results 100 --min-likes 10
```

## Architecture

```
NarrativeAlpha/
├── src/narrativealpha/
│   ├── config/         # Configuration management
│   ├── ingestion/      # Data ingestion (Twitter, Reddit, etc.)
│   ├── analysis/       # LLM narrative detection
│   ├── models/         # Data models (Pydantic)
│   └── reports/        # Report generation
├── tests/              # Test suite (pytest)
└── scripts/            # CLI utilities
```

## Features Implemented

### ✅ Phase 1: Foundation (Week 1)

- [x] **Project Structure** — Clean Python package with pyproject.toml
- [x] **Configuration** — Pydantic settings with .env support
- [x] **Data Models** — SocialPost, Tweet, RedditPost, Narrative
- [x] **Twitter Ingestion** — Async API client with rate limiting
- [x] **SQLite Storage** — Tweet storage with deduplication
- [x] **CLI Tool** — `ingest_twitter.py` for data collection
- [x] **Test Suite** — Unit tests for ingestion and models

### 🔄 Phase 1: Remaining

- [ ] Reddit ingestion
- [ ] News API ingestion
- [ ] Podcast transcription
- [ ] LLM narrative clustering
- [ ] Sentiment analysis
- [ ] Velocity/saturation scoring

### 📋 Phase 2: Intelligence

- [ ] Narrative persistence and tracking
- [ ] Trend prediction algorithms
- [ ] Report generation system
- [ ] API server
- [ ] Frontend dashboard

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=narrativealpha

# Format code
black src tests
ruff check src tests
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `TWITTER_BEARER_TOKEN` | Twitter API v2 bearer token | Yes (for Twitter) |
| `TWITTER_API_KEY` | Twitter API key | Optional |
| `TWITTER_API_SECRET` | Twitter API secret | Optional |
| `REDDIT_CLIENT_ID` | Reddit API client ID | Yes (for Reddit) |
| `REDDIT_CLIENT_SECRET` | Reddit API secret | Yes (for Reddit) |
| `OPENAI_API_KEY` | OpenAI API key | Yes (for analysis) |
| `DATABASE_URL` | SQLite database path | Optional |
| `LOG_LEVEL` | Logging level | Optional |

## Built By

**Siew's Capital** — AI-native investment research firm

---

*"The story moves the market. We find the story first."*
