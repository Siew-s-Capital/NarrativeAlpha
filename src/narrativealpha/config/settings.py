from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    # Twitter/X API
    twitter_bearer_token: str = Field(default="", alias="TWITTER_BEARER_TOKEN")
    twitter_api_key: str = Field(default="", alias="TWITTER_API_KEY")
    twitter_api_secret: str = Field(default="", alias="TWITTER_API_SECRET")
    twitter_access_token: str = Field(default="", alias="TWITTER_ACCESS_TOKEN")
    twitter_access_secret: str = Field(default="", alias="TWITTER_ACCESS_SECRET")

    # Reddit API
    reddit_client_id: str = Field(default="", alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str = Field(default="", alias="REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field(default="NarrativeAlpha/0.1.0", alias="REDDIT_USER_AGENT")

    # OpenAI
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")

    # News API
    news_api_key: str = Field(default="", alias="NEWS_API_KEY")
    news_api_base_url: str = Field(default="https://newsapi.org/v2", alias="NEWS_API_BASE_URL")

    # Database
    database_url: str = Field(default="sqlite:///data/narrativealpha.db", alias="DATABASE_URL")

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
