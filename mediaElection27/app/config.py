from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql+asyncpg://media27:media27@localhost:5432/media27",
        description="Async SQLAlchemy DSN for app code",
    )
    database_url_sync: str = Field(
        default="postgresql+psycopg://media27:media27@localhost:5432/media27",
        description="Sync DSN for alembic and seed scripts",
    )

    raw_feed_dir: Path = Field(default=Path("./raw"))
    s3_raw_bucket: str = Field(default="")
    seeds_dir: Path = Field(default=Path("./seeds"))

    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=False)

    ingest_timeout_seconds: float = Field(default=30.0)
    ingest_max_parallel: int = Field(default=8)
    ingest_user_agent: str = Field(
        default=(
            "Mozilla/5.0 (compatible; mediaElection27/0.1; "
            "+https://github.com/wfauriat/mediaElection27)"
        )
    )

    aws_region: str = Field(default="eu-west-3")


settings = Settings()
