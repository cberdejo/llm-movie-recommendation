from typing import Literal
from pydantic_settings import BaseSettings


class DBSettings(BaseSettings):
    qdrant_host: str = "localhost"
    qdrant_port: int = 6333
    qdrant_api_key: str | None = None
    qdrant_collection: str = "movie_reviews"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"


dbsettings = DBSettings()
