from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Exasol Lineage API"
    DEBUG: bool = True
    CACHE_FILE_PATH: str = str(Path(__file__).parent.parent / "data" / "lineage_cache.json")

    class Config:
        env_file = ".env"


settings = Settings()
