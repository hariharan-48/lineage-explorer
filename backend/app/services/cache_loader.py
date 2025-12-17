"""
JSON cache loader with validation and singleton pattern.
Supports loading from local file or GCS Fuse mounted path.
"""
import json
import logging
import os
from pathlib import Path
from typing import Optional
from datetime import datetime

from app.services.graph_engine import LineageGraphEngine
from app.config import settings

logger = logging.getLogger(__name__)

# GCS Fuse mount path for K8s deployment
GCS_MOUNT_PATH = "/gcs-data/GLOBAL/lineage_cache.json"


class CacheLoader:
    """
    Loads and validates the JSON lineage cache.
    Implements singleton pattern for memory efficiency.
    """

    _instance: Optional["CacheLoader"] = None
    _engine: Optional[LineageGraphEngine] = None
    _loaded_at: Optional[str] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def load(self, cache_path: Optional[Path] = None) -> LineageGraphEngine:
        """
        Load the cache file and initialize the graph engine.
        Returns cached engine if already loaded.

        Supports loading from:
        1. GCS Fuse mount (if /gcs-data exists - K8s deployment)
        2. Local file path (development)
        """
        if self._engine is not None:
            return self._engine

        # Check for GCS Fuse mount (K8s deployment)
        gcs_path = Path(GCS_MOUNT_PATH)
        if gcs_path.exists():
            cache_path = gcs_path
            logger.info(f"Loading lineage cache from GCS Fuse mount: {cache_path}")
        else:
            # Load from local file (development)
            if cache_path is None:
                cache_path = Path(settings.CACHE_FILE_PATH)
            logger.info(f"Loading lineage cache from local file: {cache_path}")

        if not cache_path.exists():
            raise FileNotFoundError(f"Cache file not found: {cache_path}")

        with open(cache_path, "r") as f:
            cache_data = json.load(f)

        # Validate cache structure
        self._validate_cache(cache_data)

        # Initialize engine
        self._engine = LineageGraphEngine()
        self._engine.load_cache(cache_data)
        self._loaded_at = datetime.now().isoformat()

        stats = self._engine.get_statistics()
        logger.info(f"Cache loaded successfully: {stats}")

        return self._engine

    def _validate_cache(self, cache_data: dict) -> None:
        """Validate cache structure."""
        required_keys = ["metadata", "objects", "dependencies"]
        for key in required_keys:
            if key not in cache_data:
                raise ValueError(f"Invalid cache: missing '{key}' section")

        if not cache_data["objects"]:
            raise ValueError("Invalid cache: no objects found")

    def reload(self, cache_path: Optional[Path] = None) -> LineageGraphEngine:
        """Force reload the cache."""
        self._engine = None
        self._loaded_at = None
        return self.load(cache_path)

    @property
    def engine(self) -> LineageGraphEngine:
        """Get the loaded engine, loading if necessary."""
        if self._engine is None:
            return self.load()
        return self._engine

    @property
    def loaded_at(self) -> Optional[str]:
        """Get the time when cache was loaded."""
        return self._loaded_at


# Global instance
_cache_loader: Optional[CacheLoader] = None


def get_cache_loader() -> CacheLoader:
    """Get the singleton cache loader instance."""
    global _cache_loader
    if _cache_loader is None:
        _cache_loader = CacheLoader()
    return _cache_loader


def get_graph_engine() -> LineageGraphEngine:
    """Dependency injection helper for FastAPI."""
    return get_cache_loader().engine
