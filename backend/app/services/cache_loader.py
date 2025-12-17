"""
JSON cache loader with validation and singleton pattern.
Supports loading from local file or Google Cloud Storage.
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


def load_from_gcs(bucket_name: str, blob_name: str, project: str = None) -> dict:
    """
    Load JSON cache from Google Cloud Storage.

    Args:
        bucket_name: GCS bucket name
        blob_name: Path to file in bucket (e.g., 'lineage_cache.json')
        project: GCP project ID (optional - auto-detected on App Engine)

    Returns:
        Parsed JSON data
    """
    try:
        from google.cloud import storage
    except ImportError:
        raise ImportError(
            "google-cloud-storage is required for GCS support. "
            "Install it with: pip install google-cloud-storage"
        )

    logger.info(f"Loading cache from GCS: gs://{bucket_name}/{blob_name}")

    # Project is auto-detected on App Engine, but can be specified via env var
    client = storage.Client(project=project) if project else storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    content = blob.download_as_text()
    return json.loads(content)


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
        1. GCS bucket (if GCS_BUCKET env var is set)
        2. Local file path
        """
        if self._engine is not None:
            return self._engine

        # GCS configuration - default bucket for production
        # Files are loaded from: gs://BUCKET_NAME/gcs-data/GLOBAL/lineage_cache.json
        gcs_bucket = os.environ.get("GCS_BUCKET")  # Set in K8s deployment yaml
        gcs_blob = "/gcs-data/GLOBAL/lineage_cache.json"
        gcs_project = os.environ.get("GCS_PROJECT")  # Optional, auto-detected in GKE
        use_gcs = os.environ.get("USE_GCS", "true").lower() == "true"

        if use_gcs:
            # Load from Google Cloud Storage
            cache_data = load_from_gcs(gcs_bucket, gcs_blob, gcs_project)
        else:
            # Load from local file
            if cache_path is None:
                cache_path = Path(settings.CACHE_FILE_PATH)

            logger.info(f"Loading lineage cache from {cache_path}")

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
