"""
FastAPI application entry point for Exasol Lineage API.
"""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.config import settings
from app.routers import objects, lineage, search
from app.services.cache_loader import get_cache_loader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load cache on startup."""
    logger.info("Starting Exasol Lineage API...")
    try:
        cache_loader = get_cache_loader()
        cache_loader.load()
        logger.info("Cache loaded successfully")
    except FileNotFoundError as e:
        logger.warning(f"Cache file not found: {e}. Run generate_sample_data.py first.")
    except Exception as e:
        logger.error(f"Failed to load cache: {e}")
    yield
    logger.info("Shutting down Exasol Lineage API...")


app = FastAPI(
    title=settings.APP_NAME,
    description="API for exploring Exasol database lineage",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(objects.router, prefix="/api/v1")
app.include_router(lineage.router, prefix="/api/v1")
app.include_router(search.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": settings.APP_NAME,
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


# Static file serving for production (App Engine / Docker)
# Frontend dist folder is copied to backend/static during deployment
STATIC_DIR = Path(__file__).parent.parent / "static"

if STATIC_DIR.exists():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    # Catch-all route for SPA - serve index.html for all non-API routes
    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        """Serve the SPA for all non-API routes."""
        # Don't intercept API routes
        if full_path.startswith("api/"):
            return {"error": "Not found"}

        index_file = STATIC_DIR / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return {"error": "Frontend not built"}
