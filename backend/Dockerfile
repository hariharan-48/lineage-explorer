FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code only (cache loaded from GCS at runtime)
COPY app/ ./app/

# Expose port
EXPOSE 8000

# Environment variables for GCS cache loading
# Set these in your K8s deployment:
#   GCS_BUCKET: your-bucket-name
#   GCS_CACHE_FILE: lineage_cache.json (default)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
