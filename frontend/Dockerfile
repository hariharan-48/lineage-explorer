# Build stage
FROM node:20-alpine AS build

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci

# Copy source and build
COPY . .
RUN npm run build

# Production stage - use GCP mirror to avoid Docker Hub rate limits
FROM gcr.io/cloud-marketplace/google/nginx1:1.26

# Install fuse for GCS Fuse sidecar compatibility (Debian-based)
RUN apt-get update && apt-get install -y --no-install-recommends fuse && rm -rf /var/lib/apt/lists/*

# Copy built assets
COPY --from=build /app/dist /usr/share/nginx/html

# Copy nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
