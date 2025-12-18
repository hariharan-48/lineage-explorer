# Build stage
FROM node:20-alpine AS build

WORKDIR /app

# Base path for K8s ingress (e.g., /lineage-web/)
ARG VITE_BASE_PATH=/lineage-web/
ENV VITE_BASE_PATH=$VITE_BASE_PATH

# Install dependencies
COPY package*.json ./
RUN npm ci

# Copy source and build with base path
COPY . .
RUN npm run build

# Production stage - use same node image to serve static files
FROM node:20-alpine

WORKDIR /app

# Install serve for static file serving
RUN npm install -g serve

# Copy built assets into the path prefix folder
# This way /lineage-web/* requests map to the correct files
COPY --from=build /app/dist ./dist/lineage-web

# Create a simple health check file
RUN echo '{"status":"healthy"}' > ./dist/lineage-web/health

# Copy index.html to root for SPA routing
COPY --from=build /app/dist/index.html ./dist/index.html

EXPOSE 80

# Serve the static files on port 80
CMD ["serve", "-s", "dist", "-l", "80"]
