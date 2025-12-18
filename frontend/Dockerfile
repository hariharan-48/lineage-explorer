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

# Copy built assets
COPY --from=build /app/dist ./dist

EXPOSE 8080

# Serve the static files on port 8080
CMD ["serve", "-s", "dist", "-l", "8080"]
