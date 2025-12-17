# Build stage
FROM node:20-alpine as build

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci

# Copy source and build
COPY . .
RUN npm run build

# Production stage
FROM nginx:alpine

# Copy built assets
COPY --from=build /app/dist /usr/share/nginx/html

# Copy nginx config template
COPY nginx.conf /etc/nginx/templates/default.conf.template

EXPOSE 80

# Default backend URL (override with env var in K8s)
ENV BACKEND_URL=http://backend:8000

# envsubst replaces ${BACKEND_URL} in template and starts nginx
CMD ["/bin/sh", "-c", "envsubst '${BACKEND_URL}' < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && nginx -g 'daemon off;'"]
