# syntax=docker/dockerfile:1
# Build context: repo root (mirrors the engine Dockerfile; root .dockerignore
# already excludes src/ui/node_modules and src/ui/dist).

# ---- build ----
FROM node:22-alpine AS build
WORKDIR /app
# Copy manifests first so the dependency layer caches on source-only changes.
COPY src/ui/package.json src/ui/package-lock.json ./
RUN npm ci
COPY src/ui/ ./
RUN npm run build

# ---- runtime: static files served + proxied by nginx ----
FROM nginx:alpine AS final
COPY src/ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
