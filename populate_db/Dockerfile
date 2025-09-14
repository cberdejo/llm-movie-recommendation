# --- BASE ---------------------
FROM python:3.13-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl \
    && rm -rf /var/lib/apt/lists/*

# --- UV INSTALLER -------------
FROM base AS uv-installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# --- BUILDER ------------------
FROM uv-installer AS builder
WORKDIR /app

COPY utils_package  /app/utils_package
COPY populate_db    /app/populate_db

RUN uv venv .venv && \
    uv pip install -e utils_package && \
    uv pip install -e populate_db

# --- PRODUCTION ----------------
FROM uv-installer AS production
WORKDIR /app
COPY --from=builder /app /app
