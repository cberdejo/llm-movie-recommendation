# --- BASE ---------------------
FROM python:3.13-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl && rm -rf /var/lib/apt/lists/*

# --- UV -----------------------
FROM base AS uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# --- PRODUCTION ---------------
FROM uv AS production
WORKDIR /app

COPY utils_package/pyproject.toml /app/utils_package/
COPY utils_package/uv.lock        /app/utils_package/
COPY utils_package/src            /app/utils_package/src

COPY populate_db/pyproject.toml   /app/populate_db/
COPY populate_db/uv.lock          /app/populate_db/
COPY populate_db/src              /app/populate_db/src

RUN uv venv .venv
RUN uv pip install -e /app/utils_package && uv pip install -e /app/populate_db 


