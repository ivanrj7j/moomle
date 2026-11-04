FROM python:3.12.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

ENV PYTHONPATH=/app/src

RUN uv sync --frozen --only-group setup

CMD ["uv", "run", "--only-group", "setup", "alembic", "upgrade", "head"]