FROM python:3.12.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY alembic.ini ./
COPY scripts ./scripts
COPY adservers_seed.txt ./

ENV PYTHONPATH=/app/src:/app

RUN uv sync --frozen --only-group setup

CMD ["uv", "run", "--only-group", "setup", "python", "scripts/migrate.py"]
