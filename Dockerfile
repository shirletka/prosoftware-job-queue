FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.8.0 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/venv PATH="/venv/bin:$PATH"
WORKDIR /srv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY app ./app
COPY worker ./worker
COPY migrations ./migrations
COPY alembic.ini ./

FROM base AS test
RUN uv sync --frozen --no-install-project
COPY tests ./tests
CMD ["pytest", "-v"]

FROM base AS runtime
RUN useradd --system --create-home app
USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
