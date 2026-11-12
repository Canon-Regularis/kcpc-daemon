FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --locked --no-dev && mkdir -p /data && useradd --system --uid 10001 daemonuser && chown daemonuser /data
COPY config.docker.yaml /app/config.yaml
USER daemonuser
EXPOSE 8080
CMD ["/app/.venv/bin/kcpc-daemon", "--config", "/app/config.yaml"]
