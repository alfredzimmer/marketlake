# Spark jobs inside the compose network. From the host, S3 traffic to SeaweedFS
# goes through Docker Desktop's port proxy, which resets connections on scans of
# a few thousand files (#18). Same Python deps as the host via uv.lock; the
# pyspark wheel bundles Spark, so only a JRE is needed.
FROM eclipse-temurin:17-jre
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /bin/uv
ENV UV_PROJECT_ENVIRONMENT=/venv UV_PYTHON_INSTALL_DIR=/python UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
ENV PATH=/venv/bin:$PATH PYTHONPATH=/app/src
