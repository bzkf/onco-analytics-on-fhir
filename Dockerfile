FROM ghcr.io/astral-sh/uv:0.12.1-python3.12-trixie-slim@sha256:fce651fe385e83c691c15c33b2574486581fbc58e307f2db3a9c3987919f8b08
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0 \
    SPARK_CONF_DIR=/app/spark/conf

WORKDIR /app

# hadolint ignore=DL3008
RUN <<EOF
set -e
apt-get update -y
# openjdk for Pathling, graphviz for the "dot" executable
apt-get install -y --no-install-recommends openjdk-21-jre-headless graphviz
rm -rf /var/lib/apt/lists/*
apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false

groupadd -r -g 65532 nonroot
useradd --create-home --shell /bin/bash --uid 65532 -g nonroot nonroot
EOF

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Copy the project into the image
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

USER 65532:65532

WORKDIR /app/analytics_on_fhir

RUN <<EOF
set -e
SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 main.py
rm -rf /tmp/spark*
EOF

ENTRYPOINT [ "python3", "main.py" ]
