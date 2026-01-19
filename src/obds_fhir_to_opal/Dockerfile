FROM docker.io/library/python:3.11.14-slim-bookworm@sha256:bcbbec29f7a3f9cbee891e3cd69d7fe4dec7e281daf36cbd415ddd8ee2ba0077
ENV PYTHONUNBUFFERED=1 \
    SPARK_CONF_DIR=/app/spark/conf

WORKDIR /app

# hadolint ignore=DL3008
RUN <<EOF
set -e
apt-get update -y
apt-get install -y --no-install-recommends openjdk-17-jre-headless
rm -rf /var/lib/apt/lists/*
apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false

groupadd -r -g 1001 spark
useradd --create-home --shell /bin/bash --uid 1001 -g spark spark
EOF

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    pip install --no-cache-dir --require-hashes -r requirements.txt

COPY . /app

ENV PATH="/app/.venv/bin:$PATH"

USER 1001:1001

RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 /app/obds_fhir_to_opal.py

ENTRYPOINT [ "python3", "/app/obds_fhir_to_opal.py" ]
