FROM docker.io/library/python:3.13.7-slim-bookworm@sha256:9b8102b7b3a61db24fe58f335b526173e5aeaaf7d13b2fbfb514e20f84f5e386
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

# ENV SPARK_JARS_IVY="/home/spark/.ivy"
# WORKDIR /opt/bitnami/spark
# USER 0
# RUN groupadd -g 1001 spark && \
#     useradd spark -u 1001 -g spark -m -s /bin/bash

# COPY requirements.txt requirements.txt
# RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# WORKDIR /home/spark

# USER 0
# COPY start.sh start.sh
# RUN chmod +x start.sh

# USER 1001:1001

# COPY obds_fhir_to_opal.py obds_fhir_to_opal.py
# COPY utils_onco_analytics.py utils_onco_analytics.py
# COPY test_utils_onco_analytics.py test_utils_onco_analytics.py
# COPY descriptions_for_data_dictionary.csv descriptions_for_data_dictionary.csv

# RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 obds_fhir_to_opal.py

# ENTRYPOINT [ "bash", "start.sh" ]
