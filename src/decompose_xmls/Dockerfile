# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
FROM docker.io/library/python:3.11.0-slim@sha256:b5ee5360b357dcd0ab2e1cc5e22fa589b6734bbd55776df6b1700994febf1527 AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install --no-cache-dir -r requirements.txt

RUN chown -R appuser:appuser /app

# Copy the source code into the container.
COPY decompose_xmls.py /app

FROM base AS test
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements-test.txt,target=requirements-test.txt \
    python -m pip install --no-cache-dir -r requirements-test.txt

COPY . .

RUN PYTHONPATH=${PWD} pytest -vv --cov

FROM base
# Switch to the non-privileged user to run the application.
USER 10001

# Run the application.
CMD ["python", "/app/decompose_xmls.py"]
