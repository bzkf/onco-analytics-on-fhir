#!/bin/sh
curl -X POST \
  -H 'Content-Type: application/json' \
  -d @onkostar-db-connector.json \
  http://localhost:8083/connectors

curl -X POST \
  -H 'Content-Type: application/json' \
  -d @onkostar-meldung-export-connector.json \
  http://localhost:8083/connectors
