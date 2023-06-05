# onkoadt-to-fhir Docker Compose Version

## Run

```sh
docker compose -f compose.onkoadt-to-fhir.yaml up
```

## Run while also starting a Kafka cluster and the rest of the infrastructure

> **Warning**
> The included Kafka cluster is not suitable for production usage

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.full.yaml up
```

Open <http://localhost:8084/> to view the cluster's topics.

## Load sample data from a ADT Sammelmeldung into the Kafka cluster

```sh
docker compose -f compose.decompose-xmls.yaml up
```

## Create connector

```sh
curl -X POST \
  -H 'Content-Type: application/json' \
  -d @onkostar-db-connector.json \
  http://localhost:8083/connectors
```
