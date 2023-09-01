# onkoadt-to-fhir Docker Compose Version

## Run only the onkoadt-to-fhir job

```sh
docker compose -f compose.onkoadt-to-fhir.yaml up
```

## Run while also starting a Kafka cluster and Kafka connect

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.full.yaml up
```

Open <http://localhost:8084/> to view the cluster's topics.

## Load sample data from a ADT Sammelmeldung into the Kafka cluster

```sh
docker compose -f compose.decompose-xmls.yaml up
```

## Convert the FHIR resources to a CSV dataset

```sh
sudo chown -R 1001:1001 ./opal-output/
docker compose -f compose.adtfhir-to-opal.yaml up
```

## Start the entire stack

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.full.yaml -f compose.decompose-xmls.yaml -f compose.adtfhir-to-opal.yaml up
```

## Enable Kafka Connect and the connector

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.full.yaml up
```

```sh
curl -X POST \
  -H 'Content-Type: application/json' \
  -d @onkostar-db-connector.json \
  http://localhost:8083/connectors
```

## Run with enabled pseudonymization

> **Warning**
> Requires gPAS to be set-up and the [anonymization.yaml](anonymization.yaml) to be configured

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.full.yaml -f compose.pseudonymization.yaml up
```

## Run with enabled pseudonymization and sending resources to a FHIR server

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.full.yaml -f compose.fhir-server.yaml -f compose.pseudonymization.yaml up
```
