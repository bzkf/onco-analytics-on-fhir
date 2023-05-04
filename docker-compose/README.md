# onkoadt-to-fhir Docker Compose Version

## Run

```sh
docker compose -f compose.onkoadt-to-fhir.yaml up
```

## Run while also starting a Kafka cluster and AKHQ

> **Warning**
> The included Kafka cluster is not suitable for production usage

```sh
docker compose -f compose.onkoadt-to-fhir.yaml -f compose.kafka.yaml up
```

Open <http://localhost:8084/> to view the cluster's topics.
