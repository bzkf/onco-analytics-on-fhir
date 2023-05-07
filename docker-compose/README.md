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

## Update the air-gapped installer script

```sh
docker compose -f compose.full.yaml -f compose.onkoadt-to-fhir.yaml config -o compose.normalized.yaml

docker run \
  --env SENZING_DOCKER_COMPOSE_FILE=/data/compose.normalized.yaml \
  --env SENZING_OUTPUT_FILE=/data/save-images.sh \
  --env SENZING_SUBCOMMAND=create-save-images \
  --interactive \
  --rm \
  --tty \
  --volume ./:/data \
  --user "${UID}" \
  docker.io/senzing/docker-compose-air-gapper:1.0.4@sha256:f519089580c5422c02100042965f14ac2bb7bab5c3321e8a668b4f4b6b03902a
```
