# diz-in-a-box Docker Compose Version

![Figure Modular Pipeline](../img/fig1.png)

## Installation

### 1. Prepare Data Import
#### a) ONKOSTAR data base connector
Configure kafka-connect in [compose.full-yaml](compose.full.yaml) and [docker-compose/kafka-connect-passwords.properties](kafka-connect-passwords.properties).
The oBDS single report XML-files will be loaded into the Kafka cluster.
For more information about kafka-connect, refer to [7. Enable Kafka Connect and the connector.](#enable-kafka-konnect-and-the-connector)
#### b) Folder import
Copy your oBDS collection report XML-files to [docker-compose/input-obds-reports]().


### 2. Start the Kafka Cluster

```sh
docker compose -f compose.full.yaml up
```

Open <http://localhost:8084/> to view the cluster's topics and the progress of your pipeline.


### 3. Load data
#### a) ONKOSTAR data base connector
The oBDS single report XML-files from ONKOSTAR data base will be loaded into the Kafka cluster with step 2.


#### b) Folder import
Decompose oBDS collection report XML-files from [docker-compose/input-obds-reports](docker-compose/input-obds-reports) into single XML reports and load them into the Kafka cluster.

```sh
docker compose -f compose.decompose-xmls.yaml up
```

### 4. Transform oBDS XML-data to FHIR

```sh
docker compose -f compose.obds-to-fhir.yaml up
```
We currently use the FHIR profiles defined under https://simplifier.net/oncology.


### 5. Convert the FHIR resources to a CSV dataset

```sh
sudo chown -R 1001:1001 ./opal-output/
docker compose -f compose.obds-fhir-to-opal.yaml up
```

### 6. Start the entire stack

```sh
docker compose -f compose.obds-to-fhir.yaml -f compose.full.yaml -f compose.decompose-xmls.yaml -f compose.obds-fhir-to-opal.yaml up
```

### 7. Enable Kafka Connect and the connector

Make sure to have access to Onkostar tables `lkr_meldung`, `lkr_meldung_export` and `erkrankung`.

The following SQL query will SELECT required information with columns filtered by type and containing required ICD_Version entry in `XML_DATEN`:

```sql
SELECT * FROM (
  SELECT YEAR(e.diagnosedatum) AS YEAR, versionsnummer AS VERSIONSNUMMER, lme.id AS ID, CONVERT(lme.xml_daten using utf8) AS XML_DATEN
    FROM lkr_meldung_export lme
    JOIN lkr_meldung lm ON lme.lkr_meldung = lm.id
    JOIN erkrankung e ON lm.erkrankung_id = e.id
    WHERE typ != '-1' AND versionsnummer IS NOT NULL AND lme.XML_DATEN LIKE '%ICD_Version%'
) o;
```

```sh
docker compose -f compose.obds-to-fhir.yaml -f compose.full.yaml up
```

```sh
curl -X POST \
  -H 'Content-Type: application/json' \
  -d @onkostar-db-connector.json \
  http://localhost:8083/connectors
```

### 8. Run with enabled pseudonymization

> **Warning**
> Requires gPAS to be set-up and the [anonymization.yaml](anonymization.yaml) to be configured

```sh
docker compose -f compose.obds-to-fhir.yaml -f compose.full.yaml -f compose.pseudonymization.yaml up
```

### 9. Run with enabled pseudonymization and sending resources to a FHIR server

```sh
docker compose -f compose.obds-to-fhir.yaml -f compose.full.yaml -f compose.fhir-server.yaml -f compose.pseudonymization.yaml up
```


### 10. Air-gapped installation

In case of absence of Internet connectivity, container images cannot be pulled from the registry. Instead, download the air-gapped installer and move it to the deployment machine:

<!-- x-release-please-start-version -->

```sh
curl -L -O https://github.com/bzkf/diz-in-a-box/releases/download/v2.2.2/air-gapped-installer.tgz
```

<!-- x-release-please-end -->

Run the following steps on the deployment machine.

Extract the archive:

```sh
tar xvzf ./air-gapped-installer.tgz
```
