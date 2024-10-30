# Trino & Pathling

> [!IMPORTANT]
> Not yet ready for production use.

## Run

```sh
docker compose up
```

This starts MinIO, Trino, Pathling Server, and the Hive metastore.
This automatically imports the sample FHIR resources from the [synthea](./synthea)-folder via the
Pathling server and stores them as relational Delta-tables in MinIO. This also automatically
registers the tables so Trino knows where to look for them.

To setup is usually done when the logs display:

```
warehousekeeper-1 exited with code 0
```

To check the tables, open <http://localhost:9001/login> and log-in using `admin` and `miniopass`.
The resources are in the `fhir` bucket.
To query them, connect to Trino at <http://localhost:8080> using DataGrip, DBeaver, or the [Trino CLI](https://trino.io/docs/current/client/cli.html).

For exampel:

```sh
curl -O https://repo1.maven.org/maven2/io/trino/trino-cli/463/trino-cli-463-executable.jar
java -jar trino-cli-463-executable.jar
```

```console
trino> SELECT COUNT(*) FROM fhir.default.Patient;
 _col0
-------
    31
(1 row)

Query 20241030_162953_00002_h43br, FINISHED, 1 node
Splits: 18 total, 18 done (100.00%)
0.29 [31 rows, 0B] [106 rows/s, 0B/s]

trino>
```
