#!/bin/sh
curl -X POST \
  http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{
  "name": "onkostar-meldung-export-connector",
  "config": {
    "tasks.max": "1",
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://onkostar-db:3306/ONKOSTAR",
    "connection.user": "onkostar_user",
    "connection.password": "${file:/tmp/kafka-connect-passwords.properties:onkostar-db-password}",
    "schema.pattern": "ONKOSTAR",
    "topic.prefix": "onkostar.MELDUNG_EXPORT",
    "query": "SELECT * FROM (SELECT versionsnummer AS VERSIONSNUMMER, id AS ID, CONVERT(lme.xml_daten using utf8) AS XML_DATEN FROM lkr_meldung_export lme WHERE typ != '-1' AND versionsnummer IS NOT NULL) AS lme_view",
    "mode": "incrementing",
    "incrementing.column.name": "ID",
    "poll.interval.ms": 86400000,
    "validate.non.null": "true",
    "numeric.mapping": "best_fit",
    "transforms": "ValueToKey",
    "transforms.ValueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.ValueToKey.fields": "ID"
  }
}'


curl -X POST \
  http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{
  "name": "onkostar-db-connector",
  "config": {
    "tasks.max": "1",
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://onkostar-db:3306/ONKOSTAR",
    "connection.user": "onkostar_user",
    "connection.password": "${file:/tmp/kafka-connect-passwords.properties:onkostar-db-password}",
    "schema.pattern": "ONKOSTAR",
    "topic.prefix": "onkostar.PATIENT",
    "query": "SELECT pat.ID, pat.PATIENTEN_ID, b.LETZTEINFORMATION, b.STERBEDATUM, pat.BEARBEITET_AM FROM patient pat JOIN prozedur pr ON (pr.patient_id = pat.id) JOIN dk_bestoftumor b ON (b.id = pr.id)",
    "mode": "timestamp",
    "timestamp.column.name": "BEARBEITET_AM",
    "validate.non.null": "true",
    "numeric.mapping": "best_fit_eager_double",
    "transforms": "ValueToKey",
    "transforms.ValueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.ValueToKey.fields": "PATIENTEN_ID",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "db.timezone": "Europe/Berlin",
    "timestamp.granularity": "micros_iso_datetime_string"
  }
}'
