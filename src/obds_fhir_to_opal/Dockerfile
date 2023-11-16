FROM docker.io/bitnami/spark:3.5.0@sha256:902f1cb8028ddba2506b1cf39fae0d75d47641a3c0e9403684999a95b26b13be
ENV SPARK_JARS_IVY="/home/spark/.ivy"
WORKDIR /opt/bitnami/spark
USER 0
RUN groupadd -g 1001 spark && \
    useradd spark -u 1001 -g spark -m -s /bin/bash

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

USER 1001:1001
RUN spark-shell -v --conf spark.jars.ivy=${SPARK_JARS_IVY}\
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,au.csiro.pathling:library-api:6.2.1,ch.cern.sparkmeasure:spark-measure_2.13:0.21,io.delta:delta-core_2.12:2.3.0"

COPY obds_fhir_to_opal.py obds_fhir_to_opal.py

ENTRYPOINT [ "python", "obds_fhir_to_opal.py" ]
