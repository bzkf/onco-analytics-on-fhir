FROM docker.io/bitnami/spark:3.3.4@sha256:0b8dfa8bf3593be450af51d1da1e04cc568ef92deb5dea0f834c0be0c912ce6c
ENV SPARK_JARS_IVY="/home/spark/.ivy"
WORKDIR /opt/bitnami/spark
USER 0
RUN groupadd -g 1001 spark && \
    useradd spark -u 1001 -g spark -m -s /bin/bash

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

USER 1001:1001
RUN spark-shell -v --conf spark.jars.ivy=${SPARK_JARS_IVY}\
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,au.csiro.pathling:library-api:6.2.1,ch.cern.sparkmeasure:spark-measure_2.13:0.21,io.delta:delta-core_2.12:2.3.0"

WORKDIR /home/spark

USER 0
COPY start.sh start.sh
RUN chmod +x start.sh
USER 1001:1001

COPY obds_fhir_to_opal.py obds_fhir_to_opal.py
COPY utils_onco_analytics.py utils_onco_analytics.py
COPY test_utils_onco_analytics.py test_utils_onco_analytics.py

ENTRYPOINT [ "bash", "start.sh" ]

#ENTRYPOINT [ "python", "obds_fhir_to_opal.py" ]
