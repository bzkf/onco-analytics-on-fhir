FROM docker.io/bitnami/spark:3.5.4@sha256:6adad33c3bbb65f20f328e6e6f37efdfd7aefafffdd172b31c617a5e3aef77bc
ENV SPARK_JARS_IVY="/home/spark/.ivy"
WORKDIR /opt/bitnami/spark
USER 0
#RUN groupadd -g 1001 spark && \
#    useradd spark -u 1001 -g spark -m -s /bin/bash

RUN id -g spark &>/dev/null || groupadd -g 1001 spark && \
    id -u spark &>/dev/null || useradd spark -u 1001 -g spark -m -s /bin/bash

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

USER 1001:1001
RUN spark-shell -v --conf spark.jars.ivy=${SPARK_JARS_IVY}\
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,au.csiro.pathling:library-api:7.2.0,io.delta:delta-spark_2.12:3.2.0"

WORKDIR /home/spark

USER 0
COPY start.sh start.sh
RUN chmod +x start.sh
#USER 1001:1001

COPY obds_fhir_to_opal.py obds_fhir_to_opal.py
COPY utils_onco_analytics.py utils_onco_analytics.py
COPY test_utils_onco_analytics.py test_utils_onco_analytics.py
COPY descriptions_for_data_dictionary.csv descriptions_for_data_dictionary.csv

ENTRYPOINT [ "bash", "start.sh" ]
