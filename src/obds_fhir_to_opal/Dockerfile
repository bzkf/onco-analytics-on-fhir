FROM docker.io/apache/spark:3.5.6-scala2.12-java17-python3-ubuntu@sha256:702d82ecea50c6f8344d3df753ba26f05ffd9d1d052e180fed9c3d6c04f77730
ENV SPARK_JARS_IVY="/home/spark/.ivy"
WORKDIR /opt/bitnami/spark
USER 0
RUN groupadd -g 1001 spark && \
    useradd spark -u 1001 -g spark -m -s /bin/bash

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

WORKDIR /home/spark

USER 0
COPY start.sh start.sh
RUN chmod +x start.sh

USER 1001:1001

COPY obds_fhir_to_opal.py obds_fhir_to_opal.py
COPY utils_onco_analytics.py utils_onco_analytics.py
COPY test_utils_onco_analytics.py test_utils_onco_analytics.py
COPY descriptions_for_data_dictionary.csv descriptions_for_data_dictionary.csv

RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 obds_fhir_to_opal.py

ENTRYPOINT [ "bash", "start.sh" ]
