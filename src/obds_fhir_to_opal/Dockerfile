FROM docker.io/bitnami/spark:3.5.5@sha256:46c2784c4f47d3f8c481c4f6ab775cfb2994fb24eaff2d8feb55b66335129ce0
ENV SPARK_JARS_IVY="/home/spark/.ivy"
WORKDIR /opt/bitnami/spark
USER 0
RUN groupadd -g 1001 spark && \
    useradd spark -u 1001 -g spark -m -s /bin/bash

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

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
