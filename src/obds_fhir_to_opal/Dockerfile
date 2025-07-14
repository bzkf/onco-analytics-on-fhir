FROM docker.io/bitnami/spark:3.5.6@sha256:353e23d86c7952781042b4e91d1e2b018335f565430e449fff017269a273097f
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
