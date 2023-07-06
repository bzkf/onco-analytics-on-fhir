FROM docker.io/jupyter/pyspark-notebook:spark-3.3.2@sha256:86f23b36bbd1900e10ce15bb29cf55ce31b10b1406c5afa6e57acf529cb10093
WORKDIR /home/jovyan
USER 1000:100

COPY requirements.txt requirements.txt

RUN <<EOF
pip install --no-cache-dir -r requirements.txt
spark-shell -v --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,au.csiro.pathling:library-api:6.2.1,ch.cern.sparkmeasure:spark-measure_2.13:0.21,io.delta:delta-core_2.12:2.3.0"
EOF

COPY adtfhir_to_opal.py adtfhir_to_opal.py

ENTRYPOINT [ "python", "adtfhir_to_opal.py" ]
