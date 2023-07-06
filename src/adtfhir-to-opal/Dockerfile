FROM jupyter/pyspark-notebook:spark-3.3.2

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

RUN spark-shell -v --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,au.csiro.pathling:library-api:6.2.1,ch.cern.sparkmeasure:spark-measure_2.13:0.21,io.delta:delta-core_2.12:2.3.0"

COPY run.py run.py

#ENTRYPOINT ["/bin/bash"]
ENTRYPOINT [ "python", "run.py" ]

# BUILDKIT_PROGRESS=plain docker build -t adt:test . && docker run --rm -it adt:test
