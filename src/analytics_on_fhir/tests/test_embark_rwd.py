import os
from pathlib import Path

import pytest
from pathling import DataSource, PathlingContext
from pyspark.sql import SparkSession

from analytics_on_fhir.embark_rwd import extract

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


@pytest.fixture(scope="session")
def data_source_fixture() -> DataSource:
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "au.csiro.pathling:library-runtime:9.1.0",
                    "io.delta:delta-spark_2.13:4.0.0",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                    "org.apache.hadoop:hadoop-aws:3.4.1",
                ]
            ),
        )
        .getOrCreate()
    )
    pc = PathlingContext.create(
        spark,
        enable_extensions=True,
        enable_delta=True,
        enable_terminology=False,
        terminology_server_url="http://localhost/not-a-real-server",
    )
    ds = pc.read.bundles(
        (HERE / "fixtures" / "fhir").as_posix(),
        resource_types=["Patient", "Condition", "Observation", "MedicationStatement"],
    )
    return ds


def test_with_empty_dataframe_should_not_fail(data_source_fixture: DataSource):
    df = extract(data_source_fixture)
    print(df)
    df.show()
    assert df is not None
