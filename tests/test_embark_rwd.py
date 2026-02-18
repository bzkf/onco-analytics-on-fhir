import os
from pathlib import Path

import pytest
from pathling import DataSource, PathlingContext
from pyspark.sql import SparkSession
from syrupy import SnapshotAssertion

from analytics_on_fhir.embark_rwd import extract

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


def read_spark_csv_dir(csv_directory_path: Path):
    parts = sorted(csv_directory_path.glob("part-*.csv"))
    return "\n".join(p.read_text() for p in parts)


@pytest.fixture(scope="session")
def data_source_fixture() -> DataSource:
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "au.csiro.pathling:library-runtime:9.3.1",
                    "io.delta:delta-spark_2.13:4.0.0",
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


def test_extract_with_sample_data_should_write_csv_and_dotfile(
    data_source_fixture: DataSource, tmp_path: Path, snapshot: SnapshotAssertion
):
    cohort = extract(data_source_fixture, tmp_path)

    cohort_csv_path = tmp_path / "embark-rwd-cohort.csv"
    cohort.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        cohort_csv_path.as_posix()
    )

    cohort_content = read_spark_csv_dir(tmp_path / "embark-rwd-cohort.csv")
    csv_content = read_spark_csv_dir(tmp_path / "medication-counts.csv")
    dot_content = (tmp_path / "embark-rwd-flowchart.gv").read_text()

    assert {
        "cohort": cohort_content,
        "csv": csv_content,
        "dot": dot_content,
    } == snapshot
