import sys
import pathlib

from loguru import logger
from pathling.context import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import SparkSession

from analytics_on_fhir.embark_rwd import run
from analytics_on_fhir.settings import settings


def main():
    spark_config = (
        SparkSession.builder.master(settings.spark.master)  # type: ignore
        .appName("analytics_on_fhir")
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
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.driver.memory",
            settings.spark.driver_memory,
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", settings.spark.warehouse_dir)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            settings.spark.s3_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            settings.spark.s3_connection_ssl_enabled,
        )
        .config("fs.s3a.committer.name", "magic")
        .config("fs.s3a.committer.magic.enabled", "true")
        .config("fs.s3a.access.key", settings.aws_access_key_id)
        .config("fs.s3a.secret.key", settings.aws_secret_access_key)
    )

    spark = spark_config.getOrCreate()

    if settings.spark.install_packages_and_exit:
        logger.info("Exiting after having installed packages")
        sys.exit()

    pc = PathlingContext.create(
        spark,
        enable_extensions=True,
        enable_delta=True,
        enable_terminology=False,
        terminology_server_url="http://localhost/not-a-real-server",
    )

    data: DataSource

    if settings.delta_database_path:
        data = pc.read.delta(settings.delta_database_path)
    else:
        data = pc.read.bundles(
            settings.fhir_bundles_path,
            resource_types=[
                "Patient",
                "Condition",
                "Observation",
                "MedicationStatement",
            ],
        )

    if settings.study_name == "embark_rwd":
        run(data, pathlib.Path(settings.results_directory_path))


if __name__ == "__main__":
    main()
