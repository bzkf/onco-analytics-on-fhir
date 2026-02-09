import pathlib
import sys

from loguru import logger
from pathling.context import PathlingContext
from pathling.datasource import DataSource
from pyspark import SparkContext
from pyspark.sql import SparkSession

from analytics_on_fhir.embark_rwd import run
from analytics_on_fhir.settings import settings
from analytics_on_fhir.study_protocol_d import StudyProtocolD
from analytics_on_fhir.study_protocol_pca1 import StudyProtocolPCa1
from analytics_on_fhir.utils import extract_df_study_protocol_a_d_mii, save_final_df


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
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.client.execution.timeout", "300000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
    )
    existing_spark = SparkSession.getActiveSession()
    if existing_spark is not None:
        print("Stopping existing Spark session before creating a new one...")
        existing_spark.catalog.clearCache()
        existing_spark.stop()  # Stop any active session
        SparkContext._active_spark_context = None
    elif existing_spark is None:
        print("no existing spark session found")

    spark = spark_config.getOrCreate()

    if settings.spark.install_packages_and_exit:
        logger.info("Exiting after having installed packages")
        sys.exit()

    logger.info(
        "spark.driver.memory = " + settings.spark.driver_memory,
    )

    if settings.spark.checkpoint_dir:
        spark.sparkContext.setCheckpointDir(settings.spark.checkpoint_dir)

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

    match settings.study_name:
        case "embark_rwd":
            run(data, pathlib.Path(settings.results_directory_path))
        case "study_protocol_a":
            df = extract_df_study_protocol_a_d_mii(
                pc,
                data,
                spark,
                settings,
            )
            save_final_df(df, settings, suffix="study_protocol_a_d_mii")
        case "study_protocol_d":
            study_protocol_d = StudyProtocolD(
                pc=pc,
                data=data,
                settings=settings,
                spark=spark,
            )
            study_protocol_d.run()
        case "study_protocol_pca1":
            study_protocol_pca1 = StudyProtocolPCa1(
                pc=pc,
                data=data,
                settings=settings,
                spark=spark,
            )
            study_protocol_pca1.run()


if __name__ == "__main__":
    main()
