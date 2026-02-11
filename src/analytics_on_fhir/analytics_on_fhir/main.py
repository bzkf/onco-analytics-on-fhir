import pathlib
import sys

from loguru import logger
from pathling.context import PathlingContext
from pathling.datasource import DataSource
from pyspark import SparkContext
from pyspark.sql import SparkSession

from analytics_on_fhir.embark_rwd import run
from analytics_on_fhir.settings import StudyNames, settings
from analytics_on_fhir.study_protocol_d import StudyProtocolD
from analytics_on_fhir.study_protocol_pca1 import StudyProtocolPCa1
from analytics_on_fhir.utils import (
    extract_df_study_protocol_a_d_mii,
    filter_aml,
    save_final_df,
)


def main():
    spark_config = (
        SparkSession.builder.master(settings.spark.master)  # type: ignore
        .appName("analytics_on_fhir")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "au.csiro.pathling:library-runtime:9.2.0",
                    "io.delta:delta-spark_2.13:4.0.0",
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

    study_name = settings.study_name
    if settings.study_name == StudyNames.ALL:
        logger.info("Running all studies in sequence")
        for study_name in StudyNames:
            # this is really hacky: currently the "settings" object
            # is a bit too coupled with some helper functions
            settings.study_name = study_name
            logger.info(f"Running {study_name.value}")
            run_study(study_name, data, pc)
    else:
        run_study(study_name, data, pc)


def run_study(study_name: StudyNames, data: DataSource, pc: PathlingContext):
    match study_name:
        case StudyNames.EMBARK_RWD:
            df = run(data, pathlib.Path(settings.results_directory_path) / "embark_rwd")
            save_final_df(
                df,
                settings,
                suffix="embark_rwd",
            )
        case StudyNames.STUDY_PROTOCOL_A:
            df = extract_df_study_protocol_a_d_mii(
                pc,
                data,
                pc.spark,
                settings,
            )
            save_final_df(
                df,
                settings,
                suffix="study_protocol_a_d_mii",
            )
        case StudyNames.STUDY_PROTOCOL_D:
            study_protocol_d = StudyProtocolD(
                pc=pc,
                data=data,
                settings=settings,
                spark=pc.spark,
            )
            study_protocol_d.run()
        case StudyNames.STUDY_PROTOCOL_PCA1:
            study_protocol_pca1 = StudyProtocolPCa1(
                pc=pc,
                data=data,
                settings=settings,
                spark=pc.spark,
            )
            study_protocol_pca1.run()
        case StudyNames.STUDY_PROTOCOL_AML:
            df = extract_df_study_protocol_a_d_mii(
                pc,
                data,
                pc.spark,
                settings,
            )
            df = filter_aml(df)
            save_final_df(
                df,
                settings,
                suffix="study_protocol_aml",
            )
        case _:
            logger.warning(f"No study case matched for: {study_name}")


if __name__ == "__main__":
    main()
