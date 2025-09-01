import os
import shutil
import sys
import time

from loguru import logger
from pathling import PathlingContext
from pydantic import BaseSettings
from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils_onco_analytics import (
    add_age_col,
    add_deceased_flag,
    clean_df,
    datashield_preps_wrapper,
    extract_df_PoC,
    extract_df_study_protocol_a0_1_3_7_d,
    extract_df_study_protocol_c,
    extract_filter_df_c61,
    extract_pca_ops,
    extract_pca_st,
    extract_pca_sys,
    generate_data_dictionary,
    plot,
    prepare_data_dictionary,
    save_final_df,
    union_sort_pivot_join,
)


class Settings(BaseSettings):
    output_folder: str = "./opal-output"
    output_filename: str = "df.csv"
    study_name: str = "pca_therapies1"
    # ⚠️ make sure these are consistent with the ones downloaded inside the Dockerfile
    jar_list: list = [
        "au.csiro.pathling:library-runtime:7.2.0",
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

    spark_app_name: str = "oBDS-FHIR-to-Opal"
    master: str = "local[*]"

    spark_driver_memory: str = "16g"
    spark_install_packages_and_exit: bool = False

    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = "miniopass"

    spark_s3_endpoint: str = "minio:9000"
    delta_bucket: str = "fhir/default"

    spark_checkpoint_dir: str = ""


settings = Settings()


def setup_spark_session(app_name: str, master: str):
    existing_spark = SparkSession.getActiveSession()
    if existing_spark is not None:
        print("Stopping existing Spark session before creating a new one...")
        existing_spark.catalog.clearCache()
        existing_spark.stop()  # Stop any active session
        SparkContext._active_spark_context = None
    elif existing_spark is None:
        print("no existing spark session found")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.ui.port", "4040")
        .config("spark.rpc.message.maxSize", "1000")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages", ",".join(settings.jar_list))
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.network.timeout", "6000s")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .config("spark.sql.broadcastTimeout", "1200s")
        .config("spark.executor.heartbeatInterval", "1200s")
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark "
            + "-XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails "
            + "-XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{settings.spark_s3_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.worker.cleanup.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    if settings.spark_checkpoint_dir:
        spark.sparkContext.setCheckpointDir(settings.spark_checkpoint_dir)

    return spark


def main():
    start = time.monotonic()

    spark = setup_spark_session(settings.spark_app_name, settings.master)
    pc = PathlingContext.create(spark=spark, enable_extensions=True)

    if settings.spark_install_packages_and_exit:
        logger.info("Exiting after installing packages")
        sys.exit()

    # read from s3 here now
    data = pc.read.delta(f"s3a://{settings.delta_bucket}")

    match settings.study_name:
        case "PoC":
            df = extract_df_PoC(pc, data)
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case "study_protocol_a0_1_3_7_d":
            df = extract_df_study_protocol_a0_1_3_7_d(pc, data, settings, spark)
            df = datashield_preps_wrapper(
                df, condition=True, patient=True, death=True, gleason=True
            )
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case "study_protocol_c":
            df = extract_df_study_protocol_c(pc, data)
            df = datashield_preps_wrapper(df, condition=True, patient=True)
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case "pca_therapies1":
            df_c61 = extract_filter_df_c61(pc, data, settings, spark)
            ops_final = extract_pca_ops(pc, data, settings, spark)
            radiotherapies_final = extract_pca_st(pc, data, settings, spark)
            systemtherapies_final = extract_pca_sys(pc, data, settings, spark)
            df = union_sort_pivot_join(
                df_c61, ops_final, systemtherapies_final, radiotherapies_final
            )
            df = add_deceased_flag(df)
            df = add_age_col(
                df=df,
                birthdate_col="birthdate",
                refdate_col="date_diagnosis",
                age_colname="age_at_diagnosis",
            )
            df = clean_df(df=df)
            plot(df, settings)
        case _:
            raise ValueError(f"Unknown study type: {settings.study_name}")

    save_final_df(df, settings, suffix=settings.study_name)

    # Clean up the delta folder after processing
    dir_path = os.path.join(settings.output_folder, "bundles-delta")
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        shutil.rmtree(dir_path)

    # Generate one data dictionary for all studies and save in parent folder
    if settings.study_name != "pca_therapies1":
        generate_data_dictionary(
            file_path=os.path.join(settings.output_folder, "data_dictionary_df.xlsx"),
            table_name="df_" + settings.study_name,
            colnames_list=df.columns,
            value_type_list=dtypes_list_df,
            description=description_list_df_dictionary,
        )
    end = time.monotonic()

    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
