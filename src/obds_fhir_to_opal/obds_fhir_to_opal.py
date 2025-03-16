import os
import time

from loguru import logger
from pathling import PathlingContext
from pydantic import BaseSettings
from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils_onco_analytics import (
    extract_df_PoC,
    extract_df_study_protocol_a,
    extract_df_study_protocol_c,
    generate_data_dictionary,
    prepare_data_dictionary,
    save_final_df,
)


class Settings(BaseSettings):
    output_folder: str = "./opal-output"
    output_filename: str = "df.csv"
    study_name: str = "study_protocol_a_c61"
    kafka_topic_year_suffix: str = ""  # ".2022"
    kafka_patient_topic: str = "fhir.obds.Patient"
    kafka_condition_topic: str = "fhir.obds.Condition"
    kafka_observation_topic: str = "fhir.obds.Observation"
    kafka_procedure_topic: str = "fhir.obds.Procedure"
    kafka_medicationstatement_topic: str = "fhir.obds.MedicationStatement"
    # ⚠️ make sure these are consistent with the ones downloaded inside the Dockerfile
    jar_list: list = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        "au.csiro.pathling:library-runtime:7.2.0",
        "io.delta:delta-spark_2.12:3.2.0",
    ]

    spark_app_name: str = "oBDS-FHIR-to-Opal"
    master: str = "local[*]"
    kafka_bootstrap_server: str = "kafka:9092"

    spark_driver_memory: str = "16g"


settings = Settings()


def setup_spark_session(appName: str, master: str):
    # muss ich ein sigint signal abfangen ctrl+c zb?
    existing_spark = SparkSession.getActiveSession()
    if existing_spark is not None:
        print("Stopping existing Spark session before creating a new one...")
        existing_spark.catalog.clearCache()  # see i
        existing_spark.stop()  # Stop any active session
        # SparkSession._instantiatedContext = None
        # Force clear the active SparkContext
        SparkContext._active_spark_context = None
    elif existing_spark is None:
        print("no existing spark session found")

    spark = (
        SparkSession.builder.appName(appName)
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
        # .config("spark.driver.maxResultSize", "8g")
        .config("spark.sql.debug.maxToStringFields", "1000")  #
        .config("spark.sql.broadcastTimeout", "1200s")
        .config("spark.executor.heartbeatInterval", "1200s")
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark "
            + "-XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails "
            + "-XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
        )
        .config("spark.task.maxDirectResultSize", "256MB")
        # try to avoid multiple spark contexts in one python process
        .config("spark.driver.allowMultipleContexts", "false")
        .getOrCreate()
    )

    return spark


def create_list_of_kafka_topics():
    return (
        settings.kafka_patient_topic
        + settings.kafka_topic_year_suffix
        + ","
        + settings.kafka_condition_topic
        + settings.kafka_topic_year_suffix
        + ","
        + settings.kafka_observation_topic
        + settings.kafka_topic_year_suffix
        + ","
        + settings.kafka_procedure_topic
        + settings.kafka_topic_year_suffix
        + ","
        + settings.kafka_medicationstatement_topic
        + settings.kafka_topic_year_suffix
    )


def read_data_from_kafka_save_delta(
    spark: SparkSession, kafka_topics: str, pc: PathlingContext
):
    logger.info("Begin reading from Kafka")
    # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_server)
        .option("subscribe", kafka_topics)
        .option("startingOffsets", "earliest")
        .load()
    )
    query = (
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream.trigger(once=True)
        .queryName("gettable")
        .format("memory")
        .start()
    )
    query.processAllAvailable()
    kafka_data = spark.sql("select * from gettable")
    kafka_data = kafka_data.select("value")
    bundle_folder = os.path.join(settings.output_folder, "bundles-delta")

    logger.info("Read all available data")

    if kafka_data is None:
        raise ValueError("kafka_data is None")

    # PATIENTS
    patients = pc.encode_bundle(kafka_data.select("value"), "Patient")
    if patients is not None:
        patients_dataset = pc.read.datasets({"Patient": patients})
        patients_dataset.write.delta(bundle_folder)

        patients.write
    else:
        print("Warning: 'patients' is None, skipping Patients dataset processing.")

    logger.info("Encoded Patient")

    # CONDITIONS
    conditions = pc.encode_bundle(kafka_data.select("value"), "Condition")
    if conditions is not None:
        conditions_dataset = pc.read.datasets({"Condition": conditions})
        conditions_dataset.write.delta(bundle_folder)
    else:
        print("Warning: 'conditions' is None, skipping Conditions dataset processing.")

    logger.info("Encoded Condition")

    # PROCEDURES
    """ procedures = pc.encode_bundle(kafka_data.select("value"), "Procedure")
    if procedures is not None:
        procedures_dataset = pc.read.datasets({"Procedure": procedures})
        procedures_dataset.write.delta(bundle_folder)
    else:
        print("Warning: 'procedures' is None, skipping Procedures dataset processing.")
 """
    # OBSERVATIONS
    observations = pc.encode_bundle(kafka_data.select("value"), "Observation")
    if observations is not None:
        observations_dataset = pc.read.datasets({"Observation": observations})
        observations_dataset.write.delta(bundle_folder)
    else:
        print(
            "Warning: 'observations' is None, skipping Observations dataset processing."
        )

    logger.info("Encoded Observation")

    # MEDICATION STATEMENTS
    """ medicationstatements = pc.encode_bundle(
        kafka_data.select("value"), "MedicationStatement"
    )
    if medicationstatements is not None:
        medicationstatements_dataset = pc.read.datasets(
            {"MedicationStatement": medicationstatements}
        )
        medicationstatements_dataset.write.delta(bundle_folder)
    else:
        print(
            "Warning: 'medicationstatements' is None, skipping MedicationStatements"
            "dataset processing."
        ) """


def main():
    start = time.monotonic()

    kafka_topics = create_list_of_kafka_topics()
    print("kafka_topics: ", kafka_topics)

    spark = setup_spark_session(settings.spark_app_name, settings.master)
    pc = PathlingContext.create(spark=spark, enable_extensions=True)

    # try:
    read_data_from_kafka_save_delta(spark, kafka_topics, pc)

    data = pc.read.delta(os.path.join(settings.output_folder, "bundles-delta"))

    match settings.study_name:
        case "PoC":
            df = extract_df_PoC(pc, data)
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case "study_protocol_a":
            df = extract_df_study_protocol_a(pc, data, settings, spark)
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case "study_protocol_a_c61":
            df = extract_df_study_protocol_a(pc, data, settings, spark, c61=True)
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case "study_protocol_c":
            df = extract_df_study_protocol_c(pc, data)
            dtypes_list_df, description_list_df_dictionary = prepare_data_dictionary(df)
        case _:
            raise ValueError(f"Unknown study type: {settings.study_name}")

    save_final_df(df, settings)

    # Clean up the delta folder after processing
    # dir_path = os.path.join(settings.output_folder, "bundles-delta")
    # if os.path.exists(dir_path) and os.path.isdir(dir_path):
    #    shutil.rmtree(dir_path)

    # Generate the data dictionary
    generate_data_dictionary(
        file_path=os.path.join(
            settings.output_folder, settings.study_name, "data_dictionary_df.xlsx"
        ),
        table_name="df",
        colnames_list=df.columns,
        value_type_list=dtypes_list_df,
        description=description_list_df_dictionary,
    )
    end = time.monotonic()

    print(f"time elapsed: {end - start}s")


"""     finally:
        # Gracefully stop the Spark session in the 'finally' block to ensure it always runs
        if 'spark' in locals() and spark is not None:
            print("Stopping Spark session end of main...")
            spark.stop()
            spark.catalog.clearCache()  # see i
            SparkSession._instantiatedContext = None
            spark.SparkContext._active_spark_context = None """


if __name__ == "__main__":
    main()
