import os
import shutil
import time

import pathling as ptl
from pathling import Expression as exp
from pathling import PathlingContext
from pathling.etc import find_jar
from pydantic import BaseSettings  # pydantic_settings ?
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.types import DoubleType, IntegerType, StringType
from utils_onco_analytics import (
    deconstruct_date,
    generate_datadictionary,
    group_entities,
    map_gender,
    map_icd10,
)


class Settings(BaseSettings):
    output_folder: str = "~/opal-output"
    output_filename: str = "df.csv"
    kafka_topic_year_suffix: str = ".2022"
    kafka_patient_topic: str = "fhir.obds.Patient"
    kafka_condition_topic: str = "fhir.obds.Condition"
    kafka_observation_topic: str = "fhir.obds.Observation"
    kafka_procedure_topic: str = "fhir.obds.Procedure"
    kafka_medicationstatement_topic: str = "fhir.obds.MedicationStatement"
    # ⚠️ make sure these are consistent with the ones downloaded inside the Dockerfile
    jar_list: list = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4",
        "au.csiro.pathling:library-api:6.2.1",
        "ch.cern.sparkmeasure:spark-measure_2.13:0.21",
        "io.delta:delta-core_2.12:2.3.0",
    ]
    spark_app_name: str = "oBDS-FHIR-to-Opal"
    master: str = "local[*]"
    kafka_bootstrap_server: str = "kafka:9092"

    spark_driver_memory: str = "18g"

    spark_jars_ivy: str = "/home/spark/.ivy2"


settings = Settings()


def setup_spark_session(appName: str, master: str):
    spark = (
        SparkSession.builder.appName(appName)
        .master(master)
        .config("spark.ui.port", "4040")
        .config("spark.rpc.message.maxSize", "1000")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages", ",".join(settings.jar_list))
        .config("spark.jars.ivy", settings.spark_jars_ivy)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.network.timeout", "6000s")
        .config("spark.driver.maxResultSize", "8g")
        .config("spark.sql.broadcastTimeout", "1200s")
        .config("spark.executor.heartbeatInterval", "1200s")
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark "
            + "-XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails "
            + "-XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
        )
        .config("spark.task.maxDirectResultSize", "256MB")
        .getOrCreate()
    )
    spark.sparkContext.addFile(find_jar())
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

    if kafka_data is None:
        raise ValueError("kafka_data is None")

    # PATIENTS
    patients = pc.encode_bundle(kafka_data.select("value"), "Patient")
    if patients is not None:
        patients_dataset = pc.read.datasets({"Patient": patients})
        patients_dataset.write.delta(bundle_folder)
    else:
        print("Warning: 'patients' is None, skipping Patients dataset processing.")

    # CONDITIONS
    conditions = pc.encode_bundle(kafka_data.select("value"), "Condition")
    if conditions is not None:
        conditions_dataset = pc.read.datasets({"Condition": conditions})
        conditions_dataset.write.delta(bundle_folder)
    else:
        print("Warning: 'conditions' is None, skipping Conditions dataset processing.")

    # PROCEDURES
    procedures = pc.encode_bundle(kafka_data.select("value"), "Procedure")
    if procedures is not None:
        procedures_dataset = pc.read.datasets({"Procedure": procedures})
        procedures_dataset.write.delta(bundle_folder)
    else:
        print("Warning: 'procedures' is None, skipping Procedures dataset processing.")

    # OBSERVATIONS
    observations = pc.encode_bundle(kafka_data.select("value"), "Observation")
    if observations is not None:
        observations_dataset = pc.read.datasets({"Observation": observations})
        observations_dataset.write.delta(bundle_folder)
    else:
        print(
            "Warning: 'observations' is None, skipping Observations dataset processing."
        )

    # MEDICATION STATEMENTS
    medicationstatements = pc.encode_bundle(
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
        )


def lookup_gender(gender_string):
    if gender_string is None or gender_string == "":
        gender_mapped = 0
    elif gender_string == "female" or gender_string == "weiblich":
        gender_mapped = 1
    elif gender_string == "male" or gender_string == "männlich":
        gender_mapped = 2
    else:
        gender_mapped = 3  # other / divers
    return gender_mapped


def return_year(deceasedDateTime):
    if deceasedDateTime is not None:
        year = deceasedDateTime[0:4]
        return year
    else:
        return deceasedDateTime


def extract_df(pc: PathlingContext, data: ptl.datasource.DataSource):
    map_icd10UDF = udf(lambda x: map_icd10(x), StringType())
    group_entitiesUDF = udf(lambda x: group_entities(x), StringType())
    map_genderUDF = udf(lambda x: map_gender(x), StringType())

    df = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                """code.coding
                .where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code""",
                "icd10_code",
            ),
            exp("Condition.subject.resolve().ofType(Patient).gender", "gender"),
        ],
    )
    # deconstruct date in YYYY, MM, DD
    df = deconstruct_date(df, "date_diagnosis")

    # map ICD10 code to numeric value for DataSHIELD, C = 3, D = 4
    df = df.withColumn(
        "icd10_mapped",
        map_icd10UDF(df.icd10_code).cast(DoubleType()),
    )
    # groupd icd10 to entities, inspired from "Jahresbericht 2023 des Bayerischen
    # Krebsregisters - Krebs in Bayern in den Jahren 2015 bis 2019 - Band 5"
    df = df.withColumn(
        "icd10_grouped_entities",
        group_entitiesUDF(df.icd10_mapped).cast(IntegerType()),
    )
    # map gender to numeric value for DataSHIELD, None = 0, "female" = 1, "male" = 2,
    # "other/diverse" = 3
    df = df.withColumn(
        "gender_mapped",
        map_genderUDF(df.gender).cast(IntegerType()),
    )
    df = df.dropDuplicates()

    return df


def save_final_df(df):
    df_with_id = df.withColumn("ID", monotonically_increasing_id())

    # rearrange columns to have ID as the first column
    df_with_id = df_with_id.select("ID", *df.columns)
    # to have only one single csv
    df_with_id = df_with_id.coalesce(1)
    # write DataFrame to CSV, rename it
    output_dir = os.path.join(settings.output_folder, "csv-dir")
    df_with_id.write.mode("overwrite").csv(output_dir, header=True)
    output_file = os.path.join(settings.output_folder, settings.output_filename)
    part_file = [file for file in os.listdir(output_dir) if file.startswith("part-")][0]
    shutil.move(os.path.join(output_dir, part_file), output_file)
    shutil.rmtree(output_dir)


def prepare_datadictionary(df):
    # generate data dictionary
    dtypes_list_df = [str(dtype[1]) for dtype in df.dtypes]

    description_list_df_dictionary = {
        "condition_id": "Condition ID, unique for each condition",
        "date_diagnosis": "date of diagnosis",
        "icd10_code": "ICD10 GM diagnosis code",
        "icd10_mapped": """ICD10 GM diagnosis code mapped A = 1, B = 2, C = 3, D = 4,
        e.g.: A01.9 = 101.9, C50.1 = 350.1 or D41.9 = 441.9""",
        "icd10_grouped_entities": """ICD10 GM diagnosis code grouped to entity groups
        from 0-23 according to LGL Report Cancer in Bavaria 2019, mapping see
        github.com/bzkf/onco-analytics-on-fhir/src/obds_fhir_to_opal
        /utils_onco_analytics.py""",
        "date_diagnosis_year": "Year of Diagnosis",
        "date_diagnosis_month": "Month of Diagnosis",
        "date_diagnosis_day": "Day of Diagnosis",
        "gender": "Gender - male, female, other/diverse",
        "gender_mapped": """Gender mapped: 0 = None, 1 = female, 2 = male,
        3 = other/diverse""",
    }

    return dtypes_list_df, description_list_df_dictionary


def main():
    start = time.monotonic()

    kafka_topics = create_list_of_kafka_topics()
    print("kafka_topics: ", kafka_topics)

    spark = setup_spark_session(settings.spark_app_name, settings.master)
    pc = PathlingContext.create(spark=spark, enable_extensions=True)

    read_data_from_kafka_save_delta(spark, kafka_topics, pc)

    data = pc.read.delta(os.path.join(settings.output_folder, "bundles-delta"))

    df = extract_df(ptl, data)

    save_final_df(df)

    shutil.rmtree(os.path.join(settings.output_folder, "bundles-delta"))

    dtypes_list_df, description_list_df_dictionary = prepare_datadictionary(df)

    generate_datadictionary(
        file_path="data_dictionary_df.xlsx",
        table_name="df",
        colnames_list=df.columns,
        value_type_list=dtypes_list_df,
        description=description_list_df_dictionary,
    )

    end = time.monotonic()
    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
