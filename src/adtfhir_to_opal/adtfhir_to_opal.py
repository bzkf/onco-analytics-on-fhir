import datetime
import os
import shutil
import time

import pyspark
from pathling import PathlingContext
from pathling.etc import find_jar
from pydantic import BaseSettings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, first, max, regexp_replace, to_date, udf
from pyspark.sql.types import StringType


class Settings(BaseSettings):
    output_folder: str = "~/opal-output"
    kafka_topic_year_suffix: str = ".2023"
    kafka_patient_topic: str = "fhir.onkoadt.Patient"
    kafka_condition_topic: str = "fhir.onkoadt.Condition"
    kafka_observation_topic: str = "fhir.onkoadt.Observation"
    kafka_procedure_topic: str = "fhir.onkoadt.Procedure"
    kafka_medicationstatement_topic: str = "fhir.onkoadt.MedicationStatement"
    jar_list: list = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
        "au.csiro.pathling:library-api:6.2.1",
        "ch.cern.sparkmeasure:spark-measure_2.13:0.21",
        "io.delta:delta-core_2.12:2.3.0",
    ]
    spark_app_name: str = "ADTFHIR-to-Opal"
    master: str = "local[*]"
    kafka_bootstrap_server: str = "kafka:9092"

    spark_worker_memory: str = "10g"
    spark_executor_memory: str = "8g"
    spark_driver_memory: str = "8g"
    spark_executor_cores: str = "4"


settings = Settings()


def setup_spark_session(appName: str, master: str):
    spark = (
        SparkSession.builder.appName(appName)
        .master(master)
        .config("spark.ui.port", "4040")
        .config("spark.rpc.message.maxSize", "1000")
        .config("spark.worker.memory", settings.spark_worker_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.cores", settings.spark_executor_cores)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages", ",".join(settings.jar_list))
        .config("spark.jars.ivy", "/home/jovyan/.ivy2")
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


def read_data_from_kafka_save_delta(spark: SparkSession, kafka_topics: str):
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
    print(
        "Heyhoy settings.output_folder + bundles-delta: ",
        settings.output_folder + "/bundles-delta",
    )
    kafka_data.write.format("delta").mode("overwrite").save(
        settings.output_folder + "/bundles-delta"
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


def calculate_age(birthdate):
    year = birthdate[0:4]
    today = datetime.date.today()
    current_year = today.year
    age = current_year - int(year)
    return age


def return_year(deceasedDateTime):
    if deceasedDateTime is not None:
        year = deceasedDateTime[0:4]
        return year
    else:
        return deceasedDateTime


def encode_patients(ptl: PathlingContext, df_bundles: pyspark.sql.dataframe.DataFrame):
    df_patients = ptl.encode_bundle(df_bundles, "Patient")
    lookup_genderUDF = udf(lambda x: lookup_gender(x), StringType())
    calculate_ageUDF = udf(lambda x: calculate_age(x), StringType())
    return_yearUDF = udf(lambda x: return_year(x), StringType())

    patients = df_patients.selectExpr(
        "id as pat_id", "gender", "birthDate", "deceasedBoolean", "deceasedDateTime"
    )

    patients = patients.withColumns(
        {
            "gender_mapped": lookup_genderUDF(patients.gender),
            "age_in_years": calculate_ageUDF(patients.birthDate),
            "deceased_year": return_yearUDF(patients.deceasedDateTime),
        }
    )

    patients = patients.select(
        patients.pat_id,
        patients.gender,
        patients.gender_mapped,
        patients.birthDate,
        patients.age_in_years,
        patients.deceasedDateTime,
        patients.deceased_year,
    )

    patients_cached = patients.cache()
    return patients_cached


def lookup_condcodingcode_mapping(icd10_code):
    if icd10_code[0] == "C":
        icd10_code = icd10_code.replace("C", "3")
    elif icd10_code[0] == "D":
        icd10_code = icd10_code.replace("D", "4")
    return icd10_code


def encode_conditions(ptl: PathlingContext, df_bundles):
    lookup_condcodingcode_mappingUDF = udf(
        lambda x: lookup_condcodingcode_mapping(x), StringType()
    )
    df_conditions = ptl.encode_bundle(df_bundles, "Condition")
    conditions = df_conditions.selectExpr(
        "id as cond_id",
        "subject.reference as subjectreference",
        "onsetDateTime as conditiondate",
        "code.coding.code[0] as condcodingcode",
        "stage.assessment as stageassessment",
        "evidence.detail as evidencedetail",
    )
    conditions = conditions.selectExpr(
        "cond_id",
        "subjectreference",
        "conditiondate",
        "condcodingcode",
        "EXPLODE_OUTER(stageassessment) as stageassessmentTNM",
        "evidencedetail",
    )
    conditions = conditions.selectExpr(
        "cond_id",
        "subjectreference",
        "conditiondate",
        "condcodingcode",
        "EXPLODE_OUTER(stageassessmentTNM) as stageassessmentTNM",
        "evidencedetail",
    )
    conditions = conditions.selectExpr(
        "cond_id",
        "subjectreference",
        "conditiondate",
        "condcodingcode",
        "stageassessmentTNM.reference as stagereference",
        "evidencedetail",
    )
    conditions = conditions.selectExpr(
        "cond_id",
        "subjectreference",
        "conditiondate",
        "condcodingcode",
        "stagereference",
        "EXPLODE_OUTER(evidencedetail) as evidencedetail",
    )
    conditions = conditions.selectExpr(
        "cond_id",
        "subjectreference",
        "conditiondate",
        "condcodingcode",
        "stagereference",
        "EXPLODE_OUTER(evidencedetail) as evidencedetail",
    )
    conditions = conditions.selectExpr(
        "cond_id",
        "subjectreference",
        "conditiondate",
        "condcodingcode",
        "stagereference",
        "evidencedetail.reference as evidencereference",
    ).withColumns(
        {
            "subjectreference": regexp_replace("subjectreference", "Patient/", ""),
            "evidencereference": regexp_replace(
                "evidencereference", "Observation/", ""
            ),
            "stagereference": regexp_replace("stagereference", "Observation/", ""),
            "conditiondate": regexp_replace("conditiondate", "T", " "),
        }
    )
    conditions = conditions.withColumn(
        "condcodingcode_mapped",
        lookup_condcodingcode_mappingUDF(conditions.condcodingcode),
    )
    conditions = conditions.select(
        "cond_id",
        "conditiondate",
        "subjectreference",
        "condcodingcode",
        "condcodingcode_mapped",
        "stagereference",
        "evidencereference",
    ).withColumn("conditiondate", to_date(conditions.conditiondate))

    conditions_cached = conditions.cache()

    return conditions_cached


def join_dataframes(df_one, partition_col_one: str, df_two, partition_col_two: str):
    # repartition before joining
    df_one_repartitioned = df_one.repartition(20, col(partition_col_one))
    df_two_repartitioned = df_two.repartition(20, col(partition_col_two))

    df_one_two_joined = df_one_repartitioned.join(
        df_two_repartitioned,
        df_one_repartitioned[partition_col_one]
        == df_two_repartitioned[partition_col_two],
        "left_outer",
    )

    df_one_two_joined_cached = df_one_two_joined.cache()
    return df_one_two_joined_cached


def lookup_obs_value_codingcode_tnm_uicc_mapping(obs_value_codingcode_tnm_UICC):
    obs_value_codingcode_tnm_uicc_mapping_dict = {
        "0": "0",
        "0a": "1",
        "0is": "2",
        "I": "3",
        "IA": "4",
        "IA1": "5",
        "IA2": "6",
        "IB": "7",
        "IB1": "8",
        "IB2": "9",
        "IC": "10",
        "II": "11",
        "IIA": "12",
        "IIA1": "13",
        "IIA2": "14",
        "IIB": "15",
        "IIC": "16",
        "III": "17",
        "IIIA": "18",
        "IIIB": "19",
        "IIIC": "20",
        "IIIC1": "21",
        "IIIC2": "22",
        "IV": "23",
        "IVA": "24",
        "IVB": "25",
        "IVC": "26",
        "IS": "27",
    }
    if obs_value_codingcode_tnm_UICC in obs_value_codingcode_tnm_uicc_mapping_dict:
        return obs_value_codingcode_tnm_uicc_mapping_dict[obs_value_codingcode_tnm_UICC]
    else:
        return obs_value_codingcode_tnm_UICC


def transform_tnmp(observations_tnmp):
    lookup_obs_value_codingcode_tnm_UICC_mappingUDF = udf(
        lambda x: lookup_obs_value_codingcode_tnm_uicc_mapping(x), StringType()
    )
    observations_tnmp = observations_tnmp.withColumn(
        "tnmp_UICC_mapped",
        lookup_obs_value_codingcode_tnm_UICC_mappingUDF(observations_tnmp.obsvaluecode),
    )
    observations_tnmp = observations_tnmp.selectExpr(
        "obssubjreference as tnm_obssubjreference",
        "obsid as tnmp_obsid",
        "obscodingcode as tnmp_codingcode",
        "obscodingdisplay as tnmp_obscodingdisplay",
        "obsvaluecode as tnmp_obsvalue_UICC",
        "obsdate as tnmp_obsdate",
        "tnmp_UICC_mapped",
    )

    observations_tnmp_cached = observations_tnmp.cache()

    return observations_tnmp_cached


def replace_element(obs_value_coding_code_hist):
    if obs_value_coding_code_hist is None:
        pass
    else:
        obs_value_coding_code_hist = obs_value_coding_code_hist.replace("/", "")

    return obs_value_coding_code_hist


def transform_histology(observations_histology):
    replace_elementUDF = udf(lambda x: replace_element(x), StringType())

    observations_histology = observations_histology.select(
        col("obssubjreference"),
        col("obsid").alias("obs_id_hist"),
        col("obscodingcode").alias("obs_coding_code_hist"),
        col("obscodingdisplay").alias("obs_coding_display_hist"),
        col("obsvaluecode").alias("obs_value_coding_code_hist"),
        col("obsdate").alias("obsdate_hist"),
    ).withColumn("obsdate_hist", regexp_replace("obsdate_hist", "T", " "))

    observations_histology = observations_histology.withColumns(
        {
            "obs_value_hist_mapped": replace_elementUDF(
                observations_histology.obs_value_coding_code_hist
            ),
            "obs_value_hist_mapped_0_4": replace_elementUDF(
                observations_histology.obs_value_coding_code_hist
            )[0:4],
            "obs_value_hist_mapped_5": replace_elementUDF(
                observations_histology.obs_value_coding_code_hist
            )[5:5],
        }
    )
    observations_histology_cached = observations_histology.cache()
    return observations_histology_cached


def encode_observations(ptl: PathlingContext, df_bundles):
    df_observations = ptl.encode_bundle(df_bundles, "Observation")
    observations = df_observations.selectExpr(
        "id as obsid",
        "subject.reference as obssubjreference",
        "code.coding[0].code as obscodingcode",
        "code.coding[0].display as obscodingdisplay",
        "valueCodeableConcept as obs_value_coding",
        "effectiveDateTime as obsdate",
    )
    observations = observations.selectExpr(
        "obsid",
        "obssubjreference",
        "obscodingcode",
        "obscodingdisplay",
        "obs_value_coding.*",
        "obsdate",
    )
    observations = observations.select(
        col("obsid"),
        col("obssubjreference"),
        col("obscodingcode"),
        col("obscodingdisplay"),
        explode("coding").alias("obs_value_coding"),
        col("obsdate"),
    )
    observations = observations.selectExpr(
        "obsid",
        "obssubjreference",
        "obscodingcode",
        "obscodingdisplay",
        "obs_value_coding.code as obsvaluecode",
        "obs_value_coding.display as obs_value_codingdisplay",
        "obsdate",
    ).withColumns(
        {
            "obssubjreference": regexp_replace("obssubjreference", "Patient/", ""),
            "obsdate": regexp_replace("obsdate", "T", " "),
        }
    )
    observations = observations.withColumn("obsdate", to_date(observations.obsdate))
    observations_cached = observations.cache()

    # tnmp
    observations_tnmp = observations_cached.where(
        (observations_cached.obscodingcode == "21902-2")
    )
    df_observations_tnmp = transform_tnmp(observations_tnmp)

    # histology
    observations_histology = observations_cached.where(
        (observations_cached.obscodingcode == "59847-4")
    )
    df_observations_histology = transform_histology(observations_histology)

    return df_observations_tnmp, df_observations_histology


def group_df(joined_dataframe):
    joined_dataframe_grouped = joined_dataframe.groupBy("cond_id").agg(
        first("pat_id").alias("pat_id"),
        first("gender_mapped").alias("gender_mapped"),
        first("conditiondate").alias("conditiondate"),
        max("condcodingcode").alias("condcodingcode"),
        first("condcodingcode_mapped").alias("condcodingcode_mapped"),
        first("deceased_year").alias("deceased_year"),
        first("age_in_years").alias("age_in_years"),
        max("tnmp_obsid").alias("tnmp_obsid"),
        max("tnmp_obsvalue_UICC").alias("tnmp_obsvalue_UICC"),
        max("tnmp_UICC_mapped").alias("tnmp_UICC_mapped"),
        max("tnmp_obsdate").alias("tnmp_obsdate"),
        first("evidencereference").alias("evidencereference"),
        max("obsdate_hist").alias("obsdate_hist"),
        first("obs_value_hist_mapped_0_4").alias("obs_value_hist_mapped_0_4"),
        first("obs_value_hist_mapped_5").alias("obs_value_hist_mapped_5"),
        first("obs_id_hist").alias("obs_id_hist"),
    )

    joined_dataframe_grouped_repartitioned = joined_dataframe_grouped.repartition(
        20, col("pat_id")
    )
    # fix order
    joined_dataframe_grouped_repartitioned = (
        joined_dataframe_grouped_repartitioned.select(
            "pat_id",
            "cond_id",
            "gender_mapped",
            "conditiondate",
            "condcodingcode",
            "condcodingcode_mapped",
            "age_in_years",
            "deceased_year",
            "tnmp_obsvalue_UICC",
            "tnmp_UICC_mapped",
            "tnmp_obsdate",
            "obs_value_hist_mapped_0_4",
            "obs_value_hist_mapped_5",
            "obsdate_hist",
        )
    )
    return joined_dataframe_grouped_repartitioned


def save_final_df(final_df):
    final_df_pandas = final_df.toPandas()
    final_df_pandas = final_df_pandas.rename_axis("ID")  # required for opal import
    output_path_filename = (
        settings.output_folder
        + "/bzkfsummerschool"
        + settings.kafka_topic_year_suffix
        + ".csv"
    )
    print("###### OUTPUTFolder settings.output_folder: ", settings.output_folder)
    print("###### OUTPUTFILE: ", output_path_filename)
    print("###### current dir: ", os.getcwd())
    final_df_pandas.to_csv(output_path_filename)


def main():
    start = time.monotonic()

    kafka_topics = create_list_of_kafka_topics()
    print("kafka_topics: ", kafka_topics)

    spark = setup_spark_session(settings.spark_app_name, settings.master)
    ptl = PathlingContext.create(spark=spark, enable_extensions=True)

    read_data_from_kafka_save_delta(spark, kafka_topics)

    df_bundles = spark.read.format("delta").load(
        settings.output_folder + "/bundles-delta"
    )

    df_patients = encode_patients(ptl, df_bundles)
    df_conditions = encode_conditions(ptl, df_bundles)

    df_pat_cond_joined = join_dataframes(
        df_patients, "pat_id", df_conditions, "subjectreference"
    )

    df_observations_tnmp, df_observations_histology = encode_observations(
        ptl, df_bundles
    )

    df_pat_cond_obstnmp_joined = join_dataframes(
        df_pat_cond_joined, "pat_id", df_observations_tnmp, "tnm_obssubjreference"
    )

    df_pat_cond_obstnmp_obshist_joined = join_dataframes(
        df_pat_cond_obstnmp_joined,
        "pat_id",
        df_observations_histology,
        "obssubjreference",
    )

    df_pat_cond_obstnmp_obshist_joined_grouped = group_df(
        df_pat_cond_obstnmp_obshist_joined
    )

    save_final_df(df_pat_cond_obstnmp_obshist_joined_grouped)

    shutil.rmtree(settings.output_folder + "/bundles-delta")
    print("########## DELETED bundles-delta folder and files")

    # df_pat_cond_obstnmp_obshist_joined_grouped.show()

    end = time.monotonic()
    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
