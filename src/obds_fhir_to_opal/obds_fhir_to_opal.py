import datetime
import math
import os
import shutil
import time

import pyspark
from pathling import PathlingContext
from pathling.etc import find_jar
from pydantic import BaseSettings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, first, regexp_replace, to_date, udf
from pyspark.sql.types import StringType


class Settings(BaseSettings):
    output_folder: str = "~/opal-output"
    output_filename: str = "bzkf_q4_22.csv"
    kafka_topic_year_suffix: str = ".2022"
    kafka_patient_topic: str = "fhir.obds.Patient"
    kafka_condition_topic: str = "fhir.obds.Condition"
    kafka_observation_topic: str = "fhir.obds.Observation"
    kafka_procedure_topic: str = "fhir.obds.Procedure"
    kafka_medicationstatement_topic: str = "fhir.obds.MedicationStatement"
    # ⚠️ make sure these are consistent with the ones downloaded inside the Dockerfile
    jar_list: list = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
        "au.csiro.pathling:library-api:6.2.1",
        "ch.cern.sparkmeasure:spark-measure_2.13:0.21",
        "io.delta:delta-core_2.12:2.3.0",
    ]
    spark_app_name: str = "oBDS-FHIR-to-Opal"
    master: str = "local[*]"
    kafka_bootstrap_server: str = "kafka:9092"

    spark_worker_memory: str = "10g"
    spark_executor_memory: str = "8g"
    spark_driver_memory: str = "8g"
    spark_executor_cores: str = "4"

    spark_jars_ivy: str = "/home/spark/.ivy2"


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
    bundle_folder = os.path.join(settings.output_folder, "bundles-delta")
    kafka_data.write.format("delta").mode("overwrite").save(bundle_folder)


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


def calculate_age_at_conditiondate(birthdate, conditiondate):
    if conditiondate is None:
        age_at_conditiondate = birthdate
    else:
        age_at_conditiondate = conditiondate - birthdate
    days_in_year = 365.2425
    age_at_conditiondate = int(age_at_conditiondate.days / days_in_year)
    return age_at_conditiondate


# 0-14, 15-19, 20-24, 25-29, 30-34, 35-39, 40-44, 45-49, 50-54, 55-59...80-84, 85+
def diagnosis_age_group_small(age_at_diagnosis):
    age_ranges = [
        14,
        19,
        24,
        29,
        34,
        39,
        44,
        49,
        54,
        59,
        64,
        69,
        74,
        79,
        84,
        math.inf,
    ]

    for index, upper_limit in enumerate(age_ranges):
        if age_at_diagnosis <= upper_limit:
            return index


# 0-10, 11-20, 21-30... 71-80, 81-90, 90+
def diagnosis_age_group_large(age_at_diagnosis):
    age_ranges = [
        10,
        20,
        30,
        40,
        50,
        60,
        70,
        80,
        90,
        math.inf,
    ]

    for index, upper_limit in enumerate(age_ranges):
        if age_at_diagnosis <= upper_limit:
            return index


def add_age_at_condition_and_groups(df_pat_cond_joined):
    calculate_age_at_conditiondateUDF = udf(
        lambda x, y: calculate_age_at_conditiondate(x, y), StringType()
    )
    df_pat_cond_joined = df_pat_cond_joined.withColumn(
        "age_at_diagnosis",
        calculate_age_at_conditiondateUDF(
            to_date(df_pat_cond_joined.birthDate), df_pat_cond_joined.conditiondate
        ),
    )

    diagnosis_age_group_small_UDF = udf(
        lambda x: diagnosis_age_group_small(x), StringType()
    )
    df_pat_cond_joined = df_pat_cond_joined.withColumn(
        "age_group_small",
        diagnosis_age_group_small_UDF(df_pat_cond_joined.age_at_diagnosis),
    )

    diagnosis_age_group_large_UDF = udf(
        lambda x: diagnosis_age_group_large(x), StringType()
    )
    df_pat_cond_joined = df_pat_cond_joined.withColumn(
        "age_group_large",
        diagnosis_age_group_large_UDF(df_pat_cond_joined.age_at_diagnosis),
    )
    return df_pat_cond_joined


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
        "EXPLODE_OUTER(identifier.value) as pat_id", "gender", "birthDate",
        "deceasedBoolean", "deceasedDateTime"
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


def group_entities(condcodingcode_mapped):
    condcodingcode_mapped = float(condcodingcode_mapped)
    ranges = [
        (300, 315, 0),  # Lippe, Mundhöhle und Rachen (C00-C14)
        (315, 316, 1),  # Speiseröhre (C15)
        (316, 317, 2),  # Magen (C16)
        (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        (322, 323, 4),  # Leber (C22)
        (323, 325, 5),  # Gallenblase und Gallenwege (C23-C24)
        (325, 326, 6),  # Bauchspeicheldrüse (C25)
        (332, 333, 7),  # Kehlkopf (C32)
        (333, 335, 8),  # Trachea, Bronchien und Lunge (C33-C34)
        (343, 344, 9),  # Malignes Melanom der Haut (C43)
        (350, 351, 10),  # Brust (C50, D05)
        (405, 406, 10),
        (353, 354, 11),  # Gebärmutterhals (C53, D06)
        (406, 407, 11),
        (354, 356, 12),  # Gebärmutterkörper (C54-C55)
        (356, 357, 13),  # Eierstöcke (C56, D39.1)
        (439.1, 439.2, 13),
        (361, 362, 14),  # Prostata (C61)
        (362, 363, 15),  # Hoden (C62)
        (364, 365, 16),  # Niere (C64)
        (367, 368, 17),  # Harnblase (C67, D09.0, D41.4)
        (409.0, 409.1, 17),
        (441.4, 441.5, 17),
        (370, 373, 18),  # Gehirn und zentrales Nervensystem (C70-C72)
        (373, 374, 19),  # Schilddrüse (C73)
        (381, 382, 20),  # Morbus Hodgkin (C81)
        (382, 389, 21),  # Non-Hodgkin-Lymphome (C82-C88, C96)
        (396, 397, 21),
        (390, 391, 22),  # Plasmozytom (C90)
        (391, 396, 23),  # Leukämien (C91-C95)
    ]

    for start, end, group in ranges:
        if start <= condcodingcode_mapped < end:
            return group

    return 100


def encode_conditions(ptl: PathlingContext, df_bundles):
    lookup_condcodingcode_mappingUDF = udf(
        lambda x: lookup_condcodingcode_mapping(x), StringType()
    )
    group_entities_UDF = udf(lambda x: group_entities(x), StringType())
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
    conditions = conditions.withColumn(
        "entity_group",
        group_entities_UDF(conditions.condcodingcode_mapped),
    )
    conditions = conditions.select(
        "cond_id",
        "conditiondate",
        "subjectreference",
        "condcodingcode",
        "condcodingcode_mapped",
        "entity_group",
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
        "IA3": "7",
        "IB": "8",
        "IB1": "9",
        "IB2": "10",
        "IC": "11",
        "IS": "12",
        "II": "13",
        "IIA": "14",
        "IIA1": "15",
        "IIA2": "16",
        "IIB": "17",
        "IIC": "18",
        "III": "19",
        "IIIA": "20",
        "IIIB": "21",
        "IIIC": "22",
        "IIIC1": "23",
        "IIIC2": "24",
        "IV": "25",
        "IVA": "26",
        "IVB": "27",
        "IVC": "28",
    }
    if obs_value_codingcode_tnm_UICC in obs_value_codingcode_tnm_uicc_mapping_dict:
        return obs_value_codingcode_tnm_uicc_mapping_dict[obs_value_codingcode_tnm_UICC]
    else:
        return obs_value_codingcode_tnm_UICC


def lookup_grouped_uicc(obs_value_codingcode_tnm_UICC):
    grouped_uicc_dict = {
        "0": "0",
        "0a": "0",
        "0is": "0",
        "I": "1",
        "IA": "1",
        "IA1": "1",
        "IA2": "1",
        "IA3": "1",
        "IB": "1",
        "IB1": "1",
        "IB2": "1",
        "IC": "1",
        "IS": "1",
        "II": "2",
        "IIA": "2",
        "IIA1": "2",
        "IIA2": "2",
        "IIB": "2",
        "IIC": "2",
        "III": "3",
        "IIIA": "3",
        "IIIB": "3",
        "IIIC": "3",
        "IIIC1": "3",
        "IIIC2": "3",
        "IV": "4",
        "IVA": "4",
        "IVB": "4",
        "IVC": "4",
    }
    if obs_value_codingcode_tnm_UICC in grouped_uicc_dict:
        return grouped_uicc_dict[obs_value_codingcode_tnm_UICC]
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
    lookup_lookup_grouped_uiccUDF = udf(lambda x: lookup_grouped_uicc(x), StringType())
    observations_tnmp = observations_tnmp.withColumn(
        "tnmp_UICC_grouped",
        lookup_lookup_grouped_uiccUDF(observations_tnmp.obsvaluecode),
    )
    observations_tnmp = observations_tnmp.selectExpr(
        "obssubjreference as tnm_obssubjreference",
        "obsid as tnmp_obsid",
        "obscodingcode as tnmp_codingcode",
        "obscodingdisplay as tnmp_obscodingdisplay",
        "obsvaluecode as tnmp_obsvalue_UICC",
        "obsdate as tnmp_obsdate",
        "tnmp_UICC_mapped",
        "tnmp_UICC_grouped",
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
        first("condcodingcode").alias("condcodingcode"),
        first("condcodingcode_mapped").alias("condcodingcode_mapped"),
        first("entity_group").alias("entity_group"),
        first("deceased_year").alias("deceased_year"),
        first("age_in_years").alias("age_in_years"),
        first("age_at_diagnosis").alias("age_at_diagnosis"),
        first("age_group_small").alias("age_group_small"),
        first("age_group_large").alias("age_group_large"),
        first("tnmp_obsid").alias("tnmp_obsid"),
        first("tnmp_obsvalue_UICC").alias("tnmp_obsvalue_UICC"),
        first("tnmp_UICC_mapped").alias("tnmp_UICC_mapped"),
        first("tnmp_UICC_grouped").alias("tnmp_UICC_grouped"),
        first("tnmp_obsdate").alias("tnmp_obsdate"),
        first("evidencereference").alias("evidencereference"),
        first("obsdate_hist").alias("obsdate_hist"),
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
            "entity_group",
            "age_in_years",
            "age_at_diagnosis",
            "age_group_small",
            "age_group_large",
            "deceased_year",
            "tnmp_obsvalue_UICC",
            "tnmp_UICC_mapped",
            "tnmp_UICC_grouped",
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

    output_path_filename = os.path.join(
        settings.output_folder, settings.output_filename
    )
    print("###### current dir: ", os.getcwd())
    print("###### output_path_filename : ", output_path_filename)

    final_df_pandas.to_csv(output_path_filename)


def main():
    start = time.monotonic()

    kafka_topics = create_list_of_kafka_topics()
    print("kafka_topics: ", kafka_topics)

    spark = setup_spark_session(settings.spark_app_name, settings.master)
    ptl = PathlingContext.create(spark=spark, enable_extensions=True)

    read_data_from_kafka_save_delta(spark, kafka_topics)

    df_bundles = spark.read.format("delta").load(
        os.path.join(settings.output_folder, "bundles-delta")
    )

    df_patients = encode_patients(ptl, df_bundles)
    df_conditions = encode_conditions(ptl, df_bundles)

    df_pat_cond_joined = join_dataframes(
        df_patients, "pat_id", df_conditions, "subjectreference"
    )
    df_pat_cond_joined = add_age_at_condition_and_groups(df_pat_cond_joined)

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

    shutil.rmtree(os.path.join(settings.output_folder, "bundles-delta"))
    print("###### DELETED bundles-delta folder and files")

    # df_pat_cond_obstnmp_obshist_joined_grouped.show()

    end = time.monotonic()
    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
