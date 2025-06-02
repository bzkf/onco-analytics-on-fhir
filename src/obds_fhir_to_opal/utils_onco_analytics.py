import glob
import os
import re
import shutil
import sys
from datetime import datetime
from io import StringIO
from typing import List, Optional

import pandas as pd
from loguru import logger
from pathling import Expression as exp
from pathling import PathlingContext, datasource
from pydantic import BaseSettings
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs,
    col,
    dayofmonth,
    first,
    greatest,
    lower,
    month,
    regexp_extract,
    regexp_replace,
    row_number,
    udf,
    when,
    year,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window


def map_icd10(icd10_code):
    letter_to_number = {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 5,
        "F": 6,
        "G": 7,
        "H": 8,
        "I": 9,
        "J": 10,
        "K": 11,
        "L": 12,
        "M": 13,
        "N": 14,
        "O": 15,
        "P": 16,
        "Q": 17,
        "R": 18,
        "S": 19,
        "T": 20,
        "U": 21,
        "V": 22,
        "W": 23,
        "X": 24,
        "Y": 25,
        "Z": 26,
    }

    if icd10_code is None:
        return None

    icd10_code = str(icd10_code).strip().upper()

    if not icd10_code:
        return None

    first_letter = icd10_code[0]

    if not first_letter.isalpha() or len(icd10_code) < 3:
        return -1

    if first_letter in letter_to_number:
        icd10_code = icd10_code.replace(
            first_letter, str(letter_to_number[first_letter]), 1
        )

    try:
        return float(icd10_code)
    except ValueError:
        return -1


def group_icd_groups(icd10_code_mapped, start_range=0, end_range=2700):

    if icd10_code_mapped is None:
        return None

    # Check for valid ICD-10 pattern like "101.1" or "101.11"
    # or "2605.2" with Z05.3 e.g.
    if not re.match(r"^\d{3,4}\.\d{1,2}$", str(icd10_code_mapped)):
        return -1  # invalid pattern

    try:
        icd10_code_mapped = int(str(icd10_code_mapped).split(".")[0])
    except (ValueError, TypeError):
        return -1  # invalid

    if start_range <= icd10_code_mapped <= end_range:
        return icd10_code_mapped - start_range
    else:
        return -1  # invalid


# do not use this before DataSHIELD for easier validation of results - map later or map
# in addition to icd_grouped
def group_entities(icd10_code_mapped):
    if icd10_code_mapped is None:
        return None
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
        if start <= icd10_code_mapped < end:
            return group

    return -100


def group_to_icd_chapter(icd10_mapped):
    if icd10_mapped is None:
        return None

    if isinstance(icd10_mapped, float):
        icd10_mapped = int(icd10_mapped)

    ICD10_CHAPTERS = [
        # Chapter I - A00-B99
        ((100, 299), 0),
        # Chapter II - C00-D48
        ((300, 448), 1),
        # Chapter III - D50-D90
        ((400, 490), 2),
        # Chapter IV - E00-E90
        ((500, 590), 3),
        # Chapter V - F00-F99
        ((600, 699), 4),
        # Chapter VI - G00-G99
        ((700, 799), 5),
        # Chapter VII - H00-H59
        ((800, 859), 6),
        # Chapter VIII - H60-H95
        ((860, 895), 7),
        # Chapter IX - I00-I99
        ((900, 999), 8),
        # Chapter X - J00-J99
        ((1000, 1099), 9),
        # Chapter XI - K00-K93
        ((1100, 1193), 10),
        # Chapter XII - L00-L99
        ((1200, 1299), 11),
        # Chapter XIII - M00-M99
        ((1300, 1399), 12),
        # Chapter XIV - N00-N99
        ((1400, 1499), 13),
        # Chapter XV - O00-O99
        ((1500, 1599), 14),
        # Chapter XVI - P00-P96
        ((1600, 1696), 15),
        # Chapter XVII - Q00-Q99
        ((1700, 1799), 16),
        # Chapter XVIII - R00-R99
        ((1800, 1899), 17),
        # Chapter XIX - S00-T98
        ((1900, 2098), 18),
        # Chapter XX - V01-Y84
        ((2201, 2584), 19),
        # Chapter XXI - Z00-Z99
        ((2600, 2699), 20),
        # Chapter XXII - U00-U99
        ((2100, 2199), 21),
    ]

    for (start, end), chapter_number in ICD10_CHAPTERS:
        if start <= icd10_mapped <= end:
            return chapter_number

    return -1


def map_gender(df: DataFrame, column_name: str = "gender") -> DataFrame:
    return df.withColumn(
        "gender_mapped",
        when(lower(col(column_name)).isNull() | (lower(col(column_name)) == ""), 0)
        .when(lower(col(column_name)) == "female", 1)
        .when(lower(col(column_name)) == "male", 2)
        .when(lower(col(column_name)) == "weiblich", 1)
        .when(lower(col(column_name)) == "männlich", 2)
        .otherwise(3),  # Other / divers
    )


def deconstruct_date(df: DataFrame, date_col: str):
    # Extract year, month, and day from date_col
    year_col = f"{date_col}_year"
    month_col = f"{date_col}_month"
    day_col = f"{date_col}_day"

    df = df.withColumn(year_col, year(date_col))
    df = df.withColumn(month_col, month(date_col))
    df = df.withColumn(day_col, dayofmonth(date_col))

    return df


def deconstruct_date_year_only(df, date_col):
    year_col = f"{date_col}_year"

    df = df.withColumn(year_col, year(date_col))

    return df


def return_year(deceasedDateTime):
    if deceasedDateTime is not None:
        year = deceasedDateTime[0:4]
        return year
    else:
        return deceasedDateTime


def calculate_age_at_condition_date(birthdate, condition_date):
    if condition_date is None or birthdate is None:
        return None

    try:
        # to do: check for pseudonymized birthdate
        birthdate_dt = datetime.strptime(birthdate, "%Y-%m")
        condition_date_dt = datetime.strptime(condition_date, "%Y-%m-%d")
    except ValueError:
        return None

    age_at_condition_date = condition_date_dt - birthdate_dt

    days_in_year = 365.2425
    age_in_years = age_at_condition_date.days / days_in_year

    return int(age_in_years)


# 0-14, 15-19, 20-24, 25-29, 30-34, 35-39, 40-44, 45-49, 50-54, 55-59...80-84, 85+
def add_age_group_small(df, age_col):
    return df.withColumn(
        "age_group_small",
        when(df[age_col] <= 14, 0)
        .when(df[age_col] <= 19, 1)
        .when(df[age_col] <= 24, 2)
        .when(df[age_col] <= 29, 3)
        .when(df[age_col] <= 34, 4)
        .when(df[age_col] <= 39, 5)
        .when(df[age_col] <= 44, 6)
        .when(df[age_col] <= 49, 7)
        .when(df[age_col] <= 54, 8)
        .when(df[age_col] <= 59, 9)
        .when(df[age_col] <= 64, 10)
        .when(df[age_col] <= 69, 11)
        .when(df[age_col] <= 74, 12)
        .when(df[age_col] <= 79, 13)
        .when(df[age_col] <= 84, 14)
        .otherwise(15),  # For 85+
    )

    # 0-10, 11-20, 21-30... 71-80, 81-90, 90+


def add_age_group_large(df, age_col):
    return df.withColumn(
        "age_group_large",
        when(df[age_col] <= 10, 0)
        .when(df[age_col] <= 20, 1)
        .when(df[age_col] <= 30, 2)
        .when(df[age_col] <= 40, 3)
        .when(df[age_col] <= 50, 4)
        .when(df[age_col] <= 60, 5)
        .when(df[age_col] <= 70, 6)
        .when(df[age_col] <= 80, 7)
        .when(df[age_col] <= 90, 8)
        .otherwise(9),  # For 90+
    )


def map_tnm_column(df: DataFrame, col_name: str) -> DataFrame:
    if col_name.endswith("T"):
        mapped_col = (
            when(col(col_name).isNull(), None)
            .when(
                col(col_name).rlike("^[0-4]"),
                regexp_extract(col(col_name), "^([0-4])", 1),
            )
            .when(col(col_name).rlike("^X"), "5")
            .when(col(col_name).rlike("^a"), "6")
            .when(col(col_name).rlike("^is"), "7")
            .otherwise("-1")
        )
    elif col_name.endswith("N"):
        mapped_col = (
            when(col(col_name).isNull(), None)
            .when(
                col(col_name).rlike("^[0-3]"),
                regexp_extract(col(col_name), "^([0-3])", 1),
            )
            .when(col(col_name).rlike("^X"), "5")
            .otherwise("-1")
        )
    elif col_name.endswith("M"):
        mapped_col = (
            when(col(col_name).isNull(), None)
            .when(
                col(col_name).rlike("^[0-1]"),
                regexp_extract(col(col_name), "^([0-1])", 1),
            )
            .otherwise("-1")
        )
    else:
        raise ValueError(f"Unexpected TNM column: {col_name}")

    df = df.withColumn(f"{col_name}_mapped", mapped_col.cast(IntegerType()))

    return df


# to do: think about removing more UDFs like this
def map_uicc_column(df: DataFrame, col_name: str) -> DataFrame:
    df = df.withColumn(
        f"{col_name}_mapped",
        when(col(col_name).startswith("0"), "0")
        .when(
            col(col_name).startswith("I")
            & ~col(col_name).startswith("II")
            & ~col(col_name).startswith("III")
            & ~col(col_name).startswith("IV")
            & ~col(col_name).startswith("IS"),
            "1",
        )
        .when(col(col_name).startswith("II") & ~col(col_name).startswith("III"), "2")
        .when(col(col_name).startswith("III"), "3")
        .when(col(col_name).startswith("IV"), "4")
        .when(col(col_name).startswith("IS"), "5")
        .otherwise(col(col_name))
        .cast(IntegerType()),
    )

    return df


def map_death_cause_tumor(
    df: DataFrame, column_name: str = "death_cause_tumor"
) -> DataFrame:
    return df.withColumn(
        "death_cause_tumor_mapped",
        when(col(column_name) == "N", 0)
        .when(col(column_name) == "J", 1)
        .when(col(column_name) == "U", 2)
        .when(col(column_name).isNull(), None)
        .otherwise(-1),
    )


# UDFs
map_icd10_udf = udf(lambda x: map_icd10(x), StringType())
group_icd_groups_udf = udf(lambda x: group_icd_groups(x), StringType())
group_entities_udf = udf(lambda x: group_entities(x), StringType())
calculate_age_at_condition_date_udf = udf(
    lambda x, y: calculate_age_at_condition_date(x, y), StringType()
)
map_tnm_column_udf = udf(map_tnm_column, IntegerType())
group_to_icd_chapter_udf = udf(group_to_icd_chapter, IntegerType())


def find_closest_to_diagnosis(df, date_diagnosis_col, other_date_col):
    df_filtered = df.filter(col(other_date_col).isNotNull())

    window = Window.partitionBy("condition_id").orderBy(
        abs(
            col(other_date_col).cast("timestamp")
            - col(date_diagnosis_col).cast("timestamp")
        )
    )

    return (
        df_filtered.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumnRenamed(other_date_col, other_date_col + "_first")
    )


def log_df_show(df, logger, n=20, truncate=False):
    buffer = StringIO()
    sys_stdout = sys.stdout  # Backup original stdout
    sys.stdout = buffer  # Redirect stdout to buffer
    try:
        df.show(n=n, truncate=truncate)
    finally:
        sys.stdout = sys_stdout  # Restore stdout
    logger.info("DataFrame preview:\n" + buffer.getvalue())


# EXTRACT FUNCTIONS


def extract_df_PoC(pc: PathlingContext, data: datasource.DataSource):
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
        map_icd10_udf(df.icd10_code).cast(DoubleType()),
    )
    # group to parent code
    df = df.withColumn(
        "icd10_grouped",
        group_icd_groups_udf(df.icd10_mapped).cast(IntegerType()),
    )
    # groupd icd10 to entities, inspired from "Jahresbericht 2023 des Bayerischen
    # Krebsregisters - Krebs in Bayern in den Jahren 2015 bis 2019 - Band 5"
    df = df.withColumn(
        "icd10_entity",
        group_entities_udf(df.icd10_grouped).cast(IntegerType()),
    )
    # map gender to numeric value for DataSHIELD, None = 0, "female" = 1, "male" = 2,
    # "other/diverse" = 3
    df = map_gender(df, "gender")

    df = df.select(
        [
            "condition_id",
            "icd10_code",
            "icd10_mapped",
            "icd10_grouped",
            "icd10_entity",
            "date_diagnosis",
            "date_diagnosis_year",
            "date_diagnosis_month",
            "date_diagnosis_day",
            "gender",
            "gender_mapped",
        ]
    )

    df = df.dropDuplicates()

    return df


# changes: directly checkpoint after extract, remove resolves completely, split A into sub dfs
# A0 + A1 + A3 + A7
def extract_df_study_protocol_a0_1_3_7(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: BaseSettings,
    spark: SparkSession,
):
    logger.info("logger start, extract patients.")
    # patients
    patients = data.extract(
        "Patient",
        columns=[
            exp("id", "patient_resource_id"),
            exp("identifier.value", "patid_pseudonym"),
            exp("birthDate", "birthdate"),
            exp("gender", "gender"),
            exp("deceasedDateTime", "deceased_datetime"),
            exp("deceasedBoolean", "deceased_boolean"),
        ],
    )

    patients = patients.checkpoint(eager=True)
    patients.count()  # enforce checkpoint

    save_final_df(patients, settings, suffix="patients")
    logger.info("saved patients.")

    conditions = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code",
                "icd10_code",
            ),
            exp("subject.reference", "cond_subject_reference"),
        ],
    )
    # conditions
    conditions = conditions.checkpoint(eager=True)
    conditions_count = conditions.count()  # enforce checkpoint

    logger.info("conditions_count = {}", conditions_count)

    # remove the "Patient/" from cond_subject_reference before the join
    conditions = conditions.withColumn(
        "cond_subject_reference",
        regexp_replace("cond_subject_reference", "^Patient/", ""),
    )
    conditions = conditions.checkpoint(eager=True)
    conditions.count()  # enforce checkpoint

    logger.info("remove the Patient/ from cond_subject_reference before the join")

    # join patients + conditions
    conditions_patients = (
        conditions.alias("c")
        .join(
            patients.alias("p"),  # Alias for patients DataFrame
            # Join condition
            conditions["cond_subject_reference"] == patients["patient_resource_id"],
            "left",  # Left join
        )
        .select("c.*", "p.*")
    )

    conditions_patients = conditions_patients.checkpoint(eager=True)
    conditions_patients_count = conditions_patients.count()

    logger.info("conditions_patients_count = {}", conditions_patients_count)

    save_final_df(conditions_patients, settings, suffix="conditions_patients")
    logger.info("saved conditions_patients.")

    # death observations
    observations_deathcause = data.extract(
        "Observation",
        columns=[
            exp("id", "observation_id"),
            exp(
                "valueCodeableConcept.coding.where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code.first()",
                "death_cause_icd10",
            ),
            exp(
                "valueCodeableConcept.coding.where(system='http://dktk.dkfz.de/fhir/onco/core/CodeSystem/JNUCS').code.first()",
                "death_cause_tumor",
            ),
            exp("effectiveDateTime", "date_death"),
            exp("subject.reference", "subject_reference"),
        ],
        filters=[
            # filter for death cause observations
            "code.coding.where(system = 'http://loinc.org' and code = '68343-3').exists()"
        ],
    )
    observations_deathcause = observations_deathcause.checkpoint(eager=True)
    observations_deathcause_count = observations_deathcause.count()
    logger.info("observations_deathcause_count = {}", observations_deathcause_count)

    # replace patient string for join
    observations_deathcause = observations_deathcause.withColumn(
        "subject_reference", regexp_replace("subject_reference", "^Patient/", "")
    )
    observations_deathcause = observations_deathcause.checkpoint(eager=True)
    observations_deathcause_count = (
        observations_deathcause.count()
    )  # enforce checkpoint
    logger.info("observations_deathcause_count = {}", observations_deathcause_count)

    # count distinct patients - remove duplicates
    observations_deathcause_distinct_patients_count = (
        observations_deathcause.select("subject_reference").distinct().count()
    )
    logger.info(
        "observations_deathcause_distinct_patients_count = {}",
        observations_deathcause_distinct_patients_count,
    )

    # safety groupby to avoid row explosion later
    # groupby patient id
    observations_deathcause = observations_deathcause.groupBy("subject_reference").agg(
        first("observation_id", ignorenulls=True).alias("observation_id"),
        first("death_cause_icd10", ignorenulls=True).alias("death_cause_icd10"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
        first("date_death", ignorenulls=True).alias("date_death"),
    )

    observations_deathcause = observations_deathcause.checkpoint(eager=True)
    observations_deathcause_count = (
        observations_deathcause.count()
    )  # enforce checkpoint
    logger.info(
        "observations_deathcause_count after groupby = {}",
        observations_deathcause_count,
    )

    # join death observations to conditions_patients
    conditions_patients_death = (
        conditions_patients.alias("c")
        .join(
            observations_deathcause.alias("o"),
            conditions_patients["patient_resource_id"]
            == observations_deathcause["subject_reference"],
            "left",
        )
        .select("c.*", "o.*")
    )
    conditions_patients_death = conditions_patients_death.checkpoint(eager=True)
    conditions_patients_death_count = conditions_patients_death.count()
    logger.info("conditions_patients_death_count = {}", conditions_patients_death_count)

    # GLEASON observations
    observations_gleason = data.extract(
        "Observation",
        columns=[
            exp("id", "obs_id_gleason"),
            exp("valueInteger", "gleason"),
            exp("effectiveDateTime", "gleason_date"),
            exp("subject.reference", "obs_subject_reference"),
        ],
        filters=[
            "code.coding.where(system = 'http://loinc.org' and code = '35266-6').exists()",
            "reverseResolve(Condition.stage.assessment).code.coding.where(system = 'http://fhir.de/CodeSystem/bfarm/icd-10-gm' and code = 'C61').exists()",
        ],
    )
    if observations_gleason.isEmpty():
        logger.warning("No GLEASON observations found – skipping join.")
        # conditions_patients_death = conditions_patients_death.drop(
        #    "cond_subject_reference", "patient_resource_id")
        return conditions_patients_death

    observations_gleason = observations_gleason.checkpoint(eager=True)
    observations_gleason_count = observations_gleason.count()
    logger.info("observations_gleason_count = {}", observations_gleason_count)

    # remove the "Patient/" from cond_subject_reference before the join
    observations_gleason = observations_gleason.withColumn(
        "obs_subject_reference",
        regexp_replace("obs_subject_reference", "^Patient/", ""),
    )

    observations_gleason = observations_gleason.checkpoint(eager=True)
    observations_gleason.count()  # enforce checkpoint
    logger.info("remove the Patient/ from obs_subject_reference before the join")

    save_final_df(observations_gleason, settings, suffix="observations_gleason")
    logger.info("saved observations_gleason.")

    # filter conditions_patients C61
    conditions_patients_death_c61 = conditions_patients_death.filter(
        col("icd10_code").startswith("C61")
    )
    conditions_patients_death_c61 = conditions_patients_death_c61.checkpoint(eager=True)
    conditions_patients_death_c61_count = (
        conditions_patients_death_c61.count()
    )  # enforce checkpoint

    logger.info(
        "conditions_patients_death_c61_count = {}", conditions_patients_death_c61_count
    )

    # join gleason obs with c61
    observations_gleason_conditions_patients_death_c61 = (
        conditions_patients_death_c61.alias("c")
        .join(
            observations_gleason.alias("o"),
            conditions_patients_death_c61["cond_subject_reference"]
            == observations_gleason["obs_subject_reference"],
            "left",
        )
        .select(
            "c.condition_id",
            "c.date_diagnosis",
            "c.cond_subject_reference",
            "c.condition_id",
            "o.*",
        )
    )
    observations_gleason_conditions_patients_death_c61 = (
        observations_gleason_conditions_patients_death_c61.checkpoint(eager=True)
    )

    observations_gleason_conditions_patients_death_c61_count = (
        observations_gleason_conditions_patients_death_c61.count()
    )

    logger.info(
        "observations_gleason_conditions_patients_death_c61_count = {}",
        observations_gleason_conditions_patients_death_c61_count,
    )

    # filter double cond_ids for find closest to diagnosis
    duplicates = (
        observations_gleason_conditions_patients_death_c61.groupBy("condition_id")
        .count()
        .filter(col("count") > 1)
        .select("condition_id")
    )

    df_duplicates = observations_gleason_conditions_patients_death_c61.join(
        duplicates, on="condition_id", how="inner"
    ).orderBy("condition_id")
    df_duplicates = df_duplicates.checkpoint(eager=True)
    df_duplicates_count = df_duplicates.count()

    logger.info("df_duplicates_count = {}", df_duplicates_count)

    # filter singular cond_ids so we can later concatenate the singular ones and the duplicates on which we perform find closest
    df_singular = observations_gleason_conditions_patients_death_c61.join(
        duplicates, on="condition_id", how="left_anti"
    ).orderBy("condition_id")

    df_singular = df_singular.checkpoint(eager=True)
    df_singular_count = df_singular.count()

    logger.info("df_singular_count = {}", df_singular_count)

    # filter only one row per condition id from the duplicates where the gleason date is closest to the diagnosis date
    df_duplicates_closest = find_closest_to_diagnosis(
        df=df_duplicates,
        date_diagnosis_col="date_diagnosis",
        other_date_col="gleason_date",
    )
    df_duplicates_closest = df_duplicates_closest.checkpoint(eager=True)
    df_duplicates_closest_count = df_duplicates_closest.count()

    logger.info("df_duplicates_closest_count = {}", df_duplicates_closest_count)

    # reunite the split c61 dfs
    df_singular = df_singular.withColumnRenamed("gleason_date", "gleason_date_first")
    df_reunite = df_singular.unionByName(df_duplicates_closest)

    df_reunite = df_reunite.checkpoint(eager=True)
    df_reunite_count = df_reunite.count()

    logger.info("df_reunite_count = {}", df_reunite_count)

    # and now final join - join conditions_patients_c61_gleason_closest back to condition_patients
    conditions_patients_death_gleason_closest = (
        conditions_patients_death.alias("cp")
        .join(df_reunite.alias("c61"), "condition_id", "left")
        .select("cp.*", "c61.obs_id_gleason", "c61.gleason", "c61.gleason_date_first")
    )
    conditions_patients_death_gleason_closest = (
        conditions_patients_death_gleason_closest.checkpoint(eager=True)
    )
    conditions_patients_death_gleason_closest_count = (
        conditions_patients_death_gleason_closest.count()
    )

    logger.info(
        "conditions_patients_death_gleason_closest_count = {}",
        conditions_patients_death_gleason_closest_count,
    )

    return conditions_patients_death_gleason_closest


def extract_df_study_protocol_c(pc: PathlingContext, data: datasource.DataSource):
    df = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code",
                "icd10_code",
            ),
            exp("subject.reference", "cond_subject_reference"),
            exp("Condition.subject.resolve().ofType(Patient).birthDate", "birthdate"),
            exp("Condition.subject.resolve().ofType(Patient).gender", "gender"),
            exp(
                "Condition.subject.resolve().ofType(Patient).address.postalCode",
                "postal_code",
            ),
            exp(
                "Condition.subject.resolve().ofType(Patient).deceasedDateTime",
                "deceased_datetime",
            ),
            exp(
                "Condition.subject.resolve().ofType(Patient).deceasedBoolean",
                "deceased_boolean",
            ),
        ],
    )
    df = df.checkpoint(eager=True)
    df_count = df.count()

    logger.info("df.count() = {}".format(df_count))
    log_df_show(df, logger)

    return df


def save_final_df(df, settings, suffix=""):
    logger.info("start save df with df.coalesce(1).write...csv() ")
    output_dir = os.path.join(settings.output_folder, settings.study_name)
    os.makedirs(output_dir, exist_ok=True)

    final_csv_path = os.path.join(output_dir, f"df_{suffix}.csv")

    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_dir)

    part_file = glob.glob(os.path.join(output_dir, "part-*.csv"))[0]

    shutil.move(part_file, final_csv_path)
    logger.info("end save df")


def datashield_preps_wrapper(
    df,
    condition: bool = False,
    patient: bool = False,
    death: bool = False,
    gleason: bool = False,
):
    if condition:
        df = deconstruct_date_year_only(df, "date_diagnosis")
        df = df.checkpoint(eager=True)
        df.count()

        df = df.withColumn(
            "icd10_mapped",
            map_icd10_udf(df.icd10_code).cast(DoubleType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("icd10_mapped")

        df = df.withColumn(
            "icd10_grouped",
            group_icd_groups_udf(df.icd10_mapped).cast(IntegerType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("icd10_grouped")

        df = df.withColumn(
            "icd10_entity",
            group_entities_udf(df.icd10_grouped).cast(IntegerType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("icd10_entity")

    if patient:
        df = deconstruct_date_year_only(df, "deceased_datetime")
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("deconstruct_date_year_only - deceased_datetime")

        df = map_gender(df, "gender")
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("map_gender")

        df = df.withColumn(
            "age_at_diagnosis",
            calculate_age_at_condition_date_udf(df.birthdate, df.date_diagnosis).cast(
                IntegerType()
            ),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("calculate_age_at_condition_date_udf")

        df = add_age_group_small(df, "age_at_diagnosis")
        df = add_age_group_large(df, "age_at_diagnosis")
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("age groups")

        columns_to_check = [
            "deceased_datetime",
            "deceased_boolean",
        ]

        df = df.withColumn(
            "dead_bool_mapped_patient",
            greatest(
                *[col(column).isNotNull().cast("int") for column in columns_to_check]
            ),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("dead_bool_mapped_patient")

    if death:
        df = deconstruct_date_year_only(df, "date_death")
        columns_to_check = ["death_cause_icd10", "death_cause_tumor", "date_death"]
        df = df.withColumn(
            "dead_bool_mapped_observation",
            greatest(
                *[col(column).isNotNull().cast("int") for column in columns_to_check]
            ),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("dead_bool_mapped_observation")

        # map death cause tumor
        df = map_death_cause_tumor(df=df, column_name="death_cause_tumor")
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("map_death_cause_tumor")

        df = df.withColumn(
            "death_cause_icd10_mapped",
            map_icd10_udf(df.death_cause_icd10).cast(DoubleType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("death_cause_icd10_mapped")

        df = df.withColumn(
            "death_cause_icd10_grouped",
            group_icd_groups_udf(df.death_cause_icd10_mapped).cast(IntegerType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("death_cause_icd10_grouped")

        df = df.withColumn(
            "death_cause_entity",
            group_entities_udf(df.death_cause_icd10_grouped).cast(IntegerType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("death_cause_entity")

        df = df.withColumn(
            "death_cause_icd10_chapter",
            group_to_icd_chapter_udf(df.death_cause_icd10_mapped).cast(IntegerType()),
        )
        df = df.checkpoint(eager=True)
        df.count()
        logger.info("death_cause_icd10_chapter")

    if gleason:
        pass  # nothing to map for now

    return df


def prepare_data_dictionary(df):
    dtypes_list_df = [str(dtype[1]) for dtype in df.dtypes]

    df_description = pd.read_csv("descriptions_for_data_dictionary.csv")
    description_dict = pd.Series(
        df_description.Description.values, index=df_description.Variable
    ).to_dict()

    return dtypes_list_df, description_dict


# optionally pass lists/dicts to fill the data_dictionary
def generate_data_dictionary(
    file_path: str = "./",
    table_name: str = "",
    colnames_list: List[str] = [],
    value_type_list: List[str] = [],
    entity_type: str = "Participant",
    referenced_entity_type: str = "",
    mime_type: str = "",
    unit: str = "",
    repeatable: int = 0,
    occurence_group: str = "",
    index: int = 1,
    label: str = "",
    description: Optional[dict] = None,
    categories: str = "",
) -> pd.DataFrame:

    if not len(colnames_list) == len(value_type_list):
        raise ValueError("All lists must have the same length as colnames_list")

    columns = [
        "table",
        "name",
        "valueType",
        "entityType",
        "referencedEntityType",
        "mimeType",
        "unit",
        "repeatable",
        "occurrenceGroup",
        "index",
        "label:en",
        "description",
        "categories",
    ]

    updated_value_types = [
        "integer" if dtype == "int" else "decimal" if dtype == "double" else dtype
        for dtype in value_type_list
    ]

    df = pd.DataFrame(columns=columns)
    df["name"] = colnames_list
    df["table"] = table_name
    df["valueType"] = updated_value_types
    df["entityType"] = entity_type
    df["referencedEntityType"] = referenced_entity_type
    df["mimeType"] = mime_type
    df["unit"] = unit
    df["repeatable"] = repeatable
    df["occurrenceGroup"] = occurence_group
    df["index"] = index
    df["label:en"] = label
    df["description"] = (
        [description[col] for col in colnames_list] if description else ""
    )
    df["categories"] = categories

    # leeres Categories-Datenblatt
    df_categories = pd.DataFrame(
        columns=["table", "variable", "name", "code", "missing"]
    )

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # create one dictionary for all tables - tested
    if os.path.exists(file_path):
        # Excel-Datei existiert – alte Daten einlesen
        df_existing = pd.read_excel(file_path, sheet_name="Variables")

        # Duplikate anhand von table + name filtern (kombinierter index aus table + name)
        existing_index = df_existing.set_index(["table", "name"]).index
        current_index = df.set_index(["table", "name"]).index

        # nur neue Zeilen behalten
        df_unique = df[~current_index.isin(existing_index)]

        with pd.ExcelWriter(
            file_path, engine="openpyxl", mode="a", if_sheet_exists="overlay"
        ) as writer:
            ws_vars = writer.book["Variables"]
            startrow_vars = ws_vars.max_row

            if not df_unique.empty:
                df_unique.to_excel(
                    writer,
                    sheet_name="Variables",
                    index=False,
                    header=False,
                    startrow=startrow_vars,
                )

    else:
        # datadictionary existiert noch nicht --> neu erstellen
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Variables")
            df_categories.to_excel(writer, index=False, sheet_name="Categories")

    return df
