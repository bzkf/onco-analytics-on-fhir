import glob
import os
import re
import shutil
import sys
from datetime import datetime
from functools import reduce
from io import StringIO
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from loguru import logger
from matplotlib.figure import Figure
from pathling import Expression as exp
from pathling import PathlingContext, datasource
from pydantic import BaseSettings
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (  # to do: hier auch F verwenden
    abs,
    col,
    count,
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

FHIR_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_SYSTEM_JNU = "http://dktk.dkfz.de/fhir/onco/core/CodeSystem/JNUCS"
FHIR_SYSTEM_LOINC = "http://loinc.org"
FHIR_SYSTEM_IDENTIFIER_TYPE = "http://terminology.hl7.org/CodeSystem/v2-0203"


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
                f"""code.coding
                    .where(system='{FHIR_SYSTEM_ICD10}').code""",
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


# changes: directly checkpoint after extract,
# remove resolves completely, split A into sub dfs
# A0 + A1 + A3 + A7 + Add double patid column for study protocol D
def extract_df_study_protocol_a0_1_3_7_d(
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
                f"code.coding.where(system='{FHIR_SYSTEM_ICD10}').code",
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
                f"""valueCodeableConcept.coding
                        .where(system='{FHIR_SYSTEM_ICD10}').code.first()""",
                "death_cause_icd10",
            ),
            exp(
                f"""valueCodeableConcept.coding
                        .where(system='{FHIR_SYSTEM_JNU}')
                        .code.first()
                """,
                "death_cause_tumor",
            ),
            exp("effectiveDateTime", "date_death"),
            exp("subject.reference", "subject_reference"),
        ],
        filters=[
            # filter for death cause observations
            f"""code.coding
                    .where(system = '{FHIR_SYSTEM_LOINC}' and code = '68343-3')
                    .exists()"""
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

    # add double_patid column for studyprotocol D
    window_spec = Window.partitionBy("patid_pseudonym")

    conditions_patients_death = conditions_patients_death.withColumn(
        "double_patid",
        when(count("patid_pseudonym").over(window_spec) > 1, 1).otherwise(0),
    ).orderBy("patid_pseudonym")

    conditions_patients_death = conditions_patients_death.checkpoint(eager=True)
    conditions_patients_death_count = conditions_patients_death.count()
    logger.info(
        "added double_patid col for study protocol D, \
        conditions_patients_death_count = {}",
        conditions_patients_death_count,
    )

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
            f"""code.coding
                    .where(system = '{FHIR_SYSTEM_LOINC}' and code = '35266-6')
                    .exists()
            """,
        ],
    )
    if observations_gleason.isEmpty():
        logger.warning("No GLEASON observations found - skipping join.")

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

    # filter singular cond_ids so we can later concatenate the singular
    # ones and the duplicates on which we perform find closest
    df_singular = observations_gleason_conditions_patients_death_c61.join(
        duplicates, on="condition_id", how="left_anti"
    ).orderBy("condition_id")

    df_singular = df_singular.checkpoint(eager=True)
    df_singular_count = df_singular.count()

    logger.info("df_singular_count = {}", df_singular_count)

    # filter only one row per condition id from the duplicates where the
    # gleason date is closest to the diagnosis date
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

    # and now final join - join conditions_patients_c61_gleason_closest
    # back to condition_patients
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
                f"code.coding.where(system='{FHIR_SYSTEM_ICD10}').code",
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
                "Condition.subject.resolve().ofType(Patient).address.country", "country"
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
    temp_dir = os.path.join(output_dir, "_tmp_csv")

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_dir)
    part_file = glob.glob(os.path.join(temp_dir, "part-*.csv"))[0]
    shutil.move(part_file, final_csv_path)
    shutil.rmtree(temp_dir)

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

        # Duplikate anhand von table + name filtern
        # (kombinierter index aus table + name)
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


# prostate-ca therapies
def add_months_diff_col(df, therapy_startdate: str, date_diagnosis: str):
    df = df.withColumn(
        "months_diff",
        F.round(
            F.months_between(F.col(therapy_startdate), F.col(date_diagnosis)), 2
        ).cast("double"),
    )

    df = df.checkpoint(eager=True)
    print(f" count = {df.count()}")

    return df


def preprocess_therapy_df(df: DataFrame) -> DataFrame:
    # Transform dates
    df = df.withColumn(
        "therapy_startdate", F.to_date(F.col("therapy_startdate"))
    ).withColumn("date_diagnosis", F.to_date(F.col("date_diagnosis")))

    logger.info(
        "distinct therapy_id count = {}", df.select("therapy_id").distinct().count()
    )
    logger.info(
        "distinct condition_id count = {}", df.select("condition_id").distinct().count()
    )

    # Group one therapy per condition_id per startdate
    df_grouped = df.groupBy("therapy_startdate", "condition_id").agg(
        F.first("therapy_id", ignorenulls=True).alias("therapy_id"),
        F.first("therapy_type", ignorenulls=True).alias("therapy_type"),
        F.first("therapy_intention", ignorenulls=True).alias("therapy_intention"),
        F.first("date_diagnosis", ignorenulls=True).alias("date_diagnosis"),
        F.first("icd10_code", ignorenulls=True).alias("icd10_code"),
    )
    df_grouped = df_grouped.checkpoint(eager=True)
    logger.info("systemtherapies_grouped_count = {}", df_grouped.count())
    logger.info(
        "distinct therapy_id count (grouped) = {}",
        df_grouped.select("therapy_id").distinct().count(),
    )

    # Add months_diff
    df_grouped = df_grouped.withColumn(
        "months_diff",
        F.round(
            F.months_between(F.col("therapy_startdate"), F.col("date_diagnosis")), 2
        ).cast("double"),
    )
    df_grouped = df_grouped.checkpoint(eager=True)

    # Remove negative diffs
    df_final = df_grouped.filter(F.col("months_diff") >= 0)
    df_final = df_final.checkpoint(eager=True)

    logger.info("systemtherapies_final (months_diff >= 0) count = {}", df_final.count())
    logger.info(
        "systemtherapies_final distinct condition_id count = {}",
        df_final.select("condition_id").distinct().count(),
    )

    return df_final


def extract_filter_df_c61(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: BaseSettings,
    spark: SparkSession,
):
    df = extract_df_study_protocol_a0_1_3_7_d(pc, data, settings, spark)
    df_c61 = df.filter(F.col("icd10_code").startswith("C61"))

    df_c61 = df_c61.drop(
        "cond_subject_reference", "subject_reference", "patid_pseudonym"
    )
    df_c61 = df_c61.checkpoint(eager=True)
    logger.info("df_c61_count = {}", df_c61.count())

    return df_c61


def extract_pca_ops(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: BaseSettings,
    spark: SparkSession,
):
    logger.info("extract op.")
    ops = (
        data.extract(
            "Procedure",
            columns=[
                exp("id", "therapy_id"),
                exp("category.coding.code", "therapy_type"),
                exp(
                    """code.coding
                        .where(system='http://fhir.de/CodeSystem/bfarm/ops')
                        .code""",
                    "ops_code",
                ),
                exp("partOf.reference", "partOf"),
                exp("performedDateTime", "therapy_startdate"),
                exp(
                    """extension(
                        'http://dktk.dkfz.de/fhir/StructureDefinition/onco-core-Extension-OPIntention')
                        .valueCodeableConcept.first().coding.code""",
                    "therapy_intention",
                ),  # cardinality 0...1 for the extension of typ intention
                exp(
                    """Procedure.reasonReference.resolve().ofType(Condition).code.
                    coding.where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm')
                    .code""",
                    "icd10_code",
                ),
                exp(
                    "Procedure.reasonReference.resolve().ofType(Condition).id",
                    "condition_id",
                ),
                exp(
                    """Procedure.reasonReference.resolve().ofType(Condition)
                    .onsetDateTime""",
                    "date_diagnosis",
                ),
            ],
        )
        .filter((F.col("therapy_type") == "OP") & F.col("icd10_code").like("C61%"))
        .orderBy(F.col("condition_id"))
    )

    ops = ops.checkpoint(eager=True)
    logger.info("ops_count = {}", ops.count())  # count action for checkpoint
    # cast dates, group one therapy per cond id per start date, add months diff,
    # filter negative months diff
    ops_final = preprocess_therapy_df(ops)

    return ops_final


def extract_pca_sys(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: BaseSettings,
    spark: SparkSession,
):
    # systemtherapies
    logger.info("extract systemtherapies.")
    systemtherapies = (
        data.extract(
            "MedicationStatement",
            columns=[
                exp("id", "therapy_id"),
                exp("category.coding.code", "therapy_type"),
                exp("effectivePeriod.start", "therapy_startdate"),
                exp("medicationCodeableConcept.text", "substance"),
                exp(
                    """extension(
                        'http://dktk.dkfz.de/fhir/StructureDefinition/onco-core-Extension-SYSTIntention')
                        .valueCodeableConcept.first().coding.code""",
                    "therapy_intention",
                ),
                exp(
                    """MedicationStatement.reasonReference.resolve().ofType(Condition)
                    .code.coding.where(system=
                    'http://fhir.de/CodeSystem/bfarm/icd-10-gm').code""",
                    "icd10_code",
                ),
                exp(
                    """MedicationStatement.reasonReference.resolve()
                    .ofType(Condition).id""",
                    "condition_id",
                ),
                exp(
                    """MedicationStatement.reasonReference.resolve()
                    .ofType(Condition).onsetDateTime""",
                    "date_diagnosis",
                ),
            ],
        )
        .filter(F.col("icd10_code").like("C61%"))
        .orderBy("condition_id")
    )
    systemtherapies = systemtherapies.checkpoint(eager=True)
    logger.info(
        "systemtherapies.count() = {}", systemtherapies.count()
    )  # count action for checkpoint

    # cast dates, group one therapy per cond id per start date, add months diff,
    # filter negative months diff
    systemtherapies_final = preprocess_therapy_df(systemtherapies)

    return systemtherapies_final


def extract_pca_st(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: BaseSettings,
    spark: SparkSession,
):
    # radiotherapies
    logger.info("extract radiotherapies.")
    radiotherapies = (
        data.extract(
            "Procedure",
            columns=[
                exp("id", "therapy_id"),
                exp("category.coding.code", "therapy_type"),
                exp("performedPeriod.start", "therapy_startdate"),
                exp(
                    """extension(
                        'http://dktk.dkfz.de/fhir/StructureDefinition/onco-core-
                        Extension-SYSTIntention').valueCodeableConcept.first().coding
                        .code""",
                    "therapy_intention",
                ),  # cardinality 0...1 for the extension of typ intention
                exp(
                    """Procedure.reasonReference.resolve().ofType(Condition).code
                    .coding.where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm')
                    .code""",
                    "icd10_code",
                ),
                exp(
                    "Procedure.reasonReference.resolve().ofType(Condition).id",
                    "condition_id",
                ),
                exp(
                    """Procedure.reasonReference.resolve().ofType(Condition)
                    .onsetDateTime""",
                    "date_diagnosis",
                ),
            ],
        )
        .filter(
            (F.col("therapy_type") == "ST")  # radiotherapy
            & F.col("icd10_code").like("C61%")  # cohort selection
        )
        .orderBy(F.col("condition_id"))
    )
    radiotherapies = radiotherapies.checkpoint(eager=True)
    logger.info(
        "radiotherapies.count() = {}", radiotherapies.count()
    )  # count action for checkpoint

    # cast dates, group one therapy per cond id per start date, add months diff,
    # filter negative months diff
    radiotherapies_final = preprocess_therapy_df(radiotherapies)

    return radiotherapies_final


def union_sort_pivot_join(
    df_c61, ops_final, systemtherapies_final, radiotherapies_final
):
    # union
    radiotherapy_systemtherapy_op_long = radiotherapies_final.unionByName(
        systemtherapies_final
    ).unionByName(ops_final)
    radiotherapy_systemtherapy_op_long.checkpoint(eager=True)
    logger.info(
        "radiotherapy_systemtherapy_op_long.count() = {}",
        radiotherapy_systemtherapy_op_long.count(),
    )  # count action for checkpoint

    # sort therapy sequence by sorting months_diff
    w = Window.partitionBy("condition_id").orderBy("months_diff")

    radiotherapy_systemtherapy_op_long = radiotherapy_systemtherapy_op_long.withColumn(
        "therapy_index", F.row_number().over(w)
    )

    # pivot to wide form and rename dynamically
    pivot_cols = [
        "therapy_type",
        "therapy_id",
        "therapy_intention",
        "therapy_startdate",
        "months_diff",
    ]

    id_cols = ["condition_id"]
    pivoted_dfs = []

    # maximalen therapy_index-Wert ermitteln
    max_index = radiotherapy_systemtherapy_op_long.agg(
        F.max("therapy_index")
    ).collect()[0][0]

    # Abbruch, falls keine Therapiedaten vorhanden
    if max_index is None:
        raise ValueError(
            "Abbruch: Keine Daten in 'radiotherapy_systemtherapy_op_long' gefunden. "
            "Die Auswertungen können ohne Therapiedaten nicht durchgeführt werden."
        )

    for col_name in pivot_cols:
        df_pivot = (
            radiotherapy_systemtherapy_op_long.select(
                id_cols + ["therapy_index", col_name]
            )
            .groupBy(id_cols)
            .pivot("therapy_index")
            .agg(F.first(col_name))
        )

        # dynamisch Spalten umbenennen
        for i in range(1, max_index + 1):
            old_col = str(i)
            new_col = f"{col_name}_{i}"
            if old_col in df_pivot.columns:
                df_pivot = df_pivot.withColumnRenamed(old_col, new_col)

        pivoted_dfs.append(df_pivot)

    radiotherapy_systemtherapy_op_wide = reduce(
        lambda a, b: a.join(b, on=id_cols, how="left"), pivoted_dfs
    )
    radiotherapy_systemtherapy_op_wide.checkpoint(eager=True)
    logger.info(
        "radiotherapy_systemtherapy_op_long.count() = {}",
        radiotherapy_systemtherapy_op_wide.count(),
    )

    if "gleason" not in df_c61.columns:
        raise ValueError(
            "Abbruch: Die Spalte 'gleason' ist in df_c61 nicht vorhanden. "
            "Die Auswertungen können ohne Gleason-Werte nicht durchgeführt werden."
        )

    # join to df_c61
    df_final = (
        df_c61.alias("df_c61")
        .join(
            radiotherapy_systemtherapy_op_wide.alias("df_wide"),
            F.col("df_c61.condition_id") == F.col("df_wide.condition_id"),
            "left",
        )
        .select(
            "df_wide.*",
            "df_c61.condition_id",
            "df_c61.date_diagnosis",
            "df_c61.icd10_code",
            "df_c61.birthdate",
            "df_c61.deceased_datetime",
            "df_c61.deceased_boolean",
            "df_c61.death_cause_icd10",
            "df_c61.death_cause_tumor",
            "df_c61.date_death",
            "df_c61.gleason",
        )
        .drop(F.col("df_wide.condition_id"))
    )

    df_final = df_final.checkpoint(eager=True)
    logger.info("radiotherapy_systemtherapy_op_long.count() = {}", df_final.count())

    return df_final


def add_deceased_flag(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "deceased",
        (
            F.col("deceased_boolean")
            | F.col("deceased_datetime").isNotNull()
            | F.col("death_cause_icd10").isNotNull()
            | F.col("death_cause_tumor").isNotNull()
            | F.col("date_death").isNotNull()
        ),
    )


def add_age_col(
    df: DataFrame,
    birthdate_col: str,
    refdate_col: str,
    age_colname: str = "age_at_diagnosis",
) -> DataFrame:
    df = df.withColumn(
        age_colname,
        calculate_age_at_condition_date_udf(
            F.col(birthdate_col), F.col(refdate_col)
        ).cast(IntegerType()),
    )

    df = df.checkpoint(eager=True)
    logger.info("add_age_col df.count() = {}", df.count())

    return df


def filter_young_highrisk_cohort(
    df: DataFrame, age_col: str = "age_at_diagnosis", gleason_col: str = "gleason"
) -> DataFrame:
    cohort = df.filter((F.col(age_col) < 65) & (F.col(gleason_col) >= 8))

    cohort = cohort.checkpoint(eager=True)
    logger.info("Filtered cohort count = {}", cohort.count())

    return cohort


def clean_df(df: DataFrame) -> DataFrame:
    logger.info("df count before cleaning = {}", df.count())

    # 1. Age >= 0
    df_clean = df.filter(F.col("age_at_diagnosis") >= 0)
    logger.info(
        "df_clean count after df.filter(F.col(age_at_diagnosis) >= 0 = {}",
        df_clean.count(),
    )

    # 2. Alle months_diff_* Spalten prüfen
    months_cols = [c for c in df.columns if c.startswith("months_diff_")]
    for column in months_cols:
        df_clean = df_clean.filter((F.col(column) >= 0) | F.col(column).isNull())
    logger.info(
        "df_clean count after filtering all monts_diff_* cols >= 0 = {}",
        df_clean.count(),
    )

    df_clean = df_clean.filter(F.year("date_diagnosis") >= 1950)
    logger.info(
        "df_clean count after df.filter(F.col(date_diagnosis) >= 1950 = {}",
        df_clean.count(),
    )

    return df_clean


# Sankey Stuff - to be optimized


def count_therapy_combinations(
    df: DataFrame,
    therapy1: Optional[str] = None,
    therapy2: Optional[str] = None,
    deceased: str = "all",
    therapy1_col: str = "therapy_type_1",
    therapy2_col: str = "therapy_type_2",
) -> int:
    # Count number of patients with given therapy combinations and deceased status.
    cond = F.lit(True)

    # therapy1 filter
    if therapy1 is not None:
        col1 = F.col(therapy1_col)
        if therapy1 == "OTHER":
            cond &= col1.isNotNull() & (~col1.isin("ST", "HO", "CH", "OP"))
        elif therapy1 == "no therapy":
            cond &= col1.isNull() | (F.trim(col1) == "")
        else:
            cond &= col1 == therapy1

    # therapy2 filter
    if therapy2 is not None:
        col2 = F.col(therapy2_col)
        if therapy2 == "OTHER":
            cond &= col2.isNotNull() & (~col2.isin("ST", "HO", "CH", "OP"))
        elif therapy2 == "no therapy":
            cond &= col2.isNull() | (F.trim(col2) == "")
        else:
            cond &= col2 == therapy2

    # deceased filter
    if deceased == "true":
        cond &= F.col("deceased") == "true"
    elif deceased == "false":
        cond &= F.col("deceased").isNull()

    return df.filter(cond).count()


def get_sum(values_list, status=None, t1=None, t2=None):
    total = 0
    # unpack tuple in values_list
    for tup_status, tup_t1, tup_t2, value in values_list:
        if (
            (status is None or tup_status == status)
            and (t1 is None or tup_t1 == t1)
            and (t2 is None or tup_t2 == t2)
        ):
            total += value

    return total


def get_therapy_values(
    df: DataFrame,
    therapy_types: List[str],
    level: int,
    therapy1_col: str = "therapy_type_1",
    therapy2_col: str = "therapy_type_2",
) -> List[Tuple[str, Optional[str], Optional[str], int]]:
    """
    get therapy counts for my Sankey diagram levels 1-4,
    Args:
        df: Spark DataFrame
        therapy_types: List of therapy types = NODES in my sankey levels
        level: Sankey level (1, 2, 3, 4)
        therapy1_col: Column name for therapy1 (left node or starting node)
        therapy2_col: Column name for therapy2 (right node or goal node)
    Returns:
        List of tuples (status, therapy1, therapy2, count)
    """
    values_list: List[Tuple[str, Optional[str], Optional[str], int]] = []

    if level == 1:
        # Level 1: only therapy1, no dead/alive split
        for t in therapy_types:
            count = count_therapy_combinations(
                df, therapy1=t, deceased="all", therapy1_col=therapy1_col
            )
            values_list.append(("all", t, None, count))

    elif level == 2:
        for t1 in therapy_types:
            for t2 in therapy_types:
                if t2 == "no therapy":
                    count_dead = count_therapy_combinations(
                        df,
                        therapy1=t1,
                        therapy2=t2,
                        deceased="true",
                        therapy1_col=therapy1_col,
                        therapy2_col=therapy2_col,
                    )
                    count_alive = count_therapy_combinations(
                        df,
                        therapy1=t1,
                        therapy2=t2,
                        deceased="false",
                        therapy1_col=therapy1_col,
                        therapy2_col=therapy2_col,
                    )
                    values_list.append(("dead", t1, t2, count_dead))
                    values_list.append(("alive", t1, t2, count_alive))
                elif t1 == "no therapy":
                    continue
                else:
                    count_all = count_therapy_combinations(
                        df,
                        therapy1=t1,
                        therapy2=t2,
                        deceased="all",
                        therapy1_col=therapy1_col,
                        therapy2_col=therapy2_col,
                    )
                    values_list.append(("all", t1, t2, count_all))

    elif level == 3:
        for t1 in therapy_types:
            for t2 in therapy_types:
                if t1 == "no therapy":
                    continue
                elif t2 == "no therapy":
                    count_dead = count_therapy_combinations(
                        df,
                        therapy1=t1,
                        therapy2=t2,
                        deceased="true",
                        therapy1_col=therapy1_col,
                        therapy2_col=therapy2_col,
                    )
                    count_alive = count_therapy_combinations(
                        df,
                        therapy1=t1,
                        therapy2=t2,
                        deceased="false",
                        therapy1_col=therapy1_col,
                        therapy2_col=therapy2_col,
                    )
                    values_list.append(("dead", t1, t2, count_dead))
                    values_list.append(("alive", t1, t2, count_alive))
                else:
                    count_all = count_therapy_combinations(
                        df,
                        therapy1=t1,
                        therapy2=t2,
                        deceased="all",
                        therapy1_col=therapy1_col,
                        therapy2_col=therapy2_col,
                    )
                    values_list.append(("all", t1, t2, count_all))

    elif level == 4:
        # Only therapy2 matters, therapy1=None, dead/alive split
        for t in therapy_types:
            count_dead = count_therapy_combinations(
                df, therapy2=t, deceased="true", therapy2_col=therapy2_col
            )
            count_alive = count_therapy_combinations(
                df, therapy2=t, deceased="false", therapy2_col=therapy2_col
            )
            values_list.append(("dead", None, t, count_dead))
            values_list.append(("alive", None, t, count_alive))

    else:
        raise ValueError("Level must be 1, 2, 3, or 4")

    return values_list


def prepare_sankey(df):
    therapy_types = ["ST", "HO", "CH", "OP", "no therapy", "OTHER"]

    th1_values_list = get_therapy_values(
        df, therapy_types, level=1, therapy1_col="therapy_type_1"
    )

    if "therapy_type_2" in df.columns:
        th2_values_list = get_therapy_values(
            df,
            therapy_types,
            level=2,
            therapy1_col="therapy_type_1",
            therapy2_col="therapy_type_2",
        )
    else:
        th2_values_list = []

    # to do: nochmal überdenken ob ich hier den "no therapy" type brauche
    if "therapy_type_3" in df.columns:
        th3_values_list = get_therapy_values(
            df,
            therapy_types,
            level=3,
            therapy1_col="therapy_type_2",
            therapy2_col="therapy_type_3",
        )
    else:
        th3_values_list = []

    therapy_types_level4 = ["ST", "HO", "CH", "OP", "OTHER"]
    if "therapy_type_3" in df.columns:
        th4_values_list = get_therapy_values(
            df, therapy_types_level4, level=4, therapy2_col="therapy_type_3"
        )
    else:
        th4_values_list = []

    return th1_values_list, th2_values_list, th3_values_list, th4_values_list


def lighten_hex(hex_color, factor=0.5):
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return f"#{r:02x}{g:02x}{b:02x}"


# TO DO: optimize this later - besonders links, ggf. mithilfe der Tupel und einer
# Funktion, die die Layer und Node Links dynamisch baut
# oder bau die links automatisch in der value funktion mit!
def plot_sankey(
    th1_values_list,
    th2_values_list,
    th3_values_list,
    th4_values_list,
    plot_title: str = "Therapy Sankey Diagram",
    show_plot: bool = True,
):
    # Labels
    nodes_labels = []

    nodes_labels.append(f"cohort (n={get_sum(th1_values_list, status='all')})")

    for t in ["ST", "HO", "CH", "OP", "no therapy", "OTHER"]:
        nodes_labels.append(
            f"{t} (1) (n={get_sum(th1_values_list, status='all', t1=t)})"
        )

    for t in ["ST", "HO", "CH", "OP", "OTHER"]:
        nodes_labels.append(f"{t} (2) (n={get_sum(th2_values_list, t2=t)})")

    for t in ["ST", "HO", "CH", "OP", "OTHER"]:
        nodes_labels.append(f"{t} (3) (n={get_sum(th3_values_list, t2=t)})")

    n_reported_dead = (
        get_sum(th2_values_list, "dead")
        + get_sum(th3_values_list, "dead")
        + get_sum(th4_values_list, "dead")
    )
    nodes_labels.append(f"""reported dead (n={n_reported_dead})""")
    n_unknown_alive = (
        get_sum(th2_values_list, "alive")
        + get_sum(th3_values_list, "alive")
        + get_sum(th4_values_list, "alive")
    )
    nodes_labels.append(f"""unknown/alive (n={n_unknown_alive})""")

    # --- Node Colors ---
    nodes_colors = [
        "#9e9e9e",  # all patients
        "#4a90e2",
        "#50e3c2",
        "#d77aff",
        "#f5a623",
        "#7ed321",
        "#f8e71c",  # 1. Therapie
        lighten_hex("#4a90e2", 0.3),
        lighten_hex("#50e3c2", 0.3),
        lighten_hex("#d77aff", 0.3),
        lighten_hex("#f5a623", 0.3),
        lighten_hex("#f8e71c", 0.3),  # 2. Therapie
        lighten_hex("#4a90e2", 0.6),
        lighten_hex("#50e3c2", 0.6),
        lighten_hex("#d77aff", 0.6),
        lighten_hex("#f5a623", 0.6),
        lighten_hex("#f8e71c", 0.6),  # 3. Therapie
        lighten_hex("#9e9e9e", 0.6),
        lighten_hex("#9e9e9e", 0.6),  # deceased / alive
    ]

    nodes = dict(label=nodes_labels, color=nodes_colors)

    # source: Indizes der Quellen
    # target: # Indizes der Ziele
    # values: Werte der Verbindungen
    # (source, target)
    links_data = [
        # 1st therapy: all - (1)
        (0, 1),  # all - ST (1)
        (0, 2),  # all - HO
        (0, 3),  # all - CH
        (0, 4),  # all - OP
        (0, 5),  # all - no therapy
        (0, 6),  # all - OTHER
        # 2nd therapy: (1) - (2)
        (1, 7),  # ST (1) - ST (2)
        (1, 8),  # ST - HO
        (1, 9),  # ST - CH
        (1, 10),  # ST - OP
        (1, 17),  # ST - no therapy DEAD
        (1, 18),  # ST - no therapy ALIVE
        (1, 11),  # ST - OTHER
        (2, 7),  # HO - ST
        (2, 8),  # HO - HO
        (2, 9),  # HO - CH
        (2, 10),  # HO - OP
        (2, 17),  # HO - no therapy DEAD
        (2, 18),  # HO - no therapy ALIVE
        (2, 11),  # HO - OTHER
        (3, 7),  # CH - ST
        (3, 8),  # CH - HO
        (3, 9),  # CH - CH
        (3, 10),  # CH - OP
        (3, 17),  # CH - no therapy DEAD
        (3, 18),  # CH - no therapy ALIVE
        (3, 11),  # CH - OTHER
        (4, 7),  # OP - ST
        (4, 8),  # OP - HO
        (4, 9),  # OP - CH
        (4, 10),  # OP - OP
        (4, 17),  # OP - no therapy DEAD
        (4, 18),  # OP - no therapy ALIVE
        (4, 11),  # OP - OTHER
        (5, 17),  # no therapy - DEAD
        (5, 18),  # no therapy - ALIVE
        (6, 7),  # OTHER - ST
        (6, 8),  # OTHER - HO
        (6, 9),  # OTHER - CH
        (6, 10),  # OTHER - OP
        (6, 17),  # OTHER - no therapy DEAD
        (6, 18),  # OTHER - no therapy ALIVE
        (6, 11),  # OTHER - OTHER
        # 3rd therapy: (2) - (3)
        (7, 12),  # ST (2) - ST (3)
        (7, 13),  # ST (2) - HO (3)
        (7, 14),  # ST (2) - CH
        (7, 15),  # ST (2) - OP
        (7, 17),  # ST (2) - no therapy DEAD
        (7, 18),  # ST (2) - no therapy ALIVE
        (7, 16),  # ST (2) - OTHER
        (8, 12),  # HO (2) - ST (3)
        (8, 13),  # HO (2) - HO
        (8, 14),  # HO (2) - CH
        (8, 15),  # HO (2) - OP
        (8, 17),  # HO (2) - no therapy DEAD
        (8, 18),  # HO (2) - no therapy ALIVE
        (8, 16),  # HO (2) - OTHER
        (9, 12),  # CH (2) - ST (3)
        (9, 13),  # CH - HO
        (9, 14),  # CH - CH
        (9, 15),  # CH - OP
        (9, 17),  # CH - no therapy DEAD
        (9, 18),  # CH - no therapy ALIVE
        (9, 16),  # CH - OTHER
        (10, 12),  # OP (2) - ST (3)
        (10, 13),  # OP (2) - HO (3)
        (10, 14),  # OP (2) - CH
        (10, 15),  # OP (2) - OP
        (10, 17),  # OP - no therapy DEAD
        (10, 18),  # OP - no therapy ALIVE
        (10, 16),  # OP (2) - OTHER
        (11, 12),  # OTHER (2) - ST (3)
        (11, 13),  # OTHER (2) - HO (3)
        (11, 14),  # OTHER (2) - CH (3)
        (11, 15),  # OTHER (2) - OP (3)
        (11, 17),  # OTHER (2) - DEAD
        (11, 17),  # OTHER (2) - ALIVE
        (11, 16),  # OTHER (2) - OTHER (3)
        # deceased/alive
        (12, 17),  # ST (3) - DEAD
        (12, 18),  # ST (3) - ALIVE
        (13, 17),  # HO (3) - DEAD
        (13, 18),  # HO (3) - ALIVE
        (14, 17),  # CH (3) - DEAD
        (14, 18),  # CH (3) - ALIVE
        (15, 17),  # OP (3) - DEAD
        (15, 18),  # OP (3) - ALIVE
        (16, 17),  # OTHER (3) - DEAD
        (16, 18),  # OTHER (3) - ALIVE
    ]

    # values aus meinen listen je level
    all_values = th1_values_list + th2_values_list + th3_values_list + th4_values_list

    # val[-1] letzter Eintrag aus meinem Tupel
    links_with_values = [
        (src, tgt, val[-1])  # val[-1] = eigentlicher Zahlenwert
        for (src, tgt), val in zip(links_data, all_values)
    ]
    # umwandeln in separate Listen für Plotly
    source, target, value = zip(*links_with_values)

    # gleiche Farbe wie die Source-Node
    link_colors = [nodes["color"][src] for src in source]

    links = dict(
        source=list(source), target=list(target), value=list(value), color=link_colors
    )

    # Sankey-Diagramm
    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=nodes["label"],
                    color=nodes["color"],
                ),
                link=links,
            )
        ]
    )

    fig.update_layout(title_text=plot_title, font_size=12, width=1800, height=800)

    if show_plot:
        fig.show(renderer="notebook")

    return fig


# wrapper
def prepare_and_plot_sankey(
    df, settings: BaseSettings, plot_name: str = "", show_plot=False
):
    # Prepare therapy counts for Sankey
    th1_values_list, th2_values_list, th3_values_list, th4_values_list = prepare_sankey(
        df
    )

    # Create Sankey plot
    fig = plot_sankey(
        th1_values_list,
        th2_values_list,
        th3_values_list,
        th4_values_list,
        plot_title=plot_name,
        show_plot=show_plot,
    )

    # Save plot to file
    output_dir = os.path.join(settings.output_folder, settings.study_name)
    os.makedirs(output_dir, exist_ok=True)
    final_plot_path = os.path.join(output_dir, f"{plot_name}.html")
    fig.write_html(final_plot_path)


def save_plot(plot: Figure, settings: BaseSettings, plot_name: str = "") -> None:
    logger.info("start save plot")
    output_dir = os.path.join(settings.output_folder, settings.study_name)
    os.makedirs(output_dir, exist_ok=True)

    final_plot_path = os.path.join(output_dir, f"{plot_name}.png")
    plot.savefig(final_plot_path, bbox_inches="tight", dpi=300)
    logger.info("end save plot")


# summary statistic plots
def styled_bar(ax, x, values, color, label, width=0.6, offset=0.0, relative=False):
    # Füllung
    ax.bar(x + offset, values, width=width, color=color, alpha=0.4, label=None)
    # Rahmen
    bars = ax.bar(
        x + offset,
        values,
        width=width,
        fill=False,
        edgecolor=color,
        linewidth=2,
        label=label,
    )
    # Werte drüber schreiben
    for bar, v in zip(bars, values):
        if relative:
            text = f"{v:.1f}%"
        else:
            text = str(int(v))
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            v + max(values) * 0.01,
            text,
            ha="center",
            va="bottom",
            fontsize=9,
        )


def plot_summary_statistics(
    groups: Dict[str, pd.DataFrame],
    settings: BaseSettings,
    relative: bool = False,
    age_col: str = "age_at_diagnosis",
    deceased_col: str = "deceased",
    date_col: str = "date_diagnosis",
):
    colors = {"All C61": "tab:blue", "Cohort": "tab:orange", "Rest": "tab:green"}

    plot_names = [
        "distinct_condition_count",
        "age_at_diagnosis",
        "survival_status",
        "diagnosis_year",
    ]

    num_groups = len(groups)
    if relative:
        plot_names = [f"{name}_rel_{num_groups}" for name in plot_names]
    else:
        plot_names = [f"{name}_{num_groups}" for name in plot_names]

    # --- 0. Distinct condition_id counts ---
    counts_abs = {
        name: df["condition_id"].nunique() if not df.empty else 0
        for name, df in groups.items()
    }
    if relative:
        total = sum(counts_abs.values())
        if total > 0:
            counts = {k: v / total * 100 for k, v in counts_abs.items()}
        else:
            counts = {k: 0 for k in counts_abs}
        ylabel = "Percentage (%)"
    else:
        counts = counts_abs
        ylabel = "Count"

    fig, ax = plt.subplots(figsize=(6, 4))
    x = np.arange(len(counts))
    for i, (name, v) in enumerate(counts.items()):
        styled_bar(
            ax,
            np.array([i]),
            [v],
            color=colors.get(name, "gray"),
            label=f"{name} (n={counts_abs[name]})",
            width=0.6,
            relative=relative,
        )

    ax.set_xticks(x)
    ax.set_xticklabels(counts.keys())
    ax.set_ylabel(ylabel)
    ax.set_title("Distinct diagnosis count distribution")
    ax.legend()
    fig.tight_layout()
    save_plot(fig, settings, plot_names[0])
    plt.show()

    # --- 1. Altersverteilung ---
    fig, ax = plt.subplots(figsize=(12, 5))
    bins = range(30, 91, 2)
    for name, df in groups.items():
        if df.empty:
            continue
        valid_ages = df[age_col][df[age_col].between(0, 120)]
        if valid_ages.empty:
            continue
        color = colors.get(name, "gray")
        ax.hist(valid_ages, bins=bins, alpha=0.5, color=color, density=relative)
        ax.hist(
            valid_ages,
            bins=bins,
            histtype="step",
            linewidth=2,
            color=color,
            label=f"{name} (n={len(valid_ages)})",
            density=relative,
        )

    ax.set_xlabel("Age at Diagnosis")
    ax.set_ylabel("Percentage (%)" if relative else "Count")
    ax.set_title(
        "Age at Diagnosis Distribution" + (" (Relative %)" if relative else "")
    )
    if relative:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y * 100:.0f}%"))
    ax.legend()
    fig.tight_layout()
    save_plot(fig, settings, plot_names[1])
    plt.show()

    # --- 2. Survival status ---
    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.25
    x = np.arange(2)

    for i, (name, df) in enumerate(groups.items()):
        ordered: List[float] = [0.0, 0.0]
        total = 0

        if not df.empty:
            tmp = df.copy()
            tmp[deceased_col] = tmp[deceased_col].fillna(False)
            counts_val = tmp[deceased_col].value_counts()
            total = len(tmp)
            ordered = [
                float(counts_val.get(False, 0)),
                float(counts_val.get(True, 0)),
            ]
            if relative:
                if total > 0:
                    ordered = [v / total * 100 for v in ordered]
                else:
                    ordered = [0.0, 0.0]

        styled_bar(
            ax,
            x,
            ordered,
            color=colors.get(name, "gray"),
            label=f"{name} (n={total})",
            width=width,
            offset=i * width,
            relative=relative,
        )

    ax.set_xticks(x + width * (len(groups) - 1) / 2)
    ax.set_xticklabels(["Alive / Unknown", "Reported dead"])
    ax.set_ylabel("Percentage (%)" if relative else "Count")
    ax.set_title("Survival Status by Group" + (" (Relative %)" if relative else ""))
    ax.legend()
    fig.tight_layout()
    save_plot(fig, settings, plot_names[2])
    plt.show()

    # --- 3. Diagnosis year ---
    fig, ax = plt.subplots(figsize=(12, 5))
    for name, df in groups.items():
        if df.empty:
            continue
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        years = df[date_col].dt.year.dropna()
        years = years[years >= 1950]
        if years.empty:
            continue
        color = colors.get(name, "gray")
        ax.hist(years, bins=20, alpha=0.5, color=color, density=relative)
        ax.hist(
            years,
            bins=20,
            histtype="step",
            linewidth=2,
            color=color,
            label=f"{name} (n={len(years)})",
            density=relative,
        )

    ax.set_xlabel("Year of Diagnosis")
    ax.set_ylabel("Percentage (%)" if relative else "Count")
    ax.set_title(
        "Diagnosis Year Distribution (from 1950 onwards)"
        + (" (Relative %)" if relative else "")
    )
    if relative:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y * 100:.0f}%"))
    ax.xaxis.set_major_locator(ticker.MultipleLocator(5))
    plt.xticks(rotation=45)
    ax.legend()
    fig.tight_layout()
    save_plot(fig, settings, plot_names[3])
    plt.show()


def prepare_dfs_for_plotting(
    df_final_all_c61: DataFrame, df_cohort: DataFrame
) -> DataFrame:
    # Create df group
    df_rest = df_final_all_c61.join(df_cohort, on="condition_id", how="left_anti")

    # Convert Spark DataFrames to Pandas dataframes pdfs
    pdf_all = pd.DataFrame(df_final_all_c61.collect(), columns=df_final_all_c61.columns)
    pdf_cohort = pd.DataFrame(df_cohort.collect(), columns=df_cohort.columns)
    pdf_rest = pd.DataFrame(df_rest.collect(), columns=df_rest.columns)

    logger.info(
        "start save plot print(len(pdf_all), len(pdf_cohort), len(pdf_rest))",
        len(pdf_all),
        len(pdf_cohort),
        len(pdf_rest),
    )
    return pdf_all, pdf_cohort, pdf_rest


# wrapper
def plot(df_final_all_c61, settings):
    # Plot Sankey-Diagramme
    # all c61
    prepare_and_plot_sankey(
        df_final_all_c61,
        settings=settings,
        plot_name="Therapy sequence of all C61 patients",
        show_plot=False,
    )

    # Filtered cohort
    df_cohort = filter_young_highrisk_cohort(df_final_all_c61)
    prepare_and_plot_sankey(
        df_cohort,
        settings=settings,
        plot_name="Therapy sequence of C61 young high risk cohort",
        show_plot=False,
    )

    # prep for summary statistics
    pdf_all, pdf_cohort, pdf_rest = prepare_dfs_for_plotting(
        df_final_all_c61, df_cohort
    )

    # summary statistics
    plot_summary_statistics(
        groups={"All C61": pdf_all, "Cohort": pdf_cohort, "Rest": pdf_rest},
        settings=settings,
        relative=False,
        age_col="age_at_diagnosis",
        deceased_col="deceased",
        date_col="date_diagnosis",
    )
    plot_summary_statistics(
        groups={
            "All C61": pdf_all,
            "Cohort": pdf_cohort,
        },
        settings=settings,
        relative=False,
        age_col="age_at_diagnosis",
        deceased_col="deceased",
        date_col="date_diagnosis",
    )
    plot_summary_statistics(
        groups={"Cohort": pdf_cohort, "Rest": pdf_rest},
        settings=settings,
        relative=False,
        age_col="age_at_diagnosis",
        deceased_col="deceased",
        date_col="date_diagnosis",
    )
    plot_summary_statistics(
        groups={"Cohort": pdf_cohort, "Rest": pdf_rest},
        settings=settings,
        relative=True,
        age_col="age_at_diagnosis",
        deceased_col="deceased",
        date_col="date_diagnosis",
    )
