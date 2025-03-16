import os
import re
import shutil
from datetime import datetime
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
    collect_list,
    dayofmonth,
    first,
    greatest,
    lit,
    lower,
    monotonically_increasing_id,
    month,
    regexp_extract,
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


# UDF for sorting with None first, then empty string, then 0,
# then '0' and ascending order for other values


def sort_and_last(values):
    if values is None:
        return None
    # Custom sorting: (None -> empty string -> 0 -> '0' -> other values ascending)
    sorted_arr = sorted(
        values, key=lambda x: (x is not None, x != "", x != 0, x != "0", x)
    )
    print(sorted_arr)
    return sorted_arr[-1] if sorted_arr else None


# UDFs
map_icd10_udf = udf(lambda x: map_icd10(x), StringType())
group_icd_groups_udf = udf(lambda x: group_icd_groups(x), StringType())
group_entities_udf = udf(lambda x: group_entities(x), StringType())
calculate_age_at_condition_date_udf = udf(
    lambda x, y: calculate_age_at_condition_date(x, y), StringType()
)
sort_and_last_udf = udf(sort_and_last, StringType())
map_tnm_column_udf = udf(map_tnm_column, IntegerType())
group_to_icd_chapter_udf = udf(group_to_icd_chapter, IntegerType())


def find_first(df, date_diagnosis_col, other_date_col, obs_id):
    # get duplicate rows
    df_duplicate_counts = df.groupBy("condition_id").count().where("count > 1")
    df_duplicate_counts_rows = df.join(
        df_duplicate_counts, on="condition_id", how="inner"
    ).drop("count")

    # store singular rows where condition_id only appears once for joining back later
    df_single_rows = df.join(df_duplicate_counts, on="condition_id", how="left_anti")

    # remove rows where other_date_col is null
    df_duplicate_counts_rows = df_duplicate_counts_rows.filter(
        col(other_date_col).isNotNull()
    )

    # Aggregate by condition_id, keeping the last/highest non-null value
    # (handles the case where I have the same other_date but different obs values
    # - keeps the highest e.g. TNM or gleason for that date
    agg_expr = [
        sort_and_last_udf(collect_list(col_name)).alias(col_name)
        for col_name in df_duplicate_counts_rows.columns
        if col_name not in ["condition_id", other_date_col, date_diagnosis_col, obs_id]
    ]

    df_duplicate_counts_rows = df_duplicate_counts_rows.groupBy(
        "condition_id", other_date_col, date_diagnosis_col
    ).agg(*agg_expr)

    # define window spec to order by closest date
    window_spec = Window.partitionBy("condition_id").orderBy(
        abs(
            when(
                col(other_date_col).isNotNull(), col(other_date_col).cast("timestamp")
            ).otherwise(lit(None).cast("timestamp"))
            - col(date_diagnosis_col).cast("timestamp")
        ).asc()
    )

    # select the closest date = first row number in the ordered rows
    df_duplicate_counts_rows = df_duplicate_counts_rows.withColumn(
        "row_num", row_number().over(window_spec)
    )
    df_closest = df_duplicate_counts_rows.filter(col("row_num") == 1).drop("row_num")

    # rename other_date_col to other_date_col_first
    df_closest = df_closest.withColumnRenamed(other_date_col, other_date_col + "_first")

    # rename other_date_col and drop obs_id in df_single_rows
    df_single_rows = df_single_rows.withColumnRenamed(
        other_date_col, other_date_col + "_first"
    ).drop(obs_id)

    # merge both DataFrames
    final_df = df_closest.unionByName(df_single_rows)

    return final_df


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


def extract_df_study_protocol_a(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: BaseSettings,
    spark: SparkSession,
    c61: bool = False,
):
    # get all unique condition ids
    a_unique_condids = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp(
                "Condition.subject.resolve().ofType(Patient).identifier.value",
                "patid_pseudonym",
            ),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                "code.coding.where( \
                    system='http://fhir.de/CodeSystem/bfarm/icd-10-gm' \
                    ).code",
                "icd10_code",
            ),
            exp("Condition.subject.resolve().ofType(Patient).birthDate", "birthdate"),
            exp("Condition.subject.resolve().ofType(Patient).gender", "gender"),
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
    a_unique_condids = a_unique_condids.cache()

    logger.info("cached condition ids")

    # save conditions
    """ file_path = os.path.join(
        settings.output_folder,
        settings.study_name,
        "pathling-data-extracted",
        "a_unique_condids/",
    ) """

    # a_unique_condids.write.mode("overwrite").format("delta").save(file_path)
    # a_unique_condids = spark.read.format("delta").load(file_path)

    # extract the conditions with cTNM and pTNM separately for joining observations
    # later
    a_cond_ctnm = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                """Condition.stage.assessment.resolve().ofType(Observation)
                .where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').id""",
                "cond_tnm_id",
            ),
        ],
    )
    a_cond_ctnm = a_cond_ctnm.cache()

    logger.info("cached a_cond_ctnm")

    a_cond_ptnm = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                "Condition.stage.assessment.resolve().ofType(Observation) \
                .where(code.coding.system.first()='http://loinc.org' and  \
                code.coding.code.first()='21902-2').id",
                "cond_tnm_id",
            ),
        ],
    )
    a_cond_ptnm = a_cond_ptnm.cache()

    logger.info("cached a_cond_ptnm")

    # extract cTNM and pTNM observations separately
    a_observations_c_tnm = data.extract(
        "Observation",
        columns=[
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').id""",
                "obs_tnm_id",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').component.where(
                code.coding.code.first()='21905-5').valueCodeableConcept.coding.code""",
                "cT",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').component.where(code.coding.code.first()='201906-3')
                .valueCodeableConcept.coding.code""",
                "cN",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').component.where(code.coding.code.first()='21907-1')
                .valueCodeableConcept.coding.code""",
                "cM",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').valueCodeableConcept.coding.code""",
                "c_UICC",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21908-9').effectiveDateTime""",
                "cTNM_date",
            ),
        ],
    )
    a_observations_c_tnm = a_observations_c_tnm.cache()

    logger.info("cached a_observations_c_tnm")

    a_observations_p_tnm = data.extract(
        "Observation",
        columns=[
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org'
                and code.coding.code.first()='21902-2').id""",
                "obs_tnm_id",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21902-2').component.where(
                code.coding.code.first()='21899-0').valueCodeableConcept.coding.code""",
                "pT",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21902-2').component.where(
                code.coding.code.first()='21900-6').valueCodeableConcept.coding.code""",
                "pN",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21902-2').component.where(
                code.coding.code.first()='21901-4').valueCodeableConcept.coding.code""",
                "pM",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21902-2').valueCodeableConcept.coding.code""",
                "p_UICC",
            ),
            exp(
                """Observation.where(code.coding.system.first()='http://loinc.org' and
                code.coding.code.first()='21902-2').effectiveDateTime""",
                "pTNM_date",
            ),
        ],
    )
    a_observations_p_tnm = a_observations_p_tnm.cache()

    logger.info("cached a_observations_p_tnm")

    # group "multiplied" columns by observation id, merge non-null values for one
    # observation into one line
    a_observations_c_tnm_grouped = a_observations_c_tnm.groupBy("obs_tnm_id").agg(
        first("cTNM_date", ignorenulls=True).alias("cTNM_date"),
        first("cT", ignorenulls=True).alias("cT"),
        first("cN", ignorenulls=True).alias("cN"),
        first("cM", ignorenulls=True).alias("cM"),
        first("c_UICC", ignorenulls=True).alias("c_UICC"),
    )
    a_observations_p_tnm_grouped = a_observations_p_tnm.groupBy("obs_tnm_id").agg(
        first("pTNM_date", ignorenulls=True).alias("pTNM_date"),
        first("pT", ignorenulls=True).alias("pT"),
        first("pN", ignorenulls=True).alias("pN"),
        first("pM", ignorenulls=True).alias("pM"),
        first("p_UICC", ignorenulls=True).alias("p_UICC"),
    )

    logger.info("Done grouping")

    # join with condition data
    a_cond_c_tnm = a_cond_ctnm.join(
        a_observations_c_tnm_grouped,
        a_cond_ctnm["cond_tnm_id"] == a_observations_c_tnm_grouped["obs_tnm_id"],
        "right",
    )
    a_cond_c_tnm = a_cond_c_tnm.dropna(how="all")

    a_cond_p_tnm = a_cond_ptnm.join(
        a_observations_p_tnm_grouped,
        a_cond_ptnm["cond_tnm_id"] == a_observations_p_tnm_grouped["obs_tnm_id"],
        "right",
    )
    a_cond_p_tnm = a_cond_p_tnm.dropna(how="all")

    logger.info("Done joining")

    # find first cTNM and pTNM observation date closest to diagnosis date for each
    # condition
    a_cond_c_tnm_first = find_first(
        a_cond_c_tnm, "date_diagnosis", "cTNM_date", "obs_tnm_id"
    )
    a_cond_p_tnm_first = find_first(
        a_cond_p_tnm, "date_diagnosis", "pTNM_date", "obs_tnm_id"
    )

    logger.info("Done find_first")

    # join cTNM first and pTNM first to all unique condition ids from above to also
    # include those where TNM is missing
    a_cond_c_tnm_first = a_cond_c_tnm_first.drop("date_diagnosis")
    a_cond_p_tnm_first = a_cond_p_tnm_first.drop("date_diagnosis")
    a_unique_condids_c_tnm = a_unique_condids.join(
        a_cond_c_tnm_first, on=["condition_id"], how="left"
    )
    a_unique_condids_c_tnm = a_unique_condids_c_tnm.drop("cond_tnm_id", "obs_tnm_id")
    a_cond_p_tnm_first = a_cond_p_tnm_first.drop(
        "cond_tnm_id", "obs_tnm_id", "date_diagnosis"
    )
    a_unique_condids_cp_tnm = a_unique_condids_c_tnm.join(
        a_cond_p_tnm_first, on=["condition_id"], how="left"
    )
    a_unique_condids_cp_tnm = a_unique_condids_cp_tnm.cache()

    logger.info("Cached a_unique_condids_cp_tnm")

    # add death information from observation
    a6_7_obs = data.extract(
        "Observation",
        columns=[
            exp(
                "Observation.where(code.coding.system.first()='http://loinc.org' and \
                code.coding.code.first()='68343-3').id",
                "observation_id",
            ),
            exp(
                "Observation.where(code.coding.system.first()='http://loinc.org' and \
                code.coding.code.first()='68343-3').valueCodeableConcept.coding.where( \
                system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code",
                "death_cause_icd10",
            ),
            exp(
                "Observation.where(code.coding.system.first()='http://loinc.org' and \
                code.coding.code.first()='68343-3').valueCodeableConcept.coding.where( \
                system='http://dktk.dkfz.de/fhir/onco/core/CodeSystem/JNUCS').code",
                "death_cause_tumor",
            ),
            exp(
                "Observation.where(code.coding.system.first()='http://loinc.org' and \
                code.coding.code.first()='68343-3').effectiveDateTime",
                "date_death",
            ),
            exp(
                "Observation.where(code.coding.system.first()='http://loinc.org' and \
                code.coding.code.first()='68343-3').subject.resolve().ofType(Patient) \
                .identifier.value",
                "patid_pseudonym",
            ),
        ],
    )
    a6_7_obs = a6_7_obs.cache()

    logger.info("Cached a6_7_obs")

    # group "multiplied" cols to comine all non-null values into one row per obds-id
    a6_7_obs_grouped = a6_7_obs.groupBy("observation_id").agg(
        first("death_cause_icd10", ignorenulls=True).alias("death_cause_icd10"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
        first("date_death", ignorenulls=True).alias("date_death"),
        first("patid_pseudonym", ignorenulls=True).alias("patid_pseudonym"),
    )
    # join to rest
    a0_7 = a_unique_condids_cp_tnm.join(
        a6_7_obs_grouped, on="patid_pseudonym", how="left"
    )

    logger.info("joined a0_7")

    # group by condition id and date and prioritize non-null values
    a0_7_grouped = a0_7.groupBy("condition_id", "date_diagnosis").agg(
        first("patid_pseudonym", ignorenulls=True).alias("patid_pseudonym"),
        first("icd10_code", ignorenulls=True).alias("icd10_code"),
        first("birthdate", ignorenulls=True).alias("birthdate"),
        first("gender", ignorenulls=True).alias("gender"),
        first("deceased_datetime", ignorenulls=True).alias("deceased_datetime"),
        first("deceased_boolean", ignorenulls=True).alias("deceased_boolean"),
        first("cTNM_date_first", ignorenulls=True).alias("cTNM_date_first"),
        first("cT", ignorenulls=True).alias("cT"),
        first("cN", ignorenulls=True).alias("cN"),
        first("cM", ignorenulls=True).alias("cM"),
        first("c_UICC", ignorenulls=True).alias("c_UICC"),
        first("pTNM_date_first", ignorenulls=True).alias("pTNM_date_first"),
        first("pT", ignorenulls=True).alias("pT"),
        first("pN", ignorenulls=True).alias("pN"),
        first("pM", ignorenulls=True).alias("pM"),
        first("p_UICC", ignorenulls=True).alias("p_UICC"),
        first("death_cause_icd10", ignorenulls=True).alias("death_cause_icd10"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
        first("date_death", ignorenulls=True).alias("date_death"),
    )

    logger.info("group by condition id and date and prioritize non-null values")

    # DataSHIELD preparations
    # deconstruct date to extract year in new col for datashield
    a0_7_final = deconstruct_date_year_only(a0_7_grouped, "date_diagnosis")
    a0_7_final = deconstruct_date_year_only(a0_7_final, "deceased_datetime")
    a0_7_final = deconstruct_date_year_only(a0_7_final, "cTNM_date_first")
    a0_7_final = deconstruct_date_year_only(a0_7_final, "pTNM_date_first")
    a0_7_final = deconstruct_date_year_only(a0_7_final, "date_death")

    logger.info("deconstruct date to extract year in new col for datashield")

    # map TNM, UICC
    a0_7_final = map_tnm_column(a0_7_final, "cT")
    a0_7_final = map_tnm_column(a0_7_final, "cN")
    a0_7_final = map_tnm_column(a0_7_final, "cM")
    a0_7_final = map_tnm_column(a0_7_final, "pT")
    a0_7_final = map_tnm_column(a0_7_final, "pN")
    a0_7_final = map_tnm_column(a0_7_final, "pM")
    a0_7_final = map_uicc_column(a0_7_final, "c_UICC")
    a0_7_final = map_uicc_column(a0_7_final, "p_UICC")

    logger.info("map TNM, UICC")

    # map ICD10
    a0_7_final = a0_7_final.withColumn(
        "icd10_mapped",
        map_icd10_udf(a0_7_final.icd10_code).cast(DoubleType()),
    )
    a0_7_final = a0_7_final.withColumn(
        "icd10_grouped",
        group_icd_groups_udf(a0_7_final.icd10_mapped).cast(IntegerType()),
    )

    # we do need the entity groups for smaller subsets to not only generate NA in DS
    a0_7_final = a0_7_final.withColumn(
        "icd10_entity",
        group_entities_udf(a0_7_final.icd10_grouped).cast(IntegerType()),
    )
    a0_7_final = map_gender(a0_7_final, "gender")

    # age at diagnosis
    a0_7_final = a0_7_final.withColumn(
        "age_at_diagnosis",
        calculate_age_at_condition_date_udf(
            a0_7_final.birthdate, a0_7_final.date_diagnosis
        ).cast(IntegerType()),
    )
    # add age groups
    a0_7_final = add_age_group_small(a0_7_final, "age_at_diagnosis")
    a0_7_final = add_age_group_large(a0_7_final, "age_at_diagnosis")

    a0_7_final = map_death_cause_tumor(df=a0_7_final, column_name="death_cause_tumor")

    a0_7_final = a0_7_final.withColumn(
        "death_cause_icd10_mapped",
        map_icd10_udf(a0_7_final.death_cause_icd10).cast(DoubleType()),
    )

    a0_7_final = a0_7_final.withColumn(
        "death_cause_icd10_grouped",
        group_icd_groups_udf(a0_7_final.death_cause_icd10_mapped).cast(IntegerType()),
    )

    a0_7_final = a0_7_final.withColumn(
        "death_cause_entity",
        group_entities_udf(a0_7_final.death_cause_icd10_grouped).cast(IntegerType()),
    )

    a0_7_final = a0_7_final.withColumn(
        "death_cause_icd10_chapter",
        group_to_icd_chapter_udf(a0_7_final.death_cause_icd10_mapped).cast(
            IntegerType()
        ),
    )

    # map death from multiple cols to new col dead_bool_mapped
    columns_to_check = [
        "deceased_datetime",
        # "deceased_boolean",
        "death_cause_icd10",
        "death_cause_tumor",
        "date_death",
    ]

    # if any of the columns contains a non-null value: dead
    a0_7_final = a0_7_final.withColumn(
        "dead_bool_mapped",
        greatest(*[col(column).isNotNull().cast("int") for column in columns_to_check]),
    )

    logger.info("if any of the columns contains a non-null value: dead")

    # fix order
    desired_order = [
        "condition_id",
        "patid_pseudonym",
        "birthdate",
        "age_at_diagnosis",
        "age_group_small",
        "age_group_large",
        "gender",
        "gender_mapped",
        "date_diagnosis",
        "date_diagnosis_year",
        "icd10_code",
        "icd10_mapped",
        "icd10_grouped",
        "icd10_entity",
        "cTNM_date_first",
        "cTNM_date_first_year",
        "cT",
        "cN",
        "cM",
        "c_UICC",
        "pTNM_date_first",
        "pTNM_date_first_year",
        "pT",
        "pN",
        "pM",
        "p_UICC",
        "cT_mapped",
        "cN_mapped",
        "cM_mapped",
        "c_UICC_mapped",
        "pT_mapped",
        "pN_mapped",
        "pM_mapped",
        "p_UICC_mapped",
        "deceased_datetime",
        "deceased_datetime_year",
        "deceased_boolean",
        "death_cause_icd10",
        "death_cause_icd10_mapped",
        "death_cause_icd10_grouped",
        "death_cause_entity",
        "death_cause_icd10_chapter",
        "death_cause_tumor",
        "death_cause_tumor_mapped",
        "date_death",
        "date_death_year",
        "dead_bool_mapped",
    ]

    a0_7_final = a0_7_final.select(*desired_order)

    logger.info("select desired order")

    # filter years
    a0_7_final = a0_7_final[a0_7_final["date_diagnosis_year"].between(2012, 2022)]

    logger.info("filter years")

    if c61 is True:
        logger.info("filter c61")
        # filter for ICD10 Code C61
        a0_7_final_c61 = a0_7_final.filter(a0_7_final["icd10_entity"].cast("int") == 14)
        # add gleason
        observations_gleason = data.extract(
            "Observation",
            columns=[
                exp("id", "obs_id_gleason"),
                exp(
                    "Observation.where(code.coding.system.first()='http://loinc.org' "
                    + "and code.coding.code.first()='35266-6').valueInteger",
                    "gleason",
                ),
                exp(
                    "Observation.where(code.coding.system.first()='http://loinc.org' "
                    + "and code.coding.code.first()='35266-6').effectiveDateTime",
                    "gleason_date",
                ),
                exp(
                    "Observation.subject.resolve().ofType(Patient).identifier.value",
                    "patid_pseudonym",
                ),
            ],
            filters=[
                "reverseResolve(Condition.stage.assessment).code.coding.where("
                + "system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code."
                + "first() = 'C61'"
            ],
        )
        logger.info("gleason extract")
        observations_gleason = observations_gleason.filter(
            (observations_gleason["gleason"].isNotNull())  # &
            # (observations_gleason["gleason_date"].isNotNull())
        )
        # join gleason observations to whole df
        a0_7_final_c61_gleason = a0_7_final_c61.join(
            observations_gleason,
            a0_7_final_c61["patid_pseudonym"]
            == observations_gleason["patid_pseudonym"],
            "left",
        )
        logger.info("gleason join")

        a0_7_final_c61_gleason = a0_7_final_c61_gleason.drop("patid_pseudonym")
        # a0_7_final_c61_gleason = a0_7_final_c61_gleason.drop("patid_pseudonym")
        a0_7_final_c61_gleason = a0_7_final_c61_gleason.withColumn(
            "deceased_boolean", col("deceased_boolean").cast("string")
        )
        # return a0_7_final_c61_gleason
        a0_7_final_c61_gleason_first = find_first(
            a0_7_final_c61_gleason, "date_diagnosis", "gleason_date", "obs_id_gleason"
        )
        logger.info("return a0_7_final_c61_gleason")
        return a0_7_final_c61_gleason_first

    return a0_7_final


def extract_df_study_protocol_c(pc: PathlingContext, data: datasource.DataSource):
    df = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "date_diagnosis"),
            exp(
                "code.coding \
                    .where(system='http://fhir.de/CodeSystem/bfarm/icd-10-gm').code",
                "icd10_code",
            ),
            exp("Condition.subject.resolve().ofType(Patient).birthDate", "birthdate"),
            exp("Condition.subject.resolve().ofType(Patient).gender", "gender"),
            exp(
                "Condition.subject.resolve().ofType(Patient).address.postalCode",
                "postal_code",
            ),
        ],
    )

    # map icd10 A= 1, B = 2, C = 3, D = 4
    df = df.withColumn(
        "icd10_mapped",
        map_icd10_udf(df.icd10_code).cast(DoubleType()),
    )

    df = df.withColumn(
        "icd10_grouped",
        group_icd_groups_udf(df.icd10_mapped).cast(IntegerType()),
    )

    df = df.withColumn(
        "icd10_entity",
        group_entities_udf(df.icd10_grouped).cast(IntegerType()),
    )

    df = map_gender(df, "gender")

    df = df.withColumn(
        "age_at_diagnosis",
        calculate_age_at_condition_date_udf(df.birthdate, df.date_diagnosis).cast(
            IntegerType()
        ),
    )

    df = add_age_group_small(df, "age_at_diagnosis")
    df = add_age_group_large(df, "age_at_diagnosis")

    df = deconstruct_date_year_only(df, "date_diagnosis")

    df = df.select(
        [
            "condition_id",
            "icd10_code",
            "icd10_mapped",
            "icd10_grouped",
            "icd10_entity",
            "date_diagnosis",
            "date_diagnosis_year",
            "birthdate",
            "age_at_diagnosis",
            "age_group_small",
            "age_group_large",
            "gender",
            "gender_mapped",
            "postal_code",
        ]
    )

    df = df.dropDuplicates()

    return df


def save_final_df(df, settings):
    df_with_id = df.withColumn("ID", monotonically_increasing_id())
    cols = ["ID"] + [col for col in df.columns]
    df_with_id = df_with_id.select(*cols)
    df_with_id = df_with_id.coalesce(1)
    output_dir = os.path.join(settings.output_folder, settings.study_name)
    temp_dir = os.path.join(settings.output_folder, settings.study_name, "temp")

    os.makedirs(temp_dir, exist_ok=True)
    df_with_id.write.mode("overwrite").csv(temp_dir, header=True)
    output_file = os.path.join(output_dir, settings.output_filename)
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
    shutil.move(os.path.join(temp_dir, part_file), output_file)
    shutil.rmtree(temp_dir)


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

    # OPAL data_dictionary predefined colnames
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
    # exchange data type namings to fit OPAL requirements
    updated_value_types = [
        "integer" if dtype == "int" else "decimal" if dtype == "double" else dtype
        for dtype in value_type_list
    ]

    # create data_dictionary
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

    # create second (empty) sheet for data dictionary "Categories"
    categories_columns = ["table", "variable", "name", "code", "missing"]
    df_categories = pd.DataFrame(columns=categories_columns)

    parent_dir = os.path.dirname(file_path)
    os.makedirs(parent_dir, exist_ok=True)

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Variables")
        df_categories.to_excel(writer, index=False, sheet_name="Categories")

    return df
