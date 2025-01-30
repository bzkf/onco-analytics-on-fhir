from typing import List, Optional
import os
import shutil
from datetime import datetime
import re

import pandas as pd
from pathling import PathlingContext, datasource
from pathling import Expression as exp

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    first,
    regexp_replace,
    to_date,
    udf,
    max,
    count,
    unix_timestamp,
    abs,
    row_number,
    to_timestamp,
    when,
    regexp_extract,
    substring,
    year,
    month,
    dayofmonth,
    collect_list,
    lower,
    monotonically_increasing_id,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType


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


# do not use this before DataSHIELD for easier validation of results - map later or map in addition to icd_grouped
def group_entities(icd10_code_mapped):
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
        birthdate_dt = datetime.strptime(birthdate, "%Y-%m")  # pseudonymized birthdate
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


# UDFs
map_icd10_udf = udf(lambda x: map_icd10(x), StringType())
group_icd_groups_udf = udf(lambda x: group_icd_groups(x), StringType())
group_entities_udf = udf(lambda x: group_entities(x), StringType())
calculate_age_at_condition_date_udf = udf(
    lambda x, y: calculate_age_at_condition_date(x, y), StringType()
)


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


def prepare_data_dictionary_PoC(df):
    dtypes_list_df = [str(dtype[1]) for dtype in df.dtypes]

    description_list_df_dictionary = {
        "condition_id": "Condition ID, unique for each condition",
        "icd10_code": "ICD10 GM diagnosis code",
        "icd10_mapped": """ICD10 GM diagnosis code mapped A = 1, B = 2, C = 3, D = 4,
        e.g.: A01.9 = 101.9, C50.1 = 350.1 or D41.9 = 441.9""",
        "icd10_grouped": """ICD10 GM diagnosis code grouped to parent code, e.g. A01.1
        and A01.9 both belong to group 101 (remove decimal from icd10_mapped)""",
        "icd10_entity": "Entities of resulting ICD10 groups, see utils",
        "date_diagnosis": "Date of diagnosis",
        "date_diagnosis_day": "Day of Diagnosis",
        "date_diagnosis_month": "Month of Diagnosis",
        "date_diagnosis_year": "Year of diagnosis",
        "birthdate": "Birthdate",
        "age_at_diagnosis": "Age at Diagnosis",
        "age_group_small": """Age groups mapped as follows: 0 (0-14), 1 (15-19),
        2 (20-24), 3 (25-29), 4 (30-34), 5 (35-39), 6 (40-44), 7 (45-49), 8 (50-54),
        9 (55-59), 10 (60-64), 11 (65-69), 12 (70-74), 13 (75-79), 14 (80-84), and
        15 (85+)""",
        "age_group_large": """Age groups mapped as follows: 0 (0-10), 1 (11-20),
        2 (21-30), 3 (31-40), 4 (41-50), 5 (51-60), 6 (61-70), 7 (71-80), 8 (81-90),
        and 9 (90+).""",
        "gender": "Gender - male, female, other/diverse",
        "gender_mapped": """Gender mapped: 0 = None, 1 = female, 2 = male,
        3 = other/diverse""",
        "postal_code": "Postal code",
    }

    return dtypes_list_df, description_list_df_dictionary


def prepare_data_dictionary_study_protocol_c(df):
    dtypes_list_df = [str(dtype[1]) for dtype in df.dtypes]

    description_list_df_dictionary = {
        "condition_id": "Condition ID, unique for each condition",
        "icd10_code": "ICD10 GM diagnosis code",
        "icd10_mapped": """ICD10 GM diagnosis code mapped A = 1, B = 2, C = 3, D = 4,
        e.g.: A01.9 = 101.9, C50.1 = 350.1 or D41.9 = 441.9""",
        "icd10_grouped": """ICD10 GM diagnosis code grouped to parent code, e.g. A01.1
        and A01.9 both belong to group 101 (remove decimal from icd10_mapped)""",
        "icd10_entity": "Entities of resulting ICD10 groups, see utils",
        "date_diagnosis": "Date of diagnosis",
        "date_diagnosis_year": "Year of diagnosis",
        "birthdate": "Birthdate",
        "age_at_diagnosis": "Age at Diagnosis",
        "age_group_small": """Age groups mapped as follows: 0 (0-14), 1 (15-19),
        2 (20-24), 3 (25-29), 4 (30-34), 5 (35-39), 6 (40-44), 7 (45-49), 8 (50-54),
        9 (55-59), 10 (60-64), 11 (65-69), 12 (70-74), 13 (75-79), 14 (80-84), and
        15 (85+)""",
        "age_group_large": """Age groups mapped as follows: 0 (0-10), 1 (11-20),
        2 (21-30), 3 (31-40), 4 (41-50), 5 (51-60), 6 (61-70), 7 (71-80), 8 (81-90),
        and 9 (90+).""",
        "gender": "Gender - male, female, other/diverse",
        "gender_mapped": """Gender mapped: 0 = None, 1 = female, 2 = male,
        3 = other/diverse""",
        "postal_code": "Postal code",
    }

    return dtypes_list_df, description_list_df_dictionary


# optionally pass lists to fill the data_dictionary
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
