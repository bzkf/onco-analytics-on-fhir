from typing import List, Optional
import os
import shutil

import pandas as pd
from pathling import PathlingContext, datasource
from pathling import Expression as exp

from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, udf, year, month, dayofmonth
from pyspark.sql.types import DoubleType, IntegerType, StringType


def map_icd10(icd10_code):
    letter_to_number = {
        "A": "1",
        "B": "2",
        "C": "3",
        "D": "4",
        "E": "5",
        "F": "6",
        "G": "7",
        "H": "8",
        "I": "9",
        "J": "10",
        "K": "11",
        "L": "12",
        "M": "13",
        "N": "14",
        "O": "15",
        "P": "16",
        "Q": "17",
        "R": "18",
        "S": "19",
        "T": "20",
        "U": "21",
        "V": "22",
        "W": "23",
        "X": "24",
        "Y": "25",
        "Z": "26",
    }
    icd10_code = str(icd10_code)

    first_letter = icd10_code[0]

    if first_letter in letter_to_number:
        icd10_code_mapped = icd10_code.replace(
            first_letter, letter_to_number[first_letter]
        )
    else:
        icd10_code_mapped = -1

    return icd10_code_mapped


# to do: add case "unkown" here separately in the future
def map_gender(gender_string):
    if gender_string in [None, ""]:
        return 0
    elif gender_string in ["female", "weiblich"]:
        return 1
    elif gender_string in ["male", "männlich"]:
        return 2
    else:
        return 3  # other / divers / unknown


def group_entities(icd10_code_mapped):
    icd10_code_mapped = float(icd10_code_mapped)
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


def deconstruct_date(df: DataFrame, date_col: str):
    # Extract year, month, and day from date_col
    year_col = f"{date_col}_year"
    month_col = f"{date_col}_month"
    day_col = f"{date_col}_day"

    df = df.withColumn(year_col, year(date_col))
    df = df.withColumn(month_col, month(date_col))
    df = df.withColumn(day_col, dayofmonth(date_col))

    return df


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


def extract_df_PoC(pc: PathlingContext, data: datasource.DataSource):
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


def save_final_df(df, settings):
    df_with_id = df.withColumn("ID", monotonically_increasing_id())

    df_with_id = df_with_id.coalesce(1)
    output_dir = os.path.join(settings.output_folder, settings.study_name)
    temp_dir = os.path.join(settings.output_folder, settings.study_name, "temp")

    os.makedirs(temp_dir, exist_ok=True)
    df_with_id.write.mode("overwrite").csv(temp_dir, header=True)
    output_file = os.path.join(output_dir, settings.output_filename)
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
    shutil.move(os.path.join(temp_dir, part_file), output_file)
    shutil.rmtree(temp_dir)


def prepare_datadictionary_PoC(df):
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


# optionally pass lists to fill the datadictionary
def generate_datadictionary(
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

    # OPAL DataDictionary predefined colnames
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

    # create datadictionary
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
