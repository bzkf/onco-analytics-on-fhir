from typing import List, Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
def map_gender(gender_string: str) -> int:
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

    df = df.withColumn(year_col, F.year(date_col))
    df = df.withColumn(month_col, F.month(date_col))
    df = df.withColumn(day_col, F.dayofmonth(date_col))

    return df


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

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Variables")
        df_categories.to_excel(writer, index=False, sheet_name="Categories")

    return df
