import pytest
from pyspark.sql import SparkSession, Row

from utils_onco_analytics import (
    group_entities,
    map_gender,
    map_icd10,
    group_icd_groups,
    deconstruct_date,
    deconstruct_date_year_only,
    calculate_age_at_condition_date,
    add_age_group_small,
    add_age_group_large
)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_map_icd10():
    assert map_icd10("A01") == 101
    assert map_icd10("B99.7") == 299.7
    assert map_icd10("C12.3") == 312.3
    assert map_icd10("D45") == 445
    assert map_icd10("Z99") == 2699

    assert map_icd10("") is None
    assert map_icd10(None) is None

    assert map_icd10("12345") == -1
    assert map_icd10("!") == -1
    assert map_icd10("1A") == -1
    assert map_icd10("B") == -1
    assert map_icd10("A1") == -1

    assert map_icd10("a01") == 101
    assert map_icd10("b99") == 299
    assert map_icd10("  A01  ") == 101

    assert map_icd10("A1A2") == -1
    assert map_icd10("B1B2B3") == -1


def test_group_icd_groups():
    assert group_icd_groups("101.1") == 101
    assert group_icd_groups("101.9") == 101
    assert group_icd_groups("299.3") == 299
    assert group_icd_groups("2699.3") == 2699

    assert group_icd_groups("1123") == -1
    assert group_icd_groups("2699") == -1
    assert group_icd_groups("12345") == -1
    assert group_icd_groups("11") == -1
    assert group_icd_groups("!@#") == -1

    assert group_icd_groups(1) == -1

    assert group_icd_groups(None) is None


@pytest.mark.parametrize(
    "icd10_grouped, expected",
    [
        (309, 0),  # (300, 315, 0),  # Lippe, Mundhöhle und Rachen (C00-C14)
        (409.0, 17),  # (367, 368, 17),(409.0, 409.1, 17), (441.4, 441.5, 17),
        (441.4, 17),  # Harnblase (C67, D09.0, D41.4)
        (316, 2),  # (316, 317, 2),  # Magen (C16)
        (354, 12),  # (354, 356, 12),  # Gebärmutterkörper (C54-C55)
        (318, 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        (319, 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        (320, 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        (321, 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        (395, 23),  # (391, 396, 23),  # Leukämien (C91-C95)
    ],
)
def test_group_entities(icd10_grouped, expected):
    result = group_entities(icd10_grouped)
    assert result == expected


def test_map_gender(spark):
    data = [
        Row(gender=None),
        Row(gender=""),
        Row(gender="female"),
        Row(gender="weiblich"),
        Row(gender="Weiblich"),
        Row(gender="male"),
        Row(gender="männlich"),
        Row(gender="Männlich"),
        Row(gender="other"),
        Row(gender="Other"),
        Row(gender="divers"),
        Row(gender="unknown"),
    ]

    df = spark.createDataFrame(data)

    mapped_df = map_gender(df, "gender")
    results = mapped_df.select("gender_mapped").rdd.flatMap(lambda x: x).collect()

    assert results == [0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3]


def test_deconstruct_date(spark):
    data = [("2024-10-25",), ("2023-01-15",), ("",)]
    columns = ["date_col"]
    df = spark.createDataFrame(data, columns)

    deconstructed_df = deconstruct_date(df, "date_col")

    result = deconstructed_df.collect()

    expected = [
        ("2024-10-25", 2024, 10, 25),
        ("2023-01-15", 2023, 1, 15),
        ("2022-07-30", None, None, None)
    ]

    for actual, expected_row in zip(result, expected):
        assert actual[1] == expected_row[1]  # year
        assert actual[2] == expected_row[2]  # month
        assert actual[3] == expected_row[3]  # day


def test_deconstruct_date_year_only(spark):
    data = [("2024-10-25",), ("2023-01-15",), ("",)]
    columns = ["date_col"]
    df = spark.createDataFrame(data, columns)

    deconstructed_df = deconstruct_date_year_only(df, "date_col")

    result = deconstructed_df.collect()

    expected = [
        (2024,),  # Expected year for the first row
        (2023,),  # Expected year for the second row
        (None,)   # Expected None for empty date
    ]

    for actual, expected_row in zip(result, expected):
        assert actual["date_col_year"] == expected_row[0]


def test_calculate_age_at_condition_date():
    birthdate = "1950-01"
    condition_date = "2006-11-15"
    expected_age = 56
    assert calculate_age_at_condition_date(birthdate, condition_date) == expected_age

    birthdate = None
    condition_date = "2006-11-15"
    assert calculate_age_at_condition_date(birthdate, condition_date) is None

    birthdate = "1950-01"
    condition_date = None
    assert calculate_age_at_condition_date(birthdate, condition_date) is None

    birthdate = "1950-01"
    condition_date = "invalid-date"
    assert calculate_age_at_condition_date(birthdate, condition_date) is None


def test_age_group_small(spark):
    data = [(5,), (15,), (25,), (49,), (70,), (85,), (100,)]
    df = spark.createDataFrame(data, ["age_at_diagnosis"])

    result_df = add_age_group_small(df, "age_at_diagnosis")
    expected = [(5, 0), (15, 1), (25, 3), (49, 7), (70, 12), (85, 15), (100, 15)]

    result = [(row['age_at_diagnosis'], row['age_group_small'])
              for row in result_df.collect()]
    assert result == expected


def test_age_group_large(spark):
    data = [(5,), (15,), (20,), (40,), (70,), (90,), (100,)]
    df = spark.createDataFrame(data, ["age_at_diagnosis"])

    result_df = add_age_group_large(df, "age_at_diagnosis")
    expected = [(5, 0), (15, 1), (20, 1), (40, 3), (70, 6), (90, 8), (100, 9)]

    result = [(row['age_at_diagnosis'], row['age_group_large'])
              for row in result_df.collect()]
    assert result == expected
