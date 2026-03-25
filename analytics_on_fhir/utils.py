import glob
import os
import shutil
from collections.abc import Iterable

import pandas as pd
from loguru import logger
from matplotlib.figure import Figure
from pathling import PathlingContext, datasource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    abs,
    col,
    collect_set,
    concat_ws,
    count,
    create_map,
    first,
    lit,
    row_number,
    when,
)
from pyspark.sql.window import Window

HERE = os.path.abspath(os.path.dirname(__file__))

FHIR_SYSTEM_PRIMAERTUMOR = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/"
    "StructureDefinition/mii-pr-onko-diagnose-primaertumor"
)
FHIR_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_SYSTEM_JNU = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-tod"
)
FHIR_SYSTEM_LOINC = "http://loinc.org"
FHIR_SYSTEM_SCT = "http://snomed.info/sct"
FHIR_SYSTEM_METASTASIS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/"
    "CodeSystem/mii-cs-onko-fernmetastasen"
)

SCT_CODE_DEATH = "184305005"
SCT_CODE_GLEASON = "1812491000004107"
SCT_CODE_METASTASIS = "385421009"

FHIR_SYSTEMS_CONDITION_ASSERTED_DATE = (
    "http://hl7.org/fhir/StructureDefinition/condition-assertedDate"
)

FHIR_SYSTEMS_CONDITION_MORPHOLOGY = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/StructureDefinition/mii-ex-onko-histology-morphology-behavior-icdo3"

FHIR_SYSTEMS_WEITERE_KLASSIFIKATION = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/"
    "StructureDefinition/mii-pr-onko-weitere-klassifikationen"
)

FHIR_SYSTEMS_RADIOTHERAPY = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/"
    "mii-pr-onko-strahlentherapie|2026.0.0"
)
FHIR_SYSTEMS_RADIOTHERAPY_BESTRAHLUNG = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/"
    "mii-pr-onko-strahlentherapie-bestrahlung-strahlentherapie|2026.0.0"
)
FHIR_SYSTEMS_SURGERY = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/mii-pr-onko-operation|2026.0.0"
)
FHIR_SYSTEMS_SYSTEM_THERAPY = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/"
    "StructureDefinition/mii-pr-onko-systemische-therapie|2026.0.0"
)
FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/"
    "mii-ex-onko-systemische-therapie-intention"
)
FHIR_SYSTEMS_SYSTEM_THERAPY_STELLUNG_OP = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/"
    "mii-ex-onko-systemische-therapie-stellungzurop"
)
FHIR_SYSTEMS_RADIO_THERAPY_INTENTION = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/mii-ex-onko-"
    "strahlentherapie-intention"
)
FHIR_SYSTEMS_RADIO_THERAPY_STELLUNG_OP = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/"
    "mii-ex-onko-strahlentherapie-stellungzurop"
)
FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-intention"
)
FHIR_SYSTEMS_STELLUNG_OP_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-therapie-stellungzurop"
)
FHIR_SYSTEMS_SYSTEM_THERAPY_TYP_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-therapie-typ"
)
FHIR_SYSTEMS_THERAPY_END_REASON_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-therapie-ende-grund"
)
FHIR_SYSTEMS_RADIO_THERAPY_INTENTION_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-intention"
)

FHIR_SYSTEMS_RADIO_THERAPY_ZIELGEBIET_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-strahlentherapie-zielgebiet"
)
FHIR_SYSTEMS_SURGERY_INTENTION = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/StructureDefinition/mii-ex-onko-operation-intention"
)
FHIR_SYSTEMS_SURGERY_INTENTION_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-intention"
)
FHIR_SYSTEMS_SURGERY_OUTCOME_CS = (
    "https://www.medizininformatik-initiative.de/fhir/ext/"
    "modul-onko/CodeSystem/mii-cs-onko-residualstatus"
)
FHIR_SYSTEMS_SURGERY_OPS_CS = "http://fhir.de/CodeSystem/bfarm/ops"


def save_final_df(pyspark_df, settings, suffix=""):
    logger.info("start save pyspark_df with pyspark_df.coalesce(1).write...csv() ")
    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    os.makedirs(output_dir, exist_ok=True)

    final_csv_path = os.path.join(output_dir, f"df_{suffix}.csv")
    temp_dir = os.path.join(output_dir, "_tmp_csv")

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    pyspark_df.coalesce(1).write.option("header", "true").option("sep", ";").mode("overwrite").csv(
        temp_dir
    )
    part_file = glob.glob(os.path.join(temp_dir, "part-*.csv"))[0]
    shutil.move(part_file, final_csv_path)
    shutil.rmtree(temp_dir)

    logger.info("end save pyspark_df")


def save_final_df_parquet(pyspark_df, settings, suffix=""):
    base_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    parquet_dir = os.path.join(base_dir, "parquet")

    os.makedirs(parquet_dir, exist_ok=True)

    final_path = os.path.join(parquet_dir, f"df_{suffix}.parquet")

    pyspark_df.coalesce(1).write.mode("overwrite").parquet(final_path)


def save_plot(plot: Figure, settings, plot_name: str = "") -> None:
    logger.info("start save plot")
    output_dir = os.path.join(settings.results_directory_path, settings.study_name.value, "plots")
    os.makedirs(output_dir, exist_ok=True)

    final_plot_path = os.path.join(output_dir, f"{plot_name}.png")
    plot.savefig(final_plot_path, bbox_inches="tight", dpi=300)
    logger.info(f"Plot saved to: {final_plot_path}")


def find_closest_to_diagnosis(df, condition_id_colname, date_diagnosis_col, other_date_col):
    df_filtered = df.filter(col(other_date_col).isNotNull())

    window = Window.partitionBy(condition_id_colname).orderBy(
        abs(col(other_date_col).cast("timestamp") - col(date_diagnosis_col).cast("timestamp"))
    )

    return (
        df_filtered.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumnRenamed(other_date_col, other_date_col + "_first")
    )


def compute_age(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "age_at_diagnosis",
        F.round((F.datediff(df["asserted_date"], df["birthdate"]) / 365.25), 2),
    ).drop("birthdate")
    return df


def compute_month_diffs_for_all_date_cols(df: DataFrame) -> DataFrame:
    # Alle Spalten mit "date", außer asserted_date und Spalten mit "precision" oder "year"
    date_cols = [
        c
        for c in df.columns
        if "date" in c.lower()
        and c != "asserted_date"
        and "precision" not in c.lower()
        and "year" not in c.lower()
    ]

    for date_col in date_cols:
        df = df.withColumn(
            f"months_between_asserted_{date_col}",
            F.round(F.months_between(F.col(date_col), F.col("asserted_date")), 2),
        )

    return df


def drop_date_cols(df: DataFrame) -> DataFrame:
    date_cols = [
        c
        for c in df.columns
        if "date" in c.lower()
        and "precision" not in c.lower()
        and "between" not in c.lower()
        and "year" not in c.lower()
    ]

    return df.drop(*date_cols)


def create_year_col_asserted_death(df: DataFrame) -> DataFrame:
    if "asserted_date" in df.columns:
        df = df.withColumn("asserted_year", F.year("asserted_date"))
    if "deceased_datetime" in df.columns:
        df = df.withColumn("deceased_datetime_year", F.year("deceased_datetime"))
    if "date_death" in df.columns:
        df = df.withColumn("date_death_year", F.year("date_death"))

    return df


def detect_date_precision(col):
    return (
        # ISO-Formate
        F.when(col.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.lit("DAY"))
        .when(col.rlike(r"^\d{4}-\d{2}$"), F.lit("MONTH"))
        .when(col.rlike(r"^\d{4}$"), F.lit("YEAR"))
        # Deutsche Formate dd.MM.yyyy oder MM.yyyy
        .when(col.rlike(r"^\d{2}\.\d{2}\.\d{4}$"), F.lit("DAY"))
        .when(col.rlike(r"^\d{2}\.\d{4}$"), F.lit("MONTH"))
        .otherwise(F.lit(None))
    )


# Jahres-/Monatsmitte wird ergänzt wenn Wert fehlt
def parse_date_with_precision(col, precision_col):
    return (
        # Exaktes Datum
        F.when(precision_col == "DAY", F.to_date(col, "yyyy-MM-dd"))
        # Monatsmitte (15.)
        .when(
            precision_col == "MONTH",
            F.to_date(F.concat(col, F.lit("-15")), "yyyy-MM-dd"),
        )
        # Jahresmitte (1. Juli)
        .when(
            precision_col == "YEAR",
            F.to_date(F.concat(col, F.lit("-07-01")), "yyyy-MM-dd"),
        )
        .otherwise(F.lit(None).cast("date"))
    )


def months_diff(df, date_col, reference_date):
    return df.withColumn(
        date_col + "_diff",
        F.round(F.months_between(F.col(date_col), F.col(reference_date)), 2).cast("double"),
    )


def cast_study_dates(df: DataFrame, date_cols: Iterable[str]) -> DataFrame:
    for c in date_cols:
        raw_col = F.col(c)
        precision_col_name = f"{c}_precision"

        df = df.withColumn(precision_col_name, detect_date_precision(raw_col))

        df = df.withColumn(c, parse_date_with_precision(raw_col, F.col(precision_col_name)))

    return df


def add_is_deceased(df):
    return df.withColumn(
        "is_deceased",
        when(
            (col("deceased_boolean") is True)
            | col("deceased_datetime").isNotNull()
            | col("date_death").isNotNull()
            | col("death_cause_icd10").isNotNull()
            | col("death_cause_tumor").isNotNull(),
            True,
        ).otherwise(False),
    )


def deidentify(df: DataFrame, identifying_cols: list[str], df_lookup: DataFrame) -> DataFrame:
    cols_to_drop = [c for c in identifying_cols if c in df.columns]
    df = df.drop(*cols_to_drop)

    df = create_year_col_asserted_death(df)
    df = compute_month_diffs_for_all_date_cols(df)
    df = drop_date_cols(df)

    # all possible condition_id columns
    condition_cols = ["condition_id", "condition_id_1", "condition_id_2"]

    for c in condition_cols:
        if c in df.columns:
            # Lookup-DF für diese Spalte umbenennen
            df_lookup_renamed = df_lookup.withColumnRenamed("condition_id", c)
            df = df.join(df_lookup_renamed, on=c, how="left")
            df = df.drop(c)
            df = df.withColumnRenamed("condition_id_hash", f"{c}_hash")

    return df


def map_gleason_sct_to_score(df, gleason_sct_col="gleason_sct", out_col="gleason_score"):
    gleason_map_expr = create_map(
        lit("1279715000"),
        lit(6),  # 3+3
        lit("1279714001"),
        lit(7),  # 3+4
        lit("1279716004"),
        lit(7),  # 4+3
        lit("1279718003"),
        lit(8),  # 3+5
        lit("1279717008"),
        lit(8),  # 4+4
        lit("1279719006"),
        lit(8),  # 5+3
        lit("1279720000"),
        lit(9),  # 4+5
        lit("1279721001"),
        lit(9),  # 5+4
        lit("1279722008"),
        lit(10),  # 5+5
    )

    return df.withColumn(out_col, gleason_map_expr[col(gleason_sct_col)])


def extract_conditions_patients_death(
    pc: PathlingContext,
    data: datasource.DataSource,
    spark: SparkSession,
    settings,
):
    logger.info("logger start, extract_df_study_protocol_a_d_mii.")
    patients = data.view(
        "Patient",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "patient_resource_id",
                        "description": "Patient Resource Key",
                    },
                    {
                        "path": "identifier.value",
                        "name": "patid_pseudonym",
                        "description": "Patient pseudonym",
                    },
                    {
                        "path": "birthDate",
                        "name": "birthdate",
                        "description": "Birth Date",
                    },
                    {
                        "path": "gender",
                        "name": "gender",
                        "description": "Gender",
                    },
                    {
                        "path": "deceased.ofType(dateTime)",
                        "name": "deceased_datetime",
                        "description": "Deceased DateTime",
                    },
                    {
                        "path": "deceased.ofType(boolean)",
                        "name": "deceased_boolean",
                        "description": "Deceased Boolean",
                    },
                ]
            }
        ],
    )

    patients_count = patients.count()
    logger.info("patients_count = {}", patients_count)
    patients.show()

    conditions = data.view(
        "Condition",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Asserted Date",
                        "path": f"extension('{FHIR_SYSTEMS_CONDITION_ASSERTED_DATE}')"
                        + ".value.ofType(dateTime)",
                        "name": "asserted_date",
                    },
                    {
                        "description": "Recorded Date",
                        "path": "recordedDate",
                        "name": "recorded_date",
                    },
                    {
                        "path": (f"code.coding.where(system = '{FHIR_SYSTEM_ICD10}').code"),
                        "name": "icd10_code",
                    },
                    {
                        "path": "subject.getReferenceKey()",
                        "name": "condition_patient_resource_id",
                    },
                    {
                        "description": "ICD-O-3 M Morphology",
                        "path": f"extension('{FHIR_SYSTEMS_CONDITION_MORPHOLOGY}')"
                        + ".value.ofType(CodeableConcept).coding.first().code",
                        "name": "icdo3_morphology",
                    },
                ]
            }
        ],
    )

    conditions_count = conditions.count()
    logger.info("conditions_count = {}", conditions_count)
    conditions.show()

    conditions_patients = conditions.alias("c").join(
        patients.alias("p"),
        col("c.condition_patient_resource_id") == col("p.patient_resource_id"),
        "left",
    )

    conditions_patients_count = conditions_patients.count()

    logger.info("conditions_patients_count = {}", conditions_patients_count)
    conditions_patients.show()

    observation_death = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "observation_resource_id",
                    },
                    {
                        "path": (
                            f"value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_ICD10}')"
                            f".code"
                        ),
                        "name": "death_cause_icd10",
                    },
                    {
                        "path": (f"interpretation.coding.where(system = '{FHIR_SYSTEM_JNU}').code"),
                        "name": "death_cause_tumor",
                    },
                    {
                        "path": "effective.ofType(dateTime)",
                        "name": "date_death",
                    },
                    {
                        "path": "subject.getReferenceKey()",
                        "name": "observation_death_patient_resource_id",
                    },
                ]
            }
        ],
        where=[
            {
                "path": (
                    f"code.coding"
                    f""".where(system = '{FHIR_SYSTEM_SCT}' and
                    code = '{SCT_CODE_DEATH}')"""
                    f".exists()"
                )
            }
        ],
    )

    observation_death_count = observation_death.count()

    logger.info("observation_death_count = {}", observation_death_count)
    observation_death = observation_death.orderBy("observation_death_patient_resource_id")
    observation_death.show()

    # show duplicates
    duplicates = (
        observation_death.groupBy("observation_death_patient_resource_id")
        .count()
        .filter(col("count") > 1)
    )

    observation_death_dupes = observation_death.join(
        duplicates.select("observation_death_patient_resource_id"),
        on="observation_death_patient_resource_id",
        how="inner",
    )
    observation_death_dupes_count = observation_death_dupes.count()
    logger.info("observation_death_dupes_count = {}", observation_death_dupes_count)
    observation_death_dupes.orderBy("observation_death_patient_resource_id").show(100)

    # count distinct patients with death observation
    observation_death_distinct_patients_count = (
        observation_death.select("observation_death_patient_resource_id").distinct().count()
    )

    logger.info(
        "observation_death_distinct_patients_count = {}",
        observation_death_distinct_patients_count,
    )

    # ACHTUNG WORKAROUND weil doppelte Todesobservations - REMOVE
    # Vorgehen:
    # ich würd an der Stelle dann nach patient_resource_id, death_cause_icd10 und
    # date_death gruppieren und death_cause_tumor dann die falschen "N" nochmal
    # nachbearbeiten basierend auf death_cause_icd10
    observation_death = observation_death.groupBy(
        "observation_death_patient_resource_id",
        "death_cause_icd10",
        "date_death",
    ).agg(
        first("observation_resource_id", ignorenulls=True).alias("observation_resource_id"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
    )

    tumor_flag_fix_count = observation_death.filter(
        (col("death_cause_icd10").startswith("C")) & (col("death_cause_tumor").isin("U", "N"))
    ).count()

    logger.info(
        "death_cause_tumor auf 'J' gesetzt bei {} Datensätzen",
        tumor_flag_fix_count,
    )

    observation_death.orderBy("observation_death_patient_resource_id").show(100)
    observation_death_count = observation_death.count()
    logger.info("observation_death_count after grouping = {}", observation_death_count)
    # count distinct patients with death observation
    observation_death_distinct_patients_count = (
        observation_death.select("observation_death_patient_resource_id").distinct().count()
    )

    logger.info(
        "observation_death_distinct_patients_count after grouping = {}",
        observation_death_distinct_patients_count,
    )

    # dupes after grouping
    # show duplicates
    duplicates = (
        observation_death.groupBy("observation_death_patient_resource_id")
        .count()
        .filter(col("count") > 1)
    )

    observation_death_dupes = observation_death.join(
        duplicates.select("observation_death_patient_resource_id"),
        on="observation_death_patient_resource_id",
        how="inner",
    )
    observation_death_dupes_count = observation_death_dupes.count()
    logger.info(
        "observation_death_dupes_count after grouping = {}",
        observation_death_dupes_count,
    )
    observation_death_dupes.orderBy("observation_death_patient_resource_id").show(100)

    # concat multiple todesursachen icd10 into one col , seperated
    death_cause_icd10_per_patient = (
        observation_death.filter(col("death_cause_icd10").isNotNull())
        .groupBy("observation_death_patient_resource_id")
        .agg(
            concat_ws("|", F.sort_array(collect_set("death_cause_icd10"))).alias(
                "death_cause_icd10"
            )
        )
    )

    # group again by observation_death_patient_resource_id and keep first non null
    observation_death_patient = observation_death.groupBy(
        "observation_death_patient_resource_id"
    ).agg(
        first("date_death", ignorenulls=True).alias("date_death"),
        first("observation_resource_id", ignorenulls=True).alias("observation_resource_id"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
    )

    # join back comma seperated death causes
    observation_death_patient = observation_death_patient.join(
        death_cause_icd10_per_patient,
        on="observation_death_patient_resource_id",
        how="left",
    )

    observation_death_patient_count = observation_death_patient.count()
    logger.info(
        "observation_death_patient_count after grouping 2 = {}",
        observation_death_patient_count,
    )

    observation_death_patient.orderBy("observation_death_patient_resource_id").show(100)

    observation_death_patient.filter(col("death_cause_icd10").contains("|")).show(
        100, truncate=False
    )

    # join death observations to conditions_patients
    conditions_patients_death = (
        conditions_patients.alias("cp")
        .join(
            observation_death_patient.alias("od"),
            col("cp.patient_resource_id") == col("od.observation_death_patient_resource_id"),
            "left",
        )
        .select("cp.*", "od.*")
    )

    conditions_patients_death_count = conditions_patients_death.count()

    logger.info(
        "conditions_patients_death_count = {}",
        conditions_patients_death_count,
    )

    # add consolidated is_deceased flag from death information patient/observation
    conditions_patients_death = add_is_deceased(conditions_patients_death)
    logger.info("Add is deceased flag.")
    conditions_patients_death.show()

    # add double_patid column for studyprotocol D
    window_spec = Window.partitionBy("patid_pseudonym")

    conditions_patients_death = conditions_patients_death.withColumn(
        "double_patid",
        when(count("patid_pseudonym").over(window_spec) > 1, 1).otherwise(0),
    ).orderBy("patid_pseudonym")

    conditions_patients_death_count = conditions_patients_death.count()
    logger.info(
        "added double_patid col for study protocol D, \
        conditions_patients_death_count = {}",
        conditions_patients_death_count,
    )
    double_patid_count = conditions_patients_death.filter(col("double_patid") == 1).count()

    logger.info(
        "Anzahl double_patid = 1 (Patienten mit mehreren Conditions) = {}",
        double_patid_count,
    )

    return conditions_patients_death


def extract_df_study_protocol_a_d_mii(
    pc: PathlingContext,
    data: datasource.DataSource,
    spark: SparkSession,
    settings,
):
    logger.info("logger start, extract_df_study_protocol_a_d_mii.")
    patients = data.view(
        "Patient",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "patient_resource_id",
                        "description": "Patient Resource Key",
                    },
                    {
                        "path": "identifier.value",
                        "name": "patid_pseudonym",
                        "description": "Patient pseudonym",
                    },
                    {
                        "path": "birthDate",
                        "name": "birthdate",
                        "description": "Birth Date",
                    },
                    {
                        "path": "gender",
                        "name": "gender",
                        "description": "Gender",
                    },
                    {
                        "path": "deceased.ofType(dateTime)",
                        "name": "deceased_datetime",
                        "description": "Deceased DateTime",
                    },
                    {
                        "path": "deceased.ofType(boolean)",
                        "name": "deceased_boolean",
                        "description": "Deceased Boolean",
                    },
                ]
            }
        ],
    )

    patients_count = patients.count()
    logger.info("patients_count = {}", patients_count)
    patients.show()

    conditions = data.view(
        "Condition",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Asserted Date",
                        "path": f"extension('{FHIR_SYSTEMS_CONDITION_ASSERTED_DATE}')"
                        + ".value.ofType(dateTime)",
                        "name": "asserted_date",
                    },
                    {
                        "description": "Recorded Date",
                        "path": "recordedDate",
                        "name": "recorded_date",
                    },
                    {
                        "path": (f"code.coding.where(system = '{FHIR_SYSTEM_ICD10}').code"),
                        "name": "icd10_code",
                    },
                    {
                        "path": "subject.getReferenceKey()",
                        "name": "condition_patient_resource_id",
                    },
                ]
            }
        ],
    )

    conditions_count = conditions.count()
    logger.info("conditions_count = {}", conditions_count)
    conditions.show()

    conditions_patients = conditions.alias("c").join(
        patients.alias("p"),
        col("c.condition_patient_resource_id") == col("p.patient_resource_id"),
        "left",
    )

    conditions_patients_count = conditions_patients.count()

    logger.info("conditions_patients_count = {}", conditions_patients_count)
    conditions_patients.show()

    observation_death = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "observation_resource_id",
                    },
                    {
                        "path": (
                            f"value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_ICD10}')"
                            f".code"
                        ),
                        "name": "death_cause_icd10",
                    },
                    {
                        "path": (f"interpretation.coding.where(system = '{FHIR_SYSTEM_JNU}').code"),
                        "name": "death_cause_tumor",
                    },
                    {
                        "path": "effective.ofType(dateTime)",
                        "name": "date_death",
                    },
                    {
                        "path": "subject.getReferenceKey()",
                        "name": "observation_death_patient_resource_id",
                    },
                ]
            }
        ],
        where=[
            {
                "path": (
                    f"code.coding"
                    f""".where(system = '{FHIR_SYSTEM_SCT}' and
                    code = '{SCT_CODE_DEATH}')"""
                    f".exists()"
                )
            }
        ],
    )

    observation_death_count = observation_death.count()

    logger.info("observation_death_count = {}", observation_death_count)
    observation_death = observation_death.orderBy("observation_death_patient_resource_id")
    observation_death.show()

    # show duplicates
    duplicates = (
        observation_death.groupBy("observation_death_patient_resource_id")
        .count()
        .filter(col("count") > 1)
    )

    observation_death_dupes = observation_death.join(
        duplicates.select("observation_death_patient_resource_id"),
        on="observation_death_patient_resource_id",
        how="inner",
    )
    observation_death_dupes_count = observation_death_dupes.count()
    logger.info("observation_death_dupes_count = {}", observation_death_dupes_count)
    observation_death_dupes.orderBy("observation_death_patient_resource_id").show(100)

    # count distinct patients with death observation
    observation_death_distinct_patients_count = (
        observation_death.select("observation_death_patient_resource_id").distinct().count()
    )

    logger.info(
        "observation_death_distinct_patients_count = {}",
        observation_death_distinct_patients_count,
    )

    # ACHTUNG WORKAROUND weil doppelte Todesobservations - REMOVE
    # Vorgehen:
    # ich würd an der Stelle dann nach patient_resource_id, death_cause_icd10 und
    # date_death gruppieren und death_cause_tumor dann die falschen "N" nochmal
    # nachbearbeiten basierend auf death_cause_icd10
    observation_death = observation_death.groupBy(
        "observation_death_patient_resource_id",
        "death_cause_icd10",
        "date_death",
    ).agg(
        first("observation_resource_id", ignorenulls=True).alias("observation_resource_id"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
    )

    tumor_flag_fix_count = observation_death.filter(
        (col("death_cause_icd10").startswith("C")) & (col("death_cause_tumor").isin("U", "N"))
    ).count()

    logger.info(
        "death_cause_tumor auf 'J' gesetzt bei {} Datensätzen",
        tumor_flag_fix_count,
    )

    observation_death = observation_death.withColumn(
        "death_cause_tumor",
        when(
            (col("death_cause_icd10").startswith("C")) & (col("death_cause_tumor").isin("U", "N")),
            "J",
        ).otherwise(col("death_cause_tumor")),
    )

    observation_death.orderBy("observation_death_patient_resource_id").show(100)
    observation_death_count = observation_death.count()
    logger.info("observation_death_count after grouping = {}", observation_death_count)
    # count distinct patients with death observation
    observation_death_distinct_patients_count = (
        observation_death.select("observation_death_patient_resource_id").distinct().count()
    )

    logger.info(
        "observation_death_distinct_patients_count after grouping = {}",
        observation_death_distinct_patients_count,
    )

    # dupes after grouping
    # show duplicates
    duplicates = (
        observation_death.groupBy("observation_death_patient_resource_id")
        .count()
        .filter(col("count") > 1)
    )

    observation_death_dupes = observation_death.join(
        duplicates.select("observation_death_patient_resource_id"),
        on="observation_death_patient_resource_id",
        how="inner",
    )
    observation_death_dupes_count = observation_death_dupes.count()
    logger.info(
        "observation_death_dupes_count after grouping = {}",
        observation_death_dupes_count,
    )
    observation_death_dupes.orderBy("observation_death_patient_resource_id").show(100)

    # concat multiple todesursachen icd10 into one col , seperated
    death_cause_icd10_per_patient = (
        observation_death.filter(col("death_cause_icd10").isNotNull())
        .groupBy("observation_death_patient_resource_id")
        .agg(
            concat_ws("|", F.sort_array(collect_set("death_cause_icd10"))).alias(
                "death_cause_icd10"
            )
        )
    )

    # group again by observation_death_patient_resource_id and keep first non null
    observation_death_patient = observation_death.groupBy(
        "observation_death_patient_resource_id"
    ).agg(
        first("date_death", ignorenulls=True).alias("date_death"),
        first("observation_resource_id", ignorenulls=True).alias("observation_resource_id"),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
    )

    # join back comma seperated death causes
    observation_death_patient = observation_death_patient.join(
        death_cause_icd10_per_patient,
        on="observation_death_patient_resource_id",
        how="left",
    )

    observation_death_patient_count = observation_death_patient.count()
    logger.info(
        "observation_death_patient_count after grouping 2 = {}",
        observation_death_patient_count,
    )

    observation_death_patient.orderBy("observation_death_patient_resource_id").show(100)

    observation_death_patient.filter(col("death_cause_icd10").contains(",")).show(
        100, truncate=False
    )

    # join death observations to conditions_patients
    conditions_patients_death = (
        conditions_patients.alias("cp")
        .join(
            observation_death_patient.alias("od"),
            col("cp.patient_resource_id") == col("od.observation_death_patient_resource_id"),
            "left",
        )
        .select("cp.*", "od.*")
    )

    conditions_patients_death_count = conditions_patients_death.count()

    logger.info(
        "conditions_patients_death_count = {}",
        conditions_patients_death_count,
    )

    # add consolidated is_deceased flag from death information patient/observation
    conditions_patients_death = add_is_deceased(conditions_patients_death)
    logger.info("Add is deceased flag.")
    conditions_patients_death.show()

    # add double_patid column for studyprotocol D
    window_spec = Window.partitionBy("patid_pseudonym")

    conditions_patients_death = conditions_patients_death.withColumn(
        "double_patid",
        when(count("patid_pseudonym").over(window_spec) > 1, 1).otherwise(0),
    ).orderBy("patid_pseudonym")

    conditions_patients_death_count = conditions_patients_death.count()
    logger.info(
        "added double_patid col for study protocol D, \
        conditions_patients_death_count = {}",
        conditions_patients_death_count,
    )
    double_patid_count = conditions_patients_death.filter(col("double_patid") == 1).count()

    logger.info(
        "Anzahl double_patid = 1 (Patienten mit mehreren Conditions) = {}",
        double_patid_count,
    )

    # GLEASON observations
    observations_gleason = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "description": "Observation ID",
                        "path": "getResourceKey()",
                        "name": "observation_gleason_resource_id",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "observation_gleason_condition_resource_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "observation_gleason_patient_resource_id",
                    },
                    {
                        "description": "Gleason Grade Group (ordinalValue)",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            ".coding"
                            f".where(system = '{FHIR_SYSTEM_SCT}')"
                            ".code"
                        ),
                        "name": "gleason_sct",
                    },
                    {
                        "description": "Gleason Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "gleason_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": "Histologic grade of primary malignant neoplasm of prostate",
                "path": (
                    "code.coding.exists(system = "
                    f"'{FHIR_SYSTEM_SCT}' and code = '{SCT_CODE_GLEASON}')"
                ),
            }
        ],
    )
    if observations_gleason.isEmpty():
        logger.warning("No GLEASON observations found - skipping join.")

        return conditions_patients_death

    observations_gleason_count = observations_gleason.count()
    logger.info("observations_gleason_count = {}", observations_gleason_count)
    observations_gleason.show()

    logger.info(
        "Number of Gleason observations not null = {}",
        observations_gleason.filter(col("gleason_sct").isNotNull()).count(),
    )

    # join condition ids
    conditions_gleason_raw = (
        conditions.alias("c")
        .join(
            observations_gleason.alias("o"),
            col("c.condition_id") == col("o.observation_gleason_condition_resource_id"),
            "left",
        )
        .select(
            col("c.condition_id"),
            col("c.asserted_date"),
            col("o.observation_gleason_resource_id"),
            col("o.gleason_sct"),
            col("o.gleason_date"),
        )
    )

    # find closest to diagnosis
    conditions_gleason_first = find_closest_to_diagnosis(
        df=conditions_gleason_raw,
        condition_id_colname="condition_id",
        date_diagnosis_col="asserted_date",
        other_date_col="gleason_date",
    )

    conditions_gleason_first = conditions_gleason_first.select(
        "condition_id",
        "gleason_sct",
        "gleason_date_first",
    )

    # map sct to score
    conditions_gleason_first = map_gleason_sct_to_score(
        conditions_gleason_first,
        gleason_sct_col="gleason_sct",
        out_col="gleason_score",
    )

    conditions_gleason_first_count = conditions_gleason_first.count()

    logger.info(
        "conditions_gleason_first_count (1 Gleason pro Condition) = {}",
        conditions_gleason_first_count,
    )
    conditions_gleason_first.show()

    logger.info(
        "sanity check double condition_ids count in conditions_gleason_first = {}",
        conditions_gleason_first.groupBy("condition_id").count().filter(col("count") > 1).count(),
    )

    conditions_patients_death_gleason = conditions_patients_death.join(
        conditions_gleason_first,
        on="condition_id",
        how="left",
    )

    conditions_patients_death_gleason_count = conditions_patients_death_gleason.count()

    logger.info(
        "conditions_patients_death_gleason_count (1 Gleason pro Condition) = {}",
        conditions_patients_death_gleason_count,
    )
    conditions_patients_death_gleason.show()

    # Fernmetastase observations
    observations_metastasis = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "description": "Observation ID",
                        "path": "getResourceKey()",
                        "name": "observation_metastasis_resource_id",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "observation_metastasis_condition_resource_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "observation_metastasis_patient_resource_id",
                    },
                    {
                        "description": "Metastasis Localization",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_METASTASIS}')"
                            ".code"
                        ),
                        "name": "metastasis_loc",
                    },
                    {
                        "description": "Metastasis Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "metastasis_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": "Anatomic location of metastatic spread of malignant "
                "neoplasm (observable entity)",
                "path": (
                    "code.coding.exists(system = "
                    f"'{FHIR_SYSTEM_SCT}' and code = '{SCT_CODE_METASTASIS}')"
                ),
            }
        ],
    )
    logger.info(
        "observations_metastasis count = {}",
        observations_metastasis.count(),
    )
    # count distinct conditions
    observation_death_distinct_patients_count = (
        observation_death.select("observation_death_patient_resource_id").distinct().count()
    )

    logger.info(
        "observations_metastasis distinct condition id count = {}",
        observations_metastasis.select("observation_metastasis_condition_resource_id")
        .distinct()
        .count(),
    )

    metastasis_by_cond_id_and_date_count = observations_metastasis.groupBy(
        "observation_metastasis_condition_resource_id",
        "metastasis_date",
    ).count()

    logger.info(
        "metastasis_by_cond_id_and_date_count = {}",
        metastasis_by_cond_id_and_date_count.count(),
    )

    observations_metastasis.show()

    metastasis_grouped = observations_metastasis.groupBy(
        "observation_metastasis_condition_resource_id",
        "metastasis_date",
    ).agg(
        concat_ws("|", F.sort_array(collect_set("metastasis_loc"))).alias("metastasis_loc"),
        first("observation_metastasis_patient_resource_id", ignorenulls=True).alias(
            "observation_metastasis_patient_resource_id"
        ),
    )

    logger.info(
        "metastasis_grouped count = {}",
        metastasis_grouped.count(),
    )
    metastasis_grouped.show()

    # find closest to diagnosis: join condition ids for asserted date
    metastasis_grouped = (
        conditions.alias("c")
        .join(
            metastasis_grouped.alias("m"),
            col("c.condition_id") == col("m.observation_metastasis_condition_resource_id"),
            "left",
        )
        .select(
            col("c.asserted_date"),
            col("m.observation_metastasis_condition_resource_id"),
            col("m.metastasis_date"),
            col("m.metastasis_loc"),
        )
    )
    logger.info(
        "metastasis_grouped count after join to conditions with asserted = {}",
        metastasis_grouped.count(),
    )
    metastasis_grouped.show()

    # find closest to diagnosis
    metastasis_grouped_first = find_closest_to_diagnosis(
        df=metastasis_grouped,
        condition_id_colname="observation_metastasis_condition_resource_id",
        date_diagnosis_col="asserted_date",
        other_date_col="metastasis_date",
    )

    metastasis_grouped_first = metastasis_grouped_first.select(
        "observation_metastasis_condition_resource_id",
        "metastasis_loc",
        "metastasis_date_first",
    )

    logger.info(
        "metastasis_grouped_first count after find closest to diagnosis = {}",
        metastasis_grouped_first.count(),
    )

    conditions_patients_death_gleason_metastasis = conditions_patients_death_gleason.alias(
        "c"
    ).join(
        metastasis_grouped_first.alias("m"),
        col("c.condition_id") == col("m.observation_metastasis_condition_resource_id"),
        "left",
    )

    conditions_patients_death_gleason_metastasis_count = (
        conditions_patients_death_gleason_metastasis.count()
    )

    logger.info(
        "conditions_patients_death_gleason_metastasis_count = {}",
        conditions_patients_death_gleason_metastasis_count,
    )
    conditions_patients_death_gleason_metastasis.show()

    return conditions_patients_death_gleason_metastasis


def extract_metastasis(
    pc: PathlingContext,
    data: datasource.DataSource,
    spark: SparkSession,
    settings,
):
    # Fernmetastase observations
    observations_metastasis = data.view(
        "Observation",
        select=[
            {
                "column": [
                    # {
                    #    "description": "Observation ID",
                    #    "path": "getResourceKey()",
                    #    "name": "observation_metastasis_resource_id",
                    # },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    # {
                    #    "description": "Patient ID",
                    #    "path": "subject.getReferenceKey()",
                    #    "name": "observation_metastasis_patient_resource_id",
                    # },
                    {
                        "description": "Metastasis Localization",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_METASTASIS}')"
                            ".code"
                        ),
                        "name": "metastasis_loc",
                    },
                    {
                        "description": "Metastasis Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "metastasis_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": "Anatomic location of metastatic spread of malignant "
                "neoplasm (observable entity)",
                "path": (
                    "code.coding.exists(system = "
                    f"'{FHIR_SYSTEM_SCT}' and code = '{SCT_CODE_METASTASIS}')"
                ),
            }
        ],
    )
    logger.info(
        "observations_metastasis count = {}",
        observations_metastasis.count(),
    )
    return observations_metastasis


def extract_systemtherapies(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:
    logger.info("extract procedures / system therapies.")

    df_procedures = data.view(
        "Procedure",
        select=[
            {
                "column": [
                    {
                        "description": "Procedure ID",
                        "path": "getResourceKey()",
                        "name": "therapy_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "reason_reference",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "subject_reference",
                    },
                    {
                        "description": "Intention System Therapy",
                        "path": f"extension('{FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION}')"
                        ".value.ofType(CodeableConcept).coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION_CS}')"
                        ".code",
                        "name": "therapy_intention",
                    },
                    {
                        "description": "Stellung zur Op System Therapy",
                        "path": (
                            f"extension('{FHIR_SYSTEMS_SYSTEM_THERAPY_STELLUNG_OP}')"
                            ".value.ofType(CodeableConcept).coding"
                            ".where(system = "
                            f"'{FHIR_SYSTEMS_STELLUNG_OP_CS}')"
                            ".code"
                        ),
                        "name": "stellung_op",
                    },
                    {
                        "description": "Art der systemischen oder abwartenden Therapie",
                        "path": (
                            "code.coding"
                            f".where(system = '{FHIR_SYSTEMS_SYSTEM_THERAPY_TYP_CS}')"
                            ".code"
                        ),
                        "name": "therapy_type",
                    },
                    {
                        "description": "Systemische Therapie Beginn",
                        "path": "performed.ofType(Period).start",
                        "name": "therapy_start_date",
                    },
                    {
                        "description": "Systemische Therapie Ende",
                        "path": "performed.ofType(Period).end",
                        "name": "therapy_end_date",
                    },
                    {
                        "description": "Systemische Therapie Ende Grund",
                        "path": "outcome.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_THERAPY_END_REASON_CS}')"
                        ".code",
                        "name": "therapy_end_reason",
                    },
                    {
                        "description": "Protokoll der systemischen Therapie",
                        "path": "usedCode.text",
                        "name": "therapy_protocol_text",
                    },
                ],
            }
        ],
        where=[
            {
                "description": "Only SYSTEM THERAPY Procedures",
                "path": (f"meta.profile.exists($this = '{FHIR_SYSTEMS_SYSTEM_THERAPY}')"),
            }
        ],
    )
    logger.info("df_procedures count = {}", df_procedures.count())
    logger.info(
        "df_procedures distinct therapy_id count = {}",
        df_procedures.select("therapy_id").distinct().count(),
    )
    logger.info(
        "df_procedures distinct subject_reference patient id count = {}",
        df_procedures.select("subject_reference").distinct().count(),
    )
    df_procedures.orderBy(F.col("reason_reference")).show(truncate=False)

    logger.info("extract medication statements / system therapies.")

    df_medication_statements = data.view(
        "MedicationStatement",
        select=[
            {
                "column": [
                    {
                        "description": "MedicationStatement ID",
                        "path": "getResourceKey()",
                        "name": "medication_statement_id",
                    },
                    {
                        "description": "Procedure Reference",
                        "path": "partOf.getReferenceKey()",
                        "name": "part_of_reference",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "subject_reference",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "reason_reference",
                    },
                    {
                        "description": "Effective Start Date",
                        "path": "effective.ofType(Period).start",
                        "name": "medication_statement_start_date",
                    },
                    {
                        "description": "Medication Text",
                        "path": "medication.ofType(CodeableConcept).text",
                        "name": "medication_statement_text",
                    },
                    {
                        "description": "Medication Code",
                        "path": "medication.ofType(CodeableConcept).coding"
                        + ".where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                        "name": "medication_statement_atc_code",
                    },
                ],
            }
        ],
    )

    # TO DO: split this later
    # return df_procedures, df_medication_statements

    # erst joinen (procedure+medicationstatements) und danach gruppieren
    # Substanzen pro condition und therapy start date |-separiert abspeichern
    df_procedures_medication_statements = (
        df_procedures.alias("p")
        .join(
            df_medication_statements.alias("m"),
            F.col("p.therapy_id") == F.col("m.part_of_reference"),
            how="left",
        )
        .select(
            F.col("p.therapy_id"),
            F.coalesce(
                F.col("p.subject_reference"),
                F.col("m.subject_reference"),
            ).alias("subject_reference"),
            F.coalesce(
                F.col("p.reason_reference"),
                F.col("m.reason_reference"),
            ).alias("reason_reference"),
            "p.meta_profile",
            "p.therapy_intention",
            "p.stellung_op",
            "p.therapy_type",
            "p.therapy_start_date",
            "p.therapy_end_date",
            "p.therapy_end_reason",
            "p.therapy_protocol_text",
            "m.medication_statement_start_date",
            "m.medication_statement_text",
            "m.medication_statement_atc_code",
            "m.part_of_reference",
        )
    )

    # pro condition und start date: | separiert abspeichern
    df_procedures_medication_statements_grouped = df_procedures_medication_statements.groupBy(
        "subject_reference",
        "reason_reference",
        "therapy_start_date",
        "part_of_reference",
    ).agg(
        F.concat_ws("| ", F.sort_array(F.collect_set("medication_statement_text"))).alias(
            "medication_statement_text"
        ),
        F.concat_ws("| ", F.sort_array(F.collect_set("medication_statement_atc_code"))).alias(
            "medication_statement_atc_code"
        ),
    )
    logger.info(
        "df_procedures_medication_statements_grouped count = {}",
        df_procedures_medication_statements_grouped.count(),
    )

    # join back missing cols
    df_procedures_medication_statements_final = (
        df_procedures.alias("p")
        .join(
            df_procedures_medication_statements_grouped.alias("pg"),
            F.col("p.therapy_id") == F.col("pg.part_of_reference"),
            how="left",
        )
        .select(
            F.col("p.therapy_id").alias("therapy_id"),
            F.col("p.subject_reference"),
            F.col("p.reason_reference"),
            F.col("p.meta_profile").alias("meta_profile"),
            F.col("p.therapy_intention").alias("therapy_intention"),
            F.col("p.stellung_op").alias("stellung_op"),
            F.col("p.therapy_type").alias("therapy_type"),
            F.col("p.therapy_start_date").alias("therapy_start_date"),
            F.col("p.therapy_end_date").alias("therapy_end_date"),
            F.col("p.therapy_end_reason").alias("therapy_end_reason"),
            F.col("p.therapy_protocol_text").alias("therapy_protocol_text"),
            F.col("pg.medication_statement_text").alias("medication_statement_text"),
            F.col("pg.medication_statement_atc_code").alias("medication_statement_atc_code"),
        )
    )

    df_procedures_medication_statements_final.orderBy(F.col("reason_reference")).show()

    return df_procedures_medication_statements_final


def extract_radiotherapies(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:
    logger.info("extract radiotherapies.")

    df_procedures = data.view(
        "Procedure",
        select=[
            {
                "column": [
                    {
                        "description": "Procedure ID",
                        "path": "getResourceKey()",
                        "name": "therapy_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Procedure Reference",
                        "path": "partOf.getReferenceKey()",
                        "name": "part_of_reference",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "reason_reference",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "subject_reference",
                    },
                    {
                        "description": "Intention Radio Therapy",
                        "path": f"extension('{FHIR_SYSTEMS_RADIO_THERAPY_INTENTION}')"
                        ".value.ofType(CodeableConcept).coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_RADIO_THERAPY_INTENTION_CS}')"
                        ".code",
                        "name": "therapy_intention",
                    },
                    {
                        "description": "Stellung zur Op Radio Therapy",
                        "path": (
                            f"extension('{FHIR_SYSTEMS_RADIO_THERAPY_STELLUNG_OP}')"
                            ".value.ofType(CodeableConcept).coding"
                            ".where(system = "
                            f"'{FHIR_SYSTEMS_STELLUNG_OP_CS}')"
                            ".code"
                        ),
                        "name": "stellung_op",  # sieht kaputt aus to do
                    },
                    {
                        "description": "Radio Therapy Start",
                        "path": "performed.ofType(Period).start",
                        "name": "therapy_start_date",
                    },
                    {
                        "description": "Radio Therapy Ende",
                        "path": "performed.ofType(Period).end",
                        "name": "therapy_end_date",
                    },
                    {
                        "description": "Radio Therapy Ende Grund",
                        "path": "outcome.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_THERAPY_END_REASON_CS}')"
                        ".code",
                        "name": "therapy_end_reason",
                    },
                    {
                        "description": "Radio Therapy Zielgebiet",
                        "path": (
                            "bodySite.coding.where("
                            f"system='{FHIR_SYSTEMS_RADIO_THERAPY_ZIELGEBIET_CS}'"
                            ").code"
                        ),
                        "name": "zielgebiet",
                    },
                ],
            }
        ],
    )

    df_parents_radiotherapies = df_procedures.filter(
        F.col("meta_profile") == FHIR_SYSTEMS_RADIOTHERAPY
    )

    df_children_bestrahlung = df_procedures.filter(
        F.col("meta_profile") == FHIR_SYSTEMS_RADIOTHERAPY_BESTRAHLUNG
    )

    return df_parents_radiotherapies, df_children_bestrahlung


def join_radiotherapies(
    df_parents_radiotherapies: DataFrame,
    df_children_bestrahlung: DataFrame,
) -> DataFrame:

    df_radiotherapies = (
        df_parents_radiotherapies.alias("p")
        .join(
            df_children_bestrahlung.alias("c"),
            F.col("p.therapy_id") == F.col("c.part_of_reference"),
            how="left",
        )
        .select(
            F.col("p.therapy_id").alias("therapy_id"),
            F.col("p.meta_profile"),
            F.col("p.subject_reference"),
            F.col("p.reason_reference"),
            F.col("p.therapy_intention"),
            F.col("p.stellung_op"),
            F.col("p.therapy_start_date"),
            F.col("p.therapy_end_date"),
            F.col("p.therapy_end_reason"),
            F.col("c.zielgebiet").alias("zielgebiet"),
            F.col("c.therapy_id").alias("therapy_id_child"),
        )
    )

    """ logger.info("df_radiotherapies count = {}", df_radiotherapies.count())

    df_radiotherapies.orderBy(F.col("therapy_id")).show() """

    # pro therapy id, condition id - Zielgebiete | separiert abspeichern
    df_radiotherapies_grouped = df_radiotherapies.groupBy(
        "therapy_id",
        "subject_reference",
        "reason_reference",
    ).agg(
        F.concat_ws("| ", F.sort_array(F.collect_set("zielgebiet"))).alias("zielgebiet"),
        F.concat_ws("| ", F.sort_array(F.collect_set("therapy_id_child"))).alias(
            "therapy_id_child"
        ),
    )
    """ logger.info(
        "df_radiotherapies_grouped count = {}",
        df_radiotherapies_grouped.count(),
    )
    df_radiotherapies_grouped.show() """

    # join back missing cols
    df_radiotherapies_final = (
        df_parents_radiotherapies.alias("p")
        .join(
            df_radiotherapies_grouped.alias("rg"),
            F.col("p.therapy_id") == F.col("rg.therapy_id"),
            how="left",
        )
        .select(
            F.col("p.therapy_id"),
            F.col("p.meta_profile"),
            F.col("p.subject_reference"),
            F.col("p.reason_reference"),
            F.col("p.therapy_intention"),
            F.col("p.stellung_op"),
            F.col("p.therapy_start_date"),
            F.col("p.therapy_end_date"),
            F.col("p.therapy_end_reason"),
            # nur gewünschte Child-Spalten
            F.col("rg.zielgebiet"),
            F.col("rg.therapy_id_child"),
        )
    )
    """ logger.info(
        "df_radiotherapies_final count = {}",
        df_radiotherapies_final.count(),
    )
    df_radiotherapies_final.show() """
    # clean
    return df_radiotherapies_final


def extract_surgeries(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:
    logger.info("extract pca ops.")

    df_ops = data.view(
        "Procedure",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "description": "Procedure ID",
                        "name": "therapy_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "reason_reference",
                    },
                    {
                        "description": "Procedure Reference",
                        "path": "partOf.getReferenceKey()",
                        "name": "part_of_reference",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "subject_reference",
                    },
                    {
                        "description": "Intention Surgery",
                        "path": f"extension('{FHIR_SYSTEMS_SURGERY_INTENTION}')"
                        ".value.ofType(CodeableConcept).coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SURGERY_INTENTION_CS}')"
                        ".code",
                        "name": "therapy_intention",
                    },
                    {
                        "description": "Surgery Date",
                        "path": "performedDateTime",
                        "name": "therapy_start_date",
                    },
                    {
                        "description": "Surgery Outcome",
                        "path": "outcome.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SURGERY_OUTCOME_CS}')"
                        ".code",
                        "name": "surgery_outcome",
                    },
                    {
                        "description": "OPS Code",
                        "path": f"code.coding.where(system = '{FHIR_SYSTEMS_SURGERY_OPS_CS}').code",
                        "name": "ops_code",
                    },
                ]
            }
        ],
        where=[
            {
                "description": "Only Surgical Procedures",
                "path": (f"meta.profile.exists($this = '{FHIR_SYSTEMS_SURGERY}')"),
            }
        ],
    )

    return df_ops


def group_ops(df_ops: DataFrame) -> DataFrame:
    # ursprüngliche Reihenfolge beibehalten hier in den aggs
    window_spec = Window.partitionBy(
        "meta_profile",
        "reason_reference",
        "subject_reference",
        "therapy_start_date",
    ).orderBy("therapy_id")

    df_ops_with_index = df_ops.withColumn("row_idx", F.row_number().over(window_spec))

    df_ops_grouped = df_ops_with_index.groupBy(
        "meta_profile",
        "reason_reference",
        "subject_reference",
        "therapy_start_date",
    ).agg(
        F.expr(
            """
        concat_ws('| ',
            transform(
                array_sort(collect_list(struct(row_idx, ops_code))),
                x -> x.ops_code
            )
        )
    """
        ).alias("ops_code"),
        F.expr(
            """
        concat_ws('| ',
            transform(
                array_sort(collect_list(struct(row_idx, surgery_outcome))),
                x -> x.surgery_outcome
            )
        )
    """
        ).alias("surgery_outcome"),
        F.expr(
            """
        concat_ws('| ',
            transform(
                array_sort(collect_list(struct(row_idx, therapy_intention))),
                x -> x.therapy_intention
            )
        )
    """
        ).alias("therapy_intention"),
        F.expr(
            """
        concat_ws('| ',
            transform(
                array_sort(collect_list(struct(row_idx, therapy_id))),
                x -> x.therapy_id
            )
        )
    """
        ).alias("therapy_id"),
    )

    """ logger.info(
        "df_ops_grouped count = {}",
        df_ops_grouped.count(),
    )
    df_ops_grouped.orderBy(F.col("reason_reference")).show(truncate=False) """

    return df_ops_grouped


def filter_aml(df):
    return df.filter(
        (col("icd10_code").isin("C83.3", "C83.1"))
        | (col("icd10_code").rlike(r"^(C90|C91|C92|C93|C94|C95|D46|C82|C85)"))
    )


def extract_weitere_klassifikation(
    pc: PathlingContext,
    data: datasource.DataSource,
    spark: SparkSession,
    settings,
):
    observations_weitere_klassifikation = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "description": "Observation ID",
                        "path": "getResourceKey()",
                        "name": "observation_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.first().getReferenceKey()",
                        "name": "focus_reference",
                    },
                    {
                        "description": "Weitere Klassifikation Code Text",
                        "path": "code.text",
                        "name": "weitere_klassifikation_code_text",
                    },
                    {
                        "description": "Weitere Klassifikation Code",
                        "path": "value.ofType(CodeableConcept).coding.first().code",
                        "name": "weitere_klassifikation_code",
                    },
                    {
                        "description": "Weitere Klassifikation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "weitere_klassifikation_date",
                    },
                ]
            }
        ],
    )
    logger.info(
        "observations_weitere_klassifikation count = {}",
        observations_weitere_klassifikation.count(),
    )
    observations_weitere_klassifikation.show()

    observations_weitere_klassifikation = observations_weitere_klassifikation.filter(
        F.col("meta_profile").startswith(FHIR_SYSTEMS_WEITERE_KLASSIFIKATION)
    )
    logger.info(
        "observations_weitere_klassifikation count after filter = {}",
        observations_weitere_klassifikation.count(),
    )
    observations_weitere_klassifikation.show()

    return observations_weitere_klassifikation


def extract_and_join_weitere_klassifikation(
    df: DataFrame,
    pc: PathlingContext,
    data: datasource.DataSource,
    spark: SparkSession,
    settings,
):
    observations_weitere_klassifikation = extract_weitere_klassifikation(pc, data, spark, settings)
    logger.info(
        "observations_weitere_klassifikation count = {}",
        observations_weitere_klassifikation.count(),
    )
    observations_weitere_klassifikation.show()

    df_weitere_klassifikation = df.join(
        observations_weitere_klassifikation,
        df["condition_id"] == observations_weitere_klassifikation["focus_reference"],
        how="left",
    )
    logger.info(
        "df_weitere_klassifikation count = {}",
        df_weitere_klassifikation.count(),
    )
    df_weitere_klassifikation.show()

    return df_weitere_klassifikation


def filter_reacto(df, settings):
    df = cast_study_dates(
        df,
        [
            "asserted_date",
        ],
    )
    # filter ösophagus C15, colon C18, lungs C34,
    # pancreas exokrin C25* ohne C25.4 and glioblastom C71 + WHO IV
    df_filtered = df.filter(
        F.col("icd10_code").startswith("C15")
        | F.col("icd10_code").startswith("C18")
        | F.col("icd10_code").startswith("C34")
        | (F.col("icd10_code").startswith("C25") & (~F.col("icd10_code").startswith("C25.4")))
        | F.col("icd10_code").startswith("C71")
    )
    # from 2018 - 2025
    df_filtered = df_filtered.filter(
        (F.year("asserted_date") >= 2018) & (F.year("asserted_date") <= 2025)
    )
    save_final_df(
        df_filtered,
        settings,
        suffix="df_reacto",
    )

    df_counts = (
        df_filtered.withColumn("icd10_parent", F.expr("substring(icd10_code, 1, 3)"))
        .groupBy("icd10_parent")
        .agg(
            F.count("*").alias("total_count"),
            F.sum(F.when(F.col("gender") == "male", 1).otherwise(0)).alias("male_count"),
            F.sum(F.when(F.col("gender") == "female", 1).otherwise(0)).alias("female_count"),
        )
        .orderBy("icd10_parent")
    )
    # weitere Klassifikation WHO IV für Glioblastom
    df_c71_iv_counts = (
        df_filtered.filter(
            F.col("icd10_code").startswith("C71")
            & F.lower(F.col("weitere_klassifikation_code_text")).contains("who")
            & (
                (F.col("weitere_klassifikation_code") == "IV")
                | (F.col("weitere_klassifikation_code") == "4")
            )
        )
        .agg(
            F.count("*").alias("total_count"),
            F.sum(F.when(F.col("gender") == "male", 1).otherwise(0)).alias("male_count"),
            F.sum(F.when(F.col("gender") == "female", 1).otherwise(0)).alias("female_count"),
        )
        .withColumn("icd10_parent", F.lit("C71_IV"))
        .select("icd10_parent", "total_count", "male_count", "female_count")
    )
    df_counts = df_counts.unionByName(df_c71_iv_counts).orderBy("icd10_parent")

    logger.info("reacto df_counts ")
    df_counts.show()
    save_final_df(df_counts, settings, suffix=f"reacto_counts_{settings.location}")


def filter_primaerdiagnose(df):
    return df.filter(F.col("meta_profile").startswith(FHIR_SYSTEM_PRIMAERTUMOR))


def clean_datetime_values(df, column):
    date = df[column].astype(str).str.strip()
    mask = ~date.str.contains(r"[T ]", regex=True)
    date.loc[mask] = date.loc[mask] + "T00:00:00+01:00"
    df["diagnosis_recordedDate"] = pd.to_datetime(date, utc=True)
    return df


def keep_only_first_diagnosis(df, columns):
    df_sorted = df.sort_values(by=columns + ["diagnosis_recordedDate"])
    return df_sorted.drop_duplicates(subset=columns, keep="first")
    return df_sorted.drop_duplicates(subset=columns, keep="first")
    return df_sorted.drop_duplicates(subset=columns, keep="first")
    return df_sorted.drop_duplicates(subset=columns, keep="first")
