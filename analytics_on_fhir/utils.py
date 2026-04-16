import glob
import hashlib
import hmac
import os
import shutil
from collections.abc import Iterable

import pandas as pd
from fhir_constants import (
    FHIR_SYSTEM_ICD10,
    FHIR_SYSTEM_ICDO3_T,
    FHIR_SYSTEM_JNU,
    FHIR_SYSTEM_LOINC,
    FHIR_SYSTEM_METASTASIS,
    FHIR_SYSTEM_PRIMAERTUMOR,
    FHIR_SYSTEM_SCT,
    FHIR_SYSTEM_TNM_UICC,
    FHIR_SYSTEMS_CONDITION_ASSERTED_DATE,
    FHIR_SYSTEMS_CONDITION_MORPHOLOGY,
    FHIR_SYSTEMS_RADIO_THERAPY_INTENTION,
    FHIR_SYSTEMS_RADIO_THERAPY_INTENTION_CS,
    FHIR_SYSTEMS_RADIO_THERAPY_STELLUNG_OP,
    FHIR_SYSTEMS_RADIO_THERAPY_ZIELGEBIET_CS,
    FHIR_SYSTEMS_RADIOTHERAPY,
    FHIR_SYSTEMS_RADIOTHERAPY_BESTRAHLUNG,
    FHIR_SYSTEMS_STELLUNG_OP_CS,
    FHIR_SYSTEMS_SURGERY,
    FHIR_SYSTEMS_SURGERY_INTENTION,
    FHIR_SYSTEMS_SURGERY_INTENTION_CS,
    FHIR_SYSTEMS_SURGERY_OPS_CS,
    FHIR_SYSTEMS_SURGERY_OUTCOME_CS,
    FHIR_SYSTEMS_SYSTEM_THERAPY,
    FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION,
    FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION_CS,
    FHIR_SYSTEMS_SYSTEM_THERAPY_STELLUNG_OP,
    FHIR_SYSTEMS_SYSTEM_THERAPY_TYP_CS,
    FHIR_SYSTEMS_THERAPY_END_REASON_CS,
    FHIR_SYSTEMS_WEITERE_KLASSIFIKATION,
    SCT_CODE_DEATH,
    SCT_CODE_GLEASON,
    SCT_CODE_METASTASIS,
)
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
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

HERE = os.path.abspath(os.path.dirname(__file__))

# ggf ändern, alle spalten, die id enthalten
IDENTIFYING_COLS = [
    "meta_profile",
    "condition_patient_resource_id",
    "patid_pseudonym",
    "deceased_boolean",
    "observation_death_patient_resource_id",
    "observation_resource_id",
    "reason_reference",
    "subject_reference",
    "therapy_id",
    "therapy_id_child",
    "observation_id",
    "observation_gleason_resource_id",
    "observation_gleason_condition_resource_id",
    "observation_gleason_patient_resource_id",
    "observation_metastasis_resource_id",
    "observation_patient_reference",
]


def save_final_df(pyspark_df, settings, suffix="", deidentified=False):
    logger.info("start save pyspark_df with pyspark_df.coalesce(1).write...csv() ")
    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    if deidentified:
        output_dir = os.path.join(
            HERE, settings.results_directory_path, settings.study_name.value, "deidentified"
        )

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


def save_final_df_parquet(pyspark_df, settings, suffix="", deidentified=False):
    base_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    if deidentified:
        base_dir = os.path.join(
            HERE, settings.results_directory_path, settings.study_name.value, "deidentified"
        )
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
    )
    return df


def compute_month_diffs_for_all_date_cols(df: DataFrame) -> DataFrame:
    # Alle Spalten mit "date", außer asserted_date und Spalten mit "precision" oder "year"
    date_cols = [
        c
        for c in df.columns
        if "date" in c.lower()
        and "asserted_date" not in c.lower()
        and "diff" not in c.lower()
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


def _make_hash_udf(crypto_key):

    def _hash(val):
        if val is None:
            return None
        return hmac.new(
            crypto_key,
            str(val).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    return F.udf(_hash, StringType())


def deidentify(
    df: DataFrame,
    identifying_cols: list[str],
    crypto_key,
    drop_original: bool = True,
) -> DataFrame:

    crypto_key = bytes.fromhex(crypto_key)

    hash_target_cols = [
        "condition_id",
        "condition_id_1",
        "condition_id_2",
        "condition_id_mii",
        "patient_resource_id",
        "condition_patient_reference",
    ]
    hash_udf = _make_hash_udf(crypto_key)

    for col_name in hash_target_cols:
        if col_name in df.columns:
            df = df.withColumn(f"{col_name}_hash", hash_udf(F.col(col_name).cast("string")))

    if drop_original:
        df = df.drop(*hash_target_cols)

    cols_to_drop = [c for c in identifying_cols if c in df.columns]
    df = df.drop(*cols_to_drop)

    df = compute_month_diffs_for_all_date_cols(df)
    df = create_year_col_asserted_death(df)
    df = drop_date_cols(df)

    return df


def deidentify_pandas(df, crypto_key, drop_original=True):
    crypto_key = bytes.fromhex(crypto_key)

    hash_target_cols = [
        "condition_id",
        "condition_patient_reference",
        "observation_patient_reference",
    ]

    for col_name in hash_target_cols:
        if col_name in df.columns:
            df[f"{col_name}_hash"] = df[col_name].apply(
                lambda val: (
                    None
                    if val is None
                    else hmac.new(
                        crypto_key,
                        str(val).encode("utf-8"),
                        hashlib.sha256,
                    ).hexdigest()
                )
            )

    if drop_original:
        df = df.drop(columns=[c for c in hash_target_cols if c in df.columns])

    return df


def export_data_dictionary(output_dir, DATA_DICTIONARY):

    for table_name, columns in DATA_DICTIONARY.items():
        df = pd.DataFrame(
            [{"column_name": col, "description": desc} for col, desc in columns.items()]
        )

        df.to_csv(
            os.path.join(output_dir, f"{table_name}_data_dictionary.csv"),
            index=False,
        )


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
                    {
                        "description": "ICD-O-3 Topography",
                        "path": (
                            f"bodySite.coding.where(system = '{FHIR_SYSTEM_ICDO3_T}').code.first()"
                        ),
                        "name": "icdo3_topography",
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


def extract_gleason(
    pc: PathlingContext,
    data: datasource.DataSource,
    spark: SparkSession,
    settings,
):
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

    observations_gleason_count = observations_gleason.count()
    logger.info("observations_gleason_count = {}", observations_gleason_count)
    observations_gleason.show()

    logger.info(
        "Number of Gleason observations not null = {}",
        observations_gleason.filter(col("gleason_sct").isNotNull()).count(),
    )

    return observations_gleason


def extract_t_tnm(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:

    logger.info("extract T from tnm observations")

    observations_t_tnm = data.view(
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
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "TNM T category",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            ".coding.where(system = 'https://www.uicc.org/resources/tnm')"
                            ".code"
                        ),
                        "name": "t_tnm",
                    },
                    {
                        "description": "TNM c/p prefix",
                        "path": (
                            "code.extension.where("
                            "url = 'https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/StructureDefinition/mii-ex-onko-tnm-cp-praefix'"
                            ").value.ofType(CodeableConcept).coding.code.first()"
                        ),
                        "name": "t_tnm_prefix",
                    },
                    {
                        "description": "TNM prefix SCT code",
                        "path": f"code.coding.where(system = '{FHIR_SYSTEM_SCT}').code",
                        "name": "t_tnm_c_p_snomed_prefix",
                    },
                    {
                        "description": "TNM Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "t_tnm_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": (
                    "Nur Observations, deren Observation.code.coding.code "
                    "einer der erlaubten SCT codes ist."
                ),
                "path": (
                    "code.coding.where("
                    f"system = '{FHIR_SYSTEM_SCT}' and "
                    "(code = '78873005' or code = '399504009' or code = '384625004')"
                    ")"
                    ".exists()"
                ),
            }
        ],
    )

    # until t_tnm_prefix extraction works
    observations_t_tnm = observations_t_tnm.withColumn(
        "t_tnm_prefix_sct",
        F.when(F.col("t_tnm_c_p_snomed_prefix") == "78873005", F.lit("T"))
        .when(F.col("t_tnm_c_p_snomed_prefix") == "399504009", F.lit("cT"))
        .when(F.col("t_tnm_c_p_snomed_prefix") == "384625004", F.lit("pT"))
        .otherwise(None),
    )

    logger.info(
        "observations_t_tnm count = {}",
        observations_t_tnm.count(),
    )
    logger.info(
        "observations_t_tnm distinct condition_id count = {}",
        observations_t_tnm.select("condition_id").distinct().count(),
    )
    observations_t_tnm.orderBy(F.col("condition_id")).show()

    return observations_t_tnm


def extract_n_tnm(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:

    logger.info("extract N from tnm observations")

    observations_n_tnm = data.view(
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
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "TNM N category",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            ".coding.where(system = 'https://www.uicc.org/resources/tnm')"
                            ".code"
                        ),
                        "name": "n_tnm",
                    },
                    {
                        "description": "TNM c/p prefix",
                        "path": (
                            "code.extension.where("
                            "url = 'https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/StructureDefinition/mii-ex-onko-tnm-cp-praefix'"
                            ").value.ofType(CodeableConcept).coding.code.first()"
                        ),
                        "name": "n_tnm_prefix",
                    },
                    {
                        "description": "TNM prefix SCT code",
                        "path": f"code.coding.where(system = '{FHIR_SYSTEM_SCT}').code",
                        "name": "n_tnm_c_p_snomed_prefix",
                    },
                    {
                        "description": "TNM Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "n_tnm_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": (
                    "Nur Observations, deren Observation.code.coding.code "
                    "einer der erlaubten SCT codes ist."
                ),
                "path": (
                    "code.coding.where("
                    f"system = '{FHIR_SYSTEM_SCT}' and "
                    "(code = '277206009' or code = '399534004' or code = '371494008')"
                    ")"
                    ".exists()"
                ),
            }
        ],
    )

    # until n_tnm_prefix extraction works
    observations_n_tnm = observations_n_tnm.withColumn(
        "n_tnm_prefix_sct",
        F.when(F.col("n_tnm_c_p_snomed_prefix") == "277206009", F.lit("N"))
        .when(F.col("n_tnm_c_p_snomed_prefix") == "399534004", F.lit("cN"))
        .when(F.col("n_tnm_c_p_snomed_prefix") == "371494008", F.lit("pN"))
        .otherwise(None),
    )

    logger.info(
        "observations_n_tnm count = {}",
        observations_n_tnm.count(),
    )
    logger.info(
        "observations_n_tnm distinct condition_id count = {}",
        observations_n_tnm.select("condition_id").distinct().count(),
    )
    observations_n_tnm.orderBy(F.col("condition_id")).show()

    return observations_n_tnm


def extract_m_tnm(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:
    logger.info("extract M from tnm observations")

    observations_m_tnm = data.view(
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
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "TNM M category",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            ".coding.where(system = 'https://www.uicc.org/resources/tnm')"
                            ".code"
                        ),
                        "name": "m_tnm",
                    },
                    {
                        "description": "TNM c/p prefix",
                        "path": (
                            "code.extension.where("
                            "url = 'https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/StructureDefinition/mii-ex-onko-tnm-cp-praefix'"
                            ").value.ofType(CodeableConcept).coding.code.first()"
                        ),
                        "name": "m_tnm_prefix",
                    },
                    {
                        "description": "TNM prefix SCT code",
                        "path": f"code.coding.where(system = '{FHIR_SYSTEM_SCT}').code",
                        "name": "m_tnm_c_p_snomed_prefix",
                    },
                    {
                        "description": "TNM Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "m_tnm_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": (
                    "Nur Observations, deren Observation.code.coding.code "
                    "einer der erlaubten SCT codes ist."
                ),
                "path": (
                    "code.coding.where("
                    f"system = '{FHIR_SYSTEM_SCT}' and "
                    "(code = '277208005' or code = '399387003' or code = '371497001')"
                    ")"
                    ".exists()"
                ),
            }
        ],
    )

    # until n_tnm_prefix extraction works
    observations_m_tnm = observations_m_tnm.withColumn(
        "m_tnm_prefix_sct",
        F.when(F.col("m_tnm_c_p_snomed_prefix") == "277208005", F.lit("M"))
        .when(F.col("m_tnm_c_p_snomed_prefix") == "399387003", F.lit("cM"))
        .when(F.col("m_tnm_c_p_snomed_prefix") == "371497001", F.lit("pM"))
        .otherwise(None),
    )

    logger.info(
        "observations_m_tnm count = {}",
        observations_m_tnm.count(),
    )
    logger.info(
        "observations_m_tnm distinct condition_id count = {}",
        observations_m_tnm.select("condition_id").distinct().count(),
    )

    observations_m_tnm.orderBy(F.col("condition_id")).show()

    return observations_m_tnm


def extract_uicc_tnm(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:

    logger.info("extract UICC from tnm observations")

    observations_uicc_tnm = data.view(
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
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "TNM UICC category",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_TNM_UICC}')"
                            ".code"
                        ),
                        "name": "uicc_tnm",
                    },
                    {
                        "description": "UICC TNM prefix SCT code",
                        "path": f"code.coding.where(system = '{FHIR_SYSTEM_SCT}').code",
                        "name": "uicc_tnm_c_p_snomed_prefix",
                    },
                    {
                        "description": "TNM Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "uicc_tnm_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": (
                    "Nur Observations, deren Observation.code.coding.code einer dieser"
                    + "sct codes ist"
                ),
                "path": (
                    "code.coding.where("
                    f"system = '{FHIR_SYSTEM_SCT}' and "
                    "(code = '399390009' or code = '399537006' or code = '399588009')"
                    ")"
                    ".exists()"
                ),
            }
        ],
    )

    observations_uicc_tnm = observations_uicc_tnm.withColumn(
        "uicc_tnm_prefix_sct",
        F.when(F.col("uicc_tnm_c_p_snomed_prefix") == "399390009", F.lit("UICC"))
        .when(F.col("uicc_tnm_c_p_snomed_prefix") == "399537006", F.lit("cUICC"))
        .when(F.col("uicc_tnm_c_p_snomed_prefix") == "399588009", F.lit("pUICC"))
        .otherwise(None),
    )

    logger.info(
        "observations_uicc_tnm count = {}",
        observations_uicc_tnm.count(),
    )
    logger.info(
        "observations_uicc_tnm distinct condition_id count = {}",
        observations_uicc_tnm.select("condition_id").distinct().count(),
    )

    observations_uicc_tnm.orderBy(F.col("condition_id")).show()

    return observations_uicc_tnm


def extract_y_tnm(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings,
    spark: SparkSession,
) -> DataFrame:

    logger.info("extract N from tnm observations")

    observations_y_tnm = data.view(
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
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "TNM Y category",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_SCT}')"
                            ".code"
                        ),
                        "name": "y_tnm_421755005",
                    },
                    {
                        "description": "TNM Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "y_tnm_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": (
                    "Nur Observations, deren Observation.code.coding.code dieser loinc code ist"
                ),
                "path": (
                    "code.coding.where("
                    f"system = '{FHIR_SYSTEM_LOINC}' and "
                    "code = '101658-3')"
                    ".exists()"
                ),
            }
        ],
    )

    logger.info(
        "observations_y_tnm count = {}",
        observations_y_tnm.count(),
    )
    logger.info(
        "observations_y_tnm distinct condition_id count = {}",
        observations_y_tnm.select("condition_id").distinct().count(),
    )

    observations_y_tnm.orderBy(F.col("condition_id")).show()

    return observations_y_tnm


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
                    {
                        "description": "Observation ID",
                        "path": "getResourceKey()",
                        "name": "observation_metastasis_resource_id",
                    },
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
    )

    # hacky: filter where col meta_profile starts with {FHIR_SYSTEMS_SYSTEM_THERAPY}
    df_sys_procedures = df_procedures.filter(
        col("meta_profile").startswith(FHIR_SYSTEMS_SYSTEM_THERAPY)
    )
    logger.info("df_sys_procedures count = {}", df_sys_procedures.count())
    logger.info(
        "df_sys_procedures distinct therapy_id count = {}",
        df_sys_procedures.select("therapy_id").distinct().count(),
    )
    logger.info(
        "df_sys_procedures distinct subject_reference patient id count = {}",
        df_sys_procedures.select("subject_reference").distinct().count(),
    )
    df_sys_procedures.orderBy(F.col("reason_reference")).show(truncate=False)

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
                    {
                        "description": "Medication Reference",
                        "path": "medication.ofType(Reference).getReferenceKey()",
                        "name": "medication_reference",
                    },
                ],
            }
        ],
    )

    medications = data.view(
        "Medication",
        select=[
            {
                "column": [
                    {
                        "description": "Medication ID",
                        "path": "getResourceKey()",
                        "name": "medication_id",
                    },
                    {
                        "description": "Medication ATC Code",
                        "path": "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').first().code",
                        "name": "medication_medication_atc_code",
                    },
                    {
                        "description": "Medication Text",
                        "path": "code.text",
                        "name": "medication_medication_text",
                    },
                ],
            }
        ],
    )

    medications.show()

    df_medication_statements = (
        df_medication_statements.join(
            medications,
            df_medication_statements.medication_reference == medications.medication_id,
            how="left",
        )
        .withColumn(
            "medication_statement_text",
            F.coalesce(F.col("medication_statement_text"), F.col("medication_medication_text")),
        )
        .withColumn(
            "medication_statement_atc_code",
            F.coalesce(
                F.col("medication_statement_atc_code"), F.col("medication_medication_atc_code")
            ),
        )
    )

    df_medication_statements.show()

    # TO DO: split this later
    # return df_sys_procedures, df_medication_statements

    # erst joinen (procedure+medicationstatements) und danach gruppieren
    # Substanzen pro condition und therapy start date |-separiert abspeichern
    df_sys_procedures_medication_statements = (
        df_sys_procedures.alias("p")
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
    df_sys_procedures_medication_statements.show()

    # pro condition und start date: | separiert abspeichern
    df_procedures_medication_statements_grouped = df_sys_procedures_medication_statements.groupBy(
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
        df_sys_procedures.alias("p")
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

    # hacky - to do
    df_parents_radiotherapies = df_procedures.filter(
        F.col("meta_profile").startswith(FHIR_SYSTEMS_RADIOTHERAPY)
    )

    df_children_bestrahlung = df_procedures.filter(
        F.col("meta_profile").startswith(FHIR_SYSTEMS_RADIOTHERAPY_BESTRAHLUNG)
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
                "description": "Only Procedures with OPS code",
                "path": f"code.coding.where(system='{FHIR_SYSTEMS_SURGERY_OPS_CS}').exists()",
            }
        ],
    )
    logger.info("df_ops count before filter = {}", df_ops.count())
    # hacky - to do
    df_ops = df_ops.filter(F.col("meta_profile").startswith(FHIR_SYSTEMS_SURGERY))
    logger.info("df_ops count after filter = {}", df_ops.count())

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


def normalize_array_columns(df: pd.DataFrame) -> pd.DataFrame:
    for colname in df.columns:
        # detect columns that contain lists/arrays in ANY row
        if df[colname].apply(lambda x: isinstance(x, (list, tuple))).any():
            df[colname] = df[colname].apply(
                lambda x: ",".join(map(str, x)) if isinstance(x, (list, tuple)) else x
            )
    return df


def group_entity_or_parent(df, code_col="icd10_code", target_col="entity_and_parent"):
    parent_col = "icd10_parent_tmp"
    df = df.withColumn(parent_col, F.regexp_replace(F.col(code_col), r"\..*$", ""))

    df = df.withColumn(
        target_col,
        F.when(F.col(parent_col).between("C00", "C14"), F.lit("C00-C14"))
        .when(F.col(parent_col) == "C15", F.lit("C15"))
        .when(F.col(parent_col) == "C16", F.lit("C16"))
        .when(F.col(parent_col).between("C18", "C21"), F.lit("C18-C21"))
        .when(F.col(parent_col) == "C22", F.lit("C22"))
        .when(F.col(parent_col).between("C23", "C24"), F.lit("C23-C24"))
        .when(F.col(parent_col) == "C25", F.lit("C25"))
        .when(F.col(parent_col) == "C32", F.lit("C32"))
        .when(F.col(parent_col).between("C33", "C34"), F.lit("C33-C34"))
        .when(F.col(parent_col) == "C43", F.lit("C43"))
        .when(F.col(parent_col) == "C50", F.lit("C50"))
        .when(F.col(parent_col) == "C53", F.lit("C53"))
        .when(F.col(parent_col).between("C54", "C55"), F.lit("C54-C55"))
        .when(F.col(parent_col) == "C56", F.lit("C56"))
        .when(F.col(parent_col) == "C61", F.lit("C61"))
        .when(F.col(parent_col) == "C62", F.lit("C62"))
        .when(F.col(parent_col) == "C64", F.lit("C64"))
        .when(F.col(parent_col) == "C67", F.lit("C67"))
        .when(F.col(parent_col).between("C70", "C72"), F.lit("C70-C72"))
        .when(F.col(parent_col) == "C73", F.lit("C73"))
        .when(F.col(parent_col) == "C81", F.lit("C81"))
        .when(F.col(parent_col).between("C82", "C88"), F.lit("C82-C88"))
        .when(F.col(parent_col) == "C90", F.lit("C90"))
        .when(F.col(parent_col).between("C91", "C95"), F.lit("C91-C95"))
        .otherwise(
            F.col(parent_col)
        ),  # fallback = parent code - so verlieren wir nicht so viele (vgl entity)
    )

    df = df.drop(parent_col)
    return df
