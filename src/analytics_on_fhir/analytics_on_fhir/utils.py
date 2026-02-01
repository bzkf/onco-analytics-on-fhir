import glob
import os
import shutil
from typing import Iterable

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

FHIR_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_SYSTEM_JNU = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/"
    "CodeSystem/mii-cs-onko-tod"
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


def save_final_df(pyspark_df, settings, suffix=""):
    logger.info("start save pyspark_df with pyspark_df.coalesce(1).write...csv() ")
    output_dir = os.path.join(
        HERE, settings.results_directory_path, settings.study_name
    )
    os.makedirs(output_dir, exist_ok=True)

    final_csv_path = os.path.join(output_dir, f"df_{suffix}.csv")
    temp_dir = os.path.join(output_dir, "_tmp_csv")

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    pyspark_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(
        temp_dir
    )
    part_file = glob.glob(os.path.join(temp_dir, "part-*.csv"))[0]
    shutil.move(part_file, final_csv_path)
    shutil.rmtree(temp_dir)

    logger.info("end save pyspark_df")


def save_plot(plot: Figure, settings, plot_name: str = "") -> None:
    logger.info("start save plot")
    output_dir = os.path.join(
        settings.results_directory_path, settings.study_name, "plots"
    )
    os.makedirs(output_dir, exist_ok=True)

    final_plot_path = os.path.join(output_dir, f"{plot_name}.png")
    plot.savefig(final_plot_path, bbox_inches="tight", dpi=300)
    logger.info(f"Plot saved to: {final_plot_path}")


def find_closest_to_diagnosis(
    df, condition_id_colname, date_diagnosis_col, other_date_col
):
    df_filtered = df.filter(col(other_date_col).isNotNull())

    window = Window.partitionBy(condition_id_colname).orderBy(
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


def compute_age(df: DataFrame) -> DataFrame:
    # birthdate (YYYY-MM → YYYY-MM-15)
    df = df.withColumn(
        "birthdate",
        F.when(
            F.col("birthdate").rlike(r"^\d{4}-\d{2}$"),  # nur YYYY-MM
            F.to_date(F.concat(F.col("birthdate"), F.lit("-15")), "yyyy-MM-dd"),
        ).otherwise(F.to_date(F.col("birthdate"), "yyyy-MM-dd")),
    )

    df = df.withColumn(
        "age_at_diagnosis",
        F.round((F.datediff(df["asserted_date"], df["birthdate"]) / 365.25), 2),
    ).drop("birthdate")
    return df


def cast_study_dates(df: DataFrame, date_cols: Iterable[str]) -> DataFrame:
    for c in date_cols:
        df = df.withColumn(c, F.to_date(c))
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


def map_gleason_sct_to_score(
    df, gleason_sct_col="gleason_sct", out_col="gleason_score"
):

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
                    {"path": "gender", "name": "gender", "description": "Gender"},
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
                        "path": (
                            f"code.coding"
                            f".where(system = '{FHIR_SYSTEM_ICD10}')"
                            f".code"
                        ),
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
                        "path": (
                            f"interpretation"
                            f".coding.where(system = '{FHIR_SYSTEM_JNU}')"
                            f".code"
                        ),
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
    observation_death = observation_death.orderBy(
        "observation_death_patient_resource_id"
    )
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
        observation_death.select("observation_death_patient_resource_id")
        .distinct()
        .count()
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
        first("observation_resource_id", ignorenulls=True).alias(
            "observation_resource_id"
        ),
        first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
    )

    tumor_flag_fix_count = observation_death.filter(
        (col("death_cause_icd10").startswith("C"))
        & (col("death_cause_tumor").isin("U", "N"))
    ).count()

    logger.info(
        "death_cause_tumor auf 'J' gesetzt bei {} Datensätzen",
        tumor_flag_fix_count,
    )

    observation_death = observation_death.withColumn(
        "death_cause_tumor",
        when(
            (col("death_cause_icd10").startswith("C"))
            & (col("death_cause_tumor").isin("U", "N")),
            "J",
        ).otherwise(col("death_cause_tumor")),
    )

    observation_death.orderBy("observation_death_patient_resource_id").show(100)
    observation_death_count = observation_death.count()
    logger.info("observation_death_count after grouping = {}", observation_death_count)
    # count distinct patients with death observation
    observation_death_distinct_patients_count = (
        observation_death.select("observation_death_patient_resource_id")
        .distinct()
        .count()
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
            concat_ws(",", collect_set("death_cause_icd10")).alias("death_cause_icd10")
        )
    )

    # group again by observation_death_patient_resource_id and keep first non null
    observation_death_patient = observation_death.groupBy(
        "observation_death_patient_resource_id"
    ).agg(
        first("date_death", ignorenulls=True).alias("date_death"),
        first("observation_resource_id", ignorenulls=True).alias(
            "observation_resource_id"
        ),
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
            col("cp.patient_resource_id")
            == col("od.observation_death_patient_resource_id"),
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
    double_patid_count = conditions_patients_death.filter(
        col("double_patid") == 1
    ).count()

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
                "description": "Histologic grade of primary malignant neoplasm "
                "of prostate",
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
        conditions_gleason_first, gleason_sct_col="gleason_sct", out_col="gleason_score"
    )

    conditions_gleason_first_count = conditions_gleason_first.count()

    logger.info(
        "conditions_gleason_first_count (1 Gleason pro Condition) = {}",
        conditions_gleason_first_count,
    )
    conditions_gleason_first.show()

    logger.info(
        "sanity check double condition_ids count in conditions_gleason_first = {}",
        conditions_gleason_first.groupBy("condition_id")
        .count()
        .filter(col("count") > 1)
        .count(),
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
        observation_death.select("observation_death_patient_resource_id")
        .distinct()
        .count()
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
        concat_ws(",", collect_set("metastasis_loc")).alias("metastasis_loc"),
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
            col("c.condition_id")
            == col("m.observation_metastasis_condition_resource_id"),
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

    conditions_patients_death_gleason_metastasis = (
        conditions_patients_death_gleason.alias("c").join(
            metastasis_grouped_first.alias("m"),
            col("c.condition_id")
            == col("m.observation_metastasis_condition_resource_id"),
            "left",
        )
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
