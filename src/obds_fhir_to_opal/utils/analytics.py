import glob
import os
import shutil

from loguru import logger
from matplotlib.figure import Figure
from pathling import Expression as exp
from pathling import PathlingContext, datasource
from pydantic import BaseSettings
from pyspark.sql import SparkSession
from pyspark.sql.functions import (  # to do: hier auch F verwenden
    abs,
    col,
    count,
    first,
    regexp_replace,
    row_number,
    when,
)
from pyspark.sql.window import Window

FHIR_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_SYSTEM_JNU = "http://dktk.dkfz.de/fhir/onco/core/CodeSystem/JNUCS"
FHIR_SYSTEM_LOINC = "http://loinc.org"
FHIR_SYSTEM_IDENTIFIER_TYPE = "http://terminology.hl7.org/CodeSystem/v2-0203"


def save_final_df(pyspark_df, settings, suffix=""):
    logger.info("start save pyspark_df with pyspark_df.coalesce(1).write...csv() ")
    output_dir = os.path.join(settings.output_folder, settings.study_name)
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


def save_plot(plot: Figure, settings: BaseSettings, plot_name: str = "") -> None:
    logger.info("start save plot")
    output_dir = os.path.join(settings.output_folder, settings.study_name, "plots")
    os.makedirs(output_dir, exist_ok=True)

    final_plot_path = os.path.join(output_dir, f"{plot_name}.png")
    plot.savefig(final_plot_path, bbox_inches="tight", dpi=300)
    logger.info(f"Plot saved to: {final_plot_path}")


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


# to do: refactor this, use pathlinv v9 and MII profiles
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
