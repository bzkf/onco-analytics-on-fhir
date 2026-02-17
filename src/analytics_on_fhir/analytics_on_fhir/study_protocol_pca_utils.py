import re
from functools import reduce

from loguru import logger
from pathling import PathlingContext, datasource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from analytics_on_fhir.settings import settings

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


def extract_systemtherapies(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: settings,
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
                "path": (
                    f"meta.profile.exists($this = '{FHIR_SYSTEMS_SYSTEM_THERAPY}')"
                ),
            }
        ],
    )
    """     logger.info("df_procedures count = {}", df_procedures.count())
    logger.info(
        "df_procedures distinct therapy_id count = {}",
        df_procedures.select("therapy_id").distinct().count(),
    )
    logger.info(
        "df_procedures distinct subject_reference patient id count = {}",
        df_procedures.select("subject_reference").distinct().count(),
    )
    df_procedures.orderBy(F.col("reason_reference")).show(truncate=False) """

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
    df_procedures_medication_statements_grouped = (
        df_procedures_medication_statements.groupBy(
            "subject_reference",
            "reason_reference",
            "therapy_start_date",
            "part_of_reference",
        ).agg(
            F.concat_ws(
                "| ", F.sort_array(F.collect_set("medication_statement_text"))
            ).alias("medication_statement_text"),
            F.concat_ws(
                "| ", F.sort_array(F.collect_set("medication_statement_atc_code"))
            ).alias("medication_statement_atc_code"),
        )
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
            F.col("pg.medication_statement_atc_code").alias(
                "medication_statement_atc_code"
            ),
        )
    )

    df_procedures_medication_statements_final.orderBy(F.col("reason_reference")).show()

    return df_procedures_medication_statements_final


def extract_radiotherapies(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: settings,
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
        F.concat_ws("| ", F.sort_array(F.collect_set("zielgebiet"))).alias(
            "zielgebiet"
        ),
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
    settings: settings,
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
                        "path": "code.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SURGERY_OPS_CS}')"
                        ".code",
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


def flag_young_highrisk_cohort(
    df: DataFrame, age_col: str = "age_at_diagnosis", gleason_col: str = "gleason"
) -> DataFrame:
    cohort_condition = (F.col(age_col) < 65) & (F.col(gleason_col) >= 8)

    df_flagged = df.withColumn(
        "cohort_flag", F.when(cohort_condition, F.lit(1)).otherwise(F.lit(0))
    )

    cohort = df_flagged.filter(F.col("cohort_flag") == 1)
    logger.info("Filtered cohort count = {}", cohort.count())

    return df_flagged


def union_sort_pivot_join(
    df_c61_clean,
    df_ops_grouped,
    df_medication_statements,
    df_radiotherapies_joined,
    first_line_months_threshold=None,
):
    # to date
    df_ops_grouped = df_ops_grouped.withColumn(
        "therapy_start_date", F.to_date(F.col("therapy_start_date"))
    )
    df_medication_statements = df_medication_statements.withColumn(
        "therapy_start_date", F.to_date(F.col("therapy_start_date"))
    )
    df_radiotherapies_joined = df_radiotherapies_joined.withColumn(
        "therapy_start_date", F.to_date(F.col("therapy_start_date"))
    )

    # union
    # Master Schema
    """ therapy_id
        meta_profile
        subject_reference
        reason_reference
        therapy_type
        therapy_intention
        stellung_op
        therapy_start_date
        therapy_end_date
        therapy_end_reason
        ops_code
        surgery_outcome
        zielgebiet
        therapy_protocol_text
        medication_statement_text
        medication_statement_atc_code """
    # OPS auffüllen
    df_ops_final = (
        df_ops_grouped.withColumn("therapy_type", F.lit("OP"))
        .withColumn("therapy_end_date", F.lit(None))
        .withColumn("therapy_end_reason", F.lit(None))
        .withColumn("zielgebiet", F.lit(None))
        .withColumn("therapy_protocol_text", F.lit(None))
        .withColumn("medication_statement_text", F.lit(None))
        .withColumn("medication_statement_atc_code", F.lit(None))
        .withColumn("stellung_op", F.lit(None))
    )

    logger.info(
        "df_ops_final count = {}",
        df_ops_final.count(),
    )
    df_ops_final.show()
    # Radiotherapie auffüllen
    df_rt_final = (
        df_radiotherapies_joined.withColumn("therapy_type", F.lit("ST"))
        .withColumn("ops_code", F.lit(None))
        .withColumn("surgery_outcome", F.lit(None))
        .withColumn("therapy_protocol_text", F.lit(None))
        .withColumn("medication_statement_text", F.lit(None))
        .withColumn("medication_statement_atc_code", F.lit(None))
    )
    df_med_final = (
        df_medication_statements.withColumn("ops_code", F.lit(None))
        .withColumn("surgery_outcome", F.lit(None))
        .withColumn("zielgebiet", F.lit(None))
        .withColumn("zielgebiet", F.lit(None))
    )
    # Spaltenreihenfolge angleichen
    common_columns = [
        "therapy_id",
        "meta_profile",
        "subject_reference",
        "reason_reference",
        "therapy_type",
        "therapy_intention",
        "stellung_op",
        "therapy_start_date",
        "therapy_end_date",
        "therapy_end_reason",
        "ops_code",
        "surgery_outcome",
        "zielgebiet",
        "therapy_protocol_text",
        "medication_statement_text",
        "medication_statement_atc_code",
    ]
    df_ops_final = df_ops_final.select(common_columns)
    df_rt_final = df_rt_final.select(common_columns)
    df_med_final = df_med_final.select(common_columns)
    df_all_therapies = df_ops_final.unionByName(df_rt_final).unionByName(df_med_final)
    logger.info(
        "df_all_therapies count = {}",
        df_all_therapies.count(),
    )
    df_all_therapies.show()

    # join asserted date aus df_c61_clean an df_therapies
    df_therapies_asserted = df_all_therapies.alias("t").join(
        df_c61_clean.select(
            F.col("condition_id").alias("reason_reference"), "asserted_date"
        ),
        "reason_reference",
        "left",
    )
    logger.info(
        "df_therapies_asserted count = {}",
        df_therapies_asserted.count(),
    )
    df_therapies_asserted.show()

    # Add months_diff
    df_therapies_asserted = df_therapies_asserted.withColumn(
        "months_diff",
        F.round(
            F.months_between(F.col("therapy_start_date"), F.col("asserted_date")), 2
        ).cast("double"),
    )
    logger.info(
        "df_therapies_asserted with months diff count = {}",
        df_therapies_asserted.count(),
    )
    df_therapies_asserted.show()

    # Optional first line filter
    if first_line_months_threshold is not None:
        df_therapies_asserted = df_therapies_asserted.filter(
            F.col("months_diff").isNotNull()
            & F.col("months_diff").between(0, first_line_months_threshold)
        )
        logger.info(
            "df_therapies_asserted with months diff count after first line filter = {}",
            df_therapies_asserted.count(),
        )
        df_therapies_asserted.show()

    # TO DO? check if i should remove negative months diff?
    # TO DO Wenn zwei Therapien am gleichen Tag starten, muss OP vor ST vor SYST stehen?

    # pivot
    w = Window.partitionBy("reason_reference").orderBy("months_diff")
    df_long = df_therapies_asserted.withColumn("therapy_index", F.row_number().over(w))

    pivot_cols = [
        "therapy_type",
        "therapy_id",
        "therapy_intention",
        "therapy_start_date",
        "therapy_end_date",
        "therapy_end_reason",
        "months_diff",
        "stellung_op",
        "ops_code",
        "surgery_outcome",
        "zielgebiet",
        "therapy_protocol_text",
        "medication_statement_text",
        "medication_statement_atc_code",
    ]
    id_cols = ["reason_reference"]

    # für den moment zu unübersichtlich
    # max_index = df_long.agg(F.max("therapy_index")).collect()[0][0]
    max_index = 10
    if max_index is None:
        raise ValueError("Keine Therapiedaten vorhanden.")

    pivoted_dfs = []

    for col_name in pivot_cols:
        df_pivot = (
            df_long.select(id_cols + ["therapy_index", col_name])
            .groupBy(id_cols)
            .pivot("therapy_index")
            .agg(F.first(col_name))
        )

        for i in range(1, max_index + 1):
            old_col = str(i)
            new_col = f"{col_name}_{i}"
            if old_col in df_pivot.columns:
                df_pivot = df_pivot.withColumnRenamed(old_col, new_col)

        pivoted_dfs.append(df_pivot)

    df_wide = reduce(lambda a, b: a.join(b, on=id_cols, how="left"), pivoted_dfs)

    df_final = (
        df_c61_clean.alias("c")
        .join(
            df_wide.alias("w"),
            F.col("c.condition_id") == F.col("w.reason_reference"),
            "left",
        )
        .drop("reason_reference")
    )

    # hier nach dem join nochmal filtern wenn first line
    # TO DO oder right join? think about it
    if first_line_months_threshold is not None:
        df_final = df_final.filter(F.col("months_diff_1").isNotNull())
        logger.info(
            "df_final count after first line months_diff_1 filter = {}",
            df_final.count(),
        )

    logger.info("df_final count = {}", df_final.count())
    df_final.show()
    return df_final


def map_atc_column(df, atc_col, out_col, mapping_expr):
    return df.withColumn(
        out_col,
        F.concat_ws(
            "|",
            F.transform(
                F.split(F.col(atc_col), r"\|"),
                lambda x: mapping_expr[F.trim(x)],
            ),
        ),
    )


def get_max_index_from_columns(df, prefix):
    pattern = re.compile(rf"{prefix}_(\d+)$")
    indices = []

    for c in df.columns:
        m = pattern.match(c)
        if m:
            indices.append(int(m.group(1)))

    return max(indices) if indices else 0


def with_mapped_atc_column(df, spark):
    # group ATC codes wie Prof. Wullich sagte
    mapping_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("sep", ";")
        .csv(
            "/home/coder/git/onco-analytics-on-fhir/src/analytics_on_fhir/"
            "analytics_on_fhir/"
            "Umsetzungsleitfaden_Substanzen_2025-08_Gruppen_Prof.Wullich.csv"
        )
    )

    mapping_df = (
        mapping_df.withColumn("atc_code", F.trim(F.col("atc_code")))
        .filter(F.col("atc_code").isNotNull())
        .filter(F.col("atc_code") != "")
    )

    # mapping dict
    mapping_dict = {r["atc_code"]: r["gruppe"] for r in mapping_df.collect()}

    mapping_expr = F.create_map(
        [F.lit(x) for kv in mapping_dict.items() if kv[0] is not None for x in kv]
    )

    result_df = df

    max_index = get_max_index_from_columns(df, "medication_statement_atc_code")

    for i in range(1, max_index + 1):
        result_df = map_atc_column(
            result_df,
            f"medication_statement_atc_code_{i}",
            f"gruppe_{i}",
            mapping_expr,
        )
    return result_df
