import glob
import os
import re
import shutil
from functools import reduce

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from loguru import logger
from pathling import PathlingContext, datasource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from settings import settings
from utils import HERE

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
                "path": (f"meta.profile.exists($this = '{FHIR_SYSTEMS_SYSTEM_THERAPY}')"),
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
        df_c61_clean.select(F.col("condition_id").alias("reason_reference"), "asserted_date"),
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
        F.round(F.months_between(F.col("therapy_start_date"), F.col("asserted_date")), 2).cast(
            "double"
        ),
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
        .csv(HERE + "/Umsetzungsleitfaden_Substanzen_2025-08_Gruppen_Prof.Wullich.csv")
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


# get all therapy cols dynamically since max can be different at each site
def get_therapy_cols(df, prefix="therapy_type_"):
    cols = [c for c in df.columns if re.match(rf"{prefix}\d+$", c)]
    return sorted(cols, key=lambda x: int(x.split("_")[-1]))


# maybe plot this also later -
def plot_therapy_sequence(df, df_description_for_title, settings):
    # get year min / max for current df
    year_min = df.select(F.min(F.year("asserted_date"))).first()[0]
    year_max = df.select(F.max(F.year("asserted_date"))).first()[0]
    therapy_cols = get_therapy_cols(df)
    logger.info("therapy_cols = {}", therapy_cols)
    cohort_counts = df.groupBy(therapy_cols).count().orderBy("count", ascending=False)
    logger.info("cohort_counts counts = {}", cohort_counts.count())
    cohort_counts.show(100)
    # for plot label
    cohort_counts_plot = cohort_counts.withColumn(
        "pattern", F.concat_ws("+", *[F.col(c) for c in therapy_cols])
    )
    cohort_counts_plot = cohort_counts_plot.orderBy(F.desc("count"))
    cohort_counts_plot_pd = cohort_counts_plot.toPandas()

    plt.figure()
    n_patterns = len(cohort_counts_plot_pd)
    fig_height = max(4, n_patterns * 0.5)
    plt.figure(figsize=(10, fig_height))
    ax = sns.barplot(data=cohort_counts_plot_pd, y="pattern", x="count")
    for i, v in enumerate(cohort_counts_plot_pd["count"]):
        ax.text(v, i, f" {v}", va="center")
    title = (
        f"first line (4 months after diagnosis) therapy sequence: "
        f"{df_description_for_title}, {year_min} - {year_max}, "
        f"{settings.location}"
    )
    ax.set_title(title, fontsize=15, fontweight="bold", loc="center")
    plt.show()


def plot_therapy_combinations(pdf, df_description_for_title, settings, year_min="", year_max=""):

    pdf = pdf.sort_values("count", ascending=False).reset_index(drop=True)
    combos = pdf["combo_label"]
    counts = pdf["count"]

    n = len(pdf)
    fig_height = min(6, max(3, n * 0.3))

    plt.figure(figsize=(10, fig_height))
    x = np.arange(n)

    bar_color = cm.viridis(0.7)

    plt.bar(x, counts, color=bar_color)

    for i in range(n):
        value = counts.iloc[i]

        if value > 0:
            offset = max(value * 0.02, 0.8)
            plt.text(
                i,
                value + offset,
                int(value),
                ha="center",
                va="bottom",
                color=bar_color,
                fontsize=9,
            )

    plt.xticks(x, combos, rotation=45, ha="right")

    title = (
        f"First line (4 months after diagnosis) therapy combinations: "
        f"{df_description_for_title}, {settings.location}"
    )

    plt.title(title, fontsize=15, fontweight="bold")

    plt.tight_layout()
    plt.show()


def aggregate_diagnosis_year_gleason(df, settings):
    print("hzi")
    df["asserted_date"] = pd.to_datetime(df["asserted_date"], errors="coerce")
    df["diagnosis_year"] = df["asserted_date"].dt.year

    df_diagnosis_year = pd.DataFrame(
        df.groupby(by="diagnosis_year", observed=False).size(), columns=["count"]
    )
    df_diagnosis_year_cohort = pd.DataFrame(
        df.loc[df["cohort_flag"] == 1].groupby(by="diagnosis_year", observed=False).size(),
        columns=["cohort_count"],
    )
    df_diagnosis_year = df_diagnosis_year.join(df_diagnosis_year_cohort, how="outer").fillna(0)

    for gleason in [6, 7, 8, 9, 10]:
        df_diagnosis_year_gleason = pd.DataFrame(
            df.loc[df["gleason_score"] == gleason]
            .groupby(by="diagnosis_year", observed=False)
            .size(),
            columns=[f"gleason_{gleason}_count"],
        )
        df_diagnosis_year = df_diagnosis_year.join(df_diagnosis_year_gleason, how="outer").fillna(0)

    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    df_diagnosis_year.to_csv(
        f"{output_dir}/diagnosis_year_{settings.location}.csv", index=True, sep=";"
    )

    return df_diagnosis_year


def aggregate_metastases_age(df, settings):
    df["asserted_date"] = pd.to_datetime(df["asserted_date"], errors="coerce")
    df["metastasis_date_first"] = pd.to_datetime(df["metastasis_date_first"], errors="coerce")
    df["has_metastasis"] = df["metastasis_loc"].notna()

    df["metastasis_kind"] = (
        df["metastasis_loc"]
        .fillna("")
        .apply(lambda x: "|".join(sorted(set(p.strip() for p in str(x).split("|") if p.strip()))))
        .replace("", pd.NA)
    )
    df["metastasis_kind"] = df["metastasis_kind"].apply(lambda x: "NON" if pd.isna(x) else x)
    df["days_to_metastasis"] = (df["metastasis_date_first"] - df["asserted_date"]).dt.days
    df["metastasis_synchron"] = df["days_to_metastasis"] < 121.75

    df_metastasis_loc = pd.DataFrame(
        df.groupby(by="metastasis_kind", observed=False).size(), columns=["overall_count"]
    )
    df_metastasis_loc_cohort = pd.DataFrame(
        df.loc[df["cohort_flag"] == 1].groupby(by="metastasis_kind", observed=False).size(),
        columns=["cohort_count"],
    )
    df_metastasis_loc = df_metastasis_loc.join(df_metastasis_loc_cohort, how="outer").fillna(0)

    df_metastasis_loc_synchron = pd.DataFrame(
        df.loc[df["metastasis_synchron"]].groupby(by="metastasis_kind", observed=False).size(),
        columns=["synchron_count"],
    )
    df_metastasis_loc = df_metastasis_loc.join(df_metastasis_loc_synchron, how="outer").fillna(0)

    for gleason in [6, 7, 8, 9, 10]:
        df_metastasis_loc_gleason = pd.DataFrame(
            df.loc[df["gleason_score"] == gleason]
            .groupby(by="metastasis_kind", observed=False)
            .size(),
            columns=[f"gleason_{gleason}_count"],
        )
        df_metastasis_loc = df_metastasis_loc.join(df_metastasis_loc_gleason, how="outer").fillna(0)

    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    df_metastasis_loc.to_csv(
        f"{output_dir}/metastasis_loc_{settings.location}.csv", index=True, sep=";"
    )
    return df_metastasis_loc


def aggregate_age_gleason(df, settings):
    bins = np.append(np.arange(0, 101, 5), 150)  # 0,5,10,...,100,150
    labels = bins[:-1]  # 0,5,10,...,100,

    df["age_class"] = pd.cut(df["age_at_diagnosis"], bins=bins, labels=labels)

    df_age_class = pd.DataFrame(
        df.groupby(by="age_class", observed=False).size(), columns=["count"]
    )
    df_age_class_cohort = pd.DataFrame(
        df.loc[df["cohort_flag"] == 1].groupby(by="age_class", observed=False).size(),
        columns=["cohort_count"],
    )
    df_age_class = df_age_class.join(df_age_class_cohort, how="outer").fillna(0)

    for gleason in [6, 7, 8, 9, 10]:
        df_age_class_gleason = pd.DataFrame(
            df.loc[df["gleason_score"] == gleason].groupby(by="age_class", observed=False).size(),
            columns=[f"gleason_{gleason}_count"],
        )
        df_age_class = df_age_class.join(df_age_class_gleason, how="outer").fillna(0)

    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    df_age_class.to_csv(f"{output_dir}/age_class_{settings.location}.csv", index=True, sep=";")
    return df_age_class


def aggregate_therapy_combinations(df, cohort_rest):

    therapy_cols = get_therapy_cols(df)

    therapy_values = (
        df.select(F.explode(F.array(*[F.col(c) for c in therapy_cols])).alias("therapy"))
        .filter(F.col("therapy").isNotNull())
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    for t in therapy_values:
        df = df.withColumn(
            f"has_{t}",
            F.greatest(*[F.when(F.col(c) == t, 1).otherwise(0) for c in therapy_cols]),
        )

    has_cols = [f"has_{t}" for t in therapy_values]
    combo_counts = df.groupBy(has_cols).count()
    expr = F.concat_ws("+", *[F.when(F.col(f"has_{t}") == 1, F.lit(t)) for t in therapy_values])
    combo_counts = combo_counts.withColumn("combo_label", expr)
    pdf = combo_counts.toPandas()
    pdf = pdf.sort_values("count", ascending=False)

    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    pdf.to_csv(
        f"{output_dir}/therapy_combinations_{cohort_rest}_{settings.location}.csv",
        index=True,
        sep=";",
    )
    return pdf


def aggregate_therapy_combinations_with_metastasis(df, cohort_rest):
    df = df.withColumn(
        "metastatic_flag",
        F.when(
            F.col("metastasis_loc").isNotNull() | F.col("metastasis_date_first").isNotNull(),
            "metastatic",
        ).otherwise("non_metastatic"),
    )

    therapy_cols = get_therapy_cols(df)

    therapy_values = (
        df.select(F.explode(F.array(*[F.col(c) for c in therapy_cols])).alias("therapy"))
        .filter(F.col("therapy").isNotNull())
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    for t in therapy_values:
        df = df.withColumn(
            f"has_{t}",
            F.greatest(*[F.when(F.col(c) == t, 1).otherwise(0) for c in therapy_cols]),
        )

    has_cols = [f"has_{t}" for t in therapy_values]
    combo_counts = df.groupBy(has_cols + ["metastatic_flag"]).count()
    expr = F.concat_ws("+", *[F.when(F.col(f"has_{t}") == 1, F.lit(t)) for t in therapy_values])
    combo_counts = combo_counts.withColumn("combo_label", expr)
    pdf = combo_counts.toPandas()

    pivot = pdf.pivot_table(
        index="combo_label",
        columns="metastatic_flag",
        values="count",
        fill_value=0,
    )

    if "metastatic" not in pivot.columns:
        pivot["metastatic"] = 0
    if "non_metastatic" not in pivot.columns:
        pivot["non_metastatic"] = 0

    pivot["total"] = pivot["metastatic"] + pivot["non_metastatic"]

    pivot = pivot.sort_values("total", ascending=False)

    output_dir = os.path.join(HERE, settings.results_directory_path, settings.study_name.value)
    pdf.to_csv(
        f"{output_dir}/therapy_combinations_met_{cohort_rest}_{settings.location}.csv",
        index=True,
        sep=";",
    )

    return pivot.reset_index()


def aggregate_local_csvs(pandas_df_therapy_sequence_first_line_4_months, settings):
    # rest hier unten auslagern
    # diagnosis year gleason
    df_diagnosis_year = aggregate_diagnosis_year_gleason(
        pandas_df_therapy_sequence_first_line_4_months, settings
    )
    print(df_diagnosis_year)
    # metastasis age
    df_metastases = aggregate_metastases_age(
        pandas_df_therapy_sequence_first_line_4_months, settings
    )
    print(df_metastases)
    # age classes cohort gleason
    df_age_gleason = aggregate_age_gleason(pandas_df_therapy_sequence_first_line_4_months, settings)
    print(df_age_gleason)

    # aggregate therapies
    df_therapy_sequence_first_line_4_months_cohort = pandas_df_therapy_sequence_first_line_4_months[
        pandas_df_therapy_sequence_first_line_4_months["cohort_flag"] == 1
    ]

    df_therapy_sequence_first_line_4_months_rest = pandas_df_therapy_sequence_first_line_4_months[
        pandas_df_therapy_sequence_first_line_4_months["cohort_flag"] == 0
    ]
    # to do - vereinheitlichen, keine Zeit gehabt, mitgewachsen
    aggregate_therapy_combinations(df_therapy_sequence_first_line_4_months_cohort, "cohort")
    aggregate_therapy_combinations(df_therapy_sequence_first_line_4_months_rest, "rest")
    aggregate_therapy_combinations_with_metastasis(
        df_therapy_sequence_first_line_4_months_cohort, "cohort"
    )
    aggregate_therapy_combinations_with_metastasis(
        df_therapy_sequence_first_line_4_months_rest, "rest"
    )

    # plot diagnosis year before 1990, 1990-2000, 2000-2010, 2010-2020, 2020-2026 z.B.
    # filter date diagnosis, cohort, metastasis
    # Cohort-Flag
    df_cohort = df_therapy_sequence_first_line_4_months_cohort
    df_rest = df_therapy_sequence_first_line_4_months_rest

    year_bins = [
        # ("<1990", "0000-01-01", "1990-01-01"),
        ("1990-2000", "1990-01-01", "2000-01-01"),
        ("2000-2010", "2000-01-01", "2010-01-01"),
        ("2010-2020", "2010-01-01", "2020-01-01"),
        ("2020-2026", "2020-01-01", "2026-12-31"),
    ]

    for label_flag, df_group in [("cohort", df_cohort), ("rest", df_rest)]:
        for year_label, start, end in year_bins:
            df_interval = df_group.filter(
                (F.col("asserted_date") >= F.lit(start)) & (F.col("asserted_date") < F.lit(end))
            )
            aggregate_therapy_combinations_with_metastasis(df_interval, f"{label_flag}{year_label}")


def plot_therapies_metastasis_split(
    pivot_df, df_description_for_title, settings, year_min="", year_max=""
):
    pivot_df["total"] = pivot_df["metastatic"] + pivot_df["non_metastatic"]
    pivot_df = pivot_df.sort_values("total", ascending=False).reset_index(drop=True)

    combos = pivot_df["combo_label"]
    met = pivot_df["metastatic"]
    non_met = pivot_df["non_metastatic"]

    n = len(pivot_df)
    fig_height = min(6, max(3, n * 0.3))

    plt.figure(figsize=(10, fig_height))
    x = np.arange(n)

    color_non = cm.viridis(0.3)
    color_met = cm.viridis(0.8)

    plt.bar(x, non_met, label="Non-metastatic", color=color_non)
    plt.bar(x, met, bottom=non_met, label="Metastatic", color=color_met)

    for i in range(n):
        if non_met.iloc[i] >= 5:
            plt.text(
                i,
                non_met.iloc[i] / 2,
                int(non_met.iloc[i]),
                ha="center",
                va="center",
                color="white",
                fontsize=8,
            )
        elif non_met.iloc[i] > 0:
            plt.text(
                i,
                non_met.iloc[i] + 1,
                int(non_met.iloc[i]),
                ha="center",
                va="bottom",
                color=color_non,
                fontsize=6,
            )

        if met.iloc[i] >= 5:
            plt.text(
                i,
                non_met.iloc[i] + met.iloc[i] / 2,
                int(met.iloc[i]),
                ha="center",
                va="center",
                color="white",
                fontsize=8,
            )
        elif met.iloc[i] > 0:
            plt.text(
                i,
                non_met.iloc[i] + met.iloc[i] + 1,
                int(met.iloc[i]),
                ha="center",
                va="bottom",
                color=color_met,
                fontsize=6,
            )

    plt.xticks(x, combos, rotation=45, ha="right")

    title = (
        f"Therapy combinations metastasis split: {df_description_for_title}, {settings.location}"
    )

    plt.title(title, fontsize=15, fontweight="bold")
    plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.05), ncol=2)

    plt.tight_layout()
    plt.show()


def write_merged_csv(df, destination_dir, filename, sep=";"):

    os.makedirs(destination_dir, exist_ok=True)

    tmp_dir = os.path.join(destination_dir, "__tmp_write__")

    (df.coalesce(1).write.mode("overwrite").option("header", True).option("sep", sep).csv(tmp_dir))

    part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]

    final_path = os.path.join(destination_dir, filename)

    if os.path.exists(final_path):
        os.remove(final_path)

    shutil.move(part_file, final_path)
    shutil.rmtree(tmp_dir)

    print(f"Merged CSV written to: {final_path}")


def process_csvs(spark):
    csv_dir = os.path.join(HERE, "pca_csvs")
    print(csv_dir)
    merged_csvs_dir = os.path.join(HERE, "pca_csvs_merged")
    print(merged_csvs_dir)

    aggregate_merge_write(
        spark=spark,
        source_dir=csv_dir,
        pattern="diagnosis_year_*.csv",
        group_column="diagnosis_year",
        destination_dir=merged_csvs_dir,
        output_filename="diagnosis_year_bzkf.csv",
    )

    aggregate_merge_write(
        spark=spark,
        source_dir=csv_dir,
        pattern="age_class_*.csv",
        group_column="age_class",
        destination_dir=merged_csvs_dir,
        output_filename="age_class_bzkf.csv",
    )

    aggregate_merge_write(
        spark=spark,
        source_dir=csv_dir,
        pattern="metastasis_loc_*.csv",
        group_column="metastasis_kind",
        destination_dir=merged_csvs_dir,
        output_filename="metastasis_loc_bzkf.csv",
    )

    therapy_csvs = [
        "therapy_combinations_cohort_",
        "therapy_combinations_rest_",
    ]
    for therapy_csv in therapy_csvs:
        aggregate_merge_write_therapy(
            spark=spark,
            source_dir=csv_dir,
            pattern=f"{therapy_csv}*.csv",
            group_column="combo_label",
            destination_dir=merged_csvs_dir,
            output_filename=f"{therapy_csv}bzkf.csv",
        )
    # list of all therapy csv files with metastatic_flag
    # dont list "therapy_combinations_met_cohort_" and "therapy_combinations_met_rest_" here!
    therapy_csvs = [
        "therapy_combinations_met_cohort_1990-2000_",
        "therapy_combinations_met_cohort_2000-2010_",
        "therapy_combinations_met_cohort_2010-2020_",
        "therapy_combinations_met_cohort_2020-2026_",
        "therapy_combinations_met_rest_1990-2000_",
        "therapy_combinations_met_rest_2000-2010_",
        "therapy_combinations_met_rest_2010-2020_",
        "therapy_combinations_met_rest_2020-2026_",
    ]
    for therapy_csv in therapy_csvs:
        aggregate_merge_write_therapy(
            spark=spark,
            source_dir=csv_dir,
            pattern=f"{therapy_csv}*.csv",
            group_column=["combo_label", "metastatic_flag"],
            destination_dir=merged_csvs_dir,
            output_filename=f"{therapy_csv}bzkf.csv",
        )
    # list them specifically here! otherwise will be counted double because of year files
    therapy_met_cohort_csvs = [
        "therapy_combinations_met_cohort_TUM.csv",
        "therapy_combinations_met_cohort_UKA.csv",
        "therapy_combinations_met_cohort_uker.csv",
        "therapy_combinations_met_cohort_UKW.csv",
    ]
    dfs = []
    for file in therapy_met_cohort_csvs:
        df = spark.read.option("header", True).option("sep", ";").csv(os.path.join(csv_dir, file))
        df = df.toDF(*[c.strip() for c in df.columns])
        if df.columns[0] == "" or df.columns[0].startswith("_c"):
            df = df.drop(df.columns[0])
        df = df.select("combo_label", "metastatic_flag", "count")
        df = df.withColumn("combo_label", F.lower(F.trim(F.col("combo_label"))))
        df = df.withColumn("metastatic_flag", F.lower(F.trim(F.col("metastatic_flag"))))
        df = df.withColumn("count", F.col("count").cast("double"))
        df = df.groupBy("combo_label", "metastatic_flag").agg(F.sum("count").alias("count"))
        dfs.append(df)

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df)

    df_sum = (
        df_all.groupBy("combo_label", "metastatic_flag")
        .agg(F.sum("count").alias("count"))
        .orderBy("combo_label", "metastatic_flag")
    )

    df_sum.show()

    write_merged_csv(df_sum, merged_csvs_dir, filename="therapy_combinations_met_cohort_bzkf.csv")

    therapy_met_rest_csvs = [
        "therapy_combinations_met_rest_TUM.csv",
        "therapy_combinations_met_rest_UKA.csv",
        "therapy_combinations_met_rest_uker.csv",
        "therapy_combinations_met_rest_UKW.csv",
    ]
    dfs = []

    for file in therapy_met_rest_csvs:
        df = spark.read.option("header", True).option("sep", ";").csv(os.path.join(csv_dir, file))
        df = df.toDF(*[c.strip() for c in df.columns])
        if df.columns[0] == "" or df.columns[0].startswith("_c"):
            df = df.drop(df.columns[0])
        df = df.select("combo_label", "metastatic_flag", "count")
        df = df.withColumn("combo_label", F.lower(F.trim(F.col("combo_label"))))
        df = df.withColumn("metastatic_flag", F.lower(F.trim(F.col("metastatic_flag"))))
        df = df.withColumn("count", F.col("count").cast("double"))
        df = df.groupBy("combo_label", "metastatic_flag").agg(F.sum("count").alias("count"))
        dfs.append(df)

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df)

    df_sum = (
        df_all.groupBy("combo_label", "metastatic_flag")
        .agg(F.sum("count").alias("count"))
        .orderBy("combo_label", "metastatic_flag")
    )

    df_sum.show()

    write_merged_csv(df_sum, merged_csvs_dir, filename="therapy_combinations_met_rest_bzkf.csv")


def aggregate_merge_write(
    spark,
    source_dir,
    pattern,
    group_column,
    destination_dir,
    output_filename,
):
    print(f"\nReading pattern: {pattern}")
    print(f"Source dir: {source_dir}")
    print(f"Destination dir: {destination_dir}")

    files = glob.glob(os.path.join(source_dir, pattern))
    files = [f for f in files if not os.path.basename(f).startswith("_")]

    if not files:
        raise ValueError(f"No matching files found for pattern: {pattern}")

    print("Files being read:")
    for f in files:
        print(f)

    df_all = (
        spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(files)
    )

    # drop index col if there is one
    first_col = df_all.columns[0]

    if first_col.strip() == "" or first_col.startswith("_c"):
        df_all = df_all.drop(first_col)

    if "combo_label" in df_all.columns:
        df_all = df_all.select("combo_label", "count")

    value_columns = [c for c in df_all.columns if c != group_column]

    df_sum = (
        df_all.groupBy(group_column)
        .agg(*[F.sum(c).alias(c) for c in value_columns])
        .orderBy(group_column)
    )

    df_sum.show()

    write_merged_csv(
        df_sum,
        destination_dir,
        filename=output_filename,
    )


def aggregate_merge_write_therapy(
    spark,
    source_dir,
    pattern,
    group_column: str | list[str],
    destination_dir,
    output_filename,
):
    print(f"\nReading pattern: {pattern}")
    print(f"Source dir: {source_dir}")
    print(f"Destination dir: {destination_dir}")

    if isinstance(group_column, str):
        group_column = [group_column]

    files = glob.glob(os.path.join(source_dir, pattern))
    files = [f for f in files if not os.path.basename(f).startswith("_")]

    if not files:
        raise ValueError(f"No matching files found for pattern: {pattern}")

    print("Files being read:")
    for f in files:
        print(f)

    dfs = []

    for file in files:
        df = spark.read.option("header", True).option("sep", ";").csv(file)

        df = df.toDF(*[c.strip() for c in df.columns])

        first_col = df.columns[0]
        if first_col == "" or first_col.startswith("_c"):
            df = df.drop(first_col)

        required_cols = group_column + ["count"]

        if all(col in df.columns for col in required_cols):
            df = df.select(*required_cols)

            df = df.withColumn("count", F.col("count").cast("double"))

            df = df.groupBy(*group_column).agg(F.sum("count").alias("count"))

            dfs.append(df)

    if not dfs:
        raise ValueError("No valid therapy files found.")

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df)

    df_all.show()
    df_sum = df_all.groupBy(*group_column).agg(F.sum("count").alias("count")).orderBy(*group_column)

    df_sum.show()

    write_merged_csv(
        df_sum,
        destination_dir,
        filename=output_filename,
    )


def aggregate_merge_write_therapy_met(
    spark,
    source_dir,
    pattern,
    destination_dir,
    output_filename,
):
    import glob
    import os

    from pyspark.sql import functions as F

    files = glob.glob(os.path.join(source_dir, pattern))
    files = [f for f in files if not os.path.basename(f).startswith("_")]

    if not files:
        raise ValueError(f"No matching files found for pattern: {pattern}")

    dfs = []

    for file in files:
        df = spark.read.option("header", True).option("sep", ";").csv(file)
        df = df.toDF(*[c.strip() for c in df.columns])
        if df.columns[0] == "" or df.columns[0].startswith("_c"):
            df = df.drop(df.columns[0])

        df = df.select("combo_label", "metastatic_flag", "count")
        df = df.withColumn("combo_label", F.lower(F.trim(F.col("combo_label"))))
        df = df.withColumn("metastatic_flag", F.lower(F.trim(F.col("metastatic_flag"))))
        df = df.withColumn("count", F.col("count").cast("double"))

        df = df.groupBy("combo_label", "metastatic_flag").agg(F.sum("count").alias("count"))
        dfs.append(df)

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df)

    df_sum = (
        df_all.groupBy("combo_label", "metastatic_flag")
        .agg(F.sum("count").alias("count"))
        .orderBy("combo_label", "metastatic_flag")
    )
    df_sum.show()

    write_merged_csv(df_sum, destination_dir, filename=output_filename)


def read_merged_csvs(spark):
    merged_csvs_dir = os.path.join(HERE, "pca_csvs_merged")

    print("Directory:", merged_csvs_dir)
    print("Files in dir:", os.listdir(merged_csvs_dir))

    dfs = {}

    for file in os.listdir(merged_csvs_dir):
        if file.endswith(".csv"):
            df_name = os.path.splitext(file)[0]
            file_path = os.path.join(merged_csvs_dir, file)

            df = spark.read.option("header", True).option("sep", ";").csv(file_path)
            if "count" in df.columns:
                df = df.withColumn("count", F.col("count").cast("double").cast("int"))

            dfs[df_name] = df

    return dfs


def plot_diagnosis_year(df, df_description_for_title, settings):

    numeric_cols = [
        "count",
        "cohort_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
        "diagnosis_year",
    ]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    # df["rest_count"] = df["count"] - df["cohort_count"]

    df = df.loc[df["diagnosis_year"] >= 1990]

    df["no_gleason_count"] = (
        df["count"]
        - df["gleason_6_count"]
        - df["gleason_7_count"]
        - df["gleason_8_count"]
        - df["gleason_9_count"]
        - df["gleason_10_count"]
    )
    # Overall Patient numbers by year (cohort separat)
    plt.bar(data=df, x="diagnosis_year", height="count", label="overall", color="#440154")

    plt.bar(data=df, x="diagnosis_year", height="cohort_count", label="cohort", color="#fde725")

    plt.ylabel("Number of patients")
    plt.xlabel("Year of C61 diagnosis")
    plt.legend()
    title = (
        f"Overall Patient numbers by year (cohort separate): "
        f"{df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.show()

    # Create a stacked bar chart
    plt.figure(figsize=(11, 6))
    bottom = None
    i = 0
    for col in [
        "no_gleason_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
    ]:
        color = plt.cm.viridis(1 - i / (6 - 1))
        # print(bottom)

        plt.bar(df["diagnosis_year"], df[col], bottom=bottom, label=col, color=color)
        if bottom is None:
            bottom = df[col].copy()
        else:
            bottom += df[col].copy()
        i = i + 1

    plt.xlabel("Diagnosis Year")
    plt.ylabel("Number of patients")
    title = (
        f"Stacked Bar Chart of Gleason Counts by Diagnosis Year after 1990: "
        f"{df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.legend()
    plt.show()


def plot_age_class(df, df_description_for_title, settings):
    numeric_cols = [
        "count",
        "cohort_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
        "age_class",
    ]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    # df["rest_count"] = df["count"] - df["cohort_count"]

    df["no_gleason_count"] = (
        df["count"]
        - df["gleason_6_count"]
        - df["gleason_7_count"]
        - df["gleason_8_count"]
        - df["gleason_9_count"]
        - df["gleason_10_count"]
    )

    ## Lower and Upper age limit
    low_lim = 45
    up_lim = 80

    df_age = df.copy()
    cols = [
        "count",
        "cohort_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
        "no_gleason_count",
    ]
    # sum for ages <40
    sums_lt = df.loc[df["age_class"] < low_lim, cols].sum()
    # sum for ages >85
    sums_gt = df.loc[df["age_class"] > up_lim, cols].sum()
    # keep rows between 40 and 85 inclusive
    df_mid = (
        df.loc[(df["age_class"] >= low_lim) & (df["age_class"] <= up_lim)]
        .reset_index(drop=True)
        .copy()
    )
    df_mid["age_class_label"] = (
        df_mid["age_class"].astype(str) + " - " + (df_mid["age_class"] + 4).astype(str)
    )

    row_lt = sums_lt.to_dict()
    row_lt["age_class_label"] = f"<{low_lim}"
    row_lt["age_class"] = f"<{low_lim}"
    row_gt = sums_gt.to_dict()
    row_gt["age_class_label"] = f">={up_lim + 5}"
    row_gt["age_class"] = f">{up_lim + 5}"
    df_age = pd.concat([pd.DataFrame([row_lt]), df_mid, pd.DataFrame([row_gt])], ignore_index=True)
    df_age["age_class"] = df_age["age_class"].astype(str)

    plt.bar(data=df_age, x="age_class_label", height="count", label="overall", color="#440154")

    plt.bar(
        data=df_age, x="age_class_label", height="cohort_count", label="cohort", color="#fde725"
    )

    plt.xticks(rotation=90)
    plt.ylabel("Number of patients")
    plt.xlabel("Age Class")
    plt.legend()
    title = (
        f"Overall Patient numbers by age classes: {df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.show()

    # Create a stacked bar chart
    plt.figure(figsize=(10, 6))
    bottom = None
    i = 0
    for col in [
        "no_gleason_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
    ]:
        color = plt.cm.viridis(1 - i / (6 - 1))
        # print(bottom)

        plt.bar(df_age["age_class_label"], df_age[col], bottom=bottom, label=col, color=color)
        if bottom is None:
            bottom = df_age[col].copy()
        else:
            bottom += df_age[col].copy()
        i = i + 1

    plt.xlabel("Age Class")
    plt.ylabel("Number of patients")
    title = (
        f"Stacked Bar Chart of Gleason Counts by Age Class: "
        f"{df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.legend()
    plt.show()


def plot_metastasis_loc(df, df_description_for_title, settings):
    df = df.loc[df["metastasis_kind"] != "NON"]

    numeric_cols = [
        "overall_count",
        "cohort_count",
        "synchron_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
    ]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["no_gleason_count"] = (
        df["overall_count"]
        - df["gleason_6_count"]
        - df["gleason_7_count"]
        - df["gleason_8_count"]
        - df["gleason_9_count"]
        - df["gleason_10_count"]
    )

    # df["rest_count"] = df["overall_count"] - df["cohort_count"]

    # Filter rows where both rest and cohort counts are > 3
    df_filtered = df[(df["overall_count"] > 3) | (df["cohort_count"] > 3)]

    # Sort by total height (overall_count)
    df_filtered = df_filtered.sort_values(by="overall_count", ascending=False)

    # --- Bar plot for cohort vs rest ---
    plt.figure(figsize=(12, 6))
    plt.bar(
        data=df_filtered,
        x="metastasis_kind",
        height="overall_count",
        label="overall",
        color="#440154",
    )
    plt.bar(
        data=df_filtered,
        x="metastasis_kind",
        height="cohort_count",
        label="cohort",
        color="#fde725",
    )
    plt.ylabel("Number of patients")
    plt.xlabel("Metastasis Status")
    plt.xticks(rotation=90)
    plt.legend()
    title = (
        f"Overall Patient numbers by kind of metastasis: "
        f"{df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.show()

    # synchron/metachron
    plt.figure(figsize=(12, 6))
    plt.bar(
        data=df_filtered,
        x="metastasis_kind",
        height="overall_count",
        label="overall",
        color="#440154",
    )
    plt.bar(
        data=df_filtered,
        x="metastasis_kind",
        height="synchron_count",
        label="synchron metastasis",
        color="#25fd8d",
    )

    plt.ylabel("Number of patients")
    plt.xlabel("Metastasis Status")
    plt.xticks(rotation=90)
    plt.legend()
    title = (
        f"Synchron (4 months after diagnosis) vs overall metastasis: "
        f"{df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.show()

    # --- Stacked bar chart for Gleason ---
    plt.figure(figsize=(12, 6))
    bottom = None
    i = 0
    gleason_cols = [
        "no_gleason_count",
        "gleason_6_count",
        "gleason_7_count",
        "gleason_8_count",
        "gleason_9_count",
        "gleason_10_count",
    ]
    df_gleason_filtered = df_filtered.copy()
    # Optional: filter Gleason counts <= 3
    # df_gleason_filtered[gleason_cols] = df_gleason_filtered[gleason_cols].applymap(
    #    lambda x: x if x > 3 else 0
    # )

    for i, col in enumerate(gleason_cols):
        color = plt.cm.viridis(1 - i / (len(gleason_cols) - 1))
        plt.bar(
            df_gleason_filtered["metastasis_kind"],
            df_gleason_filtered[col],
            bottom=bottom,
            label=col,
            color=color,
        )
        if bottom is None:
            bottom = df_gleason_filtered[col].copy()
        else:
            bottom += df_gleason_filtered[col].copy()

    plt.xlabel("Metastasis Status")
    plt.ylabel("Number of patients")
    plt.xticks(rotation=90)
    title = (
        f"Stacked Bar Chart of Gleason Counts by Metastasis Status: "
        f"{df_description_for_title}, {settings.location}"
    )
    plt.title(title)
    plt.legend()
    plt.show()
    plt.legend()
    plt.show()
