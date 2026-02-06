from loguru import logger
from pathling import PathlingContext, datasource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics_on_fhir.settings import settings

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
FHIR_SYSTEMS_SURGERY_OPS_CS = (
    "http://fhir.de/CodeSystem/bfarm/ops"
)


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
                        "name": "procedure_resource_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    # ! empty, created issue in obds-to-fhir, we need this for joining
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "procedure_condition_resource_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "procedure_patient_resource_id",
                    },
                    {
                        "description": "Intention System Therapy",
                        "path": f"extension('{FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION}')"
                        ".value.ofType(CodeableConcept).coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SYSTEM_THERAPY_INTENTION_CS}')"
                        ".code",
                        "name": "intention",
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
                        "name": "procedure_therapie_art",
                    },
                    {
                        "description": "Systemische Therapie Beginn",
                        "path": "performed.ofType(Period).start",
                        "name": "procedure_therapy_start_date",
                    },
                    {
                        "description": "Systemische Therapie Ende",
                        "path": "performed.ofType(Period).end",
                        "name": "procedure_therapy_end_date",
                    },
                    {
                        "description": "Systemische Therapie Ende Grund",
                        "path": "outcome.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_THERAPY_END_REASON_CS}')"
                        ".code",
                        "name": "procedure_therapy_end_reason",
                    },
                    {
                        "description": "Protokoll der systemischen Therapie",
                        "path": "usedCode.text",
                        "name": "procedure_therapy_protocol_text",
                    },
                ],
            }
        ],
        where=[
            {
                "description": "Only System Therapy Procedures",
                "path": (
                    "meta.profile.exists("
                    " $this = "
                    "'https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/"
                    "StructureDefinition/mii-pr-onko-systemische-therapie|2026.0.0'"  # very bad practice - ggf über codes machen
                    ")"
                ),
            }
        ],
    )
    logger.info("df_procedures count = {}", df_procedures.count())
    logger.info(
        "df_procedures distinct procedure_resource_id count = {}",
        df_procedures.select("procedure_resource_id").distinct().count(),
    )
    logger.info(
        "df_procedures distinct patient id count = {}",
        df_procedures.select("procedure_patient_resource_id").distinct().count(),
    )
    df_procedures.orderBy(F.col("procedure_condition_resource_id")).show(truncate=False)

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
                        "name": "medication_statement_procedure_resource_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "medication_statement_patient_resource_id",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "medication_statement_condition_resource_id",
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

    logger.info("df_medication_statements_count = {}", df_medication_statements.count())
    logger.info(
        "df_medication_statements_count distinct condition id count = {}",
        df_medication_statements.select("medication_statement_condition_resource_id")
        .distinct()
        .count(),
    )
    logger.info(
        "df_medication_statements_count distinct procedure id count = {}",
        df_medication_statements.select("medication_statement_procedure_resource_id")
        .distinct()
        .count(),
    )
    df_medication_statements = df_medication_statements.orderBy(
        F.col("medication_statement_condition_resource_id")
    )
    df_medication_statements.show()

    # ACHTUNG - hier erst joinen procedure+medicationstatements und danach gruppieren
    df_procedures_medication_statements = df_procedures.join(
        df_medication_statements,
        df_procedures["procedure_resource_id"]
        == df_medication_statements["medication_statement_procedure_resource_id"],
        how="left",
    )
    logger.info(
        "df_procedures_medication_statements count = {}",
        df_procedures_medication_statements.count(),
    )
    df_procedures_medication_statements.orderBy(
        F.col("medication_statement_condition_resource_id")
    ).show()

    # pro condition und start date: komma separiert abspeichern
    df_procedures_medication_statements_grouped = (
        df_procedures_medication_statements.groupBy(
            "medication_statement_patient_resource_id",
            "medication_statement_condition_resource_id",
            "medication_statement_start_date",
            "procedure_resource_id",
        ).agg(
            F.concat_ws(", ", F.collect_set("medication_statement_text")).alias(
                "medication_statement_text"
            ),
            F.concat_ws(", ", F.collect_set("medication_statement_atc_code")).alias(
                "medication_statement_atc_code"
            ),
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
            F.col("p.procedure_resource_id") == F.col("pg.procedure_resource_id"),
            how="left",
        )
        .drop(F.col("pg.procedure_resource_id"))
    )

    logger.info(
        "df_procedures_medication_statements_final count = {}",
        df_procedures_medication_statements_final.count(),
    )
    logger.info(
        "df_procedures_medication_statements_final distinct procedure_resource_id count = {}",
        df_procedures_medication_statements_final.select("procedure_resource_id")
        .distinct()
        .count(),
    )
    logger.info(
        "df_procedures_medication_statements_final distinct patient id count = {}",
        df_procedures_medication_statements_final.select(
            "procedure_patient_resource_id"
        )
        .distinct()
        .count(),
    )
    df_procedures_medication_statements_final.orderBy(
        F.col("medication_statement_condition_resource_id")
    ).show()

    # prüfen ob ich das noch so wie bei DKTK machen will
    # cast dates, (group one therapy per cond id per start date) - das hier will ich definitiv nicht, add months diff,
    # filter negative months diff
    # systemtherapies_final = preprocess_therapy_df(systemtherapies)

    # join on condition id later - we need reason reference in procedures for correct joining!
    # (mehrfachkarzinome machen join auf patid ungenau)
    return df_procedures_medication_statements_final

def extract_radiotherapies(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: settings,
    spark: SparkSession,
) -> DataFrame:

    logger.info("extract radiotherapies.")

    df_radiotherapies = data.view(
        "Procedure",
        select=[
            {
                "column": [
                    {
                        "description": "Procedure ID",
                        "path": "getResourceKey()",
                        "name": "procedure_id",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    # ! empty, created issue in obds-to-fhir, we need this for joining
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "procedure_condition_resource_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "procedure_patient_resource_id",
                    },
                    {
                        "description": "Intention Radio Therapy",
                        "path": f"extension('{FHIR_SYSTEMS_RADIO_THERAPY_INTENTION}')"
                        ".value.ofType(CodeableConcept).coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_RADIO_THERAPY_INTENTION_CS}')"
                        ".code",
                        "name": "intention",
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
                        "name": "stellung_op",
                    },
                    {
                        "description": "Radio Therapy Start",
                        "path": "performed.ofType(Period).start",
                        "name": "procedure_therapy_start_date",
                    },
                    {
                        "description": "Radio Therapy Ende",
                        "path": "performed.ofType(Period).end",
                        "name": "procedure_therapy_end_date",
                    },
                    {
                        "description": "Radio Therapy Ende Grund",
                        "path": "outcome.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_THERAPY_END_REASON_CS}')"
                        ".code",
                        "name": "procedure_therapy_end_reason",
                    },
                    {
                        "description": "Radio Therapy Zielgebiet",
                        "path": "bodySite.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_RADIO_THERAPY_ZIELGEBIET_CS}')"
                        ".code",
                        "name": "zielgebiet",
                    },
                ],
            }
        ],
        where=[
            {
                "description": "Only Radiotherapy Procedures",
                "path": (
                    "meta.profile.exists($this = "
                    "'https://www.medizininformatik-initiative.de/fhir/ext/"
                    "modul-onko/StructureDefinition/mii-pr-onko-strahlentherapie|2026.0.0')"  # very bad practice - ggf über codes machen
                ),
            }
        ],
    )

    logger.info("df_radiotherapies count = {}", df_radiotherapies.count())

    df_radiotherapies.orderBy(F.col("zielgebiet")).show()

    # alle ohne part of sind die klammer procedures
    # alle mit part of kann ich dann über procedure id und partof reference ranjoinen
    return df_radiotherapies


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
                        "name": "therapy_resource_id",
                        "description": "Procedure ID",
                    },
                    {
                        "description": "FHIR Profile URL",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "procedure_condition_resource_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "procedure_patient_resource_id",
                    },
                    {
                        "description": "Intention Surgery",
                        "path": f"extension('{FHIR_SYSTEMS_SURGERY_INTENTION}')"
                        ".value.ofType(CodeableConcept).coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SURGERY_INTENTION_CS}')"
                        ".code",
                        "name": "intention",
                    },
                    {
                        "description": "Surgery Date",
                        "path": "performedDateTime",
                        "name": "procedure_surgery_date",
                    },
                    {
                        "description": "Surgery Outcome",
                        "path": "outcome.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SURGERY_OUTCOME_CS}')"
                        ".code",
                        "name": "procedure_surgery_outcome",
                    },
                    {
                        "description": "OPS Code",
                        "path": "code.coding"
                        ".where(system = "
                        f"'{FHIR_SYSTEMS_SURGERY_OPS_CS}')"
                        ".code",
                        "name": "ops",
                    },
                ]
            }
        ],
        where=[
            {
                "description": "Only Surgical Procedures",
                "path": (
                    "meta.profile.exists($this = "
                    "'https://www.medizininformatik-initiative.de/fhir/ext/"
                    "modul-onko/StructureDefinition/mii-pr-onko-operation|2026.0.0')"  # very bad practice - ggf über codes machen
                ),
            }
        ],
    )

    # OLD
    # Spark-Teil wie vorher
    """ df_ops = df_ops.filter(
        (F.col("therapy_type") == "OP") & F.col("icd10_code").like("C61%")
    ).orderBy(F.col("condition_id"))
    """
    logger.info("df_ops_count = {}", df_ops.count())
    df_ops.show()

    # downstream unverändert - ggf aus altem übernehmen wenns noch passt
    # df_ops = preprocess_therapy_df(df_ops)
    df_ops.write.options(header='True').csv("test.csv")

    return df_ops