import pathlib

from graphviz import Digraph
from loguru import logger
from pathling import DataSource
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    count_distinct,
    lit,
    max,
    min,
    when,
)

from .utils import find_closest_to_diagnosis

FHIR_SYSTEMS_CONDITION_ASSERTED_DATE = (
    "http://hl7.org/fhir/StructureDefinition/condition-assertedDate"
)


def extract(data: DataSource, results_directory: pathlib.Path) -> DataFrame:
    conditions = data.view(
        "Condition",
        select=[
            {
                "column": [
                    {
                        "description": "Condition ID",
                        "path": "getResourceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "patient_id",
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
                ],
            }
        ],
        where=[
            {
                "description": "Only PCa",
                "path": "code.coding.exists(system = "
                + "'http://fhir.de/CodeSystem/bfarm/icd-10-gm' and code='C61')",
            }
        ],
    )

    tnm = data.view(
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
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "patient_id",
                    },
                    {
                        "description": "Condition ID",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Observation date time",
                        "path": "effective.ofType(dateTime)",
                        "name": "effective_date_time",
                    },
                    {
                        "description": "Code",
                        "path": "code.coding"
                        + ".where(system = 'http://snomed.info/sct').code",
                        "name": "code_coding_sct",
                    },
                    {
                        "description": "Observation value",
                        "path": "value.ofType(CodeableConcept).coding.where(system = "
                        + "'https://www.uicc.org/resources/tnm').code",
                        "name": "tnm_value",
                    },
                ],
                "select": [
                    {
                        "forEach": "code.coding",
                        "column": [
                            {
                                "description": "Observation code",
                                "path": "code",
                                "name": "code",
                            },
                            {
                                "description": "Observation display",
                                "path": "display",
                                "name": "display",
                            },
                        ],
                    },
                ],
            }
        ],
        where=[
            {
                "description": "p/cM and p/cN TNM categories",
                "path": "code.coding.exists(system = 'http://snomed.info/sct' and "
                + "(code='399387003' or code='371497001' "
                + "or code='371494008' or code='399534004'))",
            }
        ],
    )

    medication_statements = data.view(
        "MedicationStatement",
        select=[
            {
                "column": [
                    {
                        "description": "MedicationStatement ID",
                        "path": "getResourceKey()",
                        "name": "id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "patient_id",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Effective Start Date",
                        "path": "effective.ofType(Period).start",
                        "name": "medication_start_date",
                    },
                    {
                        "description": "Medication Text",
                        "path": "medication.ofType(CodeableConcept).text",
                        "name": "medication_text",
                    },
                    {
                        "description": "Medication Code",
                        "path": "medication.ofType(CodeableConcept).coding"
                        + ".where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                        "name": "medication_atc_code",
                    },
                ],
            }
        ],
    )

    counts = {}

    row = conditions.select(
        count_distinct(conditions.patient_id).alias("n_patients")
    ).first()
    # TODO: for every row is not none, set count to 0 if it is None
    if row is not None:
        counts["num_c61_patients"] = row["n_patients"]

    row = conditions.select(
        min("asserted_date").alias("min_asserted_date"),
        max("asserted_date").alias("max_asserted_date"),
        min("recorded_date").alias("min_recorded_date"),
        max("recorded_date").alias("max_recorded_date"),
    ).first()
    if row is not None:
        counts["min_asserted_date"] = row["min_asserted_date"]
        counts["max_asserted_date"] = row["max_asserted_date"]
        counts["min_recorded_date"] = row["min_recorded_date"]
        counts["max_recorded_date"] = row["max_recorded_date"]

    logger.info(
        "Num C61 patients: {num_c61_patients}",
        num_c61_patients=counts["num_c61_patients"],
    )

    tnm_with_condition = tnm.join(
        conditions.withColumnRenamed("patient_id", "condition_patient_id"),
        on="condition_id",
        how="inner",
    )

    tnm_closest_to_diagnosis = find_closest_to_diagnosis(
        tnm_with_condition, "asserted_date", "effective_date_time"
    )

    logger.info("Number of patients by TNM (Closest to diagnosis)")

    tnm_closest_to_diagnosis.groupby("code", "display", "tnm_value").agg(
        count_distinct("patient_id").alias("num_patients")
    ).show()

    tnm_closest_to_diagnosis.show()

    n0_or_m0_staging = tnm_closest_to_diagnosis.where(col("tnm_value").isin("N0", "M0"))

    row = n0_or_m0_staging.select(
        count_distinct(n0_or_m0_staging.patient_id).alias("n_patients")
    ).first()
    if row is not None:
        counts["num_tnm_m0_or_n0"] = row["n_patients"]

    logger.info(
        "Num TNM M0 or N0 patients: {num_tnm_m0_or_n0}",
        num_tnm_m0_or_n0=counts["num_tnm_m0_or_n0"],
    )

    cohort = n0_or_m0_staging.join(
        medication_statements.withColumnRenamed(
            "patient_id", "medication_statement_patient_id"
        ),
        on="condition_id",
    )
    cohort.show()

    logger.info(
        "Number of C61 patients with TNM of M0/N0 by system therapy medications"
    )

    medication_counts = cohort.groupby("medication_text", "medication_atc_code").agg(
        count_distinct("patient_id").alias("num_patients")
    )

    medication_counts_csv_file_path = results_directory / "medication-counts.csv"
    medication_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        medication_counts_csv_file_path.as_posix()
    )

    cohort = cohort.where(col("medication_atc_code") == "L02BB04")

    row = cohort.select(count_distinct(cohort.patient_id).alias("n_patients")).first()
    if row is not None:
        counts["num_treated_with_enzalutamide"] = row["n_patients"]

    logger.info(
        "Number of C61 patients with TNM of M0/N0 treated with Enzalutamide: "
        + "{num_treated_with_enzalutamide}",
        num_treated_with_enzalutamide=counts["num_treated_with_enzalutamide"],
    )

    enzalutamide_reference_date = lit("2024-04-01")
    logger.info(
        "Number of C61 patients with TNM of M0/N0 treated with "
        + "Enzalutamide after or before {enzalutamide_reference_date}",
        enzalutamide_reference_date=enzalutamide_reference_date,
    )

    result = cohort.groupBy("medication_atc_code").agg(
        count_distinct(
            when(
                col("medication_start_date") < enzalutamide_reference_date,
                col("patient_id"),
            )
        ).alias("num_patients_before"),
        count_distinct(
            when(
                col("medication_start_date") >= enzalutamide_reference_date,
                col("patient_id"),
            )
        ).alias("num_patients_after"),
    )

    result.show()

    row = result.first()
    if row is not None:
        counts["num_treated_with_enzalutamide_before_april_2024"] = row[
            "num_patients_before"
        ]
        counts["num_treated_with_enzalutamide_after_april_2024"] = row[
            "num_patients_after"
        ]

    logger.info("Counts: {counts}", counts=counts)

    dot = Digraph(name="embark-rwd-flowchart", format="png")
    dot.attr(rankdir="TB", dpi="100")

    dot.node(
        "A",
        "All C61 patients\n"
        + f"n = {counts['num_c61_patients']}\n"
        + f"Diagnosis dates: {counts['min_asserted_date']} - "
        + f"{counts['max_asserted_date']}\n"
        + f"Recorded dates: {counts['min_recorded_date']} - "
        + f"{counts['max_recorded_date']}",
    )
    dot.node("B", "With TNM c/p M0 or N0\n" + f"n = {counts['num_tnm_m0_or_n0']}")
    dot.node(
        "C", f"Treated with enzalutamide\nn = {counts['num_treated_with_enzalutamide']}"
    )
    dot.node(
        "D",
        "Before 2024-04-01\nn = "
        + f"{counts['num_treated_with_enzalutamide_before_april_2024']}",
    )
    dot.node(
        "E",
        "After 2024-04-01\nn = "
        + f"{counts['num_treated_with_enzalutamide_after_april_2024']}",
    )

    dot.edge("A", "B")
    dot.edge("B", "C")
    dot.edge("C", "D")
    dot.edge("C", "E")

    logger.info(dot.source)

    dot.render(
        directory=results_directory.as_posix(),
        format="png",
    )

    return result


def run(data: DataSource, results_directory: pathlib.Path):
    extract(data, results_directory)
