from pathling import Expression as exp
from pathling import datasource
from pyspark.sql import functions as F
from src.obds_fhir_to_opal.utils_onco_analytics import FHIR_SYSTEM_ICD10, FHIR_SYSTEM_IDENTIFIER_TYPE


def tso500_classification_extract(data: datasource.DataSource):
    conditions = data.extract(
        "Condition",
        columns=[
            exp("id", "condition_id"),
            exp("onsetDateTime", "onset_date_time"),
            exp(
                f"""code.coding
                    .where(system='{FHIR_SYSTEM_ICD10}').code""",
                "icd10_code",
            ),
            exp("subject.reference", "subject_reference"),
        ],
    )

    patients = data.extract(
        "Patient",
        columns=[
            exp("id", "patient_id"),
            exp(
                f"""identifier
                    .where(type.coding
                        .where(system='{FHIR_SYSTEM_IDENTIFIER_TYPE}' and code='MR').exists()
                    ).value""",
                "medical_record_number",
            ),
            exp("gender", "gender"),
            exp("birthDate", "birth_date"),
        ],
    )

    patients = patients.withColumn(
        "subject_reference", F.concat(F.lit("Patient/"), patients["patient_id"])
    )
    conditions = conditions.join(
        patients,
        on="subject_reference",
        how="left",
    )

    df = conditions.dropDuplicates()

    return df
