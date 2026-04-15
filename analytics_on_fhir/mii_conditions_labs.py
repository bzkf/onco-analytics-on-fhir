import os
from pathlib import Path

import pandas as pd
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from settings import Settings

FHIR_CODE_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_CODE_SYSTEM_SNOMED = "http://snomed.info/sct"

DATA_DICTIONARY = {
    "df_mii_conditions": {
        "condition_id": "Unique identifier of the Condition resource in FHIR",
        "condition_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "icd_code": "ICD-10-GM diagnosis code for AML",
        "diagnosis_recordedDate": "Date/time when the diagnosis was recorded in the system",
        "diagnosis_onsetDateTime": "Date/time when the symptoms for the specific diagnosis began",
    },
    "df_mii_labs": {
        "observation_id": "Unique identifier of the Observation resource",
        "observation_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "loinc_code": "LOINC code of the laboratory observation",
        "loinc_display": "Human-readable LOINC description",
        "lab_dateTime": "Date/time when the laboratory measurement was taken",
        "lab_quantity_value": "Numeric value of the laboratory measurement",
        "lab_quantity_unit": "Unit of the laboratory measurement",
        "lab_codeableconcept_code": "Code of the lab value result (if applicable)",
    },
}

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


class PyRateQuery:
    def __init__(self, settings: Settings):
        self.settings = settings

        # not strictly needed since their names match the default env variable names expected
        os.environ["FHIR_USER"] = settings.fhir.user
        os.environ["FHIR_PASSWORD"] = settings.fhir.password

        auth = Ahoy(
            auth_type="BasicAuth",
            auth_method="env",
        )

        if settings.fhir.base_url is None:
            raise ValueError("FHIR server URL is not set")

        self.search = Pirate(
            auth=auth,
            base_url=settings.fhir.base_url,
            print_request_url=settings.fhir.print_request_urls,
            num_processes=settings.fhir.num_processes,
        )

        self.output_dir = os.path.join(settings.results_directory_path, settings.study_name.value)

        os.makedirs(self.output_dir, exist_ok=True)

        self.export_data_dictionary(self.output_dir)

    def export_data_dictionary(self, output_dir):

        for table_name, columns in DATA_DICTIONARY.items():
            df = pd.DataFrame(
                [{"column_name": col, "description": desc} for col, desc in columns.items()]
            )

            df.to_csv(
                os.path.join(output_dir, f"{table_name}_data_dictionary.csv"),
                index=False,
            )

    def extract_conditions(self, patient_list, suffix, crypto_key):

        logger.info("input patient count: {}", len(patient_list))

        all_conditions = []

        for chunk in chunked(patient_list, self.settings.fhir.chunk_size):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            condition_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Condition",
                request_params={
                    "_elements": "subject, code, recordedDate, onset",
                    "_include": "Condition",
                    "_count": self.settings.fhir.page_count,
                },
                df_constraints={
                    "subject": "subject_list",
                },
                fhir_paths=[
                    ("condition_id", "Condition.id"),
                    ("condition_patient_reference", "Condition.subject.reference"),
                    (
                        "icd_code",
                        f"Condition.code.coding.where(system='{FHIR_CODE_SYSTEM_ICD10}')"
                        + ".first().code",
                    ),
                    ("diagnosis_recordedDate", "Condition.recordedDate[0]"),
                    ("diagnosis_onsetDateTime", "Condition.onsetDateTime"),
                ],
            )

            if len(condition_df_chunk) > 0:
                all_conditions.append(condition_df_chunk)

        if all_conditions:
            condition_df = pd.concat(all_conditions, ignore_index=True)
            condition_df.drop(columns=["subject_list"], inplace=True)
            condition_df.to_csv(
                os.path.join(self.output_dir, "df_mii_conditions" + suffix + ".csv"),
                index=False,
                sep=";",
            )
            # Parquet speichern
            condition_df.to_parquet(
                os.path.join(self.output_dir, "df_mii_conditions" + suffix + ".parquet"),
                index=False,
            )

            logger.info("condition_df size: {}", condition_df.count())
            self.post_process_values(condition_df, name="conditions", columns=["icd_code"])
            return condition_df
        else:
            logger.info("Found no conditions to given patients.")

    def extract_labs(self, patient_list, suffix, crypto_key):
        all_labs = []

        for chunk in chunked(patient_list, self.settings.fhir.chunk_size):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            lab_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Observation",
                request_params={
                    "category": "http://terminology.hl7.org/CodeSystem/observation-category|laboratory",
                    "_count": self.settings.fhir.page_count,
                    "_elements": "subject,effective,code,value",
                },
                df_constraints={
                    "subject": "subject_list",
                },
                with_ref=False,
                fhir_paths=[
                    ("observation_id", "Observation.id"),
                    ("observation_patient_reference", "subject.reference"),
                    ("loinc_code", "code.coding.where(system='http://loinc.org').code"),
                    (
                        "loinc_display",
                        "code.coding.where(system='http://loinc.org').display",
                    ),
                    ("lab_dateTime", "effectiveDateTime[0]"),
                    ("lab_quantity_value", "valueQuantity.value"),
                    ("lab_quantity_unit", "valueQuantity.code"),
                    (
                        "lab_codeableconcept_code",
                        f"Observation.valueCodeableConcept.coding.where(system='{FHIR_CODE_SYSTEM_SNOMED}')"
                        + ".first().code",
                    ),
                ],
            )

            if len(lab_df_chunk) > 0:
                all_labs.append(lab_df_chunk)

        if all_labs:
            lab_df = pd.concat(all_labs, ignore_index=True)
            lab_df.to_csv(
                os.path.join(self.output_dir, "df_mii_labs" + suffix + ".csv"), index=False
            )

            logger.info("lab_df size: {}", lab_df.count())

            self.post_process_values(
                lab_df,
                name="labs",
                columns=[
                    "loinc_code",
                    "loinc_display",
                    "lab_quantity_unit",
                    "lab_codeableconcept_code",
                ],
            )
            return lab_df
        else:
            logger.info("Found no lab values to given patients.")

    def post_process_values(self, df, name, columns):

        processed = (
            df.astype(str)
            .groupby(columns)
            .size()
            .reset_index(name="num_" + name)
            .sort_values(by="num_" + name, ascending=False)
        )

        processed.to_csv(
            os.path.join(self.output_dir, "df_mii_" + name + "_counts.csv"), index=False
        )

        return
