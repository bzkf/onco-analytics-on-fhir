import os
from pathlib import Path

import pandas as pd
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from settings import Settings

FHIR_CODE_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_IDENTIFIER_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


class AMLStudy:
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

    def extract(self):
        # 1) using csv as input, finding all patients with C92
        condition_df = pd.read_csv(HERE / "icd_codes_aml.csv")

        codes = ",".join(f"{FHIR_CODE_SYSTEM_ICD10}|" + c for c in condition_df["icd_code"])

        condition_patient_df = self.search.steal_bundles_to_dataframe(
            resource_type="Condition",
            request_params={
                "code": codes,
                "_elements": "subject,code,recordedDate",
                "_include": "Condition:patient",
                "_count": self.settings.fhir.page_count,
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
                ("diagnosis_onsetDateTime", "Condition.onsetDateTime[0]"),
                ("deceased_boolean", "Patient.deceasedBoolean"),
                ("deceased_dateTime", "Patient.deceasedDateTime[0]"),
                ("patient_id", "Patient.id"),
                (
                    "patient_mrn",
                    "Patient.identifier.where(type.coding.where("
                    + f"system='{FHIR_IDENTIFIER_TYPE_SYSTEM}' and code='MR').exists()).value",
                ),
                ("birth_date", "Patient.birthDate"),
                ("gender", "Patient.gender"),
            ],
        )

        # merging patient + condition dataframes, removing duplicates, cleaning and saving merged_df

        patient_df = condition_patient_df["Patient"].drop_duplicates(subset=["patient_id"])
        condition_df = condition_patient_df["Condition"]

        condition_df["condition_patient_reference_without_type"] = condition_df[
            "condition_patient_reference"
        ].str.replace("Patient/", "", regex=False)
        merged_df = condition_df.merge(
            patient_df,
            left_on="condition_patient_reference_without_type",
            right_on="patient_id",
            how="left",
        )
        merged_df = merged_df.drop(
            columns=["patient_id", "condition_patient_reference_without_type"]
        )
        merged_df = merged_df.drop_duplicates()

        # merging deceased columns into one column 'deceased' (true if deceased_dateTime
        # is set OR deceased_boolean is true)

        if "deceased_boolean" not in merged_df.columns:
            merged_df["deceased_boolean"] = False

        if "deceased_dateTime" not in merged_df.columns:
            merged_df["deceased_dateTime"] = pd.NaT

        merged_df["deceased"] = (
            merged_df["deceased_boolean"] | merged_df["deceased_dateTime"].notna()
        )

        merged_df.to_csv(os.path.join(self.output_dir, "patients.csv"), index=False)

        # 2) finding all lab values to the patients from step 1)
        # while splitting the query into chunks

        logger.info("merged_df size: {}", merged_df.count())
        logger.info("patient_df size: {}", patient_df.count())

        all_labs = []

        for chunk in chunked(patient_df["patient_id"], self.settings.fhir.chunk_size):
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
                    ("lab_value", "valueQuantity.value"),
                    ("lab_unit", "valueQuantity.code"),
                ],
            )

            all_labs.append(lab_df_chunk)

        lab_df = pd.concat(all_labs, ignore_index=True)
        lab_df.to_csv(os.path.join(self.output_dir, "labs.csv"), index=False)
