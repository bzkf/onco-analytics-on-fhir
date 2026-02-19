import os

import pandas as pd
from fhir_pyrate import Ahoy, Pirate
from more_itertools import chunked
from settings import Settings

FHIR_IDENTIFIER_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"
CHUNK_SIZE = 100
NUM_PROCESSES = 6
PAGE_COUNT = 10000


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

        self.search = Pirate(
            auth=auth,
            base_url=settings.fhir.base_url,
            print_request_url=True,
            num_processes=NUM_PROCESSES,
        )

        self.output_dir = os.path.join(settings.results_directory_path, settings.study_name.value)

        os.makedirs(self.output_dir, exist_ok=True)

    def extract(self):
        # 1) using csv as input, finding all patients with C92
        condition_df = pd.read_csv("icd_codes_aml.csv")

        codes = ",".join(
            "http://fhir.de/CodeSystem/bfarm/icd-10-gm|" + c for c in condition_df["icd_10"]
        )

        condition_patient_df = self.search.steal_bundles_to_dataframe(
            resource_type="Condition",
            request_params={
                "code": codes,
                "_elements": "subject, code, recordedDate",
                "_include": "Condition:patient",
                "_count": PAGE_COUNT,
            },
            fhir_paths=[
                ("condition_id", "Condition.id"),
                ("patient_ref", "Condition.subject.reference"),
                (
                    "patient_id",
                    "Patient.identifier.where(type.coding.where("
                    + f"system='{FHIR_IDENTIFIER_TYPE_SYSTEM}' and code='MR').exists()).value",
                ),
                ("icd_code", "Condition.code.coding.code"),
                ("diagnosis_recordedDate", "Condition.recordedDate[0]"),
                ("diagnosis_onsetDate", "Condition.onsetDateTime[0]"),
                ("deceased_boolean", "Patient.deceasedBoolean"),
                ("deceased_dateTime", "Patient.deceasedDateTime[0]"),
                ("patient_id2", "Patient.id"),
            ],
        )

        # merging patient + condition dataframes, removing duplicates, cleaning and saving merged_df

        patient_df = condition_patient_df["Patient"]
        condition_df = condition_patient_df["Condition"]

        condition_df["patient_id_from_ref"] = condition_df["patient_ref"].str.replace(
            "Patient/", "", regex=False
        )
        merged_df = condition_df.merge(
            patient_df,
            left_on="patient_id_from_ref",
            right_on="patient_id2",
            how="left",
        )
        merged_df = merged_df.drop(columns=["patient_id2", "patient_id_from_ref"])
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

        print(merged_df)

        all_labs = []

        for chunk in chunked(merged_df["patient_ref"], CHUNK_SIZE):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            lab_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Observation",
                request_params={
                    "category": "http://terminology.hl7.org/CodeSystem/observation-category|laboratory",
                    "_count": PAGE_COUNT,
                    "_elements": "subject,effective,code,value",
                },
                df_constraints={
                    "subject": "subject_list",
                },
                with_ref=False,
                fhir_paths=[
                    ("patient", "subject.reference"),
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

        # 3) concat the output values and save final_df

        final_df = lab_df.merge(
            merged_df,
            left_on="patient",
            right_on="patient_ref",
            how="left",
        )
        final_df.drop(columns=["patient"], inplace=True)
        final_df = final_df[
            [
                "patient_ref",
                "patient_id",
                "icd_code",
                "diagnosis_recordedDate",
                "loinc_code",
                "loinc_display",
                "lab_value",
                "lab_unit",
                "lab_dateTime",
                "deceased_dateTime",
                "deceased_boolean",
                "deceased",
            ]
        ]
        final_df.to_csv(os.path.join(self.output_dir, "uc_aml.csv"), index=False)
