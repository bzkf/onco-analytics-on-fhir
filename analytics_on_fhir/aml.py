import os
from pathlib import Path

import pandas as pd
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from settings import Settings
from utils import clean_datetime_values, keep_only_first_diagnosis

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

    def extract_patients(self):
        # using icd_codes_aml.csv as input, finding all patients with requested diagnosis
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
        
        # clean dateTime values and only keep first diagnosis for every patient
        merged_df_cleaned = clean_datetime_values(df=merged_df, column='diagnosis_recordedDate')
        merged_df_first_diagnosis = keep_only_first_diagnosis(merged_df_cleaned, ['patient_mrn', 'icd_code'])
        merged_df_first_diagnosis.to_csv(os.path.join(self.output_dir, "aml_all_patients.csv"), index=False)

        logger.info("merged_df size: {}", merged_df.count())
        logger.info("patient_df size: {}", patient_df.count())
        
        # finding all other diagnoses to the patients from above
        self.extract_diagnoses(patient_list=patient_df["patient_id"], prefix="aml_")


    def extract_diagnoses(self, patient_list, prefix):
        all_diagnoses = []

        for chunk in chunked(patient_list, self.settings.fhir.chunk_size):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            diag_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Condition",
                request_params={
                    "code": FHIR_CODE_SYSTEM_ICD10+"|",
                    "_count": self.settings.fhir.page_count,
                    "_elements": "subject, code, recordedDate",
                },
                df_constraints={
                    "subject": "subject_list",
                },
                with_ref=False,
                fhir_paths=[
                    ("condition_id", "id"),
                    ("condition_patient_reference", "subject.reference"),
                    (
                        "icd_code",
                        f"code.coding.where(system='{FHIR_CODE_SYSTEM_ICD10}')"
                        + ".first().code",
                    ),
                    ("diagnosis_recordedDate", "recordedDate[0]"),
                ],
            )

            all_diagnoses.append(diag_df_chunk)
        
        # clean dateTime values and only keep first diagnosis for every patient
        diag_df = pd.concat(all_diagnoses, ignore_index=True)
        diag_df_cleaned = clean_datetime_values(df=diag_df, column='diagnosis_recordedDate')
        diag_df_filtered = diag_df_cleaned[diag_df_cleaned['icd_code'].str.startswith('C', na=False)]
        diag_df_first_diagnosis = keep_only_first_diagnosis(diag_df_filtered, ['condition_patient_reference', 'icd_code'])
        diag_df_first_diagnosis.to_csv(os.path.join(self.output_dir, prefix+"all_diagnoses.csv"), index=False)


    def extract_labs(self, patient_list, prefix):
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
                    ("lab_value", "valueQuantity.value"),
                    ("lab_unit", "valueQuantity.code"),
                ],
            )

            all_labs.append(lab_df_chunk)

        lab_df = pd.concat(all_labs, ignore_index=True)
        lab_df.to_csv(os.path.join(self.output_dir, prefix+"labs.csv"), index=False)
        
    def join_with_drug_data(self):
        
        cytostatics_patient_ids = (pd.read_csv(
            os.path.join(HERE, self.settings.aml.csv_input_dir),
            sep=";",
            encoding="latin1",
            engine="python",
            quotechar='"',
        )[self.settings.aml.csv_patient_column].dropna().drop_duplicates()
        )
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv")
        )
        patient_ids = (
            patient_df["patient_mrn"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        filtered_ids = cytostatics_patient_ids[
            cytostatics_patient_ids.isin(patient_ids)
        ]
        filtered_refs = (
            patient_df[
                patient_df["patient_mrn"].isin(filtered_ids)
            ]["condition_patient_reference"]
            .dropna()
            .drop_duplicates()
        )

        logger.info("Cytostatics input data patient count: {}", len(cytostatics_patient_ids))
        logger.info("Matched with AML cohort patient count: {}", len(filtered_ids))

        self.extract_labs(patient_list=filtered_refs, prefix="aml_matched_cytostatics_")

    def join_patients_with_diagnoses(self):
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv")
        )
        condition_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_diagnoses.csv")
        )
        patient_part = patient_df[[
            'condition_patient_reference',
            'icd_code',
            'diagnosis_recordedDate',
            'patient_mrn',
            'birth_date',
            'gender',
            'deceased_dateTime',
            'deceased_boolean',
            'deceased'
        ]].copy()

        condition_part = condition_df[['condition_patient_reference','icd_code','diagnosis_recordedDate']].copy()

        condition_part = condition_part.merge(
        patient_df[['condition_patient_reference','patient_mrn','birth_date','gender','deceased_dateTime','deceased_boolean','deceased']],
        on='condition_patient_reference',
        how='left'
        )
        df_all = pd.concat([patient_part, condition_part], ignore_index=True)
        df_all_first_disgnosis = keep_only_first_diagnosis(df_all, ['condition_patient_reference','icd_code'])
        df_all_first_diagnosis.to_csv(os.path.join(self.output_dir, "aml_patients_conditions_joined.csv"), index=False)
