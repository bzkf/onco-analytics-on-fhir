import os
from pathlib import Path

import pandas as pd
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from settings import Settings
from utils import clean_datetime_values

FHIR_CODE_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_IDENTIFIER_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"

DATA_DICTIONARY = {
    "aml_all_patients": {
        "condition_id": "Unique identifier of the Condition resource in FHIR",
        "condition_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "icd_code": "ICD-10-GM diagnosis code for AML",
        "diagnosis_recordedDate": "Date/time when the diagnosis was recorded in the system",
        "diagnosis_onsetDateTime": "Date/time when the symptoms for the specific diagnosis began",
        "deceased_boolean": "Boolean flag indicating whether the patient is deceased",
        "deceased_dateTime": "Recorded date/time of patient death",
        "deceased": "Derived column: true if deceased_boolean or deceased_dateTime is set",
        "patient_mrn": "Medical Record Number of the patient (MR identifier from Patient.identifier)",
        "birth_date": "Patient birth date",
        "gender": "Administrative gender of the patient",
    },
    "aml_matched_cytostatics_labs": {
        "observation_id": "Unique identifier of the Observation resource",
        "observation_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "loinc_code": "LOINC code of the laboratory observation",
        "loinc_display": "Human-readable LOINC description",
        "lab_dateTime": "Date/time when the laboratory measurement was taken",
        "lab_value": "Numeric value of the laboratory measurement",
        "lab_unit": "Unit of the laboratory measurement",
    },
}

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

    def extract_patients(self):
        # using icd_codes_aml.csv as input, finding all patients with requested diagnosis
        condition_df = pd.read_csv(HERE / "icd_codes_aml.csv")

        codes = ",".join(f"{FHIR_CODE_SYSTEM_ICD10}|" + c for c in condition_df["icd_code"])

        condition_patient_df = self.search.steal_bundles_to_dataframe(
            resource_type="Condition",
            request_params={
                "code": codes,
                "_elements": "subject, code, recordedDate, onset",
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
                ("diagnosis_onsetDateTime", "Condition.onsetDateTime"),
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
        if len(condition_patient_df) > 0:
            patient_df = condition_patient_df["Patient"].drop_duplicates(subset=["patient_id"])
            condition_df = condition_patient_df["Condition"]
        else:
            logger.info("Found no patients to given AML ICD codes.")
            return

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

        # clean dateTime values
        merged_df_cleaned = clean_datetime_values(df=merged_df, column="diagnosis_recordedDate")
        merged_df_cleaned.to_csv(os.path.join(self.output_dir, "aml_all_patients.csv"), index=False)

        logger.info("merged_df size: {}", merged_df.count())
        logger.info("patient_df size: {}", patient_df.count())

    def extract_labs(self, patient_list):
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
        lab_df.to_csv(
            os.path.join(self.output_dir, "aml_matched_cytostatics_labs.csv"), index=False
        )

        self.post_process_lab_values(lab_df)

    def join_with_drug_data(self):

        cytostatics_patient_ids = (
            pd.read_csv(
                os.path.join(self.settings.aml.csv_input_dir),
                sep=";",
                encoding="latin1",
                engine="python",
                quotechar='"',
            )[self.settings.aml.csv_patient_column]
            .dropna()
            .drop_duplicates()
        )
        patient_df = pd.read_csv(os.path.join(self.output_dir, "aml_all_patients.csv"))
        patient_ids = patient_df["patient_mrn"].dropna().astype(str).str.strip()
        filtered_ids = cytostatics_patient_ids[cytostatics_patient_ids.isin(patient_ids)]
        filtered_refs = (
            patient_df[patient_df["patient_mrn"].isin(filtered_ids)]["condition_patient_reference"]
            .dropna()
            .drop_duplicates()
        )

        logger.info("Cytostatics input data patient count: {}", len(cytostatics_patient_ids))
        logger.info("Matched with AML cohort patient count: {}", len(filtered_ids))

        if len(filtered_ids) > 0:
            self.extract_labs(patient_list=filtered_refs)
        else:
            logger.info("Found no match between Cytostatics input data and AML patients.")

    def post_process_lab_values(self, lab_df):

        processed = (
            lab_df.astype(str)
            .groupby(["loinc_code", "loinc_display", "lab_unit"])
            .size()
            .reset_index(name="num_labs")
            .sort_values(by="num_labs", ascending=False)
        )

        processed.to_csv(os.path.join(self.output_dir, "aml_labs_counts.csv"), index=False)

        return
