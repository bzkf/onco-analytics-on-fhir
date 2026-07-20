import csv
import datetime
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import tempfile
import zipfile
from pathlib import Path
from decimal import Decimal

import numpy as np
import pandas as pd
import requests
import urllib3
from fhir_constants import FHIR_SYSTEMS_CONDITION_ASSERTED_DATE
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from pathling.datasource import DataSource
from pyspark.sql import functions as F
from settings import Settings
from urllib3 import Retry
from utils import save_final_df
from views import (
    vitalstatus_view,
)

pd.options.mode.string_storage = "pyarrow"
pd.options.future.infer_string = True

FHIR_CODE_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_CODE_SYSTEM_SNOMED = "http://snomed.info/sct"
FHIR_CODE_SYSTEM_TOD_TUMORBEDINGT = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-tod"
)
FHIR_CODE_SYSTEM_ECOG = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-allgemeiner-leistungszustand-ecog"
FHIR_CODE_SYSTEM_VERLAUF_GESAMTBEURTEILUNG = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-verlauf-gesamtbeurteilung"
FHIR_CODE_SYSTEM_OPS = "http://fhir.de/CodeSystem/bfarm/ops"
FHIR_CODE_SYSTEM_ATC = "http://fhir.de/CodeSystem/bfarm/atc"
FHIR_CODE_SYSTEM_ATC_US = "http://www.whocc.no/atc"
FHIR_CODE_SYSTEM_PZN = "http://fhir.de/CodeSystem/ifa/pzn"
FHIR_CODE_SYSTEM_BODYSITE = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-strahlentherapie-zielgebiet"

FHIR_PROFILE_WEITERE_KLASSIFIKATIONEN = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/StructureDefinition/mii-pr-onko-weitere-klassifikationen"

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
        "patient_mrn": "Medical Record Number of the patient (Patient ID from Patient.identifier)",
        "birth_date": "Patient birth date",
        "gender": "Administrative gender of the patient",
        "death_caused_by_tumor": "yes/no/unknown - whether the death was caused by the tumor (from "
        + "OBDS deaths data)",
        "cause_of_death": "Cause of death encoded as an ICD-10 code (from OBDS deaths data)",
        "last_follow_up_datetime": "Date/time of the last recorded vital status of 'alive' for the "
        + "patient (from OBDS vital status data)",
    },
    "aml_all_labs": {
        "observation_id": "Unique identifier of the Observation resource",
        "observation_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "loinc_code": "LOINC code of the laboratory observation",
        "loinc_display": "Human-readable LOINC description",
        "lab_dateTime": "Date/time when the laboratory measurement was taken",
        "lab_quantity_value": "Numeric value of the laboratory measurement",
        "lab_quantity_unit": "Unit of the laboratory measurement",
        "lab_codeableconcept_code": "Code of the lab value result (if applicable)",
    },
    "aml_all_meds": {
        "medication_id": "Unique identifier of the Medication resource",
        "medication_atc_code": "ATC code of the medication",
        "medication_atc_display": "Human-readable ATC description",
        "medication_pzn_code": "PZN code of the medication",
        "medication_pzn_display": "Human-readable PZN description",
        "medication_ops_code": "OPS code of the medication",
        "ingredient": "Ingredient(s) of the medication",
    },
    "aml_all_med_reqs_stats_admins": {
        "type": "FHIR Resource type of the source",
        "id": "Unique identifier of the resource",
        "patient_reference": "FHIR reference to the patient (Patient/{id})",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced Patient "
        + "resource identifier)",
        "status": "Status of the medication order or statement",
        "intent": "Intent of the medication order or statement",
        "medication_reference": "FHIR Reference to the applied medication",
        "datetime": "(Start) Datetime of the medication order or statement",
        "dosage": "Dosage of the applied medication (in json representation)",
        "period_end": "End Datetime of the medication order or statement",
    },
    "df_obds_deaths": {
        "observation_id": "Unique identifier of the death Observation resource",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced "
        + "Patient.identifier)",
        "patient_id": "The technical patient ID (Patient/{id}) referenced in the death observation",
        "death_dateTime": "Date of death for the patient (note there may be multiple deaths per "
        + "patients if multiple causes of death are recorded. Even multiple deaths per patient "
        + "with different dates are possible due to documentation errors.)",
        "cause_of_death": "Cause of death encoded as an ICD-10 code",
        "death_caused_by_tumor": "yes/no/unknown - whether the death was caused by the tumor.",
    },
    "df_obds_vitalstatus": {
        "observation_id": "Unique identifier of the vital status Observation resource",
        "meta_profile": "FHIR meta profile of the observation",
        "observation_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "effective_dateTime": "Date of the vital status assessment",
        "vitalstatus_system": "Coding system of the vital status value",
        "vitalstatus_code": "Vital status code (e.g. 'L' for alive, 'T' for deceased)",
        "patient_id": "The technical patient ID (Patient/{id}) referenced in the observation",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced "
        + "Patient.identifier)",
    },
    "df_obds_ecog_statuses": {
        "observation_id": "Unique identifier of the ECOG Observation resource",
        "patient_id": "The technical patient ID (Patient/{id}) referenced from the observation",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced "
        + "Patient.identifier)",
        "effective_dateTime": "Date of the ECOG performance status assessment",
        "ecog_performance_status": "ECOG performance status value",
    },
    "df_obds_progressions": {
        "observation_id": "Unique identifier of the tumor progression Observation resource",
        "patient_id": "The technical patient ID (Patient/{id}) referenced from the observation",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced "
        + "Patient.identifier)",
        "effective_dateTime": "Date of the tumor progression assessment",
        "overall_assessment_of_tumor_regression": "Tumor progression status value",
    },
    "df_obds_procedures": {
        "procedure_id": "Unique identifier of the performed procedure",
        "patient_id": "The technical patient ID (Patient/{id}) referenced from the Procedure",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced "
        + "Patient.identifier)",
        "performed_period_start": "Start of the procedure period",
        "performed_period_end": "End of the procedure period",
        "procedure_ops_code": "OPS code of the Procedure",
        "procedure_ops_display": "Display value of the OPS code",
        "procedure_target_bodyregion": "Target body region of the procedure",
    },
    "df_obds_weitere_klassifikationen": {
        "observation_id": "Unique identifier of the additional classification Observation resource",
        "patient_id": "The technical patient ID (Patient/{id}) referenced from the observation",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced "
        + "Patient.identifier)",
        "effective_dateTime": "Date of the classification assessment",
        "code_text": "Name of the classification as text (e.g. Binet, FAB-Klassifikation)",
        "value_text": "The classification value as text",
        "value_system": "Technical system of the value, if present.",
        "value_code": "Technical code of the value, if present.",
    },
    "df_obds_systemtherapien": {
        "medication_statement_id": "Unique identifier of the systemic therapy medication resource",
        "patient_id": "The technical patient ID (Patient/{id}) referenced from the observation",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from referenced",
        "medication_start_date": "When the medication was given (the therapy started)",
        "medication_text": "the name of medication/substance given",
        "medication_atc_code": "if present, the ATC code of the substance",
    },
}

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


_TRAILING_ZERO_DECIMAL_RE = re.compile(r"^-?\d+\.0+$")


def _clean_patient_mrn(value):
    # Some sites emit a purely-numeric identifier.value as a JSON number
    # instead of a JSON string. Mixed with a missing identifier elsewhere in the
    # same column, pandas silently upcasts the whole column to float64, which
    # would otherwise bake a trailing ".0" into the MRN once stringified.
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (float, np.floating)):
        return str(int(value)) if float(value).is_integer() else str(value)
    # Guard against MRNs already corrupted to "1234.0" by a previous pipeline run
    # (e.g. baked into an older CSV). Only strip a trailing ".0"-style decimal -
    # never touch plain digit strings, since real MRNs can be zero-padded
    # (e.g. "007123") and must not have leading zeros stripped.
    value = str(value)
    if _TRAILING_ZERO_DECIMAL_RE.match(value):
        return value.split(".", 1)[0]
    return value


def _clean_patient_mrn_series(series: pd.Series) -> pd.Series:
    return series.apply(_clean_patient_mrn).astype("string")


class AMLStudy:
    def __init__(self, settings: Settings, data: DataSource):
        self.settings = settings
        self.data = data

        # not strictly needed since their names match the default env variable names expected
        os.environ["FHIR_USER"] = settings.fhir.user
        os.environ["FHIR_PASSWORD"] = settings.fhir.password

        logger.info(f"FHIR TLS verification is set to {settings.fhir.tls_verify}")

        session = requests.Session()
        session.verify = settings.fhir.tls_verify

        if not settings.fhir.tls_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            # fhir_pyrate's TokenAuth creates its own internal session that does
            # not inherit verify=False from the session we pass in. Patching
            # Session.__init__ ensures every session created afterwards
            # (including the internal token session) also disables SSL verification.
            _orig_session_init = requests.Session.__init__

            def _patched_session_init(self, *args, **kwargs):
                _orig_session_init(self, *args, **kwargs)
                self.verify = False

            requests.Session.__init__ = _patched_session_init

        auth = Ahoy(
            auth_type=settings.fhir.auth_type,
            auth_method="env",
            auth_url=settings.fhir.token_auth_url,
            refresh_url=settings.fhir.token_refresh_url,
            session=session,
            token=settings.fhir.token,
        )

        if settings.fhir.base_url is None:
            raise ValueError("FHIR server URL is not set")

        retries = Retry(
            total=settings.fhir.retries,
            backoff_factor=0.1,
        )

        self.search = Pirate(
            auth=auth,
            base_url=settings.fhir.base_url,
            print_request_url=settings.fhir.print_request_urls,
            num_processes=settings.fhir.num_processes,
            retry_requests=retries,
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
        # make sure PATIENT_IDENTIFIER_SYSTEM is set
        if self.settings.fhir.patient_identifier_system is None:
            raise KeyError("Please set the variable FHIR_PATIENT_IDENTIFIER_SYSTEM")

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
                    "Patient.identifier.where("
                    + f"system='{self.settings.fhir.patient_identifier_system}').value",
                ),
                ("birth_date", "Patient.birthDate"),
                ("gender", "Patient.gender"),
            ],
        )

        # merging patient + condition dataframes, removing duplicates, cleaning and saving merged_df
        if len(condition_patient_df) > 0:
            patient_df = condition_patient_df["Patient"].drop_duplicates(subset=["patient_id"])
            patient_df["patient_mrn"] = _clean_patient_mrn_series(patient_df["patient_mrn"])
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

        if "diagnosis_onsetDateTime" not in merged_df.columns:
            merged_df["diagnosis_onsetDateTime"] = pd.NaT

        merged_df["deceased_dateTime"] = pd.to_datetime(
            merged_df["deceased_dateTime"], format="ISO8601", errors="raise"
        )

        merged_df["deceased"] = (
            merged_df["deceased_boolean"] | merged_df["deceased_dateTime"].notna()
        )

        existing_mrns = set(merged_df["patient_mrn"].dropna().unique())

        obds_deaths_path = os.path.join(self.output_dir, "df_obds_deaths.csv")
        if os.path.exists(obds_deaths_path):
            logger.info("Joining obds deaths data into patient table")
            obds_deaths_df = pd.read_csv(
                obds_deaths_path,
                sep=";",
                dtype={"patient_mrn": "string"},
                usecols=[
                    "patient_mrn",
                    "death_dateTime",
                    "death_caused_by_tumor",
                    "cause_of_death",
                ],
            )
            obds_deaths_df["death_dateTime"] = pd.to_datetime(
                obds_deaths_df["death_dateTime"], format="ISO8601", errors="coerce"
            )
            obds_deaths_df = (
                obds_deaths_df[obds_deaths_df["death_dateTime"].notna()]
                .sort_values("death_dateTime")
                .drop_duplicates(subset=["patient_mrn"], keep="last")
            )
            obds_deaths_df = obds_deaths_df[obds_deaths_df["patient_mrn"].isin(existing_mrns)]
            merged_df = merged_df.merge(
                obds_deaths_df,
                on="patient_mrn",
                how="left",
            )

            death_mask = merged_df["death_dateTime"].notna()
            merged_df.loc[death_mask, "deceased_dateTime"] = merged_df.loc[
                death_mask, "death_dateTime"
            ]
            merged_df["deceased"] = (
                merged_df["deceased_boolean"] | merged_df["deceased_dateTime"].notna()
            )
            merged_df = merged_df.drop(columns=["death_dateTime"])

        obds_vitalstatus_path = os.path.join(self.output_dir, "df_obds_vitalstatus.csv")
        if os.path.exists(obds_vitalstatus_path):
            logger.info("Joining obds vitalstatus data into patient table")
            vitalstatus_df = pd.read_csv(
                obds_vitalstatus_path,
                sep=";",
                dtype={"patient_mrn": "string"},
                usecols=["patient_mrn", "effective_dateTime", "vitalstatus_code"],
            )

            deceased_vs_df = (
                vitalstatus_df[vitalstatus_df["vitalstatus_code"] == "T"]
                .assign(
                    effective_dateTime=lambda df: pd.to_datetime(
                        df["effective_dateTime"], errors="coerce"
                    )
                )
                .dropna(subset=["effective_dateTime"])
                .sort_values("effective_dateTime")
                .drop_duplicates(subset=["patient_mrn"], keep="last")
                .rename(columns={"effective_dateTime": "vs_death_dateTime"})[
                    ["patient_mrn", "vs_death_dateTime"]
                ]
            )
            deceased_vs_df = deceased_vs_df[deceased_vs_df["patient_mrn"].isin(existing_mrns)]
            if not deceased_vs_df.empty:
                merged_df = merged_df.merge(deceased_vs_df, on="patient_mrn", how="left")
                death_mask = merged_df["vs_death_dateTime"].notna()
                merged_df.loc[death_mask, "deceased_dateTime"] = merged_df.loc[
                    death_mask, "vs_death_dateTime"
                ]
                merged_df["deceased"] = (
                    merged_df["deceased_boolean"] | merged_df["deceased_dateTime"].notna()
                )
                merged_df = merged_df.drop(columns=["vs_death_dateTime"])

            # coerce timestamp due to:
            # OutOfBoundsDatetime: Out of bounds nanosecond timestamp: 9999-12-31
            follow_up_df = (
                vitalstatus_df[vitalstatus_df["vitalstatus_code"] == "L"]
                .assign(
                    effective_dateTime=lambda df: pd.to_datetime(
                        df["effective_dateTime"], errors="coerce", format="ISO8601"
                    )
                )
                .dropna(subset=["effective_dateTime"])
                .sort_values("effective_dateTime")
                .drop_duplicates(subset=["patient_mrn"], keep="last")
                .rename(columns={"effective_dateTime": "last_follow_up_datetime"})[
                    ["patient_mrn", "last_follow_up_datetime"]
                ]
            )
            follow_up_df = follow_up_df[follow_up_df["patient_mrn"].isin(existing_mrns)]
            if not follow_up_df.empty:
                merged_df = merged_df.merge(follow_up_df, on="patient_mrn", how="left")
            else:
                merged_df["last_follow_up_datetime"] = pd.NaT

        merged_df.to_csv(os.path.join(self.output_dir, "aml_all_patients.csv"), index=False)

        logger.info(f"merged_df size: {merged_df.count()}. {merged_df.dtypes}")
        logger.info(f"patient_df size: {patient_df.count()}. {patient_df.dtypes}")

        patient_list = merged_df["condition_patient_reference"]
        patient_list.drop_duplicates(inplace=True)

        # Limit patients if configured (useful for debugging/testing)
        if self.settings.fhir.limit_patients is not None:
            total_available = len(patient_list)
            patient_list = patient_list.head(self.settings.fhir.limit_patients)
            logger.info(
                f"Limited patient list to {self.settings.fhir.limit_patients} patients "
                f"(total available: {total_available})"
            )

        self.extract_encounters(patient_list=patient_list)
        self.extract_conditions(patient_list=patient_list)
        self.extract_labs(patient_list=patient_list)
        self.extract_procedures(patient_list=patient_list)

    def extract_encounters(self, patient_list):
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv"), dtype={"patient_mrn": "string"}
        )
        all_encs = []

        for chunk in chunked(patient_list, self.settings.fhir.chunk_size):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            enc_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Encounter",
                request_params={
                    "_count": self.settings.fhir.page_count,
                    "_elements": "identifier, status, class, subject, period",
                },
                df_constraints={
                    "subject": "subject_list",
                },
                with_ref=False,
                fhir_paths=[
                    ("encounter_id", "Encounter.id"),
                    ("encounter_patient_reference", "subject.reference"),
                    ("status", "Encounter.status"),
                    (
                        "enc_class_code",
                        "class.where(system='http://terminology.hl7.org/CodeSystem/v3-ActCode').code",
                    ),
                    (
                        "enc_class_display",
                        "class.where(system='http://terminology.hl7.org/CodeSystem/v3-ActCode').display",
                    ),
                    (
                        "enc_period_start",
                        "period.start",
                    ),
                    (
                        "enc_period_end",
                        "period.end",
                    ),
                ],
            )

            if len(enc_df_chunk) > 0:
                all_encs.append(enc_df_chunk)

        if all_encs:
            enc_df = pd.concat(all_encs, ignore_index=True)

            patient_mrn_lookup = (
                patient_df[["condition_patient_reference", "patient_mrn"]]
                .assign(patient_mrn=lambda x: _clean_patient_mrn_series(x["patient_mrn"]))
                .sort_values("patient_mrn")
                .drop_duplicates(subset=["condition_patient_reference"], keep="first")
                .set_index("condition_patient_reference")["patient_mrn"]
            )

            enc_df["patient_mrn"] = enc_df["encounter_patient_reference"].map(patient_mrn_lookup)

            enc_df.to_csv(os.path.join(self.output_dir, "aml_all_encounters.csv"), index=False)

            logger.info(f"all_encs_df size: {enc_df.count()}. {enc_df.dtypes}")
        else:
            logger.info("Found no encounters to given patients.")

    def extract_conditions(self, patient_list):
        patient_df = pd.read_csv(os.path.join(self.output_dir, "aml_all_patients.csv"))
        all_conds = []

        for chunk in chunked(patient_list, self.settings.fhir.chunk_size):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            cond_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Condition",
                request_params={
                    "_count": self.settings.fhir.page_count,
                    "_elements": "subject, code, recordedDate, onset, encounter",
                },
                df_constraints={
                    "subject": "subject_list",
                },
                with_ref=False,
                fhir_paths=[
                    ("condition_id", "id"),
                    ("all_condition_patient_reference", "subject.reference"),
                    (
                        "icd_code",
                        f"code.coding.where(system='{FHIR_CODE_SYSTEM_ICD10}')" + ".first().code",
                    ),
                    ("diagnosis_recordedDate", "recordedDate[0]"),
                    ("diagnosis_onsetDateTime", "onsetDateTime"),
                    ("condition_encounter_reference", "encounter.reference"),
                ],
            )

            if len(cond_df_chunk) > 0:
                all_conds.append(cond_df_chunk)

        if all_conds:
            con_df = pd.concat(all_conds, ignore_index=True)

            patient_mrn_lookup = (
                patient_df[["condition_patient_reference", "patient_mrn"]]
                .assign(patient_mrn=lambda x: x["patient_mrn"].astype(str))
                .sort_values("patient_mrn")
                .drop_duplicates(subset=["condition_patient_reference"], keep="first")
                .set_index("condition_patient_reference")["patient_mrn"]
            )

            con_df["patient_mrn"] = con_df["all_condition_patient_reference"].map(
                patient_mrn_lookup
            )

            con_df.to_csv(os.path.join(self.output_dir, "aml_all_conditions.csv"), index=False)

            logger.info(f"all_conds_df size: {con_df.count()}. {con_df.dtypes}")
        else:
            logger.info("Found no conditions to given patients.")

    def extract_labs(self, patient_list):
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv"), dtype={"patient_mrn": "string"}
        )
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
                    (
                        "lab_dateTime",
                        "effectiveDateTime[0] | effectivePeriod.start | effectiveInstant | issued",
                    ),
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

            if "lab_codeableconcept_code" not in lab_df.columns:
                lab_df["lab_codeableconcept_code"] = pd.NA

            if "loinc_display" not in lab_df.columns:
                lab_df["loinc_display"] = pd.NA

            patient_mrn_lookup = (
                patient_df[["condition_patient_reference", "patient_mrn"]]
                .assign(patient_mrn=lambda x: _clean_patient_mrn_series(x["patient_mrn"]))
                .sort_values("patient_mrn")
                .drop_duplicates(subset=["condition_patient_reference"], keep="first")
                .set_index("condition_patient_reference")["patient_mrn"]
            )

            lab_df["patient_mrn"] = lab_df["observation_patient_reference"].map(patient_mrn_lookup)

            lab_df.to_csv(os.path.join(self.output_dir, "aml_all_labs.csv"), index=False)

            logger.info(f"all_labs_df size: {lab_df.count()}. {lab_df.dtypes}")

            self.post_process_lab_values(lab_df)
        else:
            logger.info("Found no lab values to given patients.")

    _MEDICATION_FHIR_PATHS = [
        ("medication_id", "Medication.id"),
        (
            "medication_atc_code",
            "code.coding"
            + f".where(system = '{FHIR_CODE_SYSTEM_ATC_US}' or system = '{FHIR_CODE_SYSTEM_ATC}')"
            + ".code",
        ),
        (
            "medication_code_text",
            "code.text",
        ),
        (
            "medication_pzn_code",
            "code.coding" + f".where(system = '{FHIR_CODE_SYSTEM_PZN}')" + ".code",
        ),
        (
            "medication_pzn_display",
            "code.coding" + f".where(system = '{FHIR_CODE_SYSTEM_PZN}')" + ".display",
        ),
        (
            "medication_ops_code",
            "code.coding" + f".where(system = '{FHIR_CODE_SYSTEM_OPS}')" + ".code",
        ),
        (
            "medication_ops_version",
            "code.coding" + f".where(system = '{FHIR_CODE_SYSTEM_OPS}')" + ".version",
        ),
        (
            "medication_ingredient_text",
            "Medication.ingredient.itemCodeableConcept.text | "
            + "Medication.ingredient.itemReference.display",
        ),
        ("ingredient", "Medication.ingredient"),
    ]

    def _fetch_medication_resource(
        self,
        patient_df: pd.DataFrame,
        resource_type: str,
        extra_fhir_paths: list[tuple],
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        common_fhir_paths = [
            ("type", f"{resource_type}.resourceType"),
            ("id", f"{resource_type}.id"),
            ("patient_reference", f"{resource_type}.subject.reference"),
            ("status", f"{resource_type}.status"),
            ("medication_reference", f"{resource_type}.medicationReference.reference"),
        ]

        fhir_paths = common_fhir_paths + extra_fhir_paths + self._MEDICATION_FHIR_PATHS

        request_params: dict = {
            "_count": self.settings.fhir.page_count,
            "_include": f"{resource_type}:medication",
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            resource_dir = Path(tmp_dir) / "resources"
            medication_dir = Path(tmp_dir) / "medications"
            resource_dir.mkdir()
            medication_dir.mkdir()

            chunk_counter = 0
            indices = list(range(len(patient_df)))
            for chunk_indices in chunked(indices, self.settings.fhir.chunk_size):
                chunk_df = patient_df.iloc[list(chunk_indices)]
                result = self.search.trade_rows_for_dataframe(
                    df=chunk_df,
                    resource_type=resource_type,
                    request_params=request_params,
                    df_constraints={"subject": "condition_patient_reference"},
                    with_ref=False,
                    fhir_paths=fhir_paths,
                )
                if resource_type in result.keys():
                    resource_chunk = result[resource_type]
                    resource_chunk = resource_chunk.drop_duplicates(subset=["type", "id"])

                    # Convert object columns (e.g. nested dosage) to JSON strings
                    # so they can be serialized to Parquet
                    for col in resource_chunk.columns:
                        if resource_chunk[col].dtype == "object":
                            resource_chunk[col] = resource_chunk[col].apply(
                                lambda v: (
                                    json.dumps(
                                        v,
                                        default=lambda o: (
                                            float(o) if isinstance(o, Decimal) else str(o)
                                        ),
                                    )
                                    if isinstance(v, (dict, list))
                                    else v
                                )
                            )
                    resource_chunk.to_parquet(
                        resource_dir / f"chunk_{chunk_counter}.parquet", index=False
                    )

                    # De-duplicate Medication resources per chunk to reduce memory and I/O
                    med_chunk = result["Medication"].drop_duplicates(subset=["medication_id"])
                    for col in med_chunk.columns:
                        if med_chunk[col].dtype == "object":
                            med_chunk[col] = med_chunk[col].apply(
                                lambda v: (
                                    json.dumps(
                                        v,
                                        default=lambda o: (
                                            float(o) if isinstance(o, Decimal) else str(o)
                                        ),
                                    )
                                    if isinstance(v, (dict, list))
                                    else v
                                )
                            )
                    med_chunk.to_parquet(
                        medication_dir / f"chunk_{chunk_counter}.parquet", index=False
                    )
                    chunk_counter += 1

            if chunk_counter == 0:
                logger.info(f"Found no {resource_type}/Medication FHIR resources")
                return None, None

            resource_df = pd.read_parquet(resource_dir)
            # Final deduplication across all chunks
            resource_df = resource_df.drop_duplicates(subset=["type", "id"])

            medication_df = pd.read_parquet(medication_dir)
            # Final deduplication across all chunks
            medication_df = medication_df.drop_duplicates(subset=["medication_id"])

        resource_df = resource_df.drop_duplicates(subset=["type", "id"])

        # add the "Medication" prefix to the id so they match the medication_reference column
        medication_df["medication_id"] = "Medication/" + medication_df["medication_id"].astype(str)

        mrn_per_ref = patient_df.groupby("condition_patient_reference")["patient_mrn"].nunique()
        conflicting = mrn_per_ref[mrn_per_ref > 1]
        if not conflicting.empty:
            logger.warning(
                "Multiple distinct patient_mrn values found for {} patient reference(s); "
                "the first (sorted) MRN will be used: {}",
                len(conflicting),
                conflicting.index.tolist(),
            )
        patient_mrn_lookup = (
            patient_df[["condition_patient_reference", "patient_mrn"]]
            .assign(patient_mrn=lambda x: _clean_patient_mrn_series(x["patient_mrn"]))
            .sort_values("patient_mrn")
            .drop_duplicates(subset=["condition_patient_reference"], keep="first")
            .set_index("condition_patient_reference")["patient_mrn"]
        )
        resource_df["patient_mrn"] = resource_df["patient_reference"].map(patient_mrn_lookup)

        logger.info(f"all_{resource_type}_df size: {len(resource_df)}. {resource_df.dtypes}")
        return resource_df, medication_df

    def load_ops_codes(self, filepath):
        ops_code_map = {}
        with open(filepath, encoding="utf-8") as file:
            content = csv.reader(file, delimiter=";")
            for row in content:
                if len(row) >= 9:
                    code = row[6]
                    title = row[8]
                    ops_code_map[code] = title
        return ops_code_map

    def extract_meds(self):
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv"), dtype={"patient_mrn": "string"}
        )

        logger.info("Fetching MedicationRequest")
        med_req_df, med_df_1 = self._fetch_medication_resource(
            patient_df=patient_df,
            resource_type="MedicationRequest",
            extra_fhir_paths=[
                ("intent", "MedicationRequest.intent"),
                (
                    "datetime",
                    "MedicationRequest.dosageInstruction.timing.event.first() "
                    + "| MedicationRequest.authoredOn",
                ),
                ("dosage", "MedicationRequest.dosageInstruction"),
                (
                    "medication_codeableconcept_code",
                    "MedicationRequest.medicationCodeableConcept.coding."
                    + f".where(system = '{FHIR_CODE_SYSTEM_ATC_US}' or system = '{FHIR_CODE_SYSTEM_ATC}')"
                    + ".code",
                ),
            ],
        )

        logger.info("Fetching MedicationStatement")
        med_statement_df, med_df_2 = self._fetch_medication_resource(
            patient_df=patient_df,
            resource_type="MedicationStatement",
            extra_fhir_paths=[
                (
                    "datetime",
                    "MedicationStatement.effectiveDateTime "
                    + "| MedicationStatement.effectivePeriod.start",
                ),
                ("period_end", "MedicationStatement.effectivePeriod.end"),
                ("dosage", "MedicationStatement.dosage"),
                (
                    "medication_codeableconcept_code",
                    "MedicationStatement.medicationCodeableConcept.coding."
                    + f".where(system = '{FHIR_CODE_SYSTEM_ATC_US}' or system = '{FHIR_CODE_SYSTEM_ATC}')"
                    + ".code",
                ),
            ],
        )

        logger.info("Fetching MedicationAdministration")
        med_administration_df, med_df_3 = self._fetch_medication_resource(
            patient_df=patient_df,
            resource_type="MedicationAdministration",
            extra_fhir_paths=[
                (
                    "datetime",
                    "MedicationAdministration.effectiveDateTime "
                    + "| MedicationAdministration.effectivePeriod.start",
                ),
                ("period_end", "MedicationAdministration.effectivePeriod.end"),
                ("dosage", "MedicationAdministration.dosage"),
                (
                    "medication_codeableconcept_code",
                    "MedicationAdministration.medicationCodeableConcept.coding."
                    + f".where(system = '{FHIR_CODE_SYSTEM_ATC_US}' or system = '{FHIR_CODE_SYSTEM_ATC}')"
                    + ".code",
                ),
            ],
        )

        if med_req_df is None and med_statement_df is None and med_administration_df is None:
            logger.info("Found no medication data to given patients")
            return

        logger.info("Combining medication data from different resource types")
        med_df = pd.concat([med_df_1, med_df_2, med_df_3])

        if "medication_atc_code" in med_df.columns:
            logger.info("Mapping ATC codes to display values using Excel sheet")
            atc_mapping_df = pd.read_excel(
                HERE / "ATC GKV-AI 2026.xlsx",
                sheet_name="WIdO-Index 2026 alphabetisch",
                usecols=["ATC-Code", "ATC-Bedeutung"],
            )
            logger.info("Loaded ATC mapping with {} entries", len(atc_mapping_df))
            med_df = med_df.merge(
                atc_mapping_df.rename(
                    columns={
                        "ATC-Code": "medication_atc_code",
                        "ATC-Bedeutung": "medication_atc_display",
                    }
                ),
                on="medication_atc_code",
                how="left",
            )

        logger.info("all_meds_df: {}", med_df.count())
        med_df.drop_duplicates(subset=["medication_id"], inplace=True)
        logger.info("all_meds_df after removing duplicates: {}", med_df.count())

        if "medication_ops_code" in med_df.columns:
            logger.info("Loading OPS mappings")
            ops_mapping = self.load_ops_codes(HERE / "ops2026syst_kodes.txt")
            med_df["medication_ops_display"] = med_df["medication_ops_code"].map(ops_mapping)

        for col in [
            "medication_code_text",
            "medication_atc_display",
            "medication_ops_display",
            "medication_pzn_display",
            "medication_ingredient_text",
        ]:
            if col not in med_df.columns:
                med_df[col] = None

        med_df["text"] = (
            med_df[
                [
                    "medication_atc_display",
                    "medication_ops_display",
                    "medication_code_text",
                    "medication_ingredient_text",
                    "medication_pzn_display",
                ]
            ]
            .bfill(axis=1)
            .iloc[:, 0]
        )

        med_df.to_csv(os.path.join(self.output_dir, "aml_all_meds.csv"), index=False)

        req_stat_admin_df = pd.concat([med_req_df, med_statement_df, med_administration_df])

        if "period_end" not in req_stat_admin_df.columns:
            req_stat_admin_df["period_end"] = pd.NaT

        if "medication_codeableconcept_code" in req_stat_admin_df.columns:
            logger.info("Mapping ATC codes to display values using Excel sheet")
            atc_mapping_df = pd.read_excel(
                HERE / "ATC GKV-AI 2026.xlsx",
                sheet_name="WIdO-Index 2026 alphabetisch",
                usecols=["ATC-Code", "ATC-Bedeutung"],
            )
            logger.info("Loaded ATC mapping with {} entries", len(atc_mapping_df))
            req_stat_admin_df = req_stat_admin_df.merge(
                atc_mapping_df.rename(
                    columns={
                        "ATC-Code": "medication_codeableconcept_code",
                        "ATC-Bedeutung": "medication_codeableconcept_display",
                    }
                ),
                on="medication_codeableconcept_code",
                how="left",
            )

        req_stat_admin_df.to_csv(
            os.path.join(self.output_dir, "aml_all_med_reqs_stats_admins.csv"), index=False
        )

    def extract_procedures(self, patient_list):
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv"), dtype={"patient_mrn": "string"}
        )

        all_procedures = []

        for chunk in chunked(patient_list, self.settings.fhir.chunk_size):
            chunk_df = pd.DataFrame({"subject_list": [",".join(chunk)]})

            procedure_df_chunk = self.search.trade_rows_for_dataframe(
                df=chunk_df,
                resource_type="Procedure",
                request_params={
                    "_count": self.settings.fhir.page_count,
                    "_elements": "subject,performed,code,status",
                },
                df_constraints={
                    "subject": "subject_list",
                },
                with_ref=False,
                fhir_paths=[
                    ("procedure_id", "id"),
                    ("procedure_patient_reference", "subject.reference"),
                    (
                        "procedure_ops_code",
                        "code.coding" + f".where(system = '{FHIR_CODE_SYSTEM_OPS}')" + ".code",
                    ),
                    (
                        "procedure_ops_version",
                        "code.coding" + f".where(system = '{FHIR_CODE_SYSTEM_OPS}')" + ".version",
                    ),
                    ("timestamp", "performedDateTime | performedPeriod.start"),
                    ("procedure_status", "status"),
                ],
            )

            all_procedures.append(procedure_df_chunk)

        procedure_df = pd.concat(all_procedures, ignore_index=True)

        if "procedure_ops_version" not in procedure_df.columns:
            procedure_df["procedure_ops_version"] = None
        else:
            procedure_df["procedure_ops_version"] = procedure_df["procedure_ops_version"].apply(
                lambda x: json.dumps(x, sort_keys=True) if isinstance(x, dict) else x
            )

        if "procedure_ops_code" in procedure_df.columns:
            procedure_df["procedure_ops_code"] = procedure_df["procedure_ops_code"].apply(
                lambda x: json.dumps(x, sort_keys=True) if isinstance(x, dict) else x
            )
            logger.info("Loading OPS mappings")
            ops_mapping = self.load_ops_codes(HERE / "ops2026syst_kodes.txt")
            procedure_df["procedure_ops_display"] = procedure_df["procedure_ops_code"].map(
                ops_mapping
            )

            # Export unmapped OPS codes (distinct by version + code)
            unmapped_ops = (
                procedure_df[
                    procedure_df["procedure_ops_display"].isna()
                    & procedure_df["procedure_ops_code"].notna()
                ][["procedure_ops_version", "procedure_ops_code"]]
                .drop_duplicates()
                .sort_values(["procedure_ops_version", "procedure_ops_code"])
            )

            unmapped_ops.to_csv(
                os.path.join(self.output_dir, "aml_unmapped_procedure_ops_codes.csv"), index=False
            )

        patient_mrn_lookup = (
            patient_df[["condition_patient_reference", "patient_mrn"]]
            .assign(patient_mrn=lambda x: _clean_patient_mrn_series(x["patient_mrn"]))
            .sort_values("patient_mrn")
            .drop_duplicates(subset=["condition_patient_reference"], keep="first")
            .set_index("condition_patient_reference")["patient_mrn"]
        )

        procedure_df["patient_mrn"] = procedure_df["procedure_patient_reference"].map(
            patient_mrn_lookup
        )

        procedure_df.to_csv(os.path.join(self.output_dir, "aml_all_procedures.csv"), index=False)

        logger.info(f"all_procedures_df size: {procedure_df.count()}. {procedure_df.dtypes}")

    def join_with_drug_data(self):
        zenzy_patient_ids = (
            pd.read_csv(
                os.path.join(self.settings.aml.csv_input_file),
                sep=";",
                encoding="latin1",
                engine="python",
                quotechar='"',
            )[self.settings.aml.csv_patient_column]
            .dropna()
            .drop_duplicates()
        )
        patient_df = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv"), dtype={"patient_mrn": "string"}
        )
        patient_ids = patient_df["patient_mrn"].dropna().astype(str).str.strip()
        filtered_ids = zenzy_patient_ids[zenzy_patient_ids.isin(patient_ids)]
        filtered_refs = (
            patient_df[patient_df["patient_mrn"].isin(filtered_ids)]["condition_patient_reference"]
            .dropna()
            .drop_duplicates()
        )

        logger.info("Zenzy input data patient count: {}", len(zenzy_patient_ids))
        logger.info("Matched with AML cohort patient count: {}", len(filtered_ids))

        if len(filtered_ids) > 0:
            lab_df = pd.read_csv(
                os.path.join(self.output_dir, "aml_all_labs.csv"),
                sep=",",
                dtype={"lab_quantity_value": "float"},
                parse_dates=["lab_dateTime"],
            )
            matched_lab_df = lab_df[lab_df["observation_patient_reference"].isin(filtered_refs)]
            matched_lab_df.to_csv(
                os.path.join(self.output_dir, "aml_matched_zenzy_labs.csv"), index=False
            )
            logger.info("matched_zenzy_labs_df size: {}", matched_lab_df.count())
            logger.info(
                "Found labs for {} patients",
                len(pd.unique(matched_lab_df["observation_patient_reference"])),
            )
        else:
            logger.info("Found no match between Zenzy input data and AML patients.")

    def post_process_lab_values(self, lab_df):

        processed = (
            lab_df.astype(str)
            .groupby(
                [
                    "loinc_code",
                    "loinc_display",
                    "lab_quantity_unit",
                    "lab_codeableconcept_code",
                ]
            )
            .size()
            .reset_index(name="num_labs")
            .sort_values(by="num_labs", ascending=False)
        )

        processed.to_csv(os.path.join(self.output_dir, "aml_labs_counts.csv"), index=False)

    def extract_from_obds(self):
        # 1. get all conditions with the AML icd codes
        # 2. get the death observations for these patients with cause of death and date of death
        # 3. get the referenced patients for these conditions with their identifier
        # 4. get the ecog score
        # 5. get the verlauf data
        # 6. get weitere klassifikationen

        if not self.settings.fhir.patient_identifier_system:
            raise ValueError("Please set the variable FHIR_PATIENT_IDENTIFIER_SYSTEM")

        patients = self.data.view(
            "Patient",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "patient_id",
                        },
                        {
                            "path": "identifier.where("
                            + f"system='{self.settings.fhir.patient_identifier_system}').value",
                            "name": "patient_mrn",
                        },
                    ],
                }
            ],
        )

        conditions = self.data.view(
            "Condition",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "condition_id",
                        },
                        {
                            "path": "subject.getReferenceKey()",
                            "name": "condition_patient_reference",
                        },
                        {
                            "path": f"extension('{FHIR_SYSTEMS_CONDITION_ASSERTED_DATE}')"
                            + ".value.ofType(dateTime)",
                            "name": "diagnosis_assertedDateTime",
                        },
                        {
                            "path": "recordedDate",
                            "name": "diagnosis_recordedDate",
                        },
                        {
                            "path": "onset.ofType(dateTime)",
                            "name": "diagnosis_onsetDateTime",
                        },
                        {
                            "path": f"code.coding.where(system = '{FHIR_CODE_SYSTEM_ICD10}').code",
                            "name": "icd_code",
                        },
                        {
                            "path": "meta.profile.first()",
                            "name": "meta_profile",
                        },
                    ],
                }
            ],
        )

        icd_codes_aml = self.data.spark.read.csv(
            (HERE / "icd_codes_aml.csv").as_posix(), header=True
        )

        aml_conditions = conditions.join(
            icd_codes_aml, conditions.icd_code == icd_codes_aml.icd_code, "inner"
        )

        aml_conditions = aml_conditions.join(
            patients.select("patient_id", "patient_mrn"),
            aml_conditions.condition_patient_reference == patients.patient_id,
            "left",
        )

        logger.info(f"Found {aml_conditions.count()} Conditions with AML ICD code")
        aml_conditions.show()

        save_final_df(aml_conditions, self.settings, suffix="obds_conditions")

        aml_patient_references = aml_conditions.select("condition_patient_reference").distinct()

        all_conditions = (
            conditions.join(
                aml_patient_references,
                on="condition_patient_reference",
                how="inner",
            )
            # AML-Diagnosen entfernen
            .join(
                icd_codes_aml.select("icd_code").distinct(),
                on="icd_code",
                how="left_anti",
            )
        )

        patient_lookup = patients.select(
            "patient_id",
            "patient_mrn",
        )

        all_conditions = all_conditions.join(
            patient_lookup,
            all_conditions.condition_patient_reference == patient_lookup.patient_id,
            "left",
        )
        logger.info(f"Found {all_conditions.count()} non-AML Conditions for all AML patients")
        all_conditions.show()

        save_final_df(all_conditions, self.settings, suffix="obds_all_conditions")

        deaths = self.data.view(
            "Observation",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "observation_id",
                        },
                        {
                            "path": "subject.getReferenceKey()",
                            "name": "observation_patient_reference",
                        },
                        {
                            "path": "effective.ofType(dateTime)",
                            "name": "death_dateTime",
                        },
                        {
                            "path": "interpretation.coding.where(system = "
                            + f"'{FHIR_CODE_SYSTEM_TOD_TUMORBEDINGT}').code",
                            "name": "death_caused_by_tumor",
                        },
                        {
                            "path": "value.ofType(CodeableConcept).coding"
                            + f".where(system = '{FHIR_CODE_SYSTEM_ICD10}').code",
                            "name": "cause_of_death",
                        },
                    ],
                }
            ],
            where=[
                {
                    "description": "Only death observations",
                    "path": f"code.coding.where(system='{FHIR_CODE_SYSTEM_SNOMED}' "
                    + "and code='184305005').exists()",
                }
            ],
        )

        deaths.show()

        deaths = deaths.join(
            aml_patient_references,
            deaths.observation_patient_reference == conditions.condition_patient_reference,
            "inner",
        )

        deaths = deaths.join(
            patients.select("patient_id", "patient_mrn"),
            deaths.observation_patient_reference == patients.patient_id,
            "left",
        )

        logger.info(f"Found {deaths.count()} deaths with AML patient matches")
        deaths.show()

        save_final_df(
            deaths,
            self.settings,
            suffix="obds_deaths",
        )

        ecogs = self.data.view(
            "Observation",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "observation_id",
                        },
                        {
                            "path": "subject.getReferenceKey()",
                            "name": "observation_patient_reference",
                        },
                        {
                            "path": "effective.ofType(dateTime)",
                            "name": "effective_dateTime",
                        },
                        {
                            "path": "value.ofType(CodeableConcept).coding"
                            + f".where(system = '{FHIR_CODE_SYSTEM_ECOG}').code",
                            "name": "ecog_performance_status",
                        },
                    ],
                }
            ],
            where=[
                {
                    "description": "Only ecog observations",
                    "path": f"code.coding.where(system='{FHIR_CODE_SYSTEM_SNOMED}' "
                    + "and code='423740007').exists()",
                }
            ],
        )

        ecogs.show()

        ecogs = ecogs.join(
            aml_patient_references,
            ecogs.observation_patient_reference == conditions.condition_patient_reference,
            "inner",
        ).select(ecogs["*"])

        ecogs = ecogs.join(
            patients.select("patient_id", "patient_mrn"),
            ecogs.observation_patient_reference == patients.patient_id,
            "left",
        )

        save_final_df(
            ecogs,
            self.settings,
            suffix="obds_ecog_statuses",
        )

        progressions = self.data.view(
            "Observation",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "observation_id",
                        },
                        {
                            "path": "subject.getReferenceKey()",
                            "name": "observation_patient_reference",
                        },
                        {
                            "path": "effective.ofType(dateTime)",
                            "name": "effective_dateTime",
                        },
                        {
                            "path": "value.ofType(CodeableConcept).coding"
                            + f".where(system = '{FHIR_CODE_SYSTEM_VERLAUF_GESAMTBEURTEILUNG}')"
                            + ".code",
                            "name": "overall_assessment_of_tumor_regression",
                        },
                    ],
                }
            ],
            where=[
                {
                    "description": "Only 'Status of regression of tumor' observations",
                    "path": f"code.coding.where(system='{FHIR_CODE_SYSTEM_SNOMED}' "
                    + "and code='396432002').exists()",
                }
            ],
        )

        progressions.show()

        progressions = progressions.join(
            aml_patient_references,
            progressions.observation_patient_reference == conditions.condition_patient_reference,
            "inner",
        ).select(progressions["*"])

        progressions = progressions.join(
            patients.select("patient_id", "patient_mrn"),
            progressions.observation_patient_reference == patients.patient_id,
            "left",
        )

        save_final_df(
            progressions,
            self.settings,
            suffix="obds_progressions",
        )

        medication_statements = self.data.view(
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
                            "description": "Patient ID",
                            "path": "subject.getReferenceKey()",
                            "name": "patient_reference",
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
                            "name": "statement_medication_text",
                        },
                        {
                            "description": "Medication Code",
                            "path": "medication.ofType(CodeableConcept).coding"
                            + ".where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                            "name": "statement_medication_atc_code",
                        },
                        {
                            "description": "Medication Reference",
                            "path": "medication.ofType(Reference).getReferenceKey()",
                            "name": "medication_reference",
                        },
                    ],
                }
            ],
        )

        medication_statements.show()

        medications = self.data.view(
            "Medication",
            select=[
                {
                    "column": [
                        {
                            "description": "Medication ID",
                            "path": "getResourceKey()",
                            "name": "medication_id",
                        },
                        {
                            "description": "Medication ATC Code",
                            "path": "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').first().code",
                            "name": "medication_medication_atc_code",
                        },
                        {
                            "description": "Medication Text",
                            "path": "code.text",
                            "name": "medication_medication_text",
                        },
                    ],
                }
            ],
        )

        medications.show()

        medication_statements = (
            medication_statements.join(
                medications,
                medication_statements.medication_reference == medications.medication_id,
                how="left",
            )
            .withColumn(
                "medication_text",
                F.coalesce(F.col("statement_medication_text"), F.col("medication_medication_text")),
            )
            .withColumn(
                "medication_atc_code",
                F.coalesce(
                    F.col("statement_medication_atc_code"), F.col("medication_medication_atc_code")
                ),
            )
            .select(
                "medication_statement_id",
                "patient_reference",
                "condition_id",
                "medication_start_date",
                "medication_text",
                "medication_atc_code",
            )
        )

        medication_statements.show()

        medication_statements = medication_statements.join(
            aml_patient_references,
            medication_statements.patient_reference == conditions.condition_patient_reference,
            "inner",
        ).select(medication_statements["*"])

        medication_statements = medication_statements.join(
            patients.select("patient_id", "patient_mrn"),
            medication_statements.patient_reference == patients.patient_id,
            "left",
        )

        aml_condition_ids = [row["condition_id"] for row in conditions.collect()]
        filtered_medication_statements = medication_statements.filter(
            F.col("condition_id").isin(aml_condition_ids)
        )

        save_final_df(
            filtered_medication_statements,
            self.settings,
            suffix="obds_systemtherapien",
        )

        procedures = self.data.view(
            "Procedure",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "procedure_id",
                        },
                        {
                            "path": "subject.getReferenceKey()",
                            "name": "procedure_patient_reference",
                        },
                        {
                            "path": "performed.ofType(Period).start",
                            "name": "performed_period_start",
                        },
                        {
                            "path": "performed.ofType(Period).end",
                            "name": "performed_period_end",
                        },
                        {
                            "path": "code.ofType(CodeableConcept).coding"
                            + f".where(system = '{FHIR_CODE_SYSTEM_OPS}')"
                            + ".code",
                            "name": "procedure_ops_code",
                        },
                        {
                            "path": "code.ofType(CodeableConcept).coding"
                            + f".where(system = '{FHIR_CODE_SYSTEM_OPS}')"
                            + ".display",
                            "name": "procedure_ops_display",
                        },
                        {
                            "path": "bodySite.coding"
                            + f".where(system = '{FHIR_CODE_SYSTEM_BODYSITE}')"
                            + ".code",
                            "name": "procedure_target_bodyregion",
                        },
                    ],
                }
            ],
            where=[
                {
                    "description": "Only Procedures with OPS code",
                    "path": f"code.coding.where(system='{FHIR_CODE_SYSTEM_OPS}').exists()",
                }
            ],
        )

        procedures.show()

        procedures = procedures.join(
            aml_patient_references,
            procedures.procedure_patient_reference == conditions.condition_patient_reference,
            "inner",
        ).select(procedures["*"])

        procedures = procedures.join(
            patients.select("patient_id", "patient_mrn"),
            procedures.procedure_patient_reference == patients.patient_id,
            "left",
        )

        save_final_df(
            procedures,
            self.settings,
            suffix="obds_procedures",
        )

        weitere_klassifikationen = self.data.view(
            "Observation",
            select=[
                {
                    "column": [
                        {
                            "path": "getResourceKey()",
                            "name": "observation_id",
                        },
                        {
                            "path": "subject.getReferenceKey()",
                            "name": "observation_patient_reference",
                        },
                        {
                            "path": "focus.first().getReferenceKey()",
                            "name": "observation_condition_reference",
                        },
                        {
                            "path": "meta.profile",
                            "name": "meta_profile",
                        },
                        {
                            "path": "effective.ofType(DateTime)",
                            "name": "effective_dateTime",
                        },
                        {
                            "path": "code.text",
                            "name": "code_text",
                        },
                        {
                            "path": "value.ofType(CodeableConcept).text",
                            "name": "value_text",
                        },
                    ],
                },
                {
                    "forEach": "value.ofType(CodeableConcept).coding",
                    "column": [
                        {"name": "value_system", "path": "system"},
                        {"name": "value_code", "path": "code"},
                    ],
                },
            ],
        )

        weitere_klassifikationen = weitere_klassifikationen.filter(
            F.col("meta_profile").isNotNull()
            & F.col("meta_profile").startswith(FHIR_PROFILE_WEITERE_KLASSIFIKATIONEN)
        ).drop("meta_profile")  # we don't need the profile after filtering

        weitere_klassifikationen = weitere_klassifikationen.join(
            aml_patient_references,
            weitere_klassifikationen.observation_patient_reference
            == conditions.condition_patient_reference,
            "inner",
        ).select(weitere_klassifikationen["*"])

        weitere_klassifikationen = weitere_klassifikationen.join(
            patients.select("patient_id", "patient_mrn"),
            weitere_klassifikationen.observation_patient_reference == patients.patient_id,
            "left",
        )

        weitere_klassifikationen.show()

        filtered_weitere_klassifikationen = weitere_klassifikationen.filter(
            F.col("observation_condition_reference").isin(aml_condition_ids)
        )

        save_final_df(
            filtered_weitere_klassifikationen,
            self.settings,
            suffix="obds_weitere_klassifikationen",
        )

        vitalstatus = vitalstatus_view(self.data)

        vitalstatus = vitalstatus.join(
            aml_patient_references,
            vitalstatus.observation_patient_reference == conditions.condition_patient_reference,
            "inner",
        ).select(vitalstatus["*"])

        vitalstatus = vitalstatus.join(
            patients.select("patient_id", "patient_mrn"),
            vitalstatus.observation_patient_reference == patients.patient_id,
            "left",
        )

        save_final_df(
            vitalstatus,
            self.settings,
            suffix="obds_vitalstatus",
        )

    def de_identify(self):
        CRYPTO_HASH_KEY = secrets.token_bytes(256)
        DAY_SHIFT = secrets.randbelow(61) - 30

        def crypto_hash(s: str):
            if pd.isna(s):
                return pd.NA

            return hmac.new(CRYPTO_HASH_KEY, s.encode("utf-8"), hashlib.sha256).hexdigest()

        def crypto_hash_nullable(value):
            if pd.isna(value):
                return pd.NA
            return crypto_hash(str(value))

        de_identified_base_dir = Path(self.output_dir) / "de-identified"
        de_identified_dir = de_identified_base_dir / self.settings.location.lower()
        de_identified_dir.mkdir(parents=True, exist_ok=True)

        patients_with_diagnoses = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_patients.csv"),
            dtype={"patient_mrn": "string"},
        )
        # Ensure date columns are datetime even when parse_dates fails
        # (e.g. all-NA columns stay as string with pyarrow string storage)
        date_cols = [
            "diagnosis_onsetDateTime",
            "diagnosis_recordedDate",
            "deceased_dateTime",
            "birth_date",
            "last_follow_up_datetime",
        ]
        for col in date_cols:
            if col in patients_with_diagnoses.columns:
                patients_with_diagnoses[col] = pd.to_datetime(
                    patients_with_diagnoses[col], errors="coerce", utc=True, format="ISO8601"
                )

        patients_with_diagnoses["condition_id"] = patients_with_diagnoses["condition_id"].apply(
            lambda x: crypto_hash(x)
        )

        patients_with_diagnoses["birth_year"] = patients_with_diagnoses["birth_date"].dt.year

        patients_with_diagnoses = patients_with_diagnoses.drop(columns=["birth_date"])

        patients_with_diagnoses["condition_patient_reference"] = patients_with_diagnoses[
            "condition_patient_reference"
        ].apply(lambda x: crypto_hash(x))

        # Build a hash -> original patient_mrn lookup table before the column below gets
        # hashed in place. This is kept outside de_identified_dir/de_identified_base_dir so
        # it is never picked up by the zip step and never ships with the de-identified export.
        patient_mrn_lookup = patients_with_diagnoses[["patient_mrn"]].dropna().drop_duplicates()
        patient_mrn_lookup["patient_mrn_hash"] = patient_mrn_lookup["patient_mrn"].apply(
            crypto_hash_nullable
        )
        patient_mrn_lookup_dir = Path(self.output_dir) / "patient_mrn_lookup"
        patient_mrn_lookup_dir.mkdir(parents=True, exist_ok=True)
        patient_mrn_lookup.to_csv(
            patient_mrn_lookup_dir / f"{self.settings.location.lower()}_aml_patient_mrn_lookup.csv",
            index=False,
        )

        patients_with_diagnoses["patient_mrn"] = patients_with_diagnoses["patient_mrn"].apply(
            crypto_hash_nullable
        )

        columns_to_shift = [
            "diagnosis_onsetDateTime",
            "diagnosis_recordedDate",
            "deceased_dateTime",
        ]
        if "last_follow_up_datetime" in patients_with_diagnoses.columns:
            columns_to_shift.append("last_follow_up_datetime")
        for column in columns_to_shift:
            patients_with_diagnoses[column] = patients_with_diagnoses[column] + pd.to_timedelta(
                DAY_SHIFT, unit="D"
            )

        patients_with_diagnoses.to_csv(de_identified_dir / "aml_diagnoses.csv", index=False)

        # FHIR Conditions
        fhir_conditions = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_conditions.csv"),
            sep=",",
            dtype={"patient_mrn": str},
        )

        columns_to_hash = [
            "condition_id",
            "all_condition_patient_reference",
            "patient_mrn",
        ]

        for column in columns_to_hash:
            fhir_conditions[column] = fhir_conditions[column].apply(crypto_hash_nullable)

        date_cols = [
            "diagnosis_recordedDate",
            "diagnosis_onsetDateTime",
        ]
        for col in date_cols:
            if col in fhir_conditions.columns:
                fhir_conditions[col] = pd.to_datetime(
                    fhir_conditions[col], errors="coerce", utc=True, format="ISO8601"
                )
                fhir_conditions[col] = fhir_conditions[col] + pd.to_timedelta(DAY_SHIFT, unit="D")

        fhir_conditions.to_csv(de_identified_dir / "aml_fhir_conditions.csv", index=False)

        # FHIR Encounters
        fhir_encounters = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_encounters.csv"),
            sep=",",
            dtype={"patient_mrn": "string"},
        )

        columns_to_hash = [
            "encounter_id",
            "encounter_patient_reference",
            "patient_mrn",
        ]

        for column in columns_to_hash:
            fhir_encounters[column] = fhir_encounters[column].apply(crypto_hash_nullable)

        date_cols = [
            "enc_period_start",
            "enc_period_end",
        ]
        for col in date_cols:
            if col in fhir_encounters.columns:
                fhir_encounters[col] = pd.to_datetime(
                    fhir_encounters[col], errors="coerce", utc=True, format="ISO8601"
                )
                fhir_encounters[col] = fhir_encounters[col] + pd.to_timedelta(DAY_SHIFT, unit="D")

        fhir_encounters.to_csv(de_identified_dir / "aml_fhir_encounters.csv", index=False)

        # Zenzy
        if os.path.exists(
            self.settings.aml.csv_input_file
        ) or self.settings.aml.csv_input_file.startswith("s3://"):
            zenzy_df = pd.read_csv(
                self.settings.aml.csv_input_file,
                sep=";",
                dtype={
                    "patient_mrn": "string",
                    self.settings.aml.csv_patient_column: "string",
                },
            )

            zenzy_df = zenzy_df[
                zenzy_df[self.settings.aml.csv_patient_column] != "*** VALUE NOT FOUND ***"
            ]

            if "Retoure" not in zenzy_df.columns:
                logger.warning(
                    "Retoure column not found in Zenzy input data. "
                    + "Setting default Retoure to 'FALSCH' for all records"
                )
                zenzy_df["Retoure"] = "FALSCH"

            if "Applikationsart" not in zenzy_df.columns:
                logger.warning(
                    "Applikationsart column not found in Zenzy input data. "
                    + "Setting default Applikationsart to 'NA' for all records"
                )
                zenzy_df["Applikationsart"] = pd.NA

            if "Zeit" not in zenzy_df.columns:
                logger.warning(
                    "Zeit column not found in Zenzy input data. "
                    + "Setting default time to 08:00 for all records"
                )
                zenzy_df["Zeit"] = "08:00"

            if "Therapieprotokoll" not in zenzy_df.columns:
                logger.warning(
                    "Therapieprotokoll column not found in Zenzy input data. "
                    + "Setting default Therapieprotokoll to 'NA' for all records"
                )
                zenzy_df["Therapieprotokoll"] = pd.NA

            if "Herstellungs-ID" not in zenzy_df.columns:
                logger.warning(
                    "Herstellungs-ID column not found in Zenzy input data. "
                    + "Setting default Herstellungs-ID for all records to the row index"
                )
                zenzy_df["Herstellungs-ID"] = zenzy_df.index

            zenzy_df["Applikationszeitpunkt"] = pd.to_datetime(
                zenzy_df["Datum"] + " " + zenzy_df["Zeit"], format="%d.%m.%Y %H:%M", errors="raise"
            )

            zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"].dt.tz_localize(
                "Europe/Berlin", ambiguous="NaT", nonexistent="shift_forward"
            )

            zenzy_df["label"] = (
                "Wirkstoff: "
                + zenzy_df["Wirkstoff"].astype(str)
                + " ("
                + "Dosis: "
                + zenzy_df["Dosis"].astype(str)
                + ") "
                + "Protokoll: "
                + zenzy_df["Therapieprotokoll"].astype(str)
                + " "
                + "Applikationsart: "
                + zenzy_df["Applikationsart"].astype(str)
                + " "
                + "Retoure?: "
                + zenzy_df["Retoure"].astype(str)
            )

            zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"] + pd.to_timedelta(
                DAY_SHIFT, unit="D"
            )

            zenzy_df["Herstellungs-ID"] = zenzy_df["Herstellungs-ID"].apply(crypto_hash_nullable)

            zenzy_df["patient_mrn"] = zenzy_df[self.settings.aml.csv_patient_column].apply(
                crypto_hash_nullable
            )

            if "Datum" not in zenzy_df.columns:
                zenzy_df["Datum"] = pd.NaT
            if "Zeit" not in zenzy_df.columns:
                zenzy_df["Zeit"] = pd.NaT
            if "Herstellungsdatum" not in zenzy_df.columns:
                zenzy_df["Herstellungsdatum"] = pd.NaT
            if "Herstellungszeit" not in zenzy_df.columns:
                zenzy_df["Herstellungszeit"] = pd.NaT

            zenzy_df = zenzy_df.drop(
                columns=[
                    self.settings.aml.csv_patient_column,
                    "Datum",
                    "Zeit",
                    "Herstellungsdatum",
                    "Herstellungszeit",
                ],
                inplace=False,
            )

            # drop the default column raw pat id column if still present
            # happens if during pseudonymization, both columns remained
            zenzy_df = zenzy_df.drop(
                columns=["KIS-Patienten-ID"],
                inplace=False,
                errors="ignore",
            )

            zenzy_df.to_csv(de_identified_dir / "aml_zenzy.csv", index=False)

        # FHIR Medikation Statements, Requests, Administrations
        fhir_medikation = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_med_reqs_stats_admins.csv"),
            sep=",",
            dtype={"patient_mrn": "string"},
        )

        columns_to_hash = [
            "id",
            "patient_reference",
            "patient_mrn",
            "medication_reference",
        ]

        for column in columns_to_hash:
            fhir_medikation[column] = fhir_medikation[column].apply(crypto_hash_nullable)

        date_cols = [
            "datetime",
            "period_end",
        ]
        for col in date_cols:
            if col in fhir_medikation.columns:
                fhir_medikation[col] = pd.to_datetime(
                    fhir_medikation[col], errors="coerce", utc=True, format="ISO8601"
                )
                fhir_medikation[col] = fhir_medikation[col] + pd.to_timedelta(DAY_SHIFT, unit="D")

        fhir_medikation.to_csv(de_identified_dir / "aml_fhir_medication.csv", index=False)

        # referenced Medications for MedicationStatements, MedicationRequests,
        # MedicationAdministrations
        fhir_all_medication = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_meds.csv"),
            sep=",",
        )

        columns_to_hash = [
            "medication_id",
        ]

        for column in columns_to_hash:
            fhir_all_medication[column] = fhir_all_medication[column].apply(crypto_hash_nullable)

        fhir_all_medication.to_csv(de_identified_dir / "aml_all_meds.csv", index=False)

        # FHIR Procedure
        fhir_procedures = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_procedures.csv"),
            sep=",",
            dtype={"patient_mrn": "string"},
        )

        columns_to_hash = [
            "procedure_id",
            "procedure_patient_reference",
            "patient_mrn",
        ]

        for column in columns_to_hash:
            fhir_procedures[column] = fhir_procedures[column].apply(crypto_hash_nullable)

        columns_to_shift = ["timestamp"]
        for column in columns_to_shift:
            fhir_procedures[column] = pd.to_datetime(
                fhir_procedures[column], errors="coerce", utc=True, format="ISO8601"
            )
            fhir_procedures[column] = fhir_procedures[column] + pd.to_timedelta(DAY_SHIFT, unit="D")

        fhir_procedures.to_csv(de_identified_dir / "aml_fhir_procedures.csv", index=False)

        unmapped_procedure_ops_codes_path = os.path.join(
            self.output_dir, "aml_unmapped_procedure_ops_codes.csv"
        )

        if os.path.exists(unmapped_procedure_ops_codes_path):
            shutil.copy2(
                unmapped_procedure_ops_codes_path,
                de_identified_dir / "aml_unmapped_procedure_ops_codes.csv",
            )

        # weitere klassifikation
        obds_weitere_klassifikationen = pd.read_csv(
            os.path.join(self.output_dir, "df_obds_weitere_klassifikationen.csv"),
            sep=";",
            dtype={"patient_mrn": "string"},
        )
        obds_weitere_klassifikationen["effective_dateTime"] = pd.to_datetime(
            obds_weitere_klassifikationen["effective_dateTime"], errors="raise", format="ISO8601"
        )

        columns_to_hash = [
            "observation_id",
            "observation_patient_reference",
            "observation_condition_reference",
            "patient_mrn",
            "patient_id",
        ]

        for column in columns_to_hash:
            obds_weitere_klassifikationen[column] = obds_weitere_klassifikationen[column].apply(
                crypto_hash_nullable
            )

        columns_to_shift = ["effective_dateTime"]
        for column in columns_to_shift:
            obds_weitere_klassifikationen[column] = obds_weitere_klassifikationen[
                column
            ] + pd.to_timedelta(DAY_SHIFT, unit="D")

        obds_weitere_klassifikationen.to_csv(
            de_identified_dir / "aml_obds_weitere_klassifikationen.csv", index=False
        )

        # ECOG
        obds_ecog = pd.read_csv(
            os.path.join(self.output_dir, "df_obds_ecog_statuses.csv"),
            sep=";",
            dtype={"patient_mrn": "string"},
        )
        obds_ecog["effective_dateTime"] = pd.to_datetime(
            obds_ecog["effective_dateTime"], errors="raise", format="ISO8601"
        )

        columns_to_hash = [
            "observation_id",
            "observation_patient_reference",
            "patient_mrn",
            "patient_id",
        ]

        for column in columns_to_hash:
            obds_ecog[column] = obds_ecog[column].apply(crypto_hash_nullable)

        columns_to_shift = ["effective_dateTime"]
        for column in columns_to_shift:
            obds_ecog[column] = obds_ecog[column] + pd.to_timedelta(DAY_SHIFT, unit="D")

        obds_ecog.to_csv(de_identified_dir / "aml_obds_ecog.csv", index=False)

        # Progress
        obds_progressions = pd.read_csv(
            os.path.join(self.output_dir, "df_obds_progressions.csv"),
            sep=";",
            dtype={"patient_mrn": "string"},
        )
        obds_progressions["effective_dateTime"] = pd.to_datetime(
            obds_progressions["effective_dateTime"], errors="raise", format="ISO8601"
        )

        columns_to_hash = [
            "observation_id",
            "observation_patient_reference",
            "patient_mrn",
            "patient_id",
        ]

        for column in columns_to_hash:
            obds_progressions[column] = obds_progressions[column].apply(crypto_hash_nullable)

        columns_to_shift = ["effective_dateTime"]
        for column in columns_to_shift:
            obds_progressions[column] = obds_progressions[column] + pd.to_timedelta(
                DAY_SHIFT, unit="D"
            )

        obds_progressions.to_csv(de_identified_dir / "aml_obds_progressions.csv", index=False)

        # obds conditions
        obds_conditions = pd.read_csv(
            os.path.join(self.output_dir, "df_obds_conditions.csv"),
            sep=";",
            dtype={"patient_mrn": "string"},
        )

        obds_all_conditions = pd.read_csv(
            os.path.join(self.output_dir, "df_obds_all_conditions.csv"),
            sep=";",
            dtype={"patient_mrn": str},
        )

        columns_to_hash = [
            "condition_id",
            "condition_patient_reference",
            "patient_mrn",
            "patient_id",
        ]

        for column in columns_to_hash:
            obds_conditions[column] = obds_conditions[column].apply(crypto_hash_nullable)
            obds_all_conditions[column] = obds_all_conditions[column].apply(crypto_hash_nullable)

        columns_to_shift = [
            "diagnosis_recordedDate",
            "diagnosis_onsetDateTime",
            "diagnosis_assertedDateTime",
        ]
        for column in columns_to_shift:
            obds_conditions[column] = pd.to_datetime(
                obds_conditions[column], errors="raise", format="ISO8601"
            )
            obds_conditions[column] = obds_conditions[column] + pd.to_timedelta(DAY_SHIFT, unit="D")

            obds_all_conditions[column] = pd.to_datetime(
                obds_all_conditions[column], errors="raise", format="ISO8601"
            )
            obds_all_conditions[column] = obds_all_conditions[column] + pd.to_timedelta(
                DAY_SHIFT, unit="D"
            )

        obds_conditions.to_csv(de_identified_dir / "aml_obds_diagnoses.csv", index=False)
        obds_all_conditions.to_csv(de_identified_dir / "aml_obds_all_diagnoses.csv", index=False)

        # SAP Medikation
        sap_medication_path = self.settings.aml.extra_medication_file
        if sap_medication_path and (
            os.path.exists(sap_medication_path)
            or self.settings.aml.csv_input_file.startswith("s3://")
        ):
            sap_medikation = pd.read_csv(
                sap_medication_path,
                sep=";",
                dtype={"patient_mrn": "string"},
            ).drop(columns=["FALL_ID", "TEILFALL_ID"], errors="ignore")

            sap_medikation["REZEPT_DATUM"] = pd.to_datetime(
                sap_medikation["REZEPT_DATUM"], format="mixed", dayfirst=True, errors="coerce"
            )

            if "AUFNAHME_DATUM" not in sap_medikation.columns:
                logger.warning(
                    "AUFNAHME_DATUM column not found in SAP medication data. "
                    + "Setting default AUFNAHME_DATUM to 'NA' for all records"
                )
                sap_medikation["AUFNAHME_DATUM"] = pd.NaT

            sap_medikation["AUFNAHME_DATUM"] = pd.to_datetime(
                sap_medikation["AUFNAHME_DATUM"], format="%Y-%m-%d", errors="coerce"
            )

            if "REZEPT_ID" not in sap_medikation.columns:
                logger.warning(
                    "REZEPT_ID column not found in SAP medication data. "
                    + "Setting default REZEPT_ID for all records to the row index"
                )
                sap_medikation["REZEPT_ID"] = sap_medikation.index

            columns_to_hash = [
                "REZEPT_ID",
                "patient_mrn",
            ]
            for column in columns_to_hash:
                sap_medikation[column] = sap_medikation[column].apply(crypto_hash_nullable)

            columns_to_shift = ["REZEPT_DATUM", "AUFNAHME_DATUM"]
            for column in columns_to_shift:
                sap_medikation[column] = sap_medikation[column] + pd.to_timedelta(
                    DAY_SHIFT, unit="D"
                )

            sap_medikation.to_csv(de_identified_dir / "aml_sap_medication.csv", index=False)

        # Lab data
        # simply copy the summary counts
        lab_counts_path = os.path.join(self.output_dir, "aml_labs_counts.csv")
        shutil.copy2(lab_counts_path, de_identified_dir / "aml_labs_counts.csv")

        # de-identify the other lab data
        lab_data = pd.read_csv(
            os.path.join(self.output_dir, "aml_all_labs.csv"),
            sep=",",
            dtype={"patient_mrn": "string"},
        )
        lab_data["lab_dateTime"] = pd.to_datetime(
            lab_data["lab_dateTime"], errors="coerce", format="ISO8601"
        )

        columns_to_hash = ["observation_id", "observation_patient_reference", "patient_mrn"]

        for column in columns_to_hash:
            lab_data[column] = lab_data[column].apply(crypto_hash_nullable)

        columns_to_shift = ["lab_dateTime"]
        for column in columns_to_shift:
            lab_data[column] = lab_data[column] + pd.to_timedelta(DAY_SHIFT, unit="D")

        lab_data.to_csv(de_identified_dir / "aml_all_labs.csv", index=False)

        date_prefix = datetime.datetime.now().strftime("%Y-%m-%d")
        zip_path = (
            Path(self.output_dir)
            / f"{date_prefix}-{self.settings.location.lower()}-aml-de-identified.zip"
        )

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_path in de_identified_base_dir.rglob("*"):
                if file_path.is_file():
                    # Preserve relative paths inside the zip
                    zipf.write(file_path, file_path.relative_to(de_identified_base_dir))
