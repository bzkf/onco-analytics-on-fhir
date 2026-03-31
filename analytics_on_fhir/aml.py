import os
from pathlib import Path

import pandas as pd
from fhir_constants import FHIR_SYSTEMS_CONDITION_ASSERTED_DATE
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from pathling.datasource import DataSource
from settings import Settings
from urllib3 import Retry
from utils import (
    save_final_df,
)

FHIR_CODE_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_CODE_SYSTEM_SNOMED = "http://snomed.info/sct"
FHIR_CODE_SYSTEM_TOD_TUMORBEDINGT = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-tod"
)
FHIR_CODE_SYSTEM_ECOG = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-allgemeiner-leistungszustand-ecog"
FHIR_CODE_SYSTEM_VERLAUF_GESAMTBEURTEILUNG = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-verlauf-gesamtbeurteilung"
FHIR_CODE_SYSTEM_OPS = "http://fhir.de/CodeSystem/bfarm/ops"
FHIR_CODE_SYSTEM_BODYSITE = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-strahlentherapie-zielgebiet"

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
    },
    "aml_labs": {
        "observation_id": "Unique identifier of the Observation resource",
        "observation_patient_reference": "FHIR reference to the patient (Patient/{id})",
        "loinc_code": "LOINC code of the laboratory observation",
        "loinc_display": "Human-readable LOINC description",
        "lab_dateTime": "Date/time when the laboratory measurement was taken",
        "lab_quantity_value": "Numeric value of the laboratory measurement",
        "lab_quantity_unit": "Unit of the laboratory measurement",
        "lab_codeableconcept_code": "Code of the lab value result (if applicable)",
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
}

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


class AMLStudy:
    def __init__(self, settings: Settings, data: DataSource):
        self.settings = settings
        self.data = data

        # not strictly needed since their names match the default env variable names expected
        os.environ["FHIR_USER"] = settings.fhir.user
        os.environ["FHIR_PASSWORD"] = settings.fhir.password

        auth = Ahoy(
            auth_type="BasicAuth",
            auth_method="env",
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

        merged_df.to_csv(os.path.join(self.output_dir, "aml_all_patients.csv"), index=False)

        logger.info("merged_df size: {}", merged_df.count())
        logger.info("patient_df size: {}", patient_df.count())

        patient_list = merged_df["condition_patient_reference"]
        patient_list.drop_duplicates(inplace=True)
        self.extract_labs(patient_list=patient_list)

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
                    ("lab_quantity_value", "valueQuantity.value"),
                    ("lab_quantity_unit", "valueQuantity.code"),
                    (
                        "lab_codeableconcept_code",
                        f"Observation.valueCodeableConcept.coding.where(system='{FHIR_CODE_SYSTEM_SNOMED}')"
                        + ".first().code",
                    ),
                ],
            )

            all_labs.append(lab_df_chunk)

        lab_df = pd.concat(all_labs, ignore_index=True)
        lab_df.to_csv(os.path.join(self.output_dir, "aml_all_labs.csv"), index=False)

        logger.info("all_labs_df size: {}", lab_df.count())

        self.post_process_lab_values(lab_df)

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
        patient_df = pd.read_csv(os.path.join(self.output_dir, "aml_all_patients.csv"))
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
            lab_df = pd.read_csv(os.path.join(self.output_dir, "aml_all_labs.csv"))
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

        return

    def extract_from_obds(self):
        # 1. get all conditions with the AML icd codes
        # 2. get the death observations for these patients with cause of death and date of death
        # 3. get the referenced patients for these conditions with their identifier
        # 4. get the ecog score
        # 5. get the verlauf data
        # 6. maybe get weitere klassifikationen

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
                    ],
                }
            ],
        )

        icd_codes_aml = self.data.spark.read.csv(
            (HERE / "icd_codes_aml.csv").as_posix(), header=True
        )

        conditions = conditions.join(
            icd_codes_aml, conditions.icd_code == icd_codes_aml.icd_code, "inner"
        )

        logger.info(f"Found {conditions.count()} Conditions with AML ICD code")
        conditions.show()

        aml_patient_references = conditions.select("condition_patient_reference").distinct()

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
                    "code": "Only Procedures with OPS code",
                    "path": f"code.coding.where(system='{FHIR_CODE_SYSTEM_OPS}')" + ".exists()",
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
