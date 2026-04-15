import os
from pathlib import Path

import pandas as pd
from fhir_constants import FHIR_SYSTEMS_CONDITION_ASSERTED_DATE
from fhir_pyrate import Ahoy, Pirate
from loguru import logger
from more_itertools import chunked
from pathling.datasource import DataSource
from pyspark.sql import functions as F
from settings import Settings
from urllib3 import Retry
from utils import save_final_df

FHIR_CODE_SYSTEM_ICD10 = "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
FHIR_CODE_SYSTEM_SNOMED = "http://snomed.info/sct"
FHIR_CODE_SYSTEM_TOD_TUMORBEDINGT = (
    "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-tod"
)
FHIR_CODE_SYSTEM_ECOG = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-allgemeiner-leistungszustand-ecog"
FHIR_CODE_SYSTEM_VERLAUF_GESAMTBEURTEILUNG = "https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/CodeSystem/mii-cs-onko-verlauf-gesamtbeurteilung"
FHIR_CODE_SYSTEM_OPS = "http://fhir.de/CodeSystem/bfarm/ops"
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
        "ingredient": "Ingredient(s) of the medication",
    },
    "aml_all_med_reqs_stats_admins": {
        "type": "FHIR Resource type of the source",
        "id": "Unique identifier of the resource",
        "patient_reference": "FHIR reference to the patient (Patient/{id})",
        "patient_mrn": "Medical Record Number of the patient (Patient ID from Patient.identifier)",
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


class AMLStudy:
    def __init__(self, settings: Settings, data: DataSource):
        self.settings = settings
        self.data = data

        # not strictly needed since their names match the default env variable names expected
        os.environ["FHIR_USER"] = settings.fhir.user
        os.environ["FHIR_PASSWORD"] = settings.fhir.password

        auth = Ahoy(
            auth_type=settings.fhir.auth_type,
            auth_method="env",
            auth_url=settings.fhir.token_auth_url,
            refresh_url=settings.fhir.token_refresh_url,
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

    def extract_meds(self):  # , patient_list):
        patient_df = pd.read_csv(os.path.join(self.output_dir, "aml_all_patients.csv"))

        # MedicationRequest + Medication
        med_df = self.search.trade_rows_for_dataframe(
            df=patient_df,
            resource_type="MedicationRequest",
            request_params={
                "_count": self.settings.fhir.page_count,
                "_include": "MedicationRequest:medication",
            },
            df_constraints={
                "subject": "condition_patient_reference",
            },
            with_ref=False,
            fhir_paths=[
                ("type", "MedicationRequest.resourceType"),
                ("id", "MedicationRequest.id"),
                ("patient_reference", "MedicationRequest.subject.reference"),
                (
                    "patient_mrn",
                    "MedicationRequest.subject.identifier.where("
                    + f"system='{self.settings.fhir.patient_identifier_system}').value",
                ),
                ("status", "MedicationRequest.status"),
                ("intent", "MedicationRequest.intent"),
                ("medication_reference", "MedicationRequest.medicationReference.reference"),
                ("datetime", "MedicationRequest.authoredOn"),
                ("dosage", "MedicationRequest.dosageInstruction"),
                ("medication_id", "Medication.id"),
                (
                    "medication_atc_code",
                    "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                ),
                (
                    "medication_atc_display",
                    "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').display",
                ),
                (
                    "medication_pzn_code",
                    "code.coding.where(system='http://fhir.de/CodeSystem/ifa/pzn').code",
                ),
                (
                    "medication_pzn_display",
                    "code.coding.where(system='http://fhir.de/CodeSystem/ifa/pzn').display",
                ),
                ("ingredient", "Medication.ingredient"),
            ],
        )
        med_req_df = med_df["MedicationRequest"]
        med_df_1 = med_df["Medication"]

        logger.info("all_med_reqs_df size: {}", med_req_df.count())

        # MedicationStatement
        med_statement = self.search.trade_rows_for_dataframe(
            df=patient_df,
            resource_type="MedicationStatement",
            request_params={
                "_count": self.settings.fhir.page_count,
                "_include": "MedicationStatement:medication",
            },
            df_constraints={
                "subject": "condition_patient_reference",
            },
            with_ref=False,
            fhir_paths=[
                ("type", "MedicationStatement.resourceType"),
                ("id", "MedicationStatement.id"),
                ("patient_reference", "MedicationStatement.subject.reference"),
                (
                    "patient_mrn",
                    "MedicationStatement.subject.identifier.where("
                    + f"system='{self.settings.fhir.patient_identifier_system}').value",
                ),
                ("status", "MedicationStatement.status"),
                ("datetime", "MedicationStatement.effectivePeriod.start"),
                ("period_end", "MedicationStatement.effectivePeriod.end"),
                ("medication_reference", "MedicationStatement.medicationReference.reference"),
                ("dosage", "MedicationStatement.dosage"),
                ("medication_id", "Medication.id"),
                (
                    "medication_atc_code",
                    "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                ),
                (
                    "medication_atc_display",
                    "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').display",
                ),
                (
                    "medication_pzn_code",
                    "code.coding.where(system='http://fhir.de/CodeSystem/ifa/pzn').code",
                ),
                (
                    "medication_pzn_display",
                    "code.coding.where(system='http://fhir.de/CodeSystem/ifa/pzn').display",
                ),
                ("ingredient", "Medication.ingredient"),
            ],
        )
        med_statement_df = med_statement["MedicationStatement"]
        med_df_2 = med_statement["Medication"]

        logger.info("all_med_statements_df size: {}", med_statement_df.count())

        # MedciationAdministration
        med_administration = self.search.trade_rows_for_dataframe(
            df=patient_df,
            resource_type="MedicationAdministration",
            request_params={
                "_count": self.settings.fhir.page_count,
                "_include": "MedicationAdministration:medication",
            },
            df_constraints={
                "subject": "condition_patient_reference",
            },
            with_ref=False,
            fhir_paths=[
                ("type", "MedicationAdministration.resourceType"),
                ("id", "MedicationAdministration.id"),
                ("patient_reference", "MedicationAdministration.subject.reference"),
                (
                    "patient_mrn",
                    "MedicationAdministration.subject.identifier.where("
                    + f"system='{self.settings.fhir.patient_identifier_system}').value",
                ),
                ("status", "MedicationAdministration.status"),
                ("datetime", "MedicationAdministration.effectiveDateTime"),
                ("medication_reference", "MedicationAdministration.medicationReference.reference"),
                ("dosage", "MedicationAdministration.dosage"),
                ("medication_id", "Medication.id"),
                (
                    "medication_atc_code",
                    "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                ),
                (
                    "medication_atc_display",
                    "code.coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').display",
                ),
                (
                    "medication_pzn_code",
                    "code.coding.where(system='http://fhir.de/CodeSystem/ifa/pzn').code",
                ),
                (
                    "medication_pzn_display",
                    "code.coding.where(system='http://fhir.de/CodeSystem/ifa/pzn').display",
                ),
                ("ingredient", "Medication.ingredient"),
            ],
        )

        med_administration_df = med_administration["MedicationAdministration"]
        med_df_3 = med_administration["Medication"]

        logger.info("all_med_administrations_df size: {}", med_administration_df.count())

        med_df = pd.concat([med_df_1, med_df_2, med_df_3])
        logger.info("all_meds_df: {}", med_df.count())
        med_df.drop_duplicates(subset=["medication_id"], inplace=True)
        logger.info("all_meds_df after removing duplicates: {}", med_df.count())
        req_stat_admin_df = pd.concat([med_req_df, med_statement_df, med_administration_df])
        med_df.to_csv(os.path.join(self.output_dir, "aml_all_meds.csv"), index=False)
        req_stat_admin_df.to_csv(
            os.path.join(self.output_dir, "aml_all_med_reqs_stats_admins.csv"), index=False
        )

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
                            "name": "effective_date_time",
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
