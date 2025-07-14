# obds_fhir_to_opal

`obds_fhir_to_opal` is a tool designed to convert FHIR data from Apache Kafka topics into a CSV file format which is then used in a Federated Analysis with DataSHIELD.

## Features

- **Convert FHIR resources from Kafka topics** into a structured CSV file.
- **Generate a data dictionary** for the Federated Analysis with DataSHIELD.
- **Data Extraction for specific studies** can be controlled via an environment variable `STUDY_NAME` in the compose.yaml file.
`STUDY_NAME:`
  - `poc`: Proof of Concept Data Extraction used in our publication https://www.jmir.org/2025/1/e65681
  - `study_protocol_a0_1_3_7_d`: data extraction script for the analysis part A and D from our BZKF study protocol
  - `study_protocol_c`: data extraction script for the analysis part C from our BZKF study protocol
- **see sample output from extracting test patient data in folder**
`src/obds_fhir_to_opal/output/PoC` or `src/obds_fhir_to_opal/output/study_protocol_c`


#### File Structure of the data extraction results
- `poc`:
  - `ID`: Unique identifier for the record.
  - `condition_id`: Identifier for the medical condition.
  - `date_diagnosis`: Date when the diagnosis was made.
  - `date_diagnosis_year`: Year when the diagnosis was made.
  - `date_diagnosis_month`: Month when the diagnosis was made.
  - `date_diagnosis_day`: Day when the diagnosis was made.
  - `icd10_code`: ICD-10 code for the diagnosis.
  - `icd10_mapped`: Mapped ICD-10 code.
  - `icd10_grouped_entities`: Grouped ICD-10 entities.
  - `gender`: Gender of the patient.
  - `gender_mapped`: Mapped gender information.
- `study_protocol_a0_1_3_7_d`:
  - `condition_id`: Identifier for the medical condition.
  - `date_diagnosis`: Date when the diagnosis was made.
  - `icd10_code`: ICD-10 code for the diagnosis.
  - `cond_subject_reference`: Reference to the subject (condition level).
  - `patient_resource_id`: Resource id of the patient resource.
  - `patid_pseudonym`: Pseudonymized patient identifier.
  - `birthdate`: Patient's date of birth.
  - `gender`: Patient's gender.
  - `deceased_datetime`: Timestamp of death, if applicable.
  - `deceased_boolean`: Boolean flag indicating if the patient is deceased.
  - `subject_reference`: Subject reference (observation level).
  - `observation_id`: Identifier for the observation record.
  - `death_cause_icd10`: ICD-10 code of cause of death.
  - `death_cause_tumor`: Tumor-related death cause (text/code).
  - `date_death`: Date of death.
  - `obs_id_gleason`: Gleason observation identifier.
  - `gleason`: Gleason score.
  - `gleason_date_first`: Date of first recorded Gleason score.
  - `date_diagnosis_year`: Year of diagnosis.
  - `deceased_datetime_year`: Year of death timestamp.
  - `gleason_date_first_year`: Year of first Gleason score.
  - `date_death_year`: Year of death.
  - `icd10_mapped`: Mapped ICD-10 code.
  - `icd10_grouped`: Grouped ICD-10 code.
  - `icd10_entity`: ICD-10 entity classification.
  - `gender_mapped`: Mapped gender.
  - `age_at_diagnosis`: Age at diagnosis.
  - `age_group_small`: Fine-grained age group.
  - `age_group_large`: Broad age group.
  - `dead_bool_mapped`: Mapped deceased status (general).
  - `death_cause_tumor_mapped`: Mapped tumor-related death cause.
  - `death_cause_icd10_mapped`: Mapped cause of death ICD-10.
  - `death_cause_icd10_grouped`: Grouped ICD-10 cause of death.
  - `death_cause_entity`: Entity classification of death cause.
  - `death_cause_icd10_chapter`: ICD-10 chapter of cause of death.
  - `dead_bool_mapped_patient`: Deceased flag mapped (patient level).
  - `dead_bool_mapped_observation`: Deceased flag mapped (death observation level).
  - `double_patid`: `1` if the same `patid_pseudonym` occurs more than once; `0` otherwise.
- `study_protocol_c`:
  - `ID`: Unique identifier for the record.
  - `condition_id`: Identifier for the medical condition.
  - `icd10_code`: ICD-10 code for the diagnosis.
  - `icd10_mapped`: Mapped ICD-10 code.
  - `icd10_grouped`: ICD-10 code grouped to parent code.
  - `icd10_entity`: Grouped ICD-10 entities.
  - `date_diagnosis`: Date when the diagnosis was made.
  - `date_diagnosis_year`: Year when the diagnosis was made.
  - `birthdate`: Birthdate of the patient.
  - `age_at_diagnosis`: Age of patient at diagnosis date.
  - `age_group_small`: Age group small.
  - `age_group_large`: Age group large.
  - `gender`: Gender of the patient.
  - `gender_mapped`: Mapped gender information.
  - `postal_code`: Postal Code.
  - `country`: patient adress country.


  For a more detailed description of the data elements, please check the `data_dictionary_df.xlsx.` for each study.

## DataSHIELD R Scripts

In addition to the main tool, this repository contains a subfolder `DataSHIELD_R-Script`. This folder contains R scripts for a Federated Analysis querying the result CSV file with help of DataSHIELD.

Results from this analysis will be published soon, Preprint available here: https://preprints.jmir.org/preprint/65681

## Contributing

Contributions are welcome! If you have any suggestions or improvements, please open an issue or submit a pull request.
