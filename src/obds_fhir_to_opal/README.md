# obds_fhir_to_opal

`obds_fhir_to_opal` is a tool designed to convert FHIR data from Apache Kafka topics into a CSV file format which is then used in a Federated Analysis with DataSHIELD.

## Features

- **Convert FHIR resources from Kafka topics** into a structured CSV file.
- **Generate a data dictionary** for the Federated Analysis with DataSHIELD.
- **Data Extraction for specific studies** can be controlled via an environment variable `STUDY_NAME` in the compose.yaml file.
`STUDY_NAME:`
  - `poc`: Proof of Concept Data Extraction used in our publication https://preprints.jmir.org/preprint/65681
  - `study_protocol_a`: data extraction script for the analysis part A from our BZKF study protocol (latest amendment from 09.12.2024)
  - `study_protocol_c`: data extraction script for the analysis part C from our BZKF study protocol (latest amendment from 09.12.2024)
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
- `study_protocol_a`:
  - ...
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

  For a more detailed description of the data elements, please check the `data_dictionary_df.xlsx.` for each study.

## DataSHIELD R Scripts

In addition to the main tool, this repository contains a subfolder `DataSHIELD_R-Script`. This folder contains R scripts for a Federated Analysis querying the result CSV file with help of DataSHIELD.

Results from this analysis will be published soon, Preprint available here: https://preprints.jmir.org/preprint/65681

## Contributing

Contributions are welcome! If you have any suggestions or improvements, please open an issue or submit a pull request.
