# obds_fhir_to_opal

`obds_fhir_to_opal` is a tool designed to convert FHIR data from Apache Kafka topics into a CSV file format which is then used in a Federated Analysis with DataSHIELD.

## Features

- **Convert FHIR resources from Kafka topics** into a structured CSV file.
- **Generate a data dictionary** for the Federated Analysis with DataSHIELD.
- **Result CSV File Columns**:
  - `ID`: Unique identifier for the record.
  - `condition_id`: Identifier for the medical condition.
  - `date_diagnosis`: Date when the diagnosis was made.
  - `icd10_code`: ICD-10 code for the diagnosis.
  - `gender`: Gender of the patient.
  - `date_diagnosis_year`: Year when the diagnosis was made.
  - `date_diagnosis_month`: Month when the diagnosis was made.
  - `date_diagnosis_day`: Day when the diagnosis was made.
  - `icd10_mapped`: Mapped ICD-10 code.
  - `icd10_grouped_entities`: Grouped ICD-10 entities.
  - `gender_mapped`: Mapped gender information.

  For a more detailed description of the data elements, please check the data_dictionary_df.xlsx.

## DataSHIELD R Scripts

In addition to the main tool, this repository contains a subfolder `DataSHIELD_R-Script`. This folder includes R scripts for a Federated Analysis querying the result CSV file with help of DataSHIELD.

Results from this analysis will be published soon. Check back for updates.

## Contributing

Contributions are welcome! If you have any suggestions or improvements, please open an issue or submit a pull request.
