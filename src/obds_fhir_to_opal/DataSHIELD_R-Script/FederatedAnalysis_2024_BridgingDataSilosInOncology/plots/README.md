# README

## Overview
This folder contains two Jupyter notebooks designed for visualizing the results of a federated analysis using DataSHIELD. The analysis focuses on the distribution of tumor entities across various hospitals in Bavaria, based on cases diagnosed within a one-year time span. The results of the DataSHIELD analysis are provided in a CSV file with the following structure:


| entity_ORDER | entity                         | Site1_total | Site1_male | Site1_female | Site2_total | Site2_male | Site2_female | Site3_total | Site3_male | Site3_female | Site4_total | Site4_male | Site4_female | Site5_total | Site5_male | Site5_female | Site6_total | Site6_male | Site6_female |
|--------------|--------------------------------|-------------|------------|--------------|-------------|------------|--------------|-------------|------------|--------------|-------------|------------|--------------|-------------|------------|--------------|-------------|------------|--------------|
| 0            | Lip, Oral Cavity, Pharynx (C00-C14) |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |
| 1            | Esophagus (C15)                |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |
| 2            | Stomach (C16)                  |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |
| 3            | Colon and Rectum (C18-C21)     |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |
| ...            | ...                    |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |
| 23           | Leukemia (C91-C95)             |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |             |            |              |


### Example Rows
- **Entity:** Represents the cancer type or tumor entity (e.g., Lip, Oral Cavity, Pharynx (C00-C14), Esophagus (C15)).
- **_total/male/female:** Columns represent the total number of cases, as well as the number of male and female cases, respectively, at each of the six sites.

## Notebooks Description

1. **Notebook 1: Tumor Entity Distribution**
   - **Purpose:** The notebook visualizes the relative numbers of cases diagnosed within a one-year period, broken down by site and tumor type.

2. **Notebook 2: Gender Distribution Among Tumor Entities**
   - **Purpose:** Visualizes the aggregated frequencies of cancer diagnoses for each entity group across all six locations and highlights relative distribution for female and male patients.

## How to Use
1. Enter the path to the CSV file with the DataSHIELD results in the notebook..
2. Open the notebooks in Jupyter and run the cells to generate the plots.

## Additional Information
- **Data Source:** The analysis is based on Real-World Data (RWD) collected from six university hospitals in Bavaria.
- **Data Format:** The CSV file includes 24 tumor entities along with corresponding case counts for each site, segmented by gender.
- **Python Version:** built with Python 3.11.9, for additional packages see requirements.txt
