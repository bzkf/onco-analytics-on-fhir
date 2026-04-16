import os
from hashlib import sha256
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from loguru import logger
from settings import settings

HERE = Path.cwd()

patients_with_diagnoses = pd.read_csv(
    HERE / "results" / "aml" / "aml_all_patients.csv",
    sep=",",
    dtype={"lab_quantity_value": "float"},
)

patients_with_diagnoses.groupby("icd_code")

patients_with_diagnoses.info()

# TODO add aml_icd_code


df = patients_with_diagnoses

# Convert dates
df["diagnosis_onsetDateTime"] = pd.to_datetime(df["diagnosis_onsetDateTime"], utc=True)
df["deceased_dateTime"] = pd.to_datetime(df["deceased_dateTime"], utc=True)
df["diagnosis_recordedDate"] = pd.to_datetime(df["diagnosis_recordedDate"], utc=True)
df["birth_date"] = pd.to_datetime(df["birth_date"], utc=True)

# Age at diagnosis
df["age_at_diagnosis"] = (df["diagnosis_recordedDate"] - df["birth_date"]).dt.days / 365.25

# Age groups
bins = [0, 18, 30, 45, 60, 75, 90, 120]
labels = ["0-18", "18-30", "30-45", "45-60", "60-75", "75-90", "90+"]

df["age_group"] = pd.cut(df["age_at_diagnosis"], bins=bins, labels=labels)

patient_df = (
    df.groupby("patient_mrn")
    .agg(
        {
            "birth_date": "first",
            "gender": "first",
            "deceased": "max",
            "age_at_diagnosis": "mean",
        }
    )
    .reset_index()
)


n_patients = len(patient_df)

summary_rows = []

# Age
summary_rows.append(
    [
        "Age at diagnosis (years), mean (SD)",
        f"{patient_df['age_at_diagnosis'].mean():.1f} ({patient_df['age_at_diagnosis'].std():.1f})",
    ]
)

summary_rows.append(
    [
        "Age at diagnosis (years), median (IQR)",
        f"{patient_df['age_at_diagnosis'].median():.1f} "
        f"({patient_df['age_at_diagnosis'].quantile(0.25):.1f}-{patient_df['age_at_diagnosis'].quantile(0.75):.1f})",
    ]
)

# Gender
gender_counts = patient_df["gender"].value_counts()

for gender, count in gender_counts.items():
    summary_rows.append([f"Gender: {gender}", f"{count} ({count / n_patients * 100:.1f}%)"])

# Mortality
deceased_count = patient_df["deceased"].sum()
summary_rows.append(["Deceased", f"{deceased_count} ({deceased_count / n_patients * 100:.1f}%)"])

# Age groups
age_group_counts = df.groupby("patient_mrn")["age_group"].first().value_counts().sort_index()

for group, count in age_group_counts.items():
    summary_rows.append([f"Age group: {group}", f"{count} ({count / n_patients * 100:.1f}%)"])

# Create final table
table1 = pd.DataFrame(summary_rows, columns=["Characteristic", "Value"])

table1


# Group by icd_code and calculate per-ICD patient stats
icd_group_df = (
    df.groupby("icd_code")
    .agg(
        {
            "patient_mrn": "nunique",  # Number of unique patients per ICD code
            "age_at_diagnosis": "mean",  # Mean age at diagnosis
            "gender": lambda x: x.value_counts(normalize=True).to_dict(),  # Gender distribution
            "deceased": "mean",  # Mortality rate (mean of deceased column)
        }
    )
    .reset_index()
)

# Format gender distribution to make it readable
icd_group_df["gender_female"] = icd_group_df["gender"].apply(
    lambda x: x.get("female", 0) * 100 if isinstance(x, dict) else 0
)
icd_group_df["gender_male"] = icd_group_df["gender"].apply(
    lambda x: x.get("male", 0) * 100 if isinstance(x, dict) else 0
)

# Drop 'gender' column as it's now split into 'gender_female' and 'gender_male'
icd_group_df.drop(columns=["gender"], inplace=True)

# Calculate the number of deceased patients and the mortality rate
icd_group_df["deceased_count"] = icd_group_df["deceased"] * icd_group_df["patient_mrn"]
icd_group_df["deceased_rate"] = icd_group_df["deceased"] * 100  # Convert to percentage

# Create a clean summary table
icd_summary = icd_group_df[
    ["icd_code", "patient_mrn", "age_at_diagnosis", "gender_female", "gender_male", "deceased_rate"]
]

# Rename the columns for readability
icd_summary.columns = [
    "ICD Code",
    "Number of Patients",
    "Mean Age at Diagnosis (years)",
    "Female (%)",
    "Male (%)",
    "Mortality Rate (%)",
]

# Sort by number of patients for better readability (optional)
icd_summary = icd_summary.sort_values("Number of Patients", ascending=False).reset_index(drop=True)

# Display the table (or export it)
icd_summary
