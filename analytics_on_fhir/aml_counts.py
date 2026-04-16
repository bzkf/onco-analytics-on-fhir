import os
from pathlib import Path

import pandas as pd
from loguru import logger
from settings import settings

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


def aml_summary_statistics():

    logger.info("Writing summary statistics for AML")

    output_dir = HERE / "results" / "aml" / "summary_statistics"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load datasets
    patients_with_diagnoses = pd.read_csv(
        HERE / "results" / "aml" / "aml_all_patients.csv",
        sep=",",
    )
    ecog_statuses = pd.read_csv(
        HERE / "results" / "aml" / "df_obds_ecog_statuses.csv",
        sep=";",
    )
    eln_klassifikation = pd.read_csv(
        HERE / "results" / "aml" / "df_obds_weitere_klassifikationen.csv",
        sep=";",
    )
    lab_values = pd.read_csv(
        HERE / "results" / "aml" / "aml_all_labs.csv",
        sep=",",
    )

    # Convert dates in patients_with_diagnoses
    patients_with_diagnoses["diagnosis_onsetDateTime"] = pd.to_datetime(
        patients_with_diagnoses["diagnosis_onsetDateTime"], utc=True
    )
    patients_with_diagnoses["deceased_dateTime"] = pd.to_datetime(
        patients_with_diagnoses["deceased_dateTime"], utc=True
    )
    patients_with_diagnoses["diagnosis_recordedDate"] = pd.to_datetime(
        patients_with_diagnoses["diagnosis_recordedDate"], utc=True
    )
    patients_with_diagnoses["birth_date"] = pd.to_datetime(
        patients_with_diagnoses["birth_date"], utc=True
    )

    # Calculate age at diagnosis
    patients_with_diagnoses["age_at_diagnosis"] = (
        patients_with_diagnoses["diagnosis_recordedDate"] - patients_with_diagnoses["birth_date"]
    ).dt.days / 365.25

    # Age groups
    bins = [0, 18, 30, 45, 60, 75, 90, 120]
    labels = ["0-18", "18-30", "30-45", "45-60", "60-75", "75-90", "90+"]
    patients_with_diagnoses["age_group"] = pd.cut(
        patients_with_diagnoses["age_at_diagnosis"], bins=bins, labels=labels
    )

    # ICD bins
    def assign_icd_bin(code):
        bin2_codes = ["C93.1", "C93.10", "C93.11"]
        bin3_codes = ["C92.3", "C92.30", "C92.31"]
        bin4_codes = ["C92.4", "C92.40", "C92.41"]
        if code in bin2_codes:
            return "C93.1"
        elif code in bin3_codes:
            return "C92.3"
        elif code in bin4_codes:
            return "C92.4"
        else:
            return "C92.0"

    patients_with_diagnoses["icd_bin"] = patients_with_diagnoses["icd_code"].apply(assign_icd_bin)

    # ECOG distribution
    ecog_statuses["effective_dateTime"] = pd.to_datetime(
        ecog_statuses["effective_dateTime"], utc=True, errors="coerce"
    )
    ecog_statuses = ecog_statuses.sort_values(
        by=["patient_mrn", "effective_dateTime"], ascending=[True, False]
    )
    ecog_distribution = (
        ecog_statuses.groupby("ecog_performance_status").size().reset_index(name="count")
    )
    ecog_distribution["percentage"] = (
        ecog_distribution["count"] / ecog_distribution["count"].sum()
    ) * 100
    ecog_distribution.to_csv(
        output_dir / (settings.location + "_ecog_distribution.csv"), index=False
    )

    # ELN distribution
    eln_klassifikation["effective_date_time"] = pd.to_datetime(
        eln_klassifikation["effective_date_time"], utc=True, errors="coerce"
    )
    eln_filtered = eln_klassifikation[
        eln_klassifikation["code_text"].str.startswith("ELN", na=False)
    ].copy()
    eln_filtered = eln_filtered.sort_values(
        by=["patient_mrn", "effective_date_time"], ascending=[True, False]
    )
    latest_eln = eln_filtered.drop_duplicates(subset="patient_mrn", keep="first")[
        ["patient_mrn", "value_code"]
    ]
    eln_distribution = latest_eln["value_code"].value_counts().reset_index()
    eln_distribution.columns = ["eln_classification", "count"]
    eln_distribution["percentage"] = (
        eln_distribution["count"] / eln_distribution["count"].sum()
    ) * 100
    eln_distribution.to_csv(output_dir / (settings.location + "_eln_distribution.csv"), index=False)

    # Summary Statistics to CSV
    age_group_distribution = patients_with_diagnoses["age_group"].value_counts().reset_index()
    age_group_distribution.columns = ["age_group", "count"]
    age_group_distribution["percentage"] = (
        age_group_distribution["count"] / age_group_distribution["count"].sum()
    ) * 100
    age_group_distribution.sort_values(by=["age_group"], inplace=True)
    age_group_distribution.to_csv(
        output_dir / (settings.location + "_age_group_distribution.csv"), index=False
    )

    gender_distribution = patients_with_diagnoses["gender"].value_counts().reset_index()
    gender_distribution.columns = ["gender", "count"]
    gender_distribution["percentage"] = (
        gender_distribution["count"] / gender_distribution["count"].sum()
    ) * 100
    gender_distribution.to_csv(
        output_dir / (settings.location + "_gender_distribution.csv"), index=False
    )

    icd_bin_distribution = patients_with_diagnoses["icd_bin"].value_counts().reset_index()
    icd_bin_distribution.columns = ["icd_bin", "count"]
    icd_bin_distribution["percentage"] = (
        icd_bin_distribution["count"] / icd_bin_distribution["count"].sum()
    ) * 100
    icd_bin_distribution.to_csv(
        output_dir / (settings.location + "_icd_bin_distribution.csv"), index=False
    )

    # Lab values
    result = pd.DataFrame(columns=["lab_value", "unit", "count", "mean"])
    lab_values["lab_dateTime"] = pd.to_datetime(
        lab_values["lab_dateTime"], utc=True, errors="coerce"
    )
    all_loinc_codes = []
    # Leukocytes
    loinc_codes_leukocytes = ["leukocytes", "26464-8", "6690-2", "806-0", "24122-4", "53964-3"]
    all_loinc_codes.append(loinc_codes_leukocytes)
    # Platelets
    loinc_codes_platelets = ["platelets", "777-3", "26515-7", "32623-1", "26515-7"]
    all_loinc_codes.append(loinc_codes_platelets)
    # Hemoglobin
    loinc_codes_hemoglobin = ["hemoglobin", "718-7", "20509-6", "30352-9", "30350-3"]
    all_loinc_codes.append(loinc_codes_hemoglobin)
    # Blasts
    loinc_codes_blasts = ["blasts", "709-6", "30376-8", "26446-5", "21114-4", "708-8", "44017-2"]
    all_loinc_codes.append(loinc_codes_blasts)
    # Macroblasts
    loinc_codes_macroblasts = ["macroblasts", "51629-4"]
    all_loinc_codes.append(loinc_codes_macroblasts)
    # Normoblasts
    loinc_codes_normoblasts = ["normoblasts", "33990-3", "715-3"]
    all_loinc_codes.append(loinc_codes_normoblasts)
    # Pronormoblasts
    loinc_codes_pronormoblasts = ["pronormoblasts", "26033-1"]
    all_loinc_codes.append(loinc_codes_pronormoblasts)
    # Myeloblasts
    loinc_codes_myeloblasts = ["myeloblasts", "11113-8"]
    all_loinc_codes.append(loinc_codes_myeloblasts)
    # Cells counted total in bone marrow
    loinc_codes_cells = ["cells in bone marrow", "38257-2"]
    all_loinc_codes.append(loinc_codes_cells)
    # Abnormal lymphocytes/Leukocytes in Blood
    loinc_codes_abnormal = [
        "abnormal lymphocytes/Leukocytes in blood",
        "30413-9",
        "735-1",
        "29261-5",
    ]
    all_loinc_codes.append(loinc_codes_abnormal)

    for lab_value in all_loinc_codes:
        lab_filtered = lab_values[lab_values["loinc_code"].isin(lab_value[1:])].copy()

        filtered_merged = pd.merge(
            patients_with_diagnoses,
            lab_filtered,
            left_on="condition_patient_reference",
            right_on="observation_patient_reference",
        )
        filtered_merged["time_diff"] = (
            filtered_merged["lab_dateTime"] - filtered_merged["diagnosis_recordedDate"]
        ).dt.days.abs()
        filtered_result = (
            filtered_merged.sort_values("time_diff").groupby("patient_mrn").first().reset_index()
        )
        filtered_result["lab_quantity_value"] = pd.to_numeric(
            filtered_result["lab_quantity_value"], errors="coerce"
        )
        unit_stats = (
            filtered_result.groupby("lab_quantity_unit")["lab_quantity_value"]
            .agg(["count", "mean"])
            .reset_index()
        )

        for _, row in unit_stats.iterrows():
            result.loc[len(result)] = [
                lab_value[0],
                row["lab_quantity_unit"],
                row["count"],
                row["mean"],
            ]

    result.to_csv(output_dir / (settings.location + "_lab_distribution.csv"), index=False)
