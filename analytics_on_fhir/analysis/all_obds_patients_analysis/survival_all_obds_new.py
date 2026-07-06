import sys
from pathlib import Path

import numpy as np
import pandas as pd
from IPython.display import display

from analytics_on_fhir.analysis.utils.load_multisite_parquets import (
    build_all_obds_paths,
    load_multisite_parquets_from_paths,
)
from analytics_on_fhir.analysis.utils.survival_utils import (
    extend_followup_times,
    finalize_km_cohort,
    km_lifeline,
    load_and_clean_base_data,
    median_iqr,
    plot_survival_cohort,
    process_survival_events,
)

# 1. KONFIGURATION
report = {}
sites = "UKER, TUM, UKA, LMU, UKR, UKW"
ASSERTED_MIN, ASSERTED_MAX = 1970, 2026
EXTEND_FOLLOWUP = True

BASE_DIR = Path(
    "/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/all_obds_patients_analysis/all_obds_data_allsites"
)

# Top 20 Entitäten-Liste
target_entities = [
    "C61",
    "C50",
    "C43",
    "C18-C21",
    "C33-C34",
    "C00-C14",
    "C82-C88",
    "C25",
    "C70-C72",
    "C73",
    "C64",
    "C91-C95",
    "C67",
    "C22",
    "C16",
    "C90",
    "C49",
    "C15",
    "C32",
    "C54-C55",
]

data_config = {
    "obds": {"file": "df_all_obds_clean_deidentified.parquet", "subdir": "parquet"},
    "vitalstatus": {
        "file": "df_vitalstatus_deidentified.parquet",
        "subdir": "parquet",
        "date_col": "months_between_asserted_effective_dateTime",
        "output_col": "vitalstatus_months",
    },
    "tnm_m": {
        "file": "df_m_tnm_deidentified.parquet",
        "subdir": "parquet",
        "date_col": "months_between_asserted_m_tnm_date",
        "output_col": "tnm_m_months",
    },
}

# 2. LADEN & BASIS-FILTERUNG
paths = build_all_obds_paths(base_dir=BASE_DIR, config=data_config, sites=sites)
dfs = {key: load_multisite_parquets_from_paths(paths[key]) for key in data_config.keys()}

# Spezifische All-OBDS Kohorten-Filter
df_obds = dfs["obds"]
df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
df_obds = df_obds[df_obds["age_at_diagnosis"] >= 18]

df_obds_clean, df_obds_first_tumor = load_and_clean_base_data(
    df_obds=df_obds,
    asserted_min=ASSERTED_MIN,
    asserted_max=ASSERTED_MAX,
    target_entities=target_entities,
)

report[f"Initial records (diagnosis year ≥ {ASSERTED_MIN})"] = df_obds_clean.shape[0]
report["Patients after first-tumor selection"] = df_obds_first_tumor.shape[0]

# 3. VITALSTATUS & JOIN
df_vital_latest, df_join = process_survival_events(
    df_first_tumor=df_obds_first_tumor, df_vital=dfs["vitalstatus"]
)

report["Vital status records"] = dfs["vitalstatus"].shape[0]
report["Latest vital status records"] = df_vital_latest.shape[0]
report["Patients after join with follow-up data"] = df_join.shape[0]

# 4. FOLLOW-UP EXTENSION (All-OBDS Modus)
if EXTEND_FOLLOWUP:
    ext_config = {
        "progressions": {
            "file": "df_progressions_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_effective_dateTime",
            "output_col": "progression_months",
        },
        "tnm_t": {
            "file": "df_t_tnm_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_t_tnm_date",
            "output_col": "tnm_t_months",
        },
        "tnm_n": {
            "file": "df_n_tnm_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_n_tnm_date",
            "output_col": "tnm_n_months",
        },
        "tnm_uicc": {
            "file": "df_uicc_tnm_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_uicc_tnm_date",
            "output_col": "tnm_uicc_months",
        },
        "ops_grouped": {
            "file": "df_ops_grouped_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_therapy_start_date",
            "output_col": "op_months",
        },
        "radiotherapies": {
            "file": "df_radiotherapies_joined_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_therapy_start_date",
            "output_col": "rt_months",
        },
        "systemtherapies": {
            "file": "df_system_therapies_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_therapy_start_date",
            "output_col": "st_months",
        },
        "metastasis": {
            "file": "df_metastasis_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_metastasis_date",
            "output_col": "metastasis_months",
        },
        "weitere_klassifikation": {
            "file": "df_weitere_klassifikation_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_weitere_klassifikation_date",
            "output_col": "weitere_klassifikation_months",
        },
    }

    paths_ext = build_all_obds_paths(base_dir=BASE_DIR, config=ext_config, sites=sites)

    dfs_ext = {key: load_multisite_parquets_from_paths(paths_ext[key]) for key in ext_config}

    df_join, ext_records_count = extend_followup_times(
        df_join=df_join,
        extension_dfs=dfs_ext,
        extension_config=ext_config,
        df_tnm_m=dfs["tnm_m"],
    )

    report[
        "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)"
    ] = ext_records_count


# 5. FINALE KAPLAN-MEIER KOHORTE FILTERN
df_km, exclusion_counts = finalize_km_cohort(df_join)
report.update(exclusion_counts)

report["Deceased patients (event=1)"] = df_km["event"].sum()
report["Censored patients (event=0) with follow-up information"] = (df_km["event"] == 0).sum()

# 6. SUMMARY & FLOWCHART
summary = pd.DataFrame.from_dict(report, orient="index", columns=["count"])
summary["pct"] = summary["count"] / report["Patients after join with follow-up data"] * 100
print(summary)

LEFT_STEPS = [
    f"Initial records from 20 most prevalent tumor entities \n(diagnosis year ≥ {ASSERTED_MIN})",
    "Patients after first-tumor selection",
]
RIGHT_STEPS = [
    "Latest vital status records",
    "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)",
]
CENTER_STEPS = [
    "Patients after join with follow-up data",
    "Excluded: unknown event status",
    "Excluded: missing survival time",
    "Excluded: survival time >100 years",
    "Excluded: negative survival time",
    "Excluded: discordant mortality records (vital_status code = L after is_deceased=True)",
    "Kaplan-Meier eligible cohort",
]

plot_survival_cohort(
    summary,
    title=f"Top-20 Survival Cohort: {sites} - diagnoses ≥ {ASSERTED_MIN}",
    left_steps=LEFT_STEPS,
    right_steps=RIGHT_STEPS,
    center_steps=CENTER_STEPS,
    cohort_control=False,
)

# 7. FOLLOW-UP STATISTIKEN (MEDIAN IQR)
followup_stats = {
    "all_patients": median_iqr(df_km["survival_time_months"]),
    "censored": median_iqr(df_km.loc[df_km["event"] == 0, "followup_time"]),
    "deceased": median_iqr(df_km.loc[df_km["event"] == 1, "death_time"]),
}

followup_table = pd.DataFrame(
    [
        {
            "group": name,
            "n": s["n"],
            "median_months": round(s["median"], 1),
            "median (IQR)": (f"{s['median']:.1f} ({s['q1']:.1f}–{s['q3']:.1f})"),
        }
        for name, s in followup_stats.items()
    ]
)

followup_table["group"] = pd.Categorical(
    followup_table["group"],
    categories=["all_patients", "censored", "deceased"],
    ordered=True,
)
followup_table = followup_table.sort_values("group")
print("\n--- Follow-up Statistics ---")
display(followup_table)


# 8. KAPLAN-MEIER PLOTS
# Gender 10y lim
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="gender",
    cat_names=[("male", "Male"), ("female", "Female")],
    title=f"10-year Survival by Gender, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=True,
    xlim=(0, 121),
)
# Gender no lim
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="gender",
    cat_names=[("male", "Male"), ("female", "Female")],
    title=f"Overall Survival by Gender, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=True,
)

# Top 1-10 Entitäten Plot
top10_entities = df_km["entity_or_parent"].value_counts().head(10).index.tolist()
cat_names_1_10 = [(entity, entity) for entity in top10_entities]
# 10y lim
km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top10_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_1_10,
    title=f"10-year Survival by Top 1-10 Tumor Entities, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
    xlim=(0, 121),
)
# no lim
km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top10_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_1_10,
    title=f"Overall Survival by Top 1-10 Tumor Entities, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
)


# Top 11-20 Entitäten Plot
top11_20_entities = df_km["entity_or_parent"].value_counts().iloc[10:20].index.tolist()
cat_names_11_20 = [(entity, entity) for entity in top11_20_entities]

km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top11_20_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_11_20,
    title=f"10-year Survival by Top 11-20 Tumor Entities, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
    xlim=(0, 121),
)
# no lim
km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top11_20_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_11_20,
    title=f"Overall Survival by Top 11-20 Tumor Entities, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
)


# 9. SURVIVAL NACH METASTASIERUNG (TNM-M)
df_tnm_m = dfs["tnm_m"].copy()
df_tnm_m["m_tnm_clean"] = df_tnm_m["m_tnm"].astype("string").str.strip()
df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] >= ASSERTED_MIN]

# MX rausfiltern
target_m_stages = sorted(
    df_tnm_m.loc[
        df_tnm_m["m_tnm_clean"].notna() & (df_tnm_m["m_tnm_clean"] != "MX"), "m_tnm_clean"
    ].unique()
)
df_tnm_m = df_tnm_m[df_tnm_m["m_tnm_clean"].isin(target_m_stages)].copy()

# EIN M-stage pro Patient (0–3 Monate nach Diagnose)
df_tnm_unique = (
    df_tnm_m[
        (df_tnm_m["months_between_asserted_m_tnm_date"] >= 0)
        & (df_tnm_m["months_between_asserted_m_tnm_date"] <= 3)
    ]
    .sort_values("m_tnm_clean", ascending=False)
    .groupby("condition_id_hash")
    .first()
    .reset_index()
)

# Inner Join mit KM-Daten - fehlende M Stages sind hier also nicht dabei!
df_km_tnm = df_km.merge(
    df_tnm_unique[["condition_id_hash", "m_tnm_clean"]], on="condition_id_hash", how="inner"
)

# Group TNM M values to 3 digits (M0, M1)
df_km_tnm["m_tnm_grouped"] = df_km_tnm["m_tnm_clean"].str[:3]
priority = ["M0", "M1", "M1a", "M1b", "M1c", "M1d"]
available = df_km_tnm["m_tnm_grouped"].dropna().unique()
cat_names_m = [(x, x) for x in priority if x in available]

km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_tnm_grouped",
    cat_names=cat_names_m,
    title=f"10y by Metastasis Stage (TNM-M) up to 3 months, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=True,
    xlim=(0, 121),
)

km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_tnm_grouped",
    cat_names=cat_names_m,
    title=f"OS by Metastasis Stage (TNM-M) up to 3 months, diagnoses ≥ {ASSERTED_MIN}, {sites}",
    stat=True,
    show_stat=True,
)
