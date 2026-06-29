import sys
from functools import reduce
from pathlib import Path

import numpy as np
import pandas as pd
from analytics_on_fhir.analysis.utils.load_multisite_parquets import (
    build_all_obds_paths, load_multisite_parquets,
    load_multisite_parquets_from_paths)
from analytics_on_fhir.analysis.utils.survival_utils import (
    km_lifeline, latest_per_condition, median_iqr, plot_survival_cohort_pca)
from IPython.display import display

report = {}
# read in all data from pca1
BASE_DIR = Path(
    "/home/coder/git/onco-analytics-on-fhir/"
    "analytics_on_fhir/analysis/study_name_pca_analysis/"
    "pca1_data_allsites"
)
# for progressions - i dont have them in pca
BASE_DIR_ALL_OBDS = Path(
    "/home/coder/git/onco-analytics-on-fhir/"
    "analytics_on_fhir/analysis/all_obds_patients_analysis/"
    "all_obds_data_allsites"
)
sites = "UKER, TUM, UKA, LMU, UKW"
# sites = "UKER"
# sites = "TUM"
# sites = "UKA"
# sites = "LMU"
# sites = "UKW"
# sites = "UKR" --> filter from all obds if u wanna use this

asserted_year_min = 2010
asserted_year_max = 2023

# COHORT or control for labs KM and Metastasis M KM
cohort_or_control = "cohort"

# augment data with verlaufsinfo - any record of the patient from progression and other obds
extend_df_km_by_progression = True

pca1_config = {
    "obds": {
        "file": "df_c61_conditions_patients_death_gleason_met_clean_deidentified.parquet",
        "subdir": "parquet",
    },
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
    "labs": {
        "file": "df_mii_labs_asserted_deidentified.parquet",
        "subdir": "parquet",
        "date_col": "months_between_asserted_effective_dateTime",
        "output_col": "lab_months",
    },
    # add other dfs here for extend follow up as well
}

paths = build_all_obds_paths(base_dir=BASE_DIR, config=pca1_config, sites=sites)
# load prograssions here seperately from BASE_DIR_ALL_OBDS

dfs = {key: load_multisite_parquets_from_paths(paths[key]) for key in pca1_config.keys()}
df_obds = dfs["obds"]

df_obds.head()

# remove C44 and all D* ICD10 codes
# df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
df_obds = df_obds[df_obds["asserted_year"] >= asserted_year_min]
df_obds = df_obds[df_obds["asserted_year"] <= asserted_year_max]


df_obds.head()
print(df_obds.shape[0])
# drop all metastasis cols as they duplicate the counts
# then drop duplicates
df_obds = df_obds.loc[:, ~df_obds.columns.str.contains("metastasis", case=False, na=False)]
df_obds = df_obds.drop_duplicates()
print(df_obds.shape[0])
df_obds.head()
report[f"Initial tumor records (diagnosis year {asserted_year_min} - {asserted_year_max})"] = (
    df_obds.shape[0]
)

n_with = df_obds["gleason_score"].notna().sum()
n_without = df_obds["gleason_score"].isna().sum()

print(f"With Gleason score: {n_with:,}")
print(f"Without Gleason score: {n_without:,}")

# in df_obds, groupby patient_resource_id_hash and only keep first tumor
# with smallest age_at_diagnosis
# wichtig für unabhängige Beobachtung Kaplan Meier --> nur für Erstdiagnose hier
df_obds_first_tumor = (
    df_obds.sort_values("age_at_diagnosis")
    .groupby("patient_resource_id_hash")
    .first()
    .reset_index()
)

report["Patients after first-tumor selection"] = df_obds_first_tumor.shape[0]
report["Unique patients after first-tumor selection"] = df_obds_first_tumor[
    "patient_resource_id_hash"
].nunique()
report

df_vital = dfs["vitalstatus"]

df_vital.head()
report["Vital status records"] = df_vital.shape[0]


df_lab = dfs["labs"]

df_lab.head()
df_lab.shape[0]
mask = (
    df_lab["loinc_code"].notna()
    & (df_lab["loinc_code"].astype(str).str.strip() != "")
    & (df_lab["loinc_code"].astype(str).str.lower() != "missing")
)

n_ops_present = mask.sum()

print(f"Ops code present: {n_ops_present:,}")
# 2010 - 2023
df_lab = df_lab[df_lab["asserted_year"] >= asserted_year_min]
df_lab = df_lab[df_lab["asserted_year"] <= asserted_year_max]
df_lab.shape[0]


print(df_vital["months_between_asserted_effective_dateTime"].dtype)
idx_raw = df_vital.groupby("patient_resource_id_hash")[
    "months_between_asserted_effective_dateTime"
].idxmax()

bad = idx_raw[idx_raw.isna()]

print(bad)
print("Anzahl problematischer Gruppen:", len(bad))

print(df_vital.index.hasnans)
print(df_vital.index[df_vital.index.isna()])
df_vital = df_vital.reset_index(drop=True)
print(df_vital["months_between_asserted_effective_dateTime"].describe())

# only the latest vitalstatus entry per patient
# based on the largest follow-up time
idx = (
    df_vital.groupby("patient_resource_id_hash")["months_between_asserted_effective_dateTime"]
    .idxmax()
    .dropna()
    .astype(int)
)

df_vital_latest = (
    df_vital.loc[idx]
    .reset_index(drop=True)
    .sort_values("months_between_asserted_effective_dateTime")
)

df_vital_latest.head(50)
df_vital_latest["vitalstatus_code"].isna().sum()
df_vital_latest["vitalstatus_code"].value_counts()

# ich will nur die patient_resource_id_hash hier behalten, dei auch in meinem df_obds nach all den
# filterungen sind
valid_patients = set(df_obds_first_tumor["patient_resource_id_hash"])

df_vital_latest = df_vital_latest[
    df_vital_latest["patient_resource_id_hash"].isin(valid_patients)
].copy()

report["Latest vital status records"] = df_vital_latest.shape[0]


# remove negative months_between_asserted_effective_dateTime
df_vital_latest = df_vital_latest[
    df_vital_latest["months_between_asserted_effective_dateTime"] >= 0
].copy()

df_vital_latest.head(50)
report["Patients with non-negative follow-up time"] = df_vital_latest.shape[0]
report["Unique patients with valid vital status information"] = df_vital_latest[
    "patient_resource_id_hash"
].nunique()


# JOIN FIRST TUMOR DF WITH LATEST VITALSTATUS
df_join = df_obds_first_tumor.merge(
    df_vital_latest, on="patient_resource_id_hash", how="left", suffixes=("", "_vital")
)
report["Patients after join with follow-up data"] = df_join.shape[0]
report["C61 patients"] = (df_join["icd10_code"] == "C61").sum()
df_join.to_csv("dj_join.csv", index=False)

# COMPARE is_deceased vs vitalstatus_code
# Absolute counts
counts = pd.crosstab(
    df_join["is_deceased"],
    df_join["vitalstatus_code"],
    dropna=False,
)

# Zeilenprozente
row_pct = (
    pd.crosstab(
        df_join["is_deceased"],
        df_join["vitalstatus_code"],
        normalize="index",
        dropna=False,
    )
    * 100
).round(1)

# Spaltenprozente
col_pct = (
    pd.crosstab(
        df_join["is_deceased"],
        df_join["vitalstatus_code"],
        normalize="columns",
        dropna=False,
    )
    * 100
).round(1)

# Long format
counts_long = counts.reset_index().melt(
    id_vars="is_deceased",
    var_name="vitalstatus_code",
    value_name="count",
)

row_pct_long = row_pct.reset_index().melt(
    id_vars="is_deceased",
    var_name="vitalstatus_code",
    value_name="row_percent",
)

col_pct_long = col_pct.reset_index().melt(
    id_vars="is_deceased",
    var_name="vitalstatus_code",
    value_name="column_percent",
)

# Alles zusammenführen
comparison_report = counts_long.merge(
    row_pct_long,
    on=["is_deceased", "vitalstatus_code"],
    how="left",
).merge(
    col_pct_long,
    on=["is_deceased", "vitalstatus_code"],
    how="left",
)

comparison_report = comparison_report.sort_values(["is_deceased", "vitalstatus_code"])

display(comparison_report)

# event = 1 = dead; 0 = censored
event = pd.Series(pd.NA, index=df_join.index, dtype="boolean")

# 1 sicher Tod
event.loc[df_join["is_deceased"].eq(True) | df_join["vitalstatus_code"].eq("T")] = True

# 0 sicher zensiert (explizit lebend) - rest na
event.loc[
    (
        df_join["vitalstatus_code"].eq("L")
        | (df_join["is_deceased"].eq(False) & df_join["vitalstatus_code"].notna())
    )
    & event.isna()
] = False

df_join["event"] = event


# kombiniere beide datumsspalten aus patient und tod observation
df_join["death_time"] = df_join["months_between_asserted_deceased_datetime"].combine_first(
    df_join["months_between_asserted_date_death"]
)

# follow up time from vitalstatus
df_join["followup_time"] = df_join["months_between_asserted_effective_dateTime"]

df_join["is_deceased"].isna().mean()
df_join["is_deceased"].isna().sum()
df_join["vitalstatus_code"].isna().mean()
df_join["vitalstatus_code"].isna().sum()


# tnm m
df_tnm_m = dfs["tnm_m"]
df_tnm_m["m_tnm_clean"] = df_tnm_m["m_tnm"].astype("string").str.strip()
df_tnm_m["m_tnm_clean"].value_counts()
df_tnm_m.head()

df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] >= asserted_year_min]
df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] <= asserted_year_max]

df_tnm_m.head()
df_tnm_m.shape
df_tnm_m["condition_id_hash"].nunique()  # 24066


# # extend df_join if extend_df_km_by_progression is set to True in the beginning of the script
# # extend it with any other most up to date and available information of the patient for censoring
# # for improving completeness of follow up time
#     # read in progressions from all_obds and filter for icd10_code == C61

# TO DO - das nach oben ziehen
if extend_df_km_by_progression:
    print("extend_df_km_by_progression")
    obds_extension_config = {
        "progressions": {
            "file": "df_progressions_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_effective_dateTime",
            "output_col": "progression_months",
        }
    }

    pca_extension_config = {
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
        # df_tnm_m schon vorhanden
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

    paths_obds = build_all_obds_paths(
        base_dir=BASE_DIR_ALL_OBDS, config=obds_extension_config, sites=sites
    )
    dfs_obds = {
        key: load_multisite_parquets_from_paths(paths_obds[key]) for key in obds_extension_config
    }
    df_progression = dfs_obds["progressions"]
    df_progression = df_progression[df_progression["icd10_code"] == "C61"]

    paths_pca = build_all_obds_paths(base_dir=BASE_DIR, config=pca_extension_config, sites=sites)
    dfs_pca = {
        key: load_multisite_parquets_from_paths(paths_pca[key]) for key in pca_extension_config
    }

    latest_dfs = {}

    for key, cfg in pca_extension_config.items():
        latest_dfs[key] = latest_per_condition(
            df=dfs_pca[key],
            date_col=cfg["date_col"],
            output_col=cfg["output_col"],
        )

    # add progressions
    latest_dfs["progressions"] = latest_per_condition(
        df=df_progression,
        date_col=obds_extension_config["progressions"]["date_col"],
        output_col=obds_extension_config["progressions"]["output_col"],
    )
    # add tnm m already existing
    latest_dfs["tnm_m"] = latest_per_condition(
        df_tnm_m,
        "months_between_asserted_m_tnm_date",
        "tnm_m_months",
    )

    # save latest record numbers
    report[
        "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)"
    ] = sum(df.shape[0] for df in latest_dfs.values())

    df_all = reduce(
        lambda left, right: left.merge(
            right,
            on="condition_id_hash",
            how="outer",
        ),
        latest_dfs.values(),
    )

    # automatisch aus config sammeln
    followup_cols = (
        [cfg["output_col"] for cfg in obds_extension_config.values()]
        + [cfg["output_col"] for cfg in pca_extension_config.values()]
        + ["tnm_m_months"]
    )

    df_all["months_followup_extended"] = df_all[followup_cols].max(axis=1)
    df_all.head()

    df_join = df_join.merge(
        df_all[["condition_id_hash", "months_followup_extended"]],
        on="condition_id_hash",
        how="left",
    )
    # fill missing followup_time directly
    df_join["followup_time"] = df_join["followup_time"].combine_first(
        df_join["months_followup_extended"]
    )
    # update event information from progressions
    df_join.loc[df_join["event"].isna() & df_join["followup_time"].notna(), "event"] = False

# find final df_km cohort
# check unknown events
df_join.head()
unknown_event_mask = df_join["event"].isna()
# year distribution of excluded patients
unknown_year_dist = (
    df_join.loc[unknown_event_mask, "asserted_year"].value_counts(dropna=False).sort_index()
)

# Baseline Characteristics der ausgeschlossenen Patienten ermitteln
df_excluded = df_join.loc[unknown_event_mask].copy()

print(f"=== BASELINE CHARACTERISTICS OF EXCLUDED PATIENTS (N = {df_excluded.shape[0]:,}) ===")

# 1. Verteilung nach Diagnosejahr
print("\n--- Distribution by Diagnosis Year ---")
unknown_year_dist = df_excluded["asserted_year"].value_counts(dropna=False).sort_index()
print(unknown_year_dist)

# 2. Verteilung nach Standorten (Sites)
if "site" in df_excluded.columns:
    print("\n--- Distribution by Site ---")
    site_dist = df_excluded["site"].value_counts(dropna=False)
    print(site_dist)

# 3. Alter bei Diagnose (Metriken)
if "age_at_diagnosis" in df_excluded.columns:
    print("\n--- Age at Diagnosis ---")
    print(df_excluded["age_at_diagnosis"].describe().round(1))

# 4. Gleason-Score Verteilung (Klinische Eigenschaft)
if "gleason_score" in df_excluded.columns:
    print("\n--- Gleason Score Distribution ---")
    gleason_dist = df_excluded["gleason_score"].value_counts(dropna=False).sort_index()
    print(gleason_dist)

print("\n=======================================================")

# remove NA events
report["Excluded: unknown event status"] = unknown_event_mask.sum()
df_join = df_join.loc[~unknown_event_mask].copy()
df_join["event"] = df_join["event"].astype(int)


# combine death and followup time for final survival time
df_join["survival_time_months"] = np.where(
    df_join["event"] == 1, df_join["death_time"], df_join["followup_time"]
)

# remove implausible survival times (>100 years)
outlier_mask = df_join["survival_time_months"] > 1200
report["Excluded: survival time >100 years"] = outlier_mask.sum()
df_join = df_join.loc[~outlier_mask].copy()

# remove missing survival times
missing_mask = df_join["survival_time_months"].isna()
report["Excluded: missing survival time"] = missing_mask.sum()

df_km = df_join.loc[~missing_mask].copy()

df_km["survival_time_months"].describe()
df_km.sort_values("survival_time_months").tail(20)

# df_km = df_km[~df_km["icd10_code"].str.startswith(("C44", "D"), na=False)]
# report["df_km_removed_C44_D*_again"] = df_km.shape[0]

# Identify discordant death-status records where the two mortality sources disagree.
#   is_deceased = True but vitalstatus_code != "T"
#   -> patient is marked as deceased in obds, but not deceased in vital status.

discordant_mask = (
    (df_km["is_deceased"])
    & (df_km["vitalstatus_code"] == "L")
    & (df_km["death_time"] < df_km["followup_time"])
)

discordant = df_km.loc[discordant_mask].copy()

report["Excluded: discordant mortality records (vital_status code = L after is_deceased=True)"] = (
    len(discordant)
)

print("Discordant cases:", len(discordant))

# remove discordant patients from KM cohort
df_km = df_km.loc[~discordant_mask].copy()
report["Kaplan-Meier eligible cohort"] = df_km.shape[0]

# deceased patients event = 1
report["Deceased patients (event=1)"] = df_km["event"].sum()
report["Deceased patients: cohort"] = ((df_km["event"] == 1) & (df_km["cohort_flag"] == 1)).sum()
report["Deceased patients: control"] = ((df_km["event"] == 1) & (df_km["cohort_flag"] == 0)).sum()


# censored patients event=0
report["Censored patients (event=0) with follow-up information"] = (df_km["event"] == 0).sum()
report["Censored patients: cohort"] = ((df_km["event"] == 0) & (df_km["cohort_flag"] == 1)).sum()
report["Censored patients: control"] = ((df_km["event"] == 0) & (df_km["cohort_flag"] == 0)).sum()

# report
summary = pd.DataFrame.from_dict(report, orient="index", columns=["count"])
summary[
    f"% of initial tumor records (diagnosis year {asserted_year_min} - {asserted_year_max})"
] = (
    summary["count"]
    / report[f"Initial tumor records (diagnosis year {asserted_year_min} - {asserted_year_max})"]
    * 100
)
summary["% of first-tumor cohort"] = np.nan

first_tumor_idx = summary.index.get_loc("Patients after first-tumor selection")

summary.iloc[first_tumor_idx:, summary.columns.get_loc("% of first-tumor cohort")] = (
    summary.iloc[first_tumor_idx:]["count"] / report["Patients after first-tumor selection"] * 100
)
summary.index.name = f"Cohort selection flow ({sites})"

summary

display(
    summary.style.set_properties(**{"text-align": "left"}).set_table_styles(
        [{"selector": "th.row_heading", "props": [("min-width", "300px")]}]
    )
)


# calculate follow up statistics
followup_stats = {}

# ----------------------------------
# alle Patienten
# ----------------------------------
followup_stats["all_patients"] = median_iqr(df_km["survival_time_months"])

# ----------------------------------
# zensierte Patienten
# ----------------------------------
followup_stats["censored"] = median_iqr(df_km.loc[df_km["event"] == 0, "followup_time"])

# ----------------------------------
# verstorbene Patienten
# ----------------------------------
followup_stats["deceased"] = median_iqr(df_km.loc[df_km["event"] == 1, "death_time"])

# ----------------------------------
# cohort
# ----------------------------------
followup_stats["cohort"] = median_iqr(
    df_km.loc[
        df_km["cohort_flag"] == 1,
        "survival_time_months",
    ]
)

# ----------------------------------
# control
# ----------------------------------
followup_stats["control"] = median_iqr(
    df_km.loc[
        df_km["cohort_flag"] == 0,
        "survival_time_months",
    ]
)

# ----------------------------------
# deceased cohort
# ----------------------------------
followup_stats["deceased_cohort"] = median_iqr(
    df_km.loc[
        (df_km["event"] == 1) & (df_km["cohort_flag"] == 1),
        "death_time",
    ]
)

# ----------------------------------
# deceased control
# ----------------------------------
followup_stats["deceased_control"] = median_iqr(
    df_km.loc[
        (df_km["event"] == 1) & (df_km["cohort_flag"] == 0),
        "death_time",
    ]
)

# ----------------------------------
# censored cohort
# ----------------------------------
followup_stats["censored_cohort"] = median_iqr(
    df_km.loc[
        (df_km["event"] == 0) & (df_km["cohort_flag"] == 1),
        "followup_time",
    ]
)

# ----------------------------------
# censored control
# ----------------------------------
followup_stats["censored_control"] = median_iqr(
    df_km.loc[
        (df_km["event"] == 0) & (df_km["cohort_flag"] == 0),
        "followup_time",
    ]
)

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

order = [
    "all_patients",
    "cohort",
    "control",
    "censored",
    "censored_cohort",
    "censored_control",
    "deceased",
    "deceased_cohort",
    "deceased_control",
]

followup_table["group"] = pd.Categorical(
    followup_table["group"],
    categories=order,
    ordered=True,
)

followup_table = followup_table.sort_values("group")
display(followup_table)

# plot
# entities cohort vs rest
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="cohort_flag",
    cat_names=[(1, "cohort"), (0, "control")],
    title=f"C61 Survival cohort vs control, diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)

# add pct
total = report["Patients after join with follow-up data"]
summary["pct"] = summary["count"] / total * 100
##

# cohort diagram
LEFT_STEPS = [
    f"Initial tumor records (diagnosis year {asserted_year_min} - {asserted_year_max})",
    "Patients after first-tumor selection",
]
RIGHT_STEPS = [
    # "Vital status records",
    # "Only latest available vital status, remove duplicates",
    # "Patients with non-negative follow-up time",
    "Latest vital status records",
    "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)",
    # "Latest progression records",
    # "Latest TNM T records",
    # "Latest TNM N records",
    # "Latest TNM M records",
    # "Latest UICC records",
    # "Latest surgery records",
    # "Latest radio therapy records",
    # "Latest systemic therapy records",
    # "Latest metastasis records",
    # "Latest other classification records"
]
CENTER_STEPS = [
    "Patients after join with follow-up data",
    "Excluded: unknown event status",
    "Excluded: missing survival time",
    "Excluded: survival time >100 years",
    "Excluded: discordant mortality records (vital_status code = L after is_deceased=True)",
    "Kaplan-Meier eligible cohort",
]
plot_survival_cohort_pca(
    summary,
    title=f"C61 Survival Cohort: {sites} - diagnosis year {asserted_year_min} - {asserted_year_max}",
    left_steps=LEFT_STEPS,
    right_steps=RIGHT_STEPS,
    center_steps=CENTER_STEPS,
)


##############################################################################################
######### SURVIVAL x METASTASIERUNG M0, M1a, M1b, M1c

# 1. M-stage filtern
target_m_stages = ["M0", "M1a", "M1b", "M1c"]

df_tnm_m = df_tnm_m[df_tnm_m["m_tnm_clean"].isin(target_m_stages)].copy()

print(df_tnm_m["m_tnm_clean"].value_counts())


# EIN M-stage pro Patient (0–4 Monate nach Diagnose)
df_tnm_unique = (
    df_tnm_m[
        (df_tnm_m["months_between_asserted_m_tnm_date"] >= 0)
        & (df_tnm_m["months_between_asserted_m_tnm_date"] <= 4)
    ]
    .sort_values("m_tnm_clean", ascending=False)
    .groupby("condition_id_hash")
    .first()
    .reset_index()
)

print(
    f"Unique patients with M-stage within 4 months: "
    f"{df_tnm_unique['condition_id_hash'].nunique():,}"
)


# SURVIVAL BASE (NICHT filtern!)
df_km_base = df_km.copy()


# Merge survival + M-stage
df_km_tnm = df_km_base.merge(
    df_tnm_unique[["condition_id_hash", "m_tnm_clean"]], on="condition_id_hash", how="inner"
)

print("After M-stage merge:", df_km_tnm.shape[0])
print(df_km_tnm["m_tnm_clean"].value_counts())


# cohort OR control
if cohort_or_control == "cohort":
    df_cohort_or_control = df_km_tnm[df_km_tnm["cohort_flag"] == 1].copy()
else:
    df_cohort_or_control = df_km_tnm[df_km_tnm["cohort_flag"] == 0].copy()


# KM: M-stage innerhalb cohort/control
km_lifeline(
    data=df_cohort_or_control,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_tnm_clean",
    cat_names=[
        ("M0", "M0"),
        ("M1a", "M1a"),
        ("M1b", "M1b"),
        ("M1c", "M1c"),
    ],
    title=f"OS by TNM-M up to 4 months after diagnosis, ({cohort_or_control}), diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)


# cross view (M × cohort/control)
df_km_tnm["m_cohort_group"] = (
    df_km_tnm["m_tnm_clean"].astype(str)
    + "_"
    + df_km_tnm["cohort_flag"].map({1: "cohort", 0: "control"})
)

order = [
    "M0_cohort",
    "M0_control",
    "M1a_cohort",
    "M1a_control",
    "M1b_cohort",
    "M1b_control",
    "M1c_cohort",
    "M1c_control",
]

df_km_tnm["m_cohort_group"] = pd.Categorical(
    df_km_tnm["m_cohort_group"], categories=order, ordered=True
)


km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_cohort_group",
    cat_names=[
        ("M0_cohort", "M0 cohort"),
        ("M0_control", "M0 control"),
    ],
    title=f"OS by TNM-M up to 4 months after diagnosis × cohort/control, diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)
km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_cohort_group",
    cat_names=[
        ("M1a_cohort", "M1a cohort"),
        ("M1a_control", "M1a control"),
    ],
    title=f"OS by TNM-M up to 4 months after diagnosis × cohort/control, diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)

km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_cohort_group",
    cat_names=[
        ("M1b_cohort", "M1b cohort"),
        ("M1b_control", "M1b control"),
    ],
    title=f"OS by TNM-M up to 4 months after diagnosis × cohort/control, diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)

km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_cohort_group",
    cat_names=[
        ("M1c_cohort", "M1c cohort"),
        ("M1c_control", "M1c control"),
    ],
    title=f"OS by TNM-M up to 4 months after diagnosis × cohort/control, diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)

"""
##############################################################################################
######### SURVIVAL x PSA ANSPRECHEN NACH THERAPIE
# PSA
loinc_codes_psa = {
    "PSA",
    "83112-3",
    "83113-1",
    "12841-3",
    "2857-1",
    "35741-8",
}


# before diagnosis
path_labs_before_diag = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/study_name_pca_analysis/pca1_data_allsites/lab_export_05062026_with_condition/{cohort_or_control}_before_diag.csv"

labs_before_diag = pd.read_csv(
    path_labs_before_diag,
    sep=";",
    dtype={
        "patient_resource_id_hash": "string",
        "condition_id_hash": "string",
        "loinc_code": "string",
        "lab_quantity_unit": "string",
        "lab_quantity_value": "float64",
        "months_between_asserted_lab_dateTime": "float64",
        "months_between_asserted_therapy_start_date": "float64",
    },
    na_values=["", " ", "NA", "NaN", "nan", None],
    keep_default_na=True,
)
print("Shape:", labs_before_diag.shape)

print("\nDtypes:")
print(labs_before_diag.dtypes)

print("\nMissing rate (numeric cols):")
print(labs_before_diag.isna().mean().sort_values(ascending=False))
labs_before_diag[labs_before_diag["lab_quantity_value"].isna()][
    ["loinc_code"]
].value_counts().head()


labs_before_diag = labs_before_diag[labs_before_diag["loinc_code"].isin(loinc_codes_psa)]
print("Shape:", labs_before_diag.shape)
labs_before_diag.head(50)
labs_before_diag["patient_resource_id_hash"].nunique()


# after diagnosis
path_labs_after_diag = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/study_name_pca_analysis/pca1_data_allsites/lab_export_05062026_with_condition/{cohort_or_control}_after_diag_before_therapy.csv"

labs_after_diag = pd.read_csv(
    path_labs_after_diag,
    sep=";",
    dtype={
        "patient_resource_id_hash": "string",
        "condition_id_hash": "string",
        "loinc_code": "string",
        "lab_quantity_unit": "string",
        "lab_quantity_value": "float64",
        "months_between_asserted_lab_dateTime": "float64",
        "months_between_asserted_therapy_start_date": "float64",
    },
    na_values=["", " ", "NA", "NaN", "nan", None],
    keep_default_na=True,
)
labs_after_diag = labs_after_diag.drop(columns=["months_between_asserted_metastasis_date"])
print("Shape:", labs_after_diag.shape)

print("\nDtypes:")
print(labs_after_diag.dtypes)

print("\nMissing rate (numeric cols):")
print(labs_after_diag.isna().mean().sort_values(ascending=False))
labs_after_diag[labs_after_diag["lab_quantity_value"].isna()][["loinc_code"]].value_counts().head()


labs_after_diag = labs_after_diag[labs_after_diag["loinc_code"].isin(loinc_codes_psa)]
print("Shape:", labs_after_diag.shape)
labs_after_diag.head(50)
labs_after_diag["patient_resource_id_hash"].nunique()

# concat before diagnosis and after diagnosis as before therapy
labs_before_therapy = pd.concat([labs_before_diag, labs_after_diag])
labs_before_therapy.head(50)
labs_before_therapy["patient_resource_id_hash"].nunique()


# after therapy
path_labs_after_therapy = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/study_name_pca_analysis/pca1_data_allsites/lab_export_05062026_with_condition/{cohort_or_control}_any_therapy_min_after_therapy.csv"

labs_after_therapy = pd.read_csv(
    path_labs_after_therapy,
    sep=";",
    dtype={
        "patient_resource_id_hash": "string",
        "condition_id_hash": "string",
        "loinc_code": "string",
        "lab_quantity_unit": "string",
        "lab_quantity_value": "float64",
        "months_between_asserted_lab_dateTime": "float64",
        "months_between_asserted_therapy_start_date": "float64",
    },
    na_values=["", " ", "NA", "NaN", "nan", None],
    keep_default_na=True,
)

print("Shape:", labs_after_therapy.shape)

print("\nDtypes:")
print(labs_after_therapy.dtypes)

print("\nMissing rate (numeric cols):")
print(labs_after_therapy.isna().mean().sort_values(ascending=False))
labs_after_therapy[labs_after_therapy["lab_quantity_value"].isna()][
    ["loinc_code"]
].value_counts().head()


labs_after_therapy = labs_after_therapy[labs_after_therapy["loinc_code"].isin(loinc_codes_psa)]
print("Shape:", labs_after_therapy.shape)
labs_after_therapy.head(50)
labs_after_therapy["patient_resource_id_hash"].nunique()

# now join them for each patient and loinc code and keep all cols with before (therapy) and after (therapy) suffix
# only keep patients with labs before AND after
df_before = labs_before_therapy.copy()
df_after = labs_after_therapy.copy()


df_panel = df_before.merge(
    df_after, on="patient_resource_id_hash", how="inner", suffixes=("_before", "_after")
)
print("Shape:", df_panel.shape)
# ACHTUNG Kreuzprodukt rausfiltern damit
df_panel = df_panel[df_panel["loinc_code_before"] == df_panel["loinc_code_after"]]
print("Shape nach LOINC-Abgleich:", df_panel.shape)
df_panel.sort_values(["patient_resource_id_hash"])
df_panel.head(50)

# drop empty lab vals
df_panel = df_panel.dropna(subset=["lab_quantity_value_before", "lab_quantity_value_after"])
df_panel.sort_values(["patient_resource_id_hash"])
df_panel.head(50)
print("Shape:", df_panel.shape)

df_panel["patient_resource_id_hash"].nunique()

# psa reduktion nach therapie
df_panel["psa_reduction"] = (
    (df_panel["lab_quantity_value_before"] - df_panel["lab_quantity_value_after"])
    / df_panel["lab_quantity_value_before"]
) * 100

# # über 90%: good responder, zwischen 50-90: partial responder, unter 50: poor responder
# df_panel["response_group"] = np.select(
#     [
#         df_panel["psa_reduction"] >= 90,
#         df_panel["psa_reduction"].between(50, 90),
#         df_panel["psa_reduction"] < 50,
#     ],
#     ["good_responder", "partial_responder", "poor_responder"],
#     default="other",
# )
# print("Shape:", df_panel.shape)
# df_panel.head(100)
# df_panel["response_group"].value_counts()

# join to df_km from above - join on patient resource id hash - chose only one lab val per patient
# df_panel["loinc_code"].value_counts()

# in case pat has multiple psa values with different loinc codes, chose max psa reduction
df_resp = (
    df_panel.groupby("patient_resource_id_hash")
    .agg(
        condition_id_hash_before=("condition_id_hash_before", "first"),
        # response_group=("response_group", "first"), - gefährlich
        max_psa_reduction=("psa_reduction", "max"),  # oder max/min je nach Definition
    )
    .reset_index()
)
df_resp.head(10)
print("Shape:", df_resp.shape)

df_resp["response_group"] = np.select(
    [
        df_resp["max_psa_reduction"] >= 90,
        df_resp["max_psa_reduction"].between(50, 90),
        df_resp["max_psa_reduction"] < 50,
    ],
    ["good_responder", "partial_responder", "poor_responder"],
    default="other",
)
df_resp["response_group"].value_counts()
df_resp.head(10)
print("Shape:", df_resp.shape)

# join to df_km survival data
# df_km_resp = df_km.merge(df_resp, on="patient_resource_id_hash", how="left")
# test right join - no matches
# df_km_resp = df_km.merge(df_resp, on="patient_resource_id_hash", how="right")
# test other colname in my df - no matches
# df_km_resp = df_km.merge(
#     df_resp, left_on="patid_pseudonym_hash", right_on="patient_resource_id_hash", how="right"
# )

df_km.head()
print(df_km.columns)
print(df_resp.columns)

if cohort_or_control == "cohort":
    df_km_cohort_or_control = df_km[df_km["cohort_flag"] == 1]
else:
    df_km_cohort_or_control = df_km[df_km["cohort_flag"] == 0]

# mach merge über condition_id_hash nachdem Dorian neu geladen hat
df_km_resp = df_km_cohort_or_control.merge(
    df_resp, left_on="condition_id_hash", right_on="condition_id_hash_before", how="left"
)
df_km_resp.head()
df_resp["response_group"].value_counts()

# fill missing lab data
df_km_resp["response_group"] = df_km_resp["response_group"].fillna("no_psa_data")
df_km_resp.head()

km_lifeline(
    data=df_km_resp,
    event_col="event",
    time_col="survival_time_months",
    cat_col="response_group",
    cat_names=[
        ("good_responder", "good_responder  (>90% reduction)"),
        ("partial_responder", "partial_responder  (50-90% reduction)"),
        ("poor_responder", "poor_responder (<50% reduction)"),
        ("no_psa_data", "no_psa_data"),
    ],
    title=f"OS by PSA response, {cohort_or_control}, diagnosis year {asserted_year_min} - {asserted_year_max}, {sites}",
    stat=True,
    show_stat=True,
)


##############################################
# check if patient_mrn_hash is in lab vals
# lab_path = "/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/results/study_protocol_pca1/deidentified/parquet/df_mii_labs_asserted_deidentified.parquet"
# lab_uker = pd.read_parquet(lab_path)
# lab_uker.head()

# print(
#     "df_join conditions (before filtering survival data):", df_join["condition_id_hash"].nunique()
# )
# print("df_km conditions:", df_km["condition_id_hash"].nunique())
# print("df_resp conditions:", df_resp["condition_id_hash_before"].nunique())

# join_cond = set(df_join["condition_id_hash"].dropna())
# km_cond = set(df_km["condition_id_hash"].dropna())
# resp_cond = set(df_resp["condition_id_hash_before"].dropna())

# print("Condition intersection df_join df_resp:", len(join_cond & resp_cond))
# print("Condition intersection df_km df_resp:", len(km_cond & resp_cond))

# print(
#     "df_join patients (before filtering survival data):",
#     df_join["patient_resource_id_hash"].nunique(),
# )
# print("df_km patients:", df_km["patient_resource_id_hash"].nunique())
# print("df_resp patients:", df_resp["patient_resource_id_hash"].nunique())

# join_pat = set(df_join["patient_resource_id_hash"].dropna())
# km_pat = set(df_km["patient_resource_id_hash"].dropna())
# resp_pat = set(df_panel["patient_resource_id_hash"].dropna())

# print("Patient intersection df_join df_resp:", len(join_pat & resp_pat))
# print("Patient intersection df_km df_resp:", len(km_pat & resp_pat))

# # warum haben wir so wenige laborwerte
# # df_join conditions (before filtering survival data): 20290
# # df_km conditions: 20282
# # df_resp conditions: 585
# # Condition intersection df_join df_resp: 577
# # Condition intersection df_km df_resp: 577
# # df_join patients (before filtering survival data): 20290
# # df_km patients: 20282
# # df_resp patients: 585
# # Patient intersection df_join df_resp: 578
# # Patient intersection df_km df_resp: 578


# join_cond = set(df_join["condition_id_hash"].dropna())
# km_cond = set(df_km["condition_id_hash"].dropna())
# resp_cond = set(df_resp["condition_id_hash_before"].dropna())

# # Jetzt korrigiert mit Set & Set
# print("Condition intersection df_join df_resp:", len(join_cond & resp_cond))
# print("Condition intersection df_km df_resp:", len(km_cond & resp_cond))

# join_pat = set(df_join["patient_resource_id_hash"].dropna())
# km_pat = set(df_km["patient_resource_id_hash"].dropna())
# # Nutze hier df_panel statt df_resp, um zu sehen, wie viele Patienten vor dem Groupby da waren
# panel_pat = set(df_panel["patient_resource_id_hash"].dropna())

# print("Patient intersection df_join df_panel:", len(join_pat & panel_pat))
# print("Patient intersection df_km df_panel:", len(km_pat & panel_pat))

# # Condition intersection df_join df_resp: 76
# # Condition intersection df_km df_resp: 76
# # Patient intersection df_join df_panel: 76
# # Patient intersection df_km df_panel: 76
 """
 """
