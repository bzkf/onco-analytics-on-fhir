import sys
from functools import reduce
from pathlib import Path

import numpy as np
import pandas as pd
from IPython.display import display

from analytics_on_fhir.analysis.utils.load_multisite_parquets import (
    build_all_obds_paths,
    load_multisite_parquets,
    load_multisite_parquets_from_paths,
)
from analytics_on_fhir.analysis.utils.survival_utils import (
    km_lifeline,
    latest_per_condition,
    median_iqr,
    plot_survival_cohort_pca,
)

report = {}

sites = "UKER, TUM, UKA, LMU, UKR, UKW"
# sites = "UKER"
# sites = "TUM"
# sites = "UKA"
# sites = "LMU"
# sites = "UKR"
# sites = "UKW"

BASE_DIR_ALL_OBDS = Path(
    "/home/coder/git/onco-analytics-on-fhir/"
    "analytics_on_fhir/analysis/all_obds_patients_analysis/"
    "all_obds_data_allsites"
)
asserted_year = 1970

# COHORT or control for labs
# cohort_or_control = "cohort"

# augment data with verlaufsinfo - any record of the patient from progression and other obds
extend_df_km_by_progression = True

all_obds_config = {
    "obds": {
        "file": "df_all_obds_clean_deidentified.parquet",
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
    # add other dfs here for extend follow up as well
}

paths = build_all_obds_paths(base_dir=BASE_DIR_ALL_OBDS, config=all_obds_config, sites=sites)
dfs = {key: load_multisite_parquets_from_paths(paths[key]) for key in all_obds_config.keys()}
df_obds = dfs["obds"]

df_obds.head()


print(df_obds.shape[0])
# remove C44 and all D* ICD10 codes, filter after 1970 (sind eh keine Daten drin, filter ich schon beim extract), > 18 J.
df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
print(df_obds.shape[0])
df_obds = df_obds[df_obds["asserted_year"] >= asserted_year]
print(df_obds.shape[0])
df_obds = df_obds[df_obds["age_at_diagnosis"] >= 18]
print(df_obds.shape[0])
df_obds.head()
site_counts = df_obds["site"].value_counts().rename_axis("site").reset_index(name="n")

site_counts["pct"] = (site_counts["n"] / site_counts["n"].sum() * 100).round(2)

print(site_counts)

# drop duplicates
df_obds = df_obds.drop_duplicates()
print(df_obds.shape[0])
df_obds.head()
report[f"Initial tumor records (diagnosis year ≥ {asserted_year})"] = df_obds.shape[0]


# Basiszahlen all obds
n_rows = len(df_obds)
n_conditions = df_obds["condition_id_hash"].nunique()
n_patients = df_obds["patient_resource_id_hash"].nunique()

print(f"Rows:                     {n_rows:,}")
print(f"Unique conditions:        {n_conditions:,}")
print(f"Unique patients:          {n_patients:,}")

# Gender (absolut + relativ) all obds
gender_counts = (
    df_obds["gender"].fillna("missing").value_counts(dropna=False).rename("n").to_frame()
)

gender_counts["pct"] = (gender_counts["n"] / gender_counts["n"].sum() * 100).round(2)

print("\nGender distribution all obds patients ")
print(gender_counts)


# Top 20 Entitäten
top20_entities = (
    df_obds["entity_or_parent"]
    .fillna("missing")
    .value_counts()
    .head(20)
    .rename_axis("entity_or_parent")
    .reset_index(name="n")
)
top20_entities

top20_entities["pct"] = (top20_entities["n"] / len(df_obds) * 100).round(2)
top20_entities
top20_entities["n"].sum()

# Male/Female pro Top-20 Entität
gender_by_entity = (
    df_obds[df_obds["gender"].isin(["male", "female", "unknown"])]
    .groupby(["entity_or_parent", "gender"])
    .size()
    .unstack(fill_value=0)
    .reset_index()
)

# absolute Zahlen
gender_by_entity["male_abs"] = gender_by_entity.get("male", 0)
gender_by_entity["female_abs"] = gender_by_entity.get("female", 0)
gender_by_entity["unknown_abs"] = gender_by_entity.get("unknown", 0)

# relative Anteile
total_gender = (
    gender_by_entity["male_abs"] + gender_by_entity["female_abs"] + gender_by_entity["unknown_abs"]
)

gender_by_entity["male_pct"] = (gender_by_entity["male_abs"] / total_gender * 100).round(2)
gender_by_entity["female_pct"] = (gender_by_entity["female_abs"] / total_gender * 100).round(2)
gender_by_entity["unknown_pct"] = (gender_by_entity["unknown_abs"] / total_gender * 100).round(2)

gender_by_entity = gender_by_entity[
    [
        "entity_or_parent",
        "male_abs",
        "female_abs",
        "unknown_abs",
        "male_pct",
        "female_pct",
        "unknown_pct",
    ]
]

top20_entities = top20_entities.merge(gender_by_entity, on="entity_or_parent", how="left")
print(top20_entities)

top20_entities["gender_sum"] = (
    top20_entities["male_abs"] + top20_entities["female_abs"] + top20_entities["unknown_abs"]
)

top20_entities["diff"] = top20_entities["n"] - top20_entities["gender_sum"]

print(top20_entities[["entity_or_parent", "n", "gender_sum", "diff"]])
df_obds_top20 = df_obds[df_obds["entity_or_parent"].isin(top20_entities["entity_or_parent"])]
df_obds_top20.shape[0]
report[
    f"Initial records from 20 most prevalent tumor entities \n(diagnosis year ≥ {asserted_year})"
] = df_obds_top20.shape[0]


# Gender (absolut + relativ) df_obds_top20
gender_counts_df_obds_top20 = (
    df_obds_top20["gender"].fillna("missing").value_counts(dropna=False).rename("n").to_frame()
)

gender_counts_df_obds_top20["pct"] = (
    gender_counts_df_obds_top20["n"] / gender_counts_df_obds_top20["n"].sum() * 100
).round(2)

print("\nGender distribution df_obds_top20 ")
print(gender_counts_df_obds_top20)

# gender stats top 20
abs_counts = df_obds_top20["gender"].value_counts(dropna=False)
rel_counts = df_obds_top20["gender"].value_counts(dropna=False, normalize=True) * 100
gender_stats = pd.DataFrame({"Absolut": abs_counts, "Relativ (%)": rel_counts.round(2)})
print(gender_stats)

# in df_obds, groupby patient_resource_id_hash and only keep first tumor
# with smallest age_at_diagnosis
# wichtig für unabhängige Beobachtung Kaplan Meier --> nur für Erstdiagnose hier
df_obds_top20_first_tumor = (
    df_obds_top20.sort_values("age_at_diagnosis")
    .groupby("patient_resource_id_hash")
    .first()
    .reset_index()
)
print("df_obds_top20_first_tumor for kaplan meier curves: ")
df_obds_top20_first_tumor.shape[0]
# gender stats top 20 first tumor for kaplan meier curves
abs_counts = df_obds_top20_first_tumor["gender"].value_counts(dropna=False)
rel_counts = df_obds_top20_first_tumor["gender"].value_counts(dropna=False, normalize=True) * 100
gender_stats_first_tumor = pd.DataFrame({"Absolut": abs_counts, "Relativ (%)": rel_counts.round(2)})
print(gender_stats_first_tumor)

report["Patients after first-tumor selection"] = df_obds_top20_first_tumor.shape[0]
report["Unique patients after first-tumor selection"] = df_obds_top20_first_tumor[
    "patient_resource_id_hash"
].nunique()


# vitalstatus
df_vital = dfs["vitalstatus"]
report["Vital status records"] = df_vital.shape[0]
print(df_vital.shape[0])
df_vital.head()

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
print(df_vital_latest.shape[0])
df_vital_latest.head(50)

# ich will nur die patient_resource_id_hash hier behalten, dei auch in meinem df_obds nach all den
# filterungen sind
valid_patients = set(df_obds_top20_first_tumor["patient_resource_id_hash"])

df_vital_latest = df_vital_latest[
    df_vital_latest["patient_resource_id_hash"].isin(valid_patients)
].copy()

report["Latest vital status records"] = df_vital_latest.shape[0]
# remove negative months_between_asserted_effective_dateTime
df_vital_latest = df_vital_latest[
    df_vital_latest["months_between_asserted_effective_dateTime"] >= 0
].copy()

df_vital_latest.head(50)
print(df_vital_latest.shape[0])
report["Patients with non-negative follow-up time"] = df_vital_latest.shape[0]
report["Unique patients with valid vital status information"] = df_vital_latest[
    "patient_resource_id_hash"
].nunique()


# JOIN FIRST TUMOR DF WITH LATEST VITALSTATUS
df_join = df_obds_top20_first_tumor.merge(
    df_vital_latest, on="patient_resource_id_hash", how="left", suffixes=("", "_vital")
)
report["Patients after join with vital status data"] = df_join.shape[0]
report["C61 patients"] = (df_join["icd10_code"] == "C61").sum()
# df_join.to_csv("dj_join.csv", index=False)
df_join.head(50)
print(df_join.shape[0])
print("check negative months between")
print(df_join["months_between_asserted_effective_dateTime"].describe())


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

df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] >= asserted_year]

df_tnm_m.head()
df_tnm_m.shape
df_tnm_m["condition_id_hash"].nunique()


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
    latest_dfs = {}

    for key, cfg in obds_extension_config.items():
        latest_dfs[key] = latest_per_condition(
            df=dfs_obds[key],
            date_col=cfg["date_col"],
            output_col=cfg["output_col"],
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
    followup_cols = [cfg["output_col"] for cfg in obds_extension_config.values()] + ["tnm_m_months"]

    # alle negativen Werte temporär auf NaN, damit sie ignoriert werden. Wir wollen nur
    # Follow-ups (Therapien, Progress, etc.), die >= 0 (also nach/zur Diagnose) stattfanden.
    df_followup_positive_only = df_all[followup_cols].where(df_all[followup_cols] >= 0)

    # JETZT das Maximum
    df_all["months_followup_extended"] = df_followup_positive_only.max(axis=1)
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
# year distribution of excluded patients
unknown_event_mask = df_join["event"].isna()
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
unknown_event_mask = df_join["event"].isna()
report["Excluded: unknown event status"] = unknown_event_mask.sum()
df_join = df_join.loc[~unknown_event_mask].copy()
df_join["event"] = df_join["event"].astype(int)

# combine death and followup time for final survival time
df_join["survival_time_months"] = np.where(
    df_join["event"] == 1, df_join["death_time"], df_join["followup_time"]
)

# remove implausible survival times (>100 years) and negative ones
# 1. Fehlende Zeiten filtern (IMMER zuerst machen, bevor man Größer/Kleiner-Vergleiche zieht)
missing_mask = df_join["survival_time_months"].isna()
report["Excluded: missing survival time"] = missing_mask.sum()
df_join = df_join.loc[~missing_mask].copy()

# 2. Implausible Zeiten (> 100 Jahre) filtern
outlier_mask = df_join["survival_time_months"] > 1200
report["Excluded: survival time >100 years"] = outlier_mask.sum()
df_join = df_join.loc[~outlier_mask].copy()

# 3. Negative Überlebenszeiten filtern (und dokumentieren!)
negative_mask = df_join["survival_time_months"] < 0
report["Excluded: negative survival time"] = negative_mask.sum()
df_km = df_join.loc[~negative_mask].copy()

# sind ein paar cases - genauer ansehen
# 1. Isoliere exakt die Patienten mit negativer Überlebenszeit
negative_mask = df_join["survival_time_months"] < 0
df_negative = df_join.loc[negative_mask].copy()

print(f"Anzahl der Patienten mit negativer Überlebenszeit: {df_negative.shape[0]}")

# 2. Definiere die wichtigsten Spalten für die Ursachenforschung
cols_to_inspect = [
    "patient_resource_id_hash",
    "condition_id_hash",
    "asserted_year",
    "event",
    "survival_time_months",
    "death_time",
    "followup_time",
    "months_followup_extended",  # Hier kommt oft der Fehler bei event=0 her!
    "is_deceased",
    "vitalstatus_code",
]

# Filtere sicherheitshalber nur auf Spalten, die auch wirklich existieren
cols_to_inspect = [c for c in cols_to_inspect if c in df_negative.columns]
df_negative_view = df_negative[cols_to_inspect].sort_values("survival_time_months")

# 3. Aggregierte Analyse: Betrifft es Verstorbene oder Zensierte?
print("\n--- Verteilung nach Event-Status ---")
event_counts = df_negative["event"].value_counts(dropna=False).rename("Anzahl").to_frame()
event_counts["Prozent"] = (event_counts["Anzahl"] / len(df_negative) * 100).round(1)
display(event_counts)

# 4. Zeige die 30 extremsten Fälle (die am weitesten in der Vergangenheit liegen)
print("\n--- Top 30 extremste Fälle mit negativer Überlebenszeit ---")
display(df_negative_view.head(30))

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


# censored patients event=0
report["Censored patients (event=0) with follow-up information"] = (df_km["event"] == 0).sum()


# report
summary = pd.DataFrame.from_dict(report, orient="index", columns=["count"])
summary[
    f"% of initial records from 20 most prevalent tumor entities (diagnosis year ≥ {asserted_year})"
] = (
    summary["count"]
    / report[
        f"Initial records from 20 most prevalent tumor entities \n(diagnosis year ≥ {asserted_year})"
    ]
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
    "censored",
    "deceased",
]

followup_table["group"] = pd.Categorical(
    followup_table["group"],
    categories=order,
    ordered=True,
)

followup_table = followup_table.sort_values("group")
display(followup_table)


# plot
# gesamt km
# km_lifeline(
#     data=df_km,
#     event_col="event",
#     time_col="survival_time_months",
#     cat_col=None,
#     title=f"Overall Survival, diagnoses > {asserted_year}, {sites}",
# )

# gender
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="gender",
    cat_names=[("male", "Male"), ("female", "Female")],
    title=f"Survival by Gender, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=True,
)

# gender 10y lim - 120 months
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="gender",
    cat_names=[("male", "Male"), ("female", "Female")],
    title=f"5/10 year Survival by Gender, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=True,
    xlim=(0, 121),
)

# top 20
top20_entities = df_km["entity_or_parent"].value_counts().head(20).index.tolist()

# cat_names automatisch erzeugen
cat_names = [(entity, f"{entity}") for entity in top20_entities]

print(cat_names)


# entities - top 1-10; top 11-20 entity or parent
top10_entities = top20_entities[:10]
top11_20_entities = top20_entities[10:20]
print("Top 1–10:")
print(top10_entities)

print("\nTop 11–20:")
print(top11_20_entities)

# Top 1–10
cat_names_1_10 = [(entity, entity) for entity in top10_entities]

km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top10_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_1_10,
    title=f"Survival by Top 1–10 Tumor Entities, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
)
# zoom 5y/10y
km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top10_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_1_10,
    title=f"5y/10y Survival by Top 1–10 Tumor Entities, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
    xlim=(0, 121),
)

# Top 11–20
cat_names_11_20 = [(entity, entity) for entity in top11_20_entities]

km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top11_20_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_11_20,
    title=f"Survival by Top 11–20 Tumor Entities, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
)
# zoom 5y/10y
km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top10_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names_1_10,
    title=f"5y/10y Survival by Top 11–20 Tumor Entities, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(12, 8),
    xlim=(0, 121),
)

# cohort diagram
LEFT_STEPS = [
    # f"Initial tumor records (diagnosis year ≥ {asserted_year})",
    f"Initial records from 20 most prevalent tumor entities \n(diagnosis year ≥ {asserted_year})",
    "Patients after first-tumor selection",
]
RIGHT_STEPS = [
    # "Vital status records",
    # "Only latest available vital status, remove duplicates",
    # "Patients with non-negative follow-up time",
    "Latest vital status records",
    "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)",
]
CENTER_STEPS = [
    "Patients after join with vital status data",
    "Excluded: unknown event status",
    "Excluded: missing survival time",
    "Excluded: survival time >100 years",
    "Excluded: negative survival time",
    "Excluded: discordant mortality records (vital_status code = L after is_deceased=True)",
    "Kaplan-Meier eligible cohort",
]
plot_survival_cohort_pca(
    summary,
    asserted_year=asserted_year,
    title=f"Survival Cohort: {sites} - diagnoses ≥ {asserted_year}",
    left_steps=LEFT_STEPS,
    right_steps=RIGHT_STEPS,
    center_steps=CENTER_STEPS,
    cohort_control=False,
)


##############################################################################################
######### SURVIVAL x METASTASIERUNG M0, M1a, M1b, M1c

# maximalwert innerhalb 4 monate nach diagnose?
# MX raus
target_m_stages = sorted(
    df_tnm_m.loc[
        df_tnm_m["m_tnm_clean"].notna() & (df_tnm_m["m_tnm_clean"] != "MX"), "m_tnm_clean"
    ].unique()
)

print(target_m_stages)
df_tnm_m = df_tnm_m[df_tnm_m["m_tnm_clean"].isin(target_m_stages)].copy()
df_tnm_m["condition_id_hash"].nunique()
df_tnm_m["m_tnm_clean"].value_counts()

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

print(
    f"\nEinzigartige Krankheitsfälle mit M-Status innerhalb von 3 Monaten: {df_tnm_unique['condition_id_hash'].nunique()}"
)
df_tnm_unique.head(50)
print(df_tnm_unique["m_tnm_clean"].value_counts())


# 5. INNER JOIN an den tnm_m DF (wir wollen nur Patienten mit bekannter M-Klassifikation plotten)
df_km_tnm = df_km.merge(
    df_tnm_unique[["condition_id_hash", "m_tnm_clean"]], on="condition_id_hash", how="inner"
)

df_km_tnm.head()
print(f"Verbleibende Patienten im KM-Plot nach TNM-M Match: {df_km_tnm.shape[0]}")
print(df_km_tnm["m_tnm_clean"].value_counts())

cat_names = [(stage, stage) for stage in sorted(df_km_tnm["m_tnm_clean"].dropna().unique())]

print(cat_names)

# km_lifeline(
#     data=df_km_tnm,
#     event_col="event",
#     time_col="survival_time_months",
#     cat_col="m_tnm_clean",
#     cat_names=cat_names,
#     title=f"OS by Metastasis Stage (TNM-M), diagnosis > {asserted_year}, {sites}",
#     stat=True,
#     show_stat=True,
# )

# group tnm m values to 3 digits
df_km_tnm["m_tnm_grouped"] = df_km_tnm["m_tnm_clean"].str[:3]
print(df_km_tnm["m_tnm_grouped"].value_counts())
priority = ["M0", "M1", "M1a", "M1b", "M1c", "M1d"]

available = df_km_tnm["m_tnm_grouped"].dropna().unique()

cat_names = [(x, x) for x in priority if x in available]
km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_tnm_grouped",
    cat_names=cat_names,
    title=f"OS by Metastasis Stage (TNM-M) up to 3 months after diagnosis, diagnosis > {asserted_year}, {sites}",
    stat=True,
    show_stat=True,
)
