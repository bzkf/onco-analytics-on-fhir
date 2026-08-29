import sys
from pathlib import Path

import numpy as np
import pandas as pd
from IPython.display import display
from lifelines import CoxPHFitter
from tableone import TableOne

from analytics_on_fhir.analysis.utils.load_multisite_parquets import (
    build_all_obds_paths,
    load_multisite_parquets_from_paths,
)
from analytics_on_fhir.analysis.utils.survival_utils import (
    extend_followup_times,
    finalize_km_cohort,
    km_lifeline,
    latest_per_condition,
    load_and_clean_base_data,
    median_iqr,
    plot_survival_cohort,
    process_survival_events,
)

# 1. KONFIGURATION PCA STUDIE

report = {}
sites = "UKER, TUM, UKA, LMU, UKW"
ASSERTED_MIN, ASSERTED_MAX = 2017, 2023
EXTEND_FOLLOWUP = True

DIR_PCA = Path(
    "/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/"
    "study_name_pca_analysis/pca1_data_allsites"
)
# To do: das hier funktioniert eh nicht weil wir unterschiedliche zufalls-hashes nicht zusammenbringen
# deshalb hier austauschen mit DIR PCA sobald neue Daten da sind, die wir aktuell extrahieren
DIR_ALL_OBDS = Path(
    "/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/"
    "all_obds_patients_analysis/all_obds_data_allsites"
)

pca_base_config = {
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
}


paths = build_all_obds_paths(base_dir=DIR_PCA, config=pca_base_config, sites=sites)
dfs = {key: load_multisite_parquets_from_paths(paths[key]) for key in pca_base_config.keys()}

# Pre-Processing PCA: Metastasen-Spalten entfernen, weil die exploden das dataset
df_obds_raw = dfs["obds"]
df_obds_pre = df_obds_raw.loc[
    :, ~df_obds_raw.columns.str.contains("metastasis", case=False, na=False)
]

# TNM-M FILTER - weil hier andere Jahrgänge mit drin sind
df_tnm_m = dfs["tnm_m"]
df_tnm_m["m_tnm_clean"] = df_tnm_m["m_tnm"].astype("string").str.strip()
df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] >= ASSERTED_MIN]
df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] <= ASSERTED_MAX]
dfs["tnm_m"] = df_tnm_m  # Zurückspeichern für die spätere Nutzung

# Kern-Funktion aufrufen (Keine target_entities nötig, da schon alles C61 ist)
df_obds_clean, df_obds_first_tumor = load_and_clean_base_data(
    df_obds=df_obds_pre,
    asserted_min=ASSERTED_MIN,
    asserted_max=ASSERTED_MAX,
    target_entities=None,
)

report[f"Initial tumor records (diagnosis year {ASSERTED_MIN} - {ASSERTED_MAX})"] = (
    df_obds_clean.shape[0]
)
report["Patients after first-tumor selection"] = df_obds_first_tumor.shape[0]


# 3. VITALSTATUS & EVENT-BERECHNUNG
df_vital_latest, df_join = process_survival_events(
    df_first_tumor=df_obds_first_tumor, df_vital=dfs["vitalstatus"]
)

report["Vital status records"] = dfs["vitalstatus"].shape[0]
report["Latest vital status records"] = df_vital_latest.shape[0]
report["Patients with non-negative follow-up time"] = df_vital_latest.shape[0]
report["Patients after join with follow-up data"] = df_join.shape[0]
report["C61 patients"] = (df_join["icd10_code"] == "C61").sum()


# 4. FOLLOW-UP ZEITEN VERLÄNGERN
if EXTEND_FOLLOWUP:
    # Lade Progressions aus All-OBDS Ordner - TO DO - delete, use PCA folder later with new data
    obds_ext_config = {
        "progressions": {
            "file": "df_progressions_deidentified.parquet",
            "subdir": "parquet",
            "date_col": "months_between_asserted_effective_dateTime",
            "output_col": "progression_months",
        }
    }
    paths_obds = build_all_obds_paths(base_dir=DIR_ALL_OBDS, config=obds_ext_config, sites=sites)
    dfs_obds = {key: load_multisite_parquets_from_paths(paths_obds[key]) for key in obds_ext_config}

    df_prog = dfs_obds["progressions"]
    df_prog = df_prog[df_prog["icd10_code"] == "C61"]
    dfs_obds["progressions"] = df_prog

    # Hash check - ist aktuell null weil unterschiedl. zufalls hash wert
    print("\n" + "=" * 50)
    print("HASH MATCHING CHECK (PCA Cohort vs. All-OBDS Progressions)")
    print("=" * 50)

    join_pats = set(df_join["patient_resource_id_hash"].dropna())
    prog_pats = set(df_prog["patient_resource_id_hash"].dropna())
    print(f"Unique Patients in PCA Cohort:     {len(join_pats):,}")
    print(f"Unique Patients in Progressions:   {len(prog_pats):,}")
    print(f"--> MATCHING PATIENTS:             {len(join_pats & prog_pats):,}")

    join_conds = set(df_join["condition_id_hash"].dropna())
    prog_conds = set(df_prog["condition_id_hash"].dropna())
    print(f"\nUnique Conditions in PCA Cohort:   {len(join_conds):,}")
    print(f"Unique Conditions in Progressions: {len(prog_conds):,}")
    print(f"--> MATCHING CONDITIONS:           {len(join_conds & prog_conds):,}")
    print("=" * 50 + "\n")

    # Lade den Rest aus dem PCA Ordner
    pca_ext_config = {
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
    paths_pca = build_all_obds_paths(base_dir=DIR_PCA, config=pca_ext_config, sites=sites)
    dfs_pca = {key: load_multisite_parquets_from_paths(paths_pca[key]) for key in pca_ext_config}

    extension_config = {**obds_ext_config, **pca_ext_config}
    dfs_ext = {**dfs_obds, **dfs_pca}

    df_join, ext_records_count = extend_followup_times(
        df_join=df_join,
        extension_dfs=dfs_ext,
        extension_config=extension_config,
        df_tnm_m=dfs["tnm_m"],
    )

    report_string = "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)"
    report[report_string] = ext_records_count


# GLEASON SCORE MISSING AUSSCHLIESSEN
# Wir machen das VOR den restlichen Survival-Filtern, direkt auf df_join
if "gleason_score" in df_join.columns:
    missing_gleason_count = df_join["gleason_score"].isna().sum()
    report["Excluded: gleason score missing"] = missing_gleason_count
    print("Excluded: gleason score missing:", missing_gleason_count)

    df_join = df_join.dropna(subset=["gleason_score"])

# 5. FINALE KAPLAN-MEIER KOHORTE FILTERN
df_km, exclusion_counts = finalize_km_cohort(df_join)
report.update(exclusion_counts)

report["Deceased patients (event=1)"] = df_km["event"].sum()
report["Censored patients (event=0) with follow-up information"] = (df_km["event"] == 0).sum()

if "cohort_flag" in df_km.columns:
    mask_cohort = df_km["cohort_flag"] == 1
    mask_control = df_km["cohort_flag"] == 0
    mask_event = df_km["event"] == 1

    report["Deceased patients: cohort"] = (mask_event & mask_cohort).sum()
    report["Deceased patients: control"] = (mask_event & mask_control).sum()
    report["Censored patients: cohort"] = (~mask_event & mask_cohort).sum()
    report["Censored patients: control"] = (~mask_event & mask_control).sum()


# 6. SUMMARY & PLOT
summary = pd.DataFrame.from_dict(report, orient="index", columns=["count"])
summary["pct"] = summary["count"] / report["Patients after join with follow-up data"] * 100
print(summary)

LEFT_STEPS = [
    f"Initial tumor records (diagnosis year {ASSERTED_MIN} - {ASSERTED_MAX})",
    "Patients after first-tumor selection",
]
RIGHT_STEPS = [
    "Latest vital status records",
    "Latest other records (progressions,\nTNM, UICC, therapies, metastasis, other classification)",
]
CENTER_STEPS = [
    "Patients after join with follow-up data",
    "Excluded: gleason score missing",
    "Excluded: unknown event status",
    "Excluded: survival time missing",
    "Excluded: survival time >100 years",
    "Excluded: negative survival time",
    "Excluded: discordant mortality records (vital_status code = L after is_deceased=True)",
    "Kaplan-Meier eligible cohort",
]

plot_survival_cohort(
    summary,
    title=f"C61 Survival Cohort: {sites} - diagnosis year {ASSERTED_MIN} - {ASSERTED_MAX}",
    left_steps=LEFT_STEPS,
    right_steps=RIGHT_STEPS,
    center_steps=CENTER_STEPS,
)


# 7. M-STAGING & BASELINE TABLE (TABLE ONE)
# 1. TNM-M filtern (M0, M1a, M1b, M1c)
target_m_stages = ["M0", "M1a", "M1b", "M1c"]
df_tnm_m = dfs["tnm_m"]
df_tnm_m_filt = df_tnm_m[df_tnm_m["m_tnm_clean"].isin(target_m_stages)].copy()

# EIN M-stage pro Patient (0–4 Monate nach Diagnose)
df_tnm_unique = (
    df_tnm_m_filt[
        (df_tnm_m_filt["months_between_asserted_m_tnm_date"] >= 0)
        & (df_tnm_m_filt["months_between_asserted_m_tnm_date"] <= 4)
    ]
    .sort_values("m_tnm_clean", ascending=False)
    .groupby("condition_id_hash")
    .first()
    .reset_index()
)

# An KM-Datensatz mergen (Left Join, um alle zu behalten)
df_km_met = df_km.merge(
    df_tnm_unique[["condition_id_hash", "m_tnm_clean"]], on="condition_id_hash", how="left"
)
df_km_met["m_tnm_clean"] = df_km_met["m_tnm_clean"].fillna("no_documented_m")

# Year-Bins erstellen (abhängig vom Min-Jahr)
if ASSERTED_MIN == 2017:
    bins = [2016, 2020, 2023]
    labels = ["2017-2020", "2021-2023"]
else:
    bins = [2009, 2012, 2016, 2020, 2023]
    labels = ["2010-2012", "2013-2016", "2017-2020", "2021-2023"]

df_km_met["year_bin"] = pd.cut(df_km_met["asserted_year"], bins=bins, labels=labels)

# 4 Kombinatorische Gruppen (Age x Grade)
conditions = [
    (df_km_met["age_at_diagnosis"] < 65) & (df_km_met["gleason_score"] >= 8),
    (df_km_met["age_at_diagnosis"] < 65) & (df_km_met["gleason_score"] < 8),
    (df_km_met["age_at_diagnosis"] >= 65) & (df_km_met["gleason_score"] >= 8),
    (df_km_met["age_at_diagnosis"] >= 65) & (df_km_met["gleason_score"] < 8),
]
df_km_met["cohort_control_4_groups"] = np.select(conditions, [1, 2, 3, 4], default=np.nan)

# Table One erstellen
cols_to_analyze = ["age_at_diagnosis", "gleason_score", "asserted_year", "year_bin", "m_tnm_clean"]
cat_cols = ["gleason_score", "year_bin", "m_tnm_clean"]

table1 = TableOne(
    df_km_met,
    columns=cols_to_analyze,
    categorical=cat_cols,
    groupby="cohort_flag",
    pval=True,
    htest_name=True,
    rename={
        "age_at_diagnosis": "Age at Diagnosis",
        "gleason_score": "Gleason Score",
        "asserted_year": "Diagnosis Year",
        "year_bin": "Diagnosis Era",
        "m_tnm_clean": "M-Stage",
    },
    sort=False,
)

print("\n--- Baseline Characteristics: Cohort vs. Control ---")
print(table1)


# 8. KAPLAN-MEIER PLOTS

# Cohort vs Rest
km_lifeline(
    data=df_km_met,
    event_col="event",
    time_col="survival_time_months",
    cat_col="cohort_flag",
    cat_names=[(1, "cohort"), (0, "control")],
    title=f"C61 OS Cohort vs Control, diagnosis year {ASSERTED_MIN}-{ASSERTED_MAX}, {sites}",
    stat=True,
    show_stat=True,
)

# Survival by Era (Diagnosis Years) + asserted_year
cat_names_year_bin = [
    (label, label) for label in labels if label in df_km_met["year_bin"].unique().astype(str)
]
km_lifeline(
    data=df_km_met,
    event_col="event",
    time_col="survival_time_months",
    cat_col="year_bin",
    cat_names=cat_names_year_bin,
    title=f"OS by Era (Diagnosis Years), {sites}",
    stat=True,
    show_stat=True,
)

# single years
years_in_data = sorted(df_km_met["asserted_year"].dropna().unique())
cat_names_asserted_years = [(year, str(int(year))) for year in years_in_data]
km_lifeline(
    data=df_km_met,
    event_col="event",
    time_col="survival_time_months",
    cat_col="asserted_year",
    cat_names=cat_names_asserted_years,
    title=f"OS by Era (Diagnosis Years), {sites}",
    stat=True,
    show_stat=True,
    xlim=(0, 85),
)

# Survival by 4-Groups
df_km_4g = df_km_met.dropna(subset=["cohort_control_4_groups"]).copy()
km_lifeline(
    data=df_km_4g,
    event_col="event",
    time_col="survival_time_months",
    cat_col="cohort_control_4_groups",
    cat_names=[
        (1, "young high grade (cohort)"),
        (2, "young low grade (control)"),
        (3, "old high grade (control)"),
        (4, "old low grade (control)"),
    ],
    title=f"C61 OS 4-Groups, diagnosis year {ASSERTED_MIN}-{ASSERTED_MAX}, {sites}",
    stat=True,
    show_stat=True,
    xlim=(0, 85),
)


# 9. COX REGRESSION MODELS
print("\n" + "=" * 50)
print("COX REGRESSION: UNADJUSTED (COHORT VS CONTROL)")
print("=" * 50)
df_unadj = df_km_met[["survival_time_months", "event", "cohort_flag"]].copy().dropna()
cph_unadj = CoxPHFitter()
cph_unadj.fit(df_unadj, duration_col="survival_time_months", event_col="event")
cph_unadj.print_summary()
cph_unadj.plot()


print("\n" + "=" * 50)
print("COX REGRESSION: ADJUSTED (COHORT VS CONTROL)")
print("=" * 50)
cols_for_model = [
    "survival_time_months",
    "event",
    "cohort_flag",
    "age_at_diagnosis",
    "gleason_score",
    "m_tnm_clean",
    # "year_bin",
    "asserted_year",
]
df_adj = df_km_met[cols_for_model].copy()

# Datentypen und Kategorien vorbereiten
df_adj["gleason_score"] = df_adj["gleason_score"].astype(str)
gleason_order = ["6.0", "7.0", "8.0", "9.0", "10.0"]
df_adj["gleason_score"] = pd.Categorical(
    df_adj["gleason_score"], categories=gleason_order, ordered=True
)

df_adj["m_tnm_clean"] = df_adj["m_tnm_clean"].astype(str).replace("nan", "Unknown")
# df_adj["year_bin"] = df_adj["year_bin"].astype(str).replace("nan", "Unknown")
df_adj["cohort_flag"] = df_adj["cohort_flag"].astype(str)

# Dummies erstellen - one-hot (drop_first=True nutzt automatisch die erste Kategorie als Basis)
df_adj_enc = pd.get_dummies(
    df_adj,
    columns=["cohort_flag", "m_tnm_clean", "gleason_score"],
    drop_first=True,
    dtype=int,
)
df_adj_enc = df_adj_enc.dropna()

cph_adj = CoxPHFitter()
cph_adj.fit(df_adj_enc, duration_col="survival_time_months", event_col="event")
cph_adj.print_summary()
cph_adj.plot()


print("\n" + "=" * 50)
print("COX REGRESSION: 4-GROUPS UNADJUSTED")
print("=" * 50)

# Nur die benötigten Spalten behalten und NAs entfernen
cols_for_4g_unadj = ["survival_time_months", "event", "cohort_control_4_groups"]
df_4g_unadj = df_km_met[cols_for_4g_unadj].copy().dropna(subset=["cohort_control_4_groups"])

# Labels für die Lesbarkeit mappen
df_4g_unadj["cohort_control_groups_label"] = df_4g_unadj["cohort_control_4_groups"].map(
    {
        1: "young_high_grade_cohort",
        2: "young_low_grade_control",
        3: "old_high_grade_control",
        4: "old_low_grade_control",
    }
)
df_4g_unadj = df_4g_unadj.drop(columns=["cohort_control_4_groups"])

# Kategoriale Reihenfolge festlegen (erstes Element wird automatisch die Referenz!)
group_order = [
    "young_high_grade_cohort",  # Basislinie (Hazard Ratio = 1.0)
    "young_low_grade_control",
    "old_high_grade_control",
    "old_low_grade_control",
]
df_4g_unadj["cohort_control_groups_label"] = pd.Categorical(
    df_4g_unadj["cohort_control_groups_label"], categories=group_order, ordered=True
)

# Dummy-Encoding durchführen
df_4g_unadj_enc = pd.get_dummies(
    df_4g_unadj,
    columns=["cohort_control_groups_label"],
    drop_first=True,
    dtype=int,
)

df_4g_unadj_enc = df_4g_unadj_enc.dropna()

cph_4g_unadj = CoxPHFitter()
cph_4g_unadj.fit(df_4g_unadj_enc, duration_col="survival_time_months", event_col="event")

cph_4g_unadj.print_summary()
cph_4g_unadj.plot()


print("\n" + "=" * 50)
print("COX REGRESSION: 4-GROUPS ADJUSTED")
print("=" * 50)
cols_for_4g = [
    "survival_time_months",
    "event",
    "cohort_control_4_groups",
    "m_tnm_clean",
    # "year_bin",
    "asserted_year",
]
df_4g_model = df_km_met[cols_for_4g].copy().dropna(subset=["cohort_control_4_groups"])

df_4g_model["cohort_control_groups_label"] = df_4g_model["cohort_control_4_groups"].map(
    {
        1: "young_high_grade_cohort",
        2: "young_low_grade_control",
        3: "old_high_grade_control",
        4: "old_low_grade_control",
    }
)
df_4g_model = df_4g_model.drop(columns=["cohort_control_4_groups"])

df_4g_model["m_tnm_clean"] = df_4g_model["m_tnm_clean"].astype(str).replace("nan", "Unknown")
# df_4g_model["year_bin"] = df_4g_model["year_bin"].astype(str).replace("nan", "Unknown")

group_order = [
    "young_high_grade_cohort",  # Basislinie
    "young_low_grade_control",
    "old_high_grade_control",
    "old_low_grade_control",
]
df_4g_model["cohort_control_groups_label"] = pd.Categorical(
    df_4g_model["cohort_control_groups_label"], categories=group_order, ordered=True
)

df_4g_enc = pd.get_dummies(
    df_4g_model,
    columns=["cohort_control_groups_label", "m_tnm_clean"],
    drop_first=True,
    dtype=int,
)
df_4g_enc = df_4g_enc.dropna()

cph_4g = CoxPHFitter()
cph_4g.fit(df_4g_enc, duration_col="survival_time_months", event_col="event")
cph_4g.print_summary()
cph_4g.plot()


print("\n" + "=" * 50)
print("COX REGRESSION: INTERACTION (AGE * GLEASON)")
print("=" * 50)

df_interaction = df_adj.copy().dropna()


formula_age_gleason = "age_at_diagnosis * gleason_score + cohort_flag + m_tnm_clean"

cph_int = CoxPHFitter()

cph_int.fit(
    df_interaction,
    duration_col="survival_time_months",
    event_col="event",
    formula=formula_age_gleason,
)  # <- Hier wählen Sie formula_1 oder formula_2

cph_int.print_summary()
cph_int.plot()


# save df for Nomogram - do it in R
# model 1 - 2 groups
df_adj_enc.to_csv("prostate_data_for_nomogram.csv", index=False)

# model 2 - 4 groups
df_4g_enc.to_csv("prostate_data_for_nomogram_4groups.csv", index=False)
# interaction
df_interaction.to_csv("prostate_data_for_nomogram_interaction_age_gleason.csv", index=False)
