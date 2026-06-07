import sys

import numpy as np
import pandas as pd
from IPython.display import display

print(sys.path)
from analytics_on_fhir.analysis.utils.survival_utils import km_lifeline, plot_survival_cohort_pca

report = {}

sites = "UKER, TUM, UKA, LMU, UKR, UKW"
# sites = "UKER"
# sites = "TUM"
# sites = "UKA"
# sites = "LMU"
# sites = "UKR"
# sites = "UKW"

asserted_year = 1970

# COHORT or control for labs
# cohort_or_control = "cohort"

obds_paths = []
vital_paths = []
tnm_m_paths = []

for site in [s.strip().lower() for s in sites.split(",")]:
    obds_path = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/all_obds_patients_analysis/all_obds_data_allsites/{site}/parquet/df_all_obds_clean_deidentified.parquet"
    vital_path = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/all_obds_patients_analysis/all_obds_data_allsites/{site}/parquet/df_vitalstatus_deidentified.parquet"
    tnm_m_path = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/all_obds_patients_analysis/all_obds_data_allsites/{site}/parquet/df_m_tnm_deidentified.parquet"

    obds_paths.append(obds_path)
    vital_paths.append(vital_path)
    tnm_m_paths.append(tnm_m_path)

print(obds_paths)
print(vital_paths)
print(tnm_m_paths)


OBDS_DTYPES = {
    "asserted_year": "Int64",
    "age_at_diagnosis": "Float64",
    # "cohort_flag": "Int64",
    "is_deceased": "boolean",
    "icd10_code": "string",
}

VITAL_DTYPES = {
    "months_between_asserted_effective_dateTime": "Float64",
    "months_between_asserted_deceased_datetime": "Float64",
    "months_between_asserted_date_death": "Float64",
    "icd10_code": "string",
}

dfs_obds = []
for path in obds_paths:
    tmp = pd.read_parquet(path)

    for col, dtype in OBDS_DTYPES.items():
        if col in tmp.columns:
            tmp[col] = tmp[col].astype(dtype)

    if "patient_resource_id_hash" in tmp.columns:
        tmp["patient_resource_id_hash"] = tmp["patient_resource_id_hash"].astype("string")

    if "condition_id_hash" in tmp.columns:
        tmp["condition_id_hash"] = tmp["condition_id_hash"].astype("string")

    if "icd10_code" in tmp.columns:
        tmp["icd10_code"] = tmp["icd10_code"].astype("string")

    tmp["site"] = path.split("data_allsites/")[1].split("/parquet")[0]
    dfs_obds.append(tmp)

df_obds = pd.concat(dfs_obds, ignore_index=True)
print(df_obds.shape[0])
# remove C44 and all D* ICD10 codes
df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
print(df_obds.shape[0])
df_obds = df_obds[df_obds["asserted_year"] >= asserted_year]
print(df_obds.shape[0])
df_obds.head()


# drop duplicates
df_obds = df_obds.drop_duplicates()
print(df_obds.shape[0])
df_obds.head()
report[f"Initial tumor records (diagnosis year ≥ {asserted_year})"] = df_obds.shape[0]

# ----------------------------------
# Basiszahlen
# ----------------------------------

n_rows = len(df_obds)
n_conditions = df_obds["condition_id_hash"].nunique()
n_patients = df_obds["patient_resource_id_hash"].nunique()

print(f"Rows:                     {n_rows:,}")
print(f"Unique conditions:        {n_conditions:,}")
print(f"Unique patients:          {n_patients:,}")

# ----------------------------------
# Gender (absolut + relativ)
# ----------------------------------

gender_counts = (
    df_obds["gender"].fillna("missing").value_counts(dropna=False).rename("n").to_frame()
)

gender_counts["pct"] = (gender_counts["n"] / gender_counts["n"].sum() * 100).round(2)

print("\nGender distribution")
print(gender_counts)

# Optional nur Male/Female
gender_mf = (
    df_obds[df_obds["gender"].isin(["male", "female"])]["gender"]
    .value_counts()
    .rename("n")
    .to_frame()
)

gender_mf["pct"] = (gender_mf["n"] / gender_mf["n"].sum() * 100).round(2)

print("\nGender distribution (male/female only)")
print(gender_mf)

# ----------------------------------
# Top 20 Entitäten
# ----------------------------------

top20_entities = (
    df_obds["entity_or_parent"]
    .fillna("missing")
    .value_counts()
    .head(20)
    .rename_axis("entity_or_parent")
    .reset_index(name="n")
)

top20_entities["pct"] = (top20_entities["n"] / len(df_obds) * 100).round(2)

print("\nTop 20 entity_or_parent")
print(top20_entities)

dfs_vital = []
for path in vital_paths:
    tmp = pd.read_parquet(path)

    for col, dtype in VITAL_DTYPES.items():
        if col in tmp.columns:
            tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype(dtype)

    if "patient_resource_id_hash" in tmp.columns:
        tmp["patient_resource_id_hash"] = tmp["patient_resource_id_hash"].astype("string")

    if "condition_id_hash" in tmp.columns:
        tmp["condition_id_hash"] = tmp["condition_id_hash"].astype("string")

    if "vitalstatus_code" in tmp.columns:
        tmp["vitalstatus_code"] = tmp["vitalstatus_code"].astype("string").str.strip().str.upper()

    tmp["site"] = path.split("data_allsites/")[1].split("/parquet")[0]
    dfs_vital.append(tmp)

df_vital = pd.concat(dfs_vital, ignore_index=True)

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

report["Only latest available vital status, remove duplicates"] = df_vital_latest.shape[0]


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


# JOIN FIRST TUMOR DF WITH LATEST VITALSTATUS
df_join = df_obds_first_tumor.merge(
    df_vital_latest, on="patient_resource_id_hash", how="left", suffixes=("", "_vital")
)
report["Patients after join with vital status data"] = df_join.shape[0]
report["C61 patients"] = (df_join["icd10_code"] == "C61").sum()
# df_join.to_csv("dj_join.csv", index=False)
df_join.head(50)
print(df_join.shape[0])

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

# remove NA events
unknown_event_mask = df_join["event"].isna()
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


# censored patients event=0
report["Censored patients (event=0) with follow-up information"] = (df_km["event"] == 0).sum()

# report
summary = pd.DataFrame.from_dict(report, orient="index", columns=["count"])
summary[f"% of initial tumor records (diagnosis year ≥ {asserted_year})"] = (
    summary["count"] / report[f"Initial tumor records (diagnosis year ≥ {asserted_year})"] * 100
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

# top 20
top20_entities = df_km["entity_or_parent"].value_counts().head(20).index.tolist()

# cat_names automatisch erzeugen
cat_names = [(entity, f"{entity}") for entity in top20_entities]

print(cat_names)

km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top20_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names,
    title=f"Survival by Top 20 Tumor Entities, diagnoses > {asserted_year}, {sites}",
    stat=True,
    show_stat=False,
    fig_size=(14, 10),
)

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

# cohort diagram
LEFT_STEPS = [
    f"Initial tumor records (diagnosis year ≥ {asserted_year})",
    "Patients after first-tumor selection",
]
RIGHT_STEPS = [
    "Vital status records",
    "Only latest available vital status, remove duplicates",
    "Patients with non-negative follow-up time",
]
CENTER_STEPS = [
    "Patients after join with vital status data",
    "Excluded: unknown event status",
    "Excluded: missing survival time",
    "Excluded: survival time >100 years",
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

TNM_M_DTYPES = {
    "asserted_year": "Int64",
    "age_at_diagnosis": "Float64",
    "months_between_asserted_m_tnm_date": "Float64",
    "site": "string",
    "m_tnm": "string",
}


dfs_tnm_m = []
for path in tnm_m_paths:
    tmp = pd.read_parquet(path)

    for col, dtype in TNM_M_DTYPES.items():
        if col in tmp.columns:
            tmp[col] = tmp[col].astype(dtype)

    # if "patient_resource_id_hash" in tmp.columns:
    #     tmp["patient_resource_id_hash"] = tmp["patient_resource_id_hash"].astype("string")

    if "condition_id_hash" in tmp.columns:
        tmp["condition_id_hash"] = tmp["condition_id_hash"].astype("string")

    # if "icd10_code" in tmp.columns:
    #     tmp["icd10_code"] = tmp["icd10_code"].astype("string")

    tmp["site"] = path.split("data_allsites/")[1].split("/parquet")[0]
    dfs_tnm_m.append(tmp)

df_tnm_m = pd.concat(dfs_tnm_m, ignore_index=True)
df_tnm_m["m_tnm_clean"] = df_tnm_m["m_tnm"].astype("string").str.strip()
df_tnm_m["m_tnm_clean"].value_counts()
df_tnm_m.head()

df_tnm_m = df_tnm_m[df_tnm_m["asserted_year"] >= asserted_year]
df_tnm_m.head()
df_tnm_m.shape
df_tnm_m["condition_id_hash"].nunique()  # 24066


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

df_tnm_unique = (
    df_tnm_m.sort_values("m_tnm_clean", ascending=False)
    .groupby("condition_id_hash")
    .first()
    .reset_index()
)

print(
    f"\nEinzigartige Krankheitsfälle mit M-Status innerhalb von 4 Monaten: {df_tnm_unique['condition_id_hash'].nunique()}"
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
    title=f"OS by Metastasis Stage (TNM-M), diagnosis > {asserted_year}, {sites}",
    stat=True,
    show_stat=True,
)
