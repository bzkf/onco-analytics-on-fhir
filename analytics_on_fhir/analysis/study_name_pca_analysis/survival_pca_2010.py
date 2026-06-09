import sys

import numpy as np
import pandas as pd
from IPython.display import display

print(sys.path)
from analytics_on_fhir.analysis.utils.survival_utils import km_lifeline, plot_survival_cohort_pca

report = {}

sites = "UKER, TUM, UKA, LMU"
# sites = "UKER"
# sites = "TUM"
# sites = "UKA"
# sites = "LMU"

asserted_year = 2010

# COHORT or control for labs
cohort_or_control = "cohort"

obds_paths = []
vital_paths = []
tnm_m_paths = []

for site in [s.strip().lower() for s in sites.split(",")]:
    obds_path = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/study_name_pca_analysis/pca1_data_allsites/{site}/parquet/df_c61_conditions_patients_death_gleason_met_clean_deidentified.parquet"
    vital_path = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/study_name_pca_analysis/pca1_data_allsites/{site}/parquet/df_vitalstatus_deidentified.parquet"
    tnm_m_path = f"/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/analysis/study_name_pca_analysis/pca1_data_allsites/{site}/parquet/df_m_tnm_deidentified.parquet"

    obds_paths.append(obds_path)
    vital_paths.append(vital_path)
    tnm_m_paths.append(tnm_m_path)

print(obds_paths)
print(vital_paths)
print(tnm_m_paths)


OBDS_DTYPES = {
    "asserted_year": "Int64",
    "age_at_diagnosis": "Float64",
    "cohort_flag": "Int64",
    "is_deceased": "boolean",
}

VITAL_DTYPES = {
    "months_between_asserted_effective_dateTime": "Float64",
    "months_between_asserted_deceased_datetime": "Float64",
    "months_between_asserted_date_death": "Float64",
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
# remove C44 and all D* ICD10 codes
# df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
df_obds = df_obds[df_obds["asserted_year"] >= asserted_year]

df_obds.head()
print(df_obds.shape[0])
# drop all metastasis cols as they duplicate the counts
# then drop duplicates
df_obds = df_obds.loc[:, ~df_obds.columns.str.contains("metastasis", case=False, na=False)]
df_obds = df_obds.drop_duplicates()
print(df_obds.shape[0])
df_obds.head()
report[f"Initial tumor records (diagnosis year ≥ {asserted_year})"] = df_obds.shape[0]


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

df_vital.head()
report["Vital status records"] = df_vital.shape[0]


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

report["Only latest available vital status, remove duplicates"] = df_vital_latest.shape[0]


# remove negative months_between_asserted_effective_dateTime
df_vital_latest = df_vital_latest[
    df_vital_latest["months_between_asserted_effective_dateTime"] >= 0
].copy()

df_vital_latest.head(50)
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

# check unknown events
df_join.head()
unknown_event_mask = df_join["event"].isna()
# year distribution of excluded patients
unknown_year_dist = (
    df_join.loc[unknown_event_mask, "asserted_year"].value_counts(dropna=False).sort_index()
)

print(unknown_year_dist)
unknown_year_dist.sum()
unknown_year_dist.head()

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
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col=None,
    title=f"C61 Overall Survival > {asserted_year}, {sites}",
)

# entities cohort vs rest
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="cohort_flag",
    cat_names=[(1, "cohort"), (0, "control")],
    title=f"C61 Survival cohort vs control > {asserted_year}, {sites}",
    stat=True,
    show_stat=True,
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
    title=f"C61 Survival Cohort: {sites} - diagnoses ≥ {asserted_year}",
    left_steps=LEFT_STEPS,
    right_steps=RIGHT_STEPS,
    center_steps=CENTER_STEPS,
)


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
    title=f"OS by PSA response, {cohort_or_control}, diagnosis > {asserted_year}, {sites}",
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
target_m_stages = ["M0", "M1a", "M1b", "M1c"]
df_tnm_m = df_tnm_m[df_tnm_m["m_tnm_clean"].isin(target_m_stages)].copy()
df_tnm_m["condition_id_hash"].nunique()
df_tnm_m["m_tnm_clean"].value_counts()

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
    f"\nEinzigartige Krankheitsfälle mit M-Status innerhalb von 4 Monaten: {df_tnm_unique['condition_id_hash'].nunique()}"
)
df_tnm_unique.head(50)
print(df_tnm_unique["m_tnm_clean"].value_counts())

if cohort_or_control == "cohort":
    df_km_filtered = df_km[df_km["cohort_flag"] == 1].copy()
else:
    df_km_filtered = df_km[df_km["cohort_flag"] == 0].copy()

# 5. INNER JOIN an den tnm_m DF (wir wollen nur Patienten mit bekannter M-Klassifikation plotten)
df_km_tnm = df_km_filtered.merge(
    df_tnm_unique[["condition_id_hash", "m_tnm_clean"]], on="condition_id_hash", how="inner"
)

df_km_tnm.head()
print(f"Verbleibende Patienten im KM-Plot nach TNM-M Match: {df_km_tnm.shape[0]}")
print(df_km_tnm["m_tnm_clean"].value_counts())

km_lifeline(
    data=df_km_tnm,
    event_col="event",
    time_col="survival_time_months",
    cat_col="m_tnm_clean",
    cat_names=[
        ("M0", "M0"),
        ("M1a", "M1a"),
        ("M1b", "M1b"),
        ("M1c", "M1c"),
    ],
    title=f"OS by Metastasis Stage (TNM-M), {cohort_or_control}, diagnosis > {asserted_year}, {sites}",
    stat=True,
    show_stat=True,
)
