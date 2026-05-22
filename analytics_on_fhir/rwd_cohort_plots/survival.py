import numpy as np
import pandas as pd
from IPython.display import display
from survival_utils import km_lifeline

report = {}

# File paths
obds_path = "/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/results/all_obds_patients/deidentified/parquet/df_all_obds_clean_deidentified.parquet"

vital_path = "/home/coder/git/onco-analytics-on-fhir/analytics_on_fhir/results/all_obds_patients/deidentified/parquet/df_vitalstatus_deidentified.parquet"

# Read parquet files
df_obds = pd.read_parquet(obds_path)

df_vital = pd.read_parquet(vital_path)[
    [
        "vitalstatus_code",
        "patient_resource_id_hash",
        "months_between_asserted_effective_dateTime",
        "condition_id_hash",
        "icd10_code",
    ]
]

df_obds.head()
df_vital.head()

report["all_obds"] = df_obds.shape[0]
report["all_vital"] = df_vital.shape[0]

# remove C44 and all D* ICD10 codes
df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
df_vital = df_vital[~df_vital["icd10_code"].str.startswith(("C44", "D"), na=False)]

report["initial_obds_without_c44_d*"] = df_obds.shape[0]
report["initial_vital_without_c44_d*"] = df_vital.shape[0]


# only the latest vitalstatus entry per patient
# based on the largest follow-up time
idx = df_vital.groupby("patient_resource_id_hash")[
    "months_between_asserted_effective_dateTime"
].idxmax()

df_vital_latest = (
    df_vital.loc[idx]
    .reset_index(drop=True)
    .sort_values("months_between_asserted_effective_dateTime")
)

df_vital_latest.head(50)

report["df_vital_latest"] = df_vital_latest.shape[0]


# remove negative months_between_asserted_effective_dateTime
df_vital_latest = df_vital_latest[
    df_vital_latest["months_between_asserted_effective_dateTime"] >= 0
].copy()

df_vital_latest.head(50)
report["df_vital_latest_negative_months_between_asserted_effective_dateTime_removed"] = (
    df_vital_latest.shape[0]
)
report["distinct_patients in df_vital"] = df_vital_latest["patient_resource_id_hash"].nunique()


# in df_obds, groupby patient_resource_id_hash and only keep first tumor
# with smallest age_at_diagnosis
# wichtig für unabhängige Beobachtung Kaplan Meier --> nur für Erstdiagnose hier
df_obds_first_tumor = (
    df_obds.sort_values("age_at_diagnosis")
    .groupby("patient_resource_id_hash")
    .first()
    .reset_index()
)

df_obds_first_tumor.head()
print("Distinct patients:", df_obds_first_tumor["patient_resource_id_hash"].nunique())
report["obds_after_first_tumor"] = df_obds_first_tumor.shape[0]
report["distinct_patients_obds_after_first_tumor"] = df_obds_first_tumor[
    "patient_resource_id_hash"
].nunique()

# ---------------------------------------------------
# JOIN FIRST TUMOR DF WITH LATEST VITALSTATUS
# ---------------------------------------------------
df_join = df_obds_first_tumor.merge(
    df_vital_latest, on="patient_resource_id_hash", how="left", suffixes=("", "_vital")
)
report["df_join"] = df_join.shape[0]

# ---------------------------------------------------
# COMPARE is_deceased vs vitalstatus_code
# ---------------------------------------------------

comparison = pd.crosstab(df_join["is_deceased"], df_join["vitalstatus_code"], dropna=False)

print(comparison)

comparison
comparison_long = comparison.reset_index().melt(
    id_vars="is_deceased", var_name="vitalstatus_code", value_name="count"
)

comparison_pct = (
    pd.crosstab(
        df_join["is_deceased"], df_join["vitalstatus_code"], normalize="index", dropna=False
    )
    * 100
)
comparison_pct = (
    (
        pd.crosstab(
            df_join["is_deceased"], df_join["vitalstatus_code"], normalize="index", dropna=False
        )
        * 100
    )
    .reset_index()
    .melt(id_vars="is_deceased", var_name="vitalstatus_code", value_name="percent")
)

comparison_merged = comparison_long.merge(
    comparison_pct, on=["is_deceased", "vitalstatus_code"], how="outer"
)

comparison_merged

df_true_L = df_join[(df_join["is_deceased"] == True) & (df_join["vitalstatus_code"] == "L")].copy()

print(df_true_L.shape)
df_true_L


# event = 1 = dead
df_join["event"] = ((df_join["is_deceased"]) | (df_join["vitalstatus_code"] == "T")).astype(int)
report["event_1_total"] = df_join["event"].sum()

# kombiniere beide spalten
df_join["death_time"] = df_join["months_between_asserted_deceased_datetime"].combine_first(
    df_join["months_between_asserted_date_death"]
)

report["missing_death_time_events"] = (
    (df_join["event"] == 1) & (df_join["death_time"].isna())
).sum()


# follow up time
df_join["followup_time"] = df_join["months_between_asserted_effective_dateTime"]

report["event_0_with_followup_time"] = (
    (df_join["event"] == 0) & (df_join["followup_time"].notna())
).sum()

# final survival time
df_join["survival_time_months"] = np.where(
    df_join["event"] == 1, df_join["death_time"], df_join["followup_time"]
)


# remove implausible survival times (>100 years)
n_removed_survival_gt_1200 = (df_join["survival_time_months"] > 1200).sum()

df_join = df_join[df_join["survival_time_months"] <= 1200]

# add to report
report["removed_survival_time_gt_1200_months"] = int(n_removed_survival_gt_1200)

report["final_km_population_after_survival_filter"] = int(len(df_join))

df_join.shape

# remove missing survival times
before_km = df_join.shape[0]

df_km = df_join[df_join["survival_time_months"].notna()].copy()

report["removed_missing_survival_time"] = before_km - df_km.shape[0]
report["final_km_population"] = df_km.shape[0]


df_km
df_km["survival_time_months"].describe()
df_km.sort_values("survival_time_months").tail(20)

df_km = df_km[~df_km["icd10_code"].str.startswith(("C44", "D"), na=False)]
report["df_km_removed_C44_D*_again"] = df_km.shape[0]


# remove asserted year before 1998
# 1998: Beginn der systematischen epidemiologische Krebsregistrierung in Bayern
# add to report
# report["asserted_year_before_1998_removed"] = (df_km["asserted_year"] < 1998).sum()

# df_km = df_km[df_km["asserted_year"] >= 1998]

# report["final_km_population_after_1998_filter"] = df_km.shape[0]


# report
summary = pd.DataFrame.from_dict(report, orient="index", columns=["count"])
summary["percent_of_initial_without_c44_d*"] = (
    summary["count"] / report["initial_obds_without_c44_d*"]
) * 100

summary

display(
    summary.style.set_properties(**{"text-align": "left"}).set_table_styles(
        [{"selector": "th.row_heading", "props": [("min-width", "300px")]}]
    )
)

comparison_merged

# plot
# gesamt km
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col=None,
    title="Overall Survival",
)

# gender
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="gender",
    cat_names=[("male", "Male"), ("female", "Female")],
    title="Survival by Gender",
    stat=True,
    show_stat=True,
)

# entities breast vs prostate
km_lifeline(
    data=df_km,
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=[("C50", "Breast"), ("C61", "Prostate")],
    title="Survival by Tumor Entity",
    stat=True,
    show_stat=True,
)

# TO DO - MULTIVARIATE TESTING if we do multiple categories

# top 20 entities nach Häufigkeit bestimmen
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
    title="Survival by Top 20 Tumor Entities",
    stat=False,  # bei 20 Gruppen sonst extrem viele pairwise Tests - extra testen + multivariate
    show_stat=False,
    fig_size=(14, 10),
)

# top 10 entities nach Häufigkeit bestimmen
top10_entities = df_km["entity_or_parent"].value_counts().head(10).index.tolist()

# cat_names automatisch erzeugen
cat_names = [(entity, f"{entity}") for entity in top20_entities]

print(cat_names)

km_lifeline(
    data=df_km[df_km["entity_or_parent"].isin(top10_entities)],
    event_col="event",
    time_col="survival_time_months",
    cat_col="entity_or_parent",
    cat_names=cat_names,
    title="Survival by Top 10 Tumor Entities",
    stat=False,
    show_stat=False,
    fig_size=(14, 10),
)
