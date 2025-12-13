from utils_onco_analytics import (

    datashield_preps_wrapper,
    extract_df_study_protocol_a0_1_3_7_d,
    save_final_df,
    calculate_age_at_condition_date_udf,
    extract_filter_df_c61,
    extract_pca_ops,
    extract_pca_st,
    extract_pca_sys,
    union_sort_pivot_join,
    add_deceased_flag,
    add_age_col,
    plot
)
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from lifelines.utils import datetimes_to_durations, survival_table_from_events, median_survival_times
from pathling import PathlingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pydantic import BaseSettings

class Settings(BaseSettings):
    output_folder: str = "./P-Ca/output_2025"
    output_filename: str = "df.csv"
    study_name: str = "pca_therapies"
    spark_checkpoint_dir: str = "/tmp/spark-checkpoints/fhir-to-opal"

settings = Settings()

spark = (
    SparkSession.builder.master("local")
    .appName("encounter-test")
    .config(
        "spark.driver.memory",
        "100g",
    )
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir(settings.spark_checkpoint_dir)

pc = PathlingContext.create(spark, enable_delta=True)

data = pc.read.tables("obds_qs_v2")

df_all = extract_df_study_protocol_a0_1_3_7_d(pc, data, settings, spark)
save_final_df(df_all, settings, suffix="all")


df_c61= extract_filter_df_c61(pc, data, settings, spark)
df_c61.printSchema()
df_c61.show(5)
#df_c61 = add_age_col(df = df_c61, birthdate_col = "birthdate", refdate_col = "date_diagnosis", age_colname = "age_at_diagnosis")

# CAST ALL DATES
# Liste der Datumsfelder
date_cols = [
    "date_diagnosis",
    "deceased_datetime",
    "date_death",
    "gleason_date_first"
]

for col_name in date_cols:
    df_c61 = df_c61.withColumn(col_name, F.to_date(F.col(col_name)))

# birthdate speziell (YYYY-MM → YYYY-MM-15)
df_c61 = df_c61.withColumn(
    "birthdate",
    F.to_date(F.concat_ws("-", F.col("birthdate"), F.lit("15")), "yyyy-MM-dd")
)

# Kontrolle
df_c61.printSchema()
df_c61.show(5)

# calculate years between diagnosis and death or today
df_c61 = df_c61.withColumn(
    "years_alive",
    F.months_between(
        F.coalesce(F.col("deceased_datetime"), F.current_date()),
        F.col("date_diagnosis")
    ) / 12
)
df_c61 = df_c61.orderBy(F.col("years_alive").desc())
df_c61.show()
print(f"ROWS c61: {df_c61.count()}")

#save_final_df(df_c61, settings, suffix="old")

# age new mit day = 15
def add_age_col(df, birthdate_col, refdate_col, age_colname):
    df = df.withColumn(
        age_colname,
        F.months_between(F.col(refdate_col), F.col("birthdate")) / 12
    )
    return df

df_c61 = add_age_col(
    df=df_c61,
    birthdate_col="birthdate",
    refdate_col="date_diagnosis",
    age_colname="age_at_diagnosis"
)
df_c61 = df_c61.withColumn("age_at_diagnosis", F.round(F.col("age_at_diagnosis"), 2))
df_c61 = df_c61.withColumn("years_alive", F.round(F.col("years_alive"), 2))
df_c61.printSchema()
df_c61.show()

save_final_df(df_c61, settings, suffix="new")


# DATENBEREINIGUNG
# alle Diagnosen vor 1950 weil da teilweise Diagnosedatum=Geburtsdatum gesetzt wurde oder Diagnosedatum 1800
df_c61 = df_c61.filter(F.col("date_diagnosis") > F.lit("1950-01-01"))
df_c61 = df_c61.orderBy(F.col("years_alive").desc())
df_c61.show()
print(f"ROWS c61: {df_c61.count()}")
# age_at_diagnosis >30
df_c61 = df_c61.filter(F.col("age_at_diagnosis") > 30)
print(f"ROWS c61: {df_c61.count()}")

# cohort flag
df_c61 = df_c61.withColumn(
        "young_highrisk_cohort",
        F.when((F.col("age_at_diagnosis") < 65) & (F.col("gleason") >= 8), "cohort")
         .otherwise("rest")
)
df_c61.show()

# Gruppieren nach cohort/rest und zählen
df_c61.groupBy("young_highrisk_cohort") \
       .count() \
       .show()

## DESCRIPTIVE STATISTICS
# summary stats all c61 patients
df_c61.describe().show()
# bei unter 6000 rows kein Problem alles nach pandas zu holen
pdf = df_c61.toPandas()

#  datetime64 - TO DO MIT ALLEN DATUMS???
pdf["date_diagnosis"] = pd.to_datetime(pdf["date_diagnosis"], errors="coerce")
pdf["deceased_datetime"] = pd.to_datetime(pdf["deceased_datetime"], errors="coerce")

# COHORT vs REST
# Gruppieren nach cohort/rest und zählen
pdf_counts = pdf.groupby("young_highrisk_cohort").size().reset_index(name="count")

# Figure und Axes erstellen
sns.set(style="whitegrid")
fig, ax = plt.subplots(figsize=(6,4))

# Barplot auf dem Axes-Objekt
sns.barplot(x="young_highrisk_cohort", y="count", data=pdf_counts, palette="Set2", ax=ax)
ax.set_title("Verteilung: Young High-Risk Cohort vs Rest")
ax.set_xlabel("Gruppe")
ax.set_ylabel("Anzahl Patienten")

# Counts über den Balken anzeigen
for p in ax.patches:
    height = p.get_height()
    ax.text(p.get_x() + p.get_width()/2., height + 5, int(height), ha="center", fontsize=12)

plt.show()


# HISTOGRAMS, BOX PLOTS
total_patients = len(pdf)
numeric_cols = ["gleason", "age_at_diagnosis", "years_alive"]
for col in numeric_cols:
    fig, ax = plt.subplots(1, 2, figsize=(10, 4))
    counts, bins, patches = ax[0].hist(
        pdf[col].dropna(),
        bins=30,
        edgecolor='black',
        alpha=0.7
    )
    ax[0].set_ylim(0, counts.max() * 1.15)
    for count, x in zip(counts, bins[:-1]):
        if count > 0:
            pct = (count / total_patients) * 100
            ax[0].text(
                x + (bins[1] - bins[0]) / 2,
                count + counts.max() * 0.01,
                f"{pct:.1f}%",
                ha='center',
                va='bottom',
                fontsize=7,
                rotation=90,
                color='black'
            )
    valid_n = pdf[col].notna().sum()
    ax[0].set_title(f"Distribution of {col}\n({valid_n} valid / {total_patients} total)")
    ax[0].set_ylabel("Count (bars), % of total (labels)")
    ax[0].set_xlabel(col)

    sns.boxplot(x=pdf[col], ax=ax[1], color="lightblue")
    ax[1].set_title(f"Boxplot of {col}")
    plt.tight_layout(pad=3)
    plt.show()

# BARPLOTS
cat_cols = ["death_cause_icd10"]

for col in cat_cols:
    plt.figure(figsize=(8,4))
    pdf[col].value_counts().head(20).plot(kind="bar")
    plt.title(f"Top 20 {col} categories")
    plt.ylabel("Count")
    plt.show()

pdf["year_diagnosis"] = pdf["date_diagnosis"].dt.year
pdf["year_death"] = pdf["deceased_datetime"].dt.year

plt.figure(figsize=(8,4))
pdf["year_diagnosis"].value_counts().sort_index().plot(kind="bar")
plt.title("Diagnoses per Year")
plt.ylabel("Number of patients")
plt.show()

plt.figure(figsize=(8,4))
pdf["year_death"].value_counts().sort_index().plot(kind="bar", color="tomato")
plt.title("Deaths per Year")
plt.ylabel("Number of deaths")
plt.show()

# SCATTER
plt.figure(figsize=(7, 5))
sns.scatterplot(
    data=pdf,
    x="age_at_diagnosis",
    y="years_alive",
    alpha=0.2,
    s=15,
    edgecolor=None
)
plt.title("Age at Diagnosis vs. Years Alive")
plt.xlabel("Age at diagnosis")
plt.ylabel("Years alive")
plt.show()

plt.figure(figsize=(7, 5))
sns.kdeplot(
    data=pdf,
    x="age_at_diagnosis",
    y="years_alive",
    fill=False,
    cmap="viridis",
    thresh=0.05
)
plt.title("Age at Diagnosis vs. Years Alive (Density)")
plt.xlabel("Age at diagnosis")
plt.ylabel("Years alive")
plt.show()


plt.figure(figsize=(7, 5))
plt.hexbin(
    pdf["age_at_diagnosis"],
    pdf["years_alive"],
    gridsize=30,
    cmap="plasma"
)
plt.colorbar(label="Number of patients")
plt.title("Age at Diagnosis vs. Years Alive (Hexbin Density)")
plt.xlabel("Age at diagnosis")
plt.ylabel("Years alive")
plt.show()


# SURVIVAL ANALYSIS
## ACHTUNG ACHTUNG - HIER NUR AB 2017 FILTERN WEIL TODESDATUM ERST AB 2017 IN DEN DATEN VERFÜGBAR
# (Leistungsdatum 01.04.2017 - ab hier wurde melden wirklich verpflichtend)
# und wir sind unsicher wie sauber die Todesinfo in den älteren Daten nachgepflegt wurde
# KAPLAN MEIER
T, E = datetimes_to_durations(
    pdf["date_diagnosis"],
    pdf["deceased_datetime"],
    freq='M'
)

print(T)
print(E)

# Gesamtpopulation
kmf_all = KaplanMeierFitter()
kmf_all.fit(T, E, label="total C61 population")

plt.figure(figsize=(6, 4))
kmf_all.plot_survival_function()
plt.title("Kaplan–Meier Estimate of Overall Survival")
plt.xlabel("Months since diagnosis")
plt.ylabel("Survival probability")
plt.show()

median_ = kmf_all.median_survival_time_
print(f"Median survival time all C61 patients: {median_:.1f} months")

# Tabelle
table = survival_table_from_events(T, E)
print(table.head())


# "cohort" vs "rest"
groups = pdf["young_highrisk_cohort"]
ix = groups == "cohort"
plt.figure(figsize=(6, 4))
kmf_cohort_rest = KaplanMeierFitter()
kmf_cohort_rest.fit(T[~ix], E[~ix], label='Rest')
kmf_cohort_rest.plot_survival_function()

kmf_cohort_rest.fit(T[ix], E[ix], label='Cohort')
kmf_cohort_rest.plot_survival_function()

plt.title("Kaplan–Meier Estimate of Overall Survival")
plt.xlabel("Months since diagnosis")
plt.ylabel("Survival probability")
plt.legend()
plt.show()



# mach danach noch scikit survival RSF
#https://scikit-survival.readthedocs.io/en/stable/user_guide/random-survival-forest.html


