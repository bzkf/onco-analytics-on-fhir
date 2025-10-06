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
from lifelines import KaplanMeierFitter
from lifelines.utils import datetimes_to_durations, survival_table_from_events, median_survival_times
from pathling import PathlingContext
from pyspark.sql import SparkSession

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

patients = data.read("Patient")
conditions = data.read("Condition")
print(f"ROWS: {patients.count()}")
print(f"ROWS: {conditions.count()}")

df_c61= extract_filter_df_c61(pc, data, settings, spark)
df_c61 = add_age_col(df = df_c61, birthdate_col = "birthdate", refdate_col = "date_diagnosis", age_colname = "age_at_diagnosis")

df_c61.show(5)

pdf = df_c61.select("date_diagnosis", "deceased_datetime").toPandas()
#  datetime64
pdf["date_diagnosis"] = pd.to_datetime(pdf["date_diagnosis"], errors="coerce")
pdf["deceased_datetime"] = pd.to_datetime(pdf["deceased_datetime"], errors="coerce")

T, E = datetimes_to_durations(
    pdf["date_diagnosis"],
    pdf["deceased_datetime"],
    freq='M'
)

print(T)
print(E)

# all c61 patients
kmf = KaplanMeierFitter()
kmf.fit(T, E)
kmf.plot_survival_function()

median_ = kmf.median_survival_time_
print(f"Median survival time all c61 patients: {median_}")

table = survival_table_from_events(T, E)
print(table.head())

# removce outliers from time between diagnosis and death

# add col "cohort" with values "cohort" or "rest"


# mach danach noch scikit survival RSF
#https://scikit-survival.readthedocs.io/en/stable/user_guide/random-survival-forest.html
