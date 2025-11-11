from utils_onco_analytics import (

    datashield_preps_wrapper,
    extract_df_study_protocol_a0_1_3_7_d,
    extract_df_study_protocol_c,
    save_final_df,
    calculate_age_at_condition_date_udf,
    extract_filter_df_c61,
    extract_pca_ops,
    extract_pca_st,
    extract_pca_sys,
    union_sort_pivot_join,
    add_deceased_flag,
    add_age_col,
    plot,
    map_icd10_udf,
    group_icd_groups_udf,
    group_entities_udf
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
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
    output_folder: str = "./study_protocol_b"
    output_filename: str = "df.csv"
    study_name: str = "study_protocol_c"
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

df_all = extract_df_study_protocol_c(pc, data, settings, spark)
df_all.show()

save_final_df(df_all, settings, suffix="c")

df_all = df_all.withColumn(
        "age_at_diagnosis",
        F.datediff(df_all["date_diagnosis"], df_all["birthdate"]) / 365.25,
    )
df_all = df_all.drop("birthdate", "condition_id", "date_diagnosis", "cond_subject_reference", "patient_resource_id", "patid_pseudonym", "subject_reference", "observation_id")
df_all.show()

# TO DO HIER: failsafe für alle standorte
# überprüfe ob postal_code vollständig ist oder nur zweistellig, mach den rest nur wenn sie vollständig ist
if df_all.filter(col("postal_code").rlike("^[0-9]{5}$")).count() > 0:
    print("Mindestens eine vollständige PLZ vorhanden. Verarbeitung starten")

    # read in allgemeine gemeindeschlüssel ags.csv
    ags = spark.read.csv("ags.csv", header=True, inferSchema=True)
    ags = ags.withColumnRenamed("einwohner", "einwohner_ags")

    # kreis code (first 5 digits of AGS)
    ags = ags.withColumn("kreis", F.substring(F.col("ags"), 1, 5))

    # aggregiere pro kreis: zähle verschiedene AGS und summiere Einwohner
    ags_kreis_info = (
        ags.groupBy("kreis")
        .agg(
            F.countDistinct("ags").alias("ags_count"),
            F.sum("einwohner_ags").alias("sum_einwohner_kreis")
        )
    )

    ags = ags.join(ags_kreis_info, on="kreis", how="left")

    # einwohner_kreis_kreisfreie_stadt_stadtstaat
    # wenn die letzten 3 Ziffern der AGS "000" sind (kreifreie stadt oder stadtstaat), dann einwohner_ags
    # wenn es mehrere AGS pro Kreis gibt, dann summe der einwohner im Kreis
    ags = ags.withColumn(
        "einwohner_kreis_kreisfreie_stadt_stadtstaat",
        F.when(F.col("ags").substr(-3, 3) == "000", F.col("einwohner_ags"))
        .when(F.col("ags_count") > 1, F.col("sum_einwohner_kreis"))
        .otherwise(F.col("einwohner_ags"))
    )

    ags = ags.drop("ags_count", "sum_einwohner_kreis")

    ags.show(200, truncate=False)

    # JOIN df_all u ags
    ags_selected_cols = [
        "kreis",
        "ags",
        "plz",
        "einwohner_ags",
        "prim_plz",
        "einwohner_kreis_kreisfreie_stadt_stadtstaat"
    ]

    df_all = df_all.join(
        ags.select(*ags_selected_cols),
        df_all["postal_code"] == ags["plz"],
        how="left"
    )

    df_all.show(100, truncate=False)

else:
    print("Keine vollständige PLZ gefunden - abbrechen")


# CAST ALL DATES
# Liste der Datumsfelder
#date_cols = [
#    "date_diagnosis",
#    "deceased_datetime",
#    "date_death",
    #"gleason_date_first"
#]

#for col_name in date_cols:
#    df_all = df_all.withColumn(col_name, F.to_date(F.col(col_name)))


