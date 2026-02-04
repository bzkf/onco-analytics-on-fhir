from functools import reduce

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import seaborn as sns
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from analytics_on_fhir.utils import save_plot


def group_entity_or_parent(df, code_col="icd10_code", target_col="entity_and_parent"):
    parent_col = "icd10_parent_tmp"
    df = df.withColumn(parent_col, F.regexp_replace(F.col(code_col), r"\..*$", ""))

    df = df.withColumn(
        target_col,
        F.when(F.col(parent_col).between("C00", "C14"), F.lit("C00-C14"))
        .when(F.col(parent_col) == "C15", F.lit("C15"))
        .when(F.col(parent_col) == "C16", F.lit("C16"))
        .when(F.col(parent_col).between("C18", "C21"), F.lit("C18-C21"))
        .when(F.col(parent_col) == "C22", F.lit("C22"))
        .when(F.col(parent_col).between("C23", "C24"), F.lit("C23-C24"))
        .when(F.col(parent_col) == "C25", F.lit("C25"))
        .when(F.col(parent_col) == "C32", F.lit("C32"))
        .when(F.col(parent_col).between("C33", "C34"), F.lit("C33-C34"))
        .when(F.col(parent_col) == "C43", F.lit("C43"))
        .when(F.col(parent_col) == "C50", F.lit("C50"))
        .when(F.col(parent_col) == "C53", F.lit("C53"))
        .when(F.col(parent_col).between("C54", "C55"), F.lit("C54-C55"))
        .when(F.col(parent_col) == "C56", F.lit("C56"))
        .when(F.col(parent_col) == "C61", F.lit("C61"))
        .when(F.col(parent_col) == "C62", F.lit("C62"))
        .when(F.col(parent_col) == "C64", F.lit("C64"))
        .when(F.col(parent_col) == "C67", F.lit("C67"))
        .when(F.col(parent_col).between("C70", "C72"), F.lit("C70-C72"))
        .when(F.col(parent_col) == "C73", F.lit("C73"))
        .when(F.col(parent_col) == "C81", F.lit("C81"))
        .when(F.col(parent_col).between("C82", "C88"), F.lit("C82-C88"))
        .when(F.col(parent_col) == "C90", F.lit("C90"))
        .when(F.col(parent_col).between("C91", "C95"), F.lit("C91-C95"))
        .otherwise(
            F.col(parent_col)
        ),  # fallback = parent code - so verlieren wir nicht so viele (vgl entity)
    )

    df = df.drop(parent_col)
    return df


def create_2_mals_df(df: DataFrame) -> DataFrame:
    # Filter patients with multiple malignancies, compute months between diagnoses
    df_multi = df.filter(F.col("double_patid") == 1)
    logger.info(f"double_patid == 1 - df_multi.count() = : {df_multi.count()}")

    # sortieren, Diagnosen nummerieren
    w = Window.partitionBy("patient_resource_id").orderBy("asserted_date")
    df_multi = (
        df_multi.withColumn("malignancy_number", F.row_number().over(w))
        .withColumn("date_prev", F.lag("asserted_date").over(w))
        .withColumn(
            "years_between",
            F.round(F.months_between("asserted_date", "date_prev") / 12, 2),
        )
        .withColumn(
            "months_between",
            F.round(F.months_between("asserted_date", "date_prev"), 1),
        )
    )
    # Nur Patienten mit >= 2 malignancies
    df_multi = (
        df_multi.groupBy("patient_resource_id")
        .count()
        .filter("count >= 2")
        .join(df_multi, on="patient_resource_id", how="inner")
        .drop("count")
    )
    # Nur erste zwei Malignome, Pats mit mehr Malignomen raus
    df_2_mals = df_multi.filter(F.col("malignancy_number") <= 2)
    df_2_mals = df_2_mals.checkpoint(eager=True)
    logger.info(f"only 2 malignancies: df_2_mals.count() = : {df_2_mals.count()}")

    return df_2_mals


def create_1_mal_df(df_all: DataFrame) -> DataFrame:
    # patients with single malignancy
    df_1_mal = df_all.filter(F.col("double_patid") == 0)
    logger.info(f"double_patid == 0 - df_1_mal.count() = : {df_1_mal.count()}")
    logger.info(
        f"""count conditions should be the same as distinct count patids.
        df_1_mal.agg(F.countDistinct('patient_resource_id')).collect()[0][0] = :
        {df_1_mal.agg(F.countDistinct("patient_resource_id")).collect()[0][0]}"""
    )

    df_1_mal = df_1_mal.checkpoint(eager=True)
    return df_1_mal


def pivot_multi_single(
    df_clean: DataFrame, df_2_mals: DataFrame, df_1_mal: DataFrame
) -> DataFrame:
    # pivot multi-malignancy patients, union with single malignancy patients

    pivot_cols = [
        "condition_id",
        "asserted_date",
        "icd10_code",
        "icd10_parent_code",
        "entity_or_parent",
        "age_at_diagnosis",
        "is_deceased",
        "gleason_sct",
        "gleason_date_first",
        "gleason_score",
        "metastasis_loc",
        "metastasis_date_first",
    ]
    # sollte 2 sein
    max_index = df_2_mals.agg(F.max("malignancy_number")).collect()[0][0]

    pivoted_dfs = []
    for col_name in pivot_cols:
        df_pivot = (
            df_2_mals.select("patient_resource_id", "malignancy_number", col_name)
            .groupBy("patient_resource_id")
            .pivot("malignancy_number")
            .agg(F.first(col_name))
        )
        for i in range(1, max_index + 1):
            old_col = str(i)
            new_col = f"{col_name}_{i}"
            if old_col in df_pivot.columns:
                df_pivot = df_pivot.withColumnRenamed(old_col, new_col)
        pivoted_dfs.append(df_pivot)

    df_2_mals_final = reduce(
        lambda a, b: a.join(b, on="patient_resource_id", how="left"), pivoted_dfs
    )

    df_1_mal_selected = df_1_mal.select(
        "patient_resource_id",
        F.col("condition_id").alias("condition_id_1"),
        F.col("asserted_date").alias("asserted_date_1"),
        F.col("icd10_code").alias("icd10_code_1"),
        F.col("icd10_parent_code").alias("icd10_parent_code_1"),
        F.col("age_at_diagnosis").alias("age_at_diagnosis_1"),
        F.col("is_deceased").alias("is_deceased_1"),
        F.col("gleason_sct").alias("gleason_sct_1"),
        F.col("gleason_date_first").alias("gleason_date_first_1"),
        F.col("gleason_score").alias("gleason_score_1"),
        F.col("metastasis_loc").alias("metastasis_loc_1"),
        F.col("metastasis_date_first").alias("metastasis_date_first_1"),
    )

    df_all_pivot = df_2_mals_final.unionByName(
        df_1_mal_selected, allowMissingColumns=True
    )

    # join back missing cols from df_clean
    meta_cols = [
        "patient_resource_id",
        "gender",
        "double_patid",
    ]
    df_meta = df_clean.select(*meta_cols).dropDuplicates(["patient_resource_id"])
    df_all_pivot = df_all_pivot.join(df_meta, on="patient_resource_id", how="left")

    # Todesdaten
    df_death = df_clean.groupBy("patient_resource_id").agg(
        F.max("deceased_datetime").alias("deceased_datetime"),
        F.first("death_cause_icd10", ignorenulls=True).alias("death_cause_icd10"),
        F.first("death_cause_tumor", ignorenulls=True).alias("death_cause_tumor"),
    )
    df_all_pivot = df_all_pivot.join(df_death, on="patient_resource_id", how="left")
    df_all_pivot = df_all_pivot.checkpoint(eager=True)

    return df_all_pivot


# presuffix="entity_or_parent" (combination) or entity (23 LGL entities) or
# icd10parent (all parent codes); save outliers too
def aggregate_malignancy_pairs(df_multiple_malignancies, presuffix="entity_or_parent"):
    df_m1 = df_multiple_malignancies.filter(
        F.col("malignancy_number") == 1
    ).withColumnRenamed(presuffix, presuffix + "_1")

    df_m2 = df_multiple_malignancies.filter(
        F.col("malignancy_number") == 2
    ).withColumnRenamed(presuffix, presuffix + "_2")

    m1_patients = df_m1.select(
        "patient_resource_id",
        presuffix + "_1",
        F.col("age_at_diagnosis").alias("age_1"),
        "gender",
    )
    m2_patients = df_m2.select(
        "patient_resource_id",
        presuffix + "_2",
        F.col("age_at_diagnosis").alias("age_2"),
        F.col("months_between").alias("months_between_2"),
    )

    m2_full = m2_patients.join(
        m1_patients.select(
            "patient_resource_id", presuffix + "_1", "age_1", "gender"
        ).distinct(),
        on="patient_resource_id",
        how="inner",
    )

    # Outliers pro Pair
    stats_for_outliers = m2_full.groupBy(presuffix + "_1", presuffix + "_2").agg(
        F.expr("percentile_approx(age_1, 0.25)").alias("q1_age1"),
        F.expr("percentile_approx(age_1, 0.75)").alias("q3_age1"),
        F.expr("percentile_approx(age_2, 0.25)").alias("q1_age2"),
        F.expr("percentile_approx(age_2, 0.75)").alias("q3_age2"),
        F.expr("percentile_approx(months_between_2, 0.25)").alias("q1_m"),
        F.expr("percentile_approx(months_between_2, 0.75)").alias("q3_m"),
    )

    joined = m2_full.join(
        stats_for_outliers, on=[presuffix + "_1", presuffix + "_2"], how="left"
    )

    # IQR & Grenzen berechnen
    joined = (
        joined.withColumn("iqr_age1", F.col("q3_age1") - F.col("q1_age1"))
        .withColumn("low_age1", F.col("q1_age1") - 1.5 * F.col("iqr_age1"))
        .withColumn("high_age1", F.col("q3_age1") + 1.5 * F.col("iqr_age1"))
        .withColumn("iqr_age2", F.col("q3_age2") - F.col("q1_age2"))
        .withColumn("low_age2", F.col("q1_age2") - 1.5 * F.col("iqr_age2"))
        .withColumn("high_age2", F.col("q3_age2") + 1.5 * F.col("iqr_age2"))
        .withColumn("iqr_m", F.col("q3_m") - F.col("q1_m"))
        .withColumn("low_m", F.col("q1_m") - 1.5 * F.col("iqr_m"))
        .withColumn("high_m", F.col("q3_m") + 1.5 * F.col("iqr_m"))
    )

    # Outliers
    joined = (
        joined.withColumn(
            "out_age1",
            (F.col("age_1") < F.col("low_age1"))
            | (F.col("age_1") > F.col("high_age1")),
        )
        .withColumn(
            "out_age2",
            (F.col("age_2") < F.col("low_age2"))
            | (F.col("age_2") > F.col("high_age2")),
        )
        .withColumn(
            "out_m",
            (F.col("months_between_2") < F.col("low_m"))
            | (F.col("months_between_2") > F.col("high_m")),
        )
    )

    agg_pairs = joined.groupBy(presuffix + "_1", presuffix + "_2").agg(
        F.count("*").alias("count_pair"),
        F.sum(F.when(F.col("gender") == "male", 1).otherwise(0)).alias(
            "count_male_pair"
        ),
        F.sum(F.when(F.col("gender") == "female", 1).otherwise(0)).alias(
            "count_female_pair"
        ),
        F.min("age_1").alias("age_min_1"),
        F.max("age_1").alias("age_max_1"),
        F.expr("percentile_approx(age_1, 0.25)").alias("age_q1_1"),
        F.expr("percentile_approx(age_1, 0.5)").alias("age_median_1"),
        F.expr("percentile_approx(age_1, 0.75)").alias("age_q3_1"),
        F.min("age_2").alias("age_min_2"),
        F.max("age_2").alias("age_max_2"),
        F.expr("percentile_approx(age_2, 0.25)").alias("age_q1_2"),
        F.expr("percentile_approx(age_2, 0.5)").alias("age_median_2"),
        F.expr("percentile_approx(age_2, 0.75)").alias("age_q3_2"),
        F.min("months_between_2").alias("months_between_min_2"),
        F.max("months_between_2").alias("months_between_max_2"),
        F.expr("percentile_approx(months_between_2, 0.25)").alias(
            "months_between_q1_2"
        ),
        F.expr("percentile_approx(months_between_2, 0.5)").alias(
            "months_between_median_2"
        ),
        F.expr("percentile_approx(months_between_2, 0.75)").alias(
            "months_between_q3_2"
        ),
        F.collect_list(F.when(F.col("out_age1"), F.col("age_1"))).alias(
            "outliers_age_1"
        ),
        F.collect_list(F.when(F.col("out_age2"), F.col("age_2"))).alias(
            "outliers_age_2"
        ),
        F.collect_list(F.when(F.col("out_m"), F.col("months_between_2"))).alias(
            "outliers_months_between"
        ),
    )

    return agg_pairs.orderBy(presuffix + "_1", presuffix + "_2")


def plot_pair_bubble(
    df,
    value_col="count_pair",
    figsize=(12, 10),
    settings=None,
    plot_name="?",
    feature="?",
    period="?",
):

    pivot = df.pivot(
        index="entity_or_parent_1", columns="entity_or_parent_2", values=value_col
    ).fillna(0)

    fig, ax = plt.subplots(figsize=figsize)

    # koordinaten
    num_rows = len(pivot.index)
    num_cols = len(pivot.columns)
    X, Y = np.meshgrid(range(num_cols), range(num_rows))
    Z = pivot.values

    # Bubble sizes
    scale = 8
    sizes = Z.flatten() * scale

    ax.scatter(
        X.flatten(),
        Y.flatten(),
        s=sizes,
        alpha=0.55,
        color="steelblue",
        edgecolors="black",
        linewidth=0.8,
    )

    ax.set_xticks(range(num_cols))
    ax.set_xticklabels(pivot.columns, rotation=90)

    ax.set_yticks(range(num_rows))
    ax.set_yticklabels(pivot.index)

    title = f"{plot_name}: {feature}, {period}, {settings.location}"

    ax.set_title(title, fontsize=15, fontweight="bold", loc="center")
    ax.set_xlabel("Second malignancy", fontsize=12)
    ax.set_ylabel("First malignancy", fontsize=12)

    # nbrs inside bubbles
    for i in range(num_rows):
        for j in range(num_cols):
            val = Z[i, j]
            if val <= 0:
                continue

            # adapt font-size
            fontsize = max(6, min(14, val**0.4 + 4))

            ax.text(
                j,
                i,
                str(int(val)),
                ha="center",
                va="center",
                fontsize=fontsize,
                color="black",
                fontweight="bold",
            )

    fig.tight_layout()

    full_plot_name = f"{plot_name}_{settings.location}"
    if settings is not None:
        save_plot(fig, settings, full_plot_name)

    plt.show()


def plot_pair_bubble_gender(
    df,
    value_col="count_pair",
    subgroup_cbar_col="count_female_pair",
    figsize=(12, 10),
    settings=None,
    plot_name="?",
    feature="?",
    period="?",
    cbar_label="female ratio",
):

    pivot_size = df.pivot(
        index="entity_or_parent_1", columns="entity_or_parent_2", values=value_col
    ).fillna(0)
    pivot_subgroup = df.pivot(
        index="entity_or_parent_1",
        columns="entity_or_parent_2",
        values=subgroup_cbar_col,
    ).fillna(0)

    # Berechne subgroup Anteil
    pivot_ratio = pivot_subgroup / pivot_size
    pivot_ratio = pivot_ratio.fillna(0)  # falls size=0

    fig, ax = plt.subplots(figsize=figsize)

    num_rows = len(pivot_size.index)
    num_cols = len(pivot_size.columns)
    X, Y = np.meshgrid(range(num_cols), range(num_rows))
    Z = pivot_size.values
    R = pivot_ratio.values

    # Bubble-Größe
    scale = 8
    sizes = Z.flatten() * scale

    # Bubble-Farbe = subgroup cbar ratio
    cmap = plt.cm.PuOr
    sc = ax.scatter(
        X.flatten(),
        Y.flatten(),
        s=sizes,
        c=R.flatten(),
        cmap=cmap,
        alpha=0.7,
        edgecolors="k",
        linewidth=0.8,
    )

    # Zahlen in der Bubble
    for i in range(num_rows):
        for j in range(num_cols):
            val = Z[i, j]
            if val <= 0:
                continue
            fontsize = max(6, min(14, val**0.4 + 4))
            ax.text(
                j,
                i,
                str(int(val)),
                ha="center",
                va="center",
                fontsize=fontsize,
                fontweight="bold",
                color="black",
            )

    title = f"{plot_name}: {feature}, {period}, {settings.location}"

    ax.set_xticks(range(num_cols))
    ax.set_xticklabels(pivot_size.columns, rotation=90)
    ax.set_yticks(range(num_rows))
    ax.set_yticklabels(pivot_size.index)
    ax.set_title(title, fontsize=16, fontweight="bold", loc="center")
    ax.set_xlabel("second malignancy")
    ax.set_ylabel("first malignancy")

    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label(cbar_label, fontsize=12)

    fig.tight_layout()

    full_plot_name = f"{plot_name}_{settings.location}"
    if settings is not None:
        save_plot(fig, settings, full_plot_name)
    plt.show()


def plot_pair_boxplot_horizontal_custom(
    df,
    plot_name,
    period,
    settings=None,
    value_col="count_pair",
    var_name=None,
    var_name_median=None,
    var_name_q1=None,
    var_name_q3=None,
    var_name_min=None,
    var_name_max=None,
    var_name_outliers=None,
    show_entity1=False,
):
    df_plot = df.sort_values(value_col, ascending=True).copy()
    # pair labels links
    df_plot["pair_label"] = (
        "1: "
        + df_plot["entity_or_parent_1"].fillna("")
        + "\n"
        + "2: "
        + df_plot["entity_or_parent_2"].fillna("")
        + "\n (n = "
        + df_plot[value_col].astype(str)
        + ")"
    )
    df_plot["female_frac"] = df_plot["count_female_pair"] / df_plot[value_col]
    df_plot["female_frac"] = df_plot["female_frac"].fillna(0)

    fig, ax = plt.subplots(figsize=(24, 0.35 * len(df_plot) + 4))
    sns.set_style("whitegrid")
    y_main = np.arange(len(df_plot))
    y_offset = 0.15  # Offset für zwei Boxplots

    cmap = plt.cm.PuOr
    norm = mpl.colors.Normalize(vmin=0, vmax=1)

    # Positionen für Boxplots
    positions = [y_main + y_offset, y_main - y_offset] if show_entity1 else [y_main]

    # Boxplots zeichnen
    for i, frac in enumerate(df_plot["female_frac"]):
        color_val = cmap(norm(frac))

        if show_entity1:  # für Alter z.B. wo ich zwei Boxplots pro Pair zeichnen will
            # Entity 1
            bp1 = ax.bxp(
                [
                    {
                        "med": df_plot[var_name_median[0]].iloc[i],
                        "q1": df_plot[var_name_q1[0]].iloc[i],
                        "q3": df_plot[var_name_q3[0]].iloc[i],
                        "whislo": df_plot[var_name_min[0]].iloc[i],
                        "whishi": df_plot[var_name_max[0]].iloc[i],
                        "fliers": df_plot[var_name_outliers[0]].iloc[i],
                    }
                ],
                positions=[positions[0][i]],
                widths=0.3,
                patch_artist=True,
                vert=False,
            )
            for patch in bp1["boxes"]:
                patch.set_facecolor(color_val)

            # Entity 2
            bp2 = ax.bxp(
                [
                    {
                        "med": df_plot[var_name_median[1]].iloc[i],
                        "q1": df_plot[var_name_q1[1]].iloc[i],
                        "q3": df_plot[var_name_q3[1]].iloc[i],
                        "whislo": df_plot[var_name_min[1]].iloc[i],
                        "whishi": df_plot[var_name_max[1]].iloc[i],
                        "fliers": df_plot[var_name_outliers[1]].iloc[i],
                    }
                ],
                positions=[positions[1][i]],
                widths=0.3,
                patch_artist=True,
                vert=False,
            )
            for patch in bp2["boxes"]:
                patch.set_facecolor(color_val)

        else:
            # Nur ein Boxplot pro Pair (z.B. für months_between wo der boxplot für
            # entity1 keinen Sinn macht)
            bp = ax.bxp(
                [
                    {
                        "med": df_plot[var_name_median].iloc[i],
                        "q1": df_plot[var_name_q1].iloc[i],
                        "q3": df_plot[var_name_q3].iloc[i],
                        "whislo": df_plot[var_name_min].iloc[i],
                        "whishi": df_plot[var_name_max].iloc[i],
                        "fliers": df_plot[var_name_outliers[0]].iloc[i],
                    }
                ],
                positions=[positions[0][i]],
                widths=0.5,
                patch_artist=True,
                vert=False,
            )
            for patch in bp["boxes"]:
                patch.set_facecolor(color_val)

        # Prozentzahl rechts
        if show_entity1:
            max_val = df_plot[[var_name_max[0], var_name_max[1]]].max(axis=1).max()
        else:
            max_val = df_plot[var_name_max].max()
        pct_x = max_val + 2
        ax.text(
            pct_x,
            y_main[i],
            f"{int(frac*100)}%",
            va="center",
            ha="left",
            fontsize=10,
            fontweight="bold",
            color="black",
        )

    # Überschrift für Female % Spalte
    ax.text(
        pct_x - 1,
        y_main.max() + 1,
        "% female",
        va="bottom",
        ha="left",
        fontsize=10,
        fontweight="bold",
    )

    sm = mpl.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(
        sm, ax=ax, orientation="vertical", fraction=0.05, pad=0, aspect=70
    )
    cbar.set_label("female ratio", fontsize=12)

    ax.set_yticks(y_main)
    ax.set_yticklabels(df_plot["pair_label"], fontsize=8)
    ax.set_xlim(left=0)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(20))
    ax.set_xlabel(var_name)
    ax.set_title(
        f"""{plot_name}: distribution for entity or parent pairs\n{period} —
        {settings.location}""",
        fontsize=14,
    )
    plt.tight_layout()

    full_plot_name = f"{plot_name}_{settings.location}"
    if settings is not None:
        save_plot(fig, settings, full_plot_name)

    plt.show()
