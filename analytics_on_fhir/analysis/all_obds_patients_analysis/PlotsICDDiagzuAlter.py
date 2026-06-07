import matplotlib.pyplot as plt
import os
from pathlib import Path
import pandas as pd
import numpy as np

#TODO: Join Fullnames
#TODO: Nebendiagnosen - TOP 20 Nebendiagnosen horizontal bar chart, Lorenzkurve (wieviel Patienten tragen wieviele Nebendiagnose-Events) oder log histogramm @Steven Böhner


def plot_population_pyramid_from_raw(
    df,
    age_col,
    sex_col,
    bins=None,
    male_label="male",
    female_label="female",
    title="Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\nGesamt Population UKer",
    show_title: bool = True,
    output_path: Path | str | None = None,
):
    data = df.copy()

    if bins is None:
        bins = list(range(0, 95, 5)) + [np.inf]

    labels = []
    for i in range(len(bins)-1):
        start = int(bins[i])
        end = bins[i+1]
        if np.isinf(end):
            labels.append(f"{start}+")
        else:
            labels.append(f"{start}-{int(end-1)}")

    data["age_group"] = pd.cut(
        data[age_col],
        bins=bins,
        labels=labels,
        right=False
    )

    grouped = (
        data
        .groupby(["age_group", sex_col])
        .size()
        .unstack(fill_value=0)
    )

    for col in [male_label, female_label]:
        if col not in grouped.columns:
            grouped[col] = 0

    grouped = grouped.reindex(labels)

    ages   = grouped.index.astype(str).values
    female = grouped[female_label].values
    male   = grouped[male_label].values

    total_female = int(female.sum())
    total_male   = int(male.sum())

    y = np.arange(len(ages))

    fig, ax = plt.subplots(figsize=(8, 6))

    ax.barh(y, -female, color="firebrick",
            label=f"{female_label} (n={total_female:,})")
    ax.barh(y, male,    color="steelblue",
            label=f"{male_label} (n={total_male:,})")

    ax.set(yticks=y, yticklabels=ages)
    ax.set_xlabel("Number of Patients")

    if show_title:
        ax.set_title(title)

    max_val = max(female.max(), male.max())
    ax.set_xlim(-max_val * 1.1, max_val * 1.1)

    xticks = ax.get_xticks()
    ax.set_xticklabels([f"{abs(int(x))}" for x in xticks])

    ax.axvline(0, color="black", linewidth=1)
    ax.legend()

    plt.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()


def plot_population_pyramid_topn(
    df,
    age_col,
    sex_col,
    diagnosis_col,
    bins=None,
    top_n=10,
    male_label="male",
    female_label="female",
    title="Population Pyramid (Top Diagnoses)",
    show_title: bool = True,
    output_path: Path | str | None = None,
):
    import matplotlib.colors as mcolors

    data = df.copy()

    # ── age groups ────────────────────────────────────────────────────────────
    if bins is None:
        bins = list(range(0, 95, 5)) + [np.inf]

    labels = [
        f"{int(bins[i])}+" if np.isinf(bins[i+1])
        else f"{int(bins[i])}-{int(bins[i+1]-1)}"
        for i in range(len(bins) - 1)
    ]

    data["age_group"] = pd.cut(
        data[age_col],
        bins=bins,
        labels=labels,
        right=False
    )

    # ── sex counts (cond_id level, one row = one condition) ───────────────────
    n_female = int((data[sex_col] == female_label).sum())
    n_male   = int((data[sex_col] == male_label).sum())

    # ── top-N diagnoses ───────────────────────────────────────────────────────
    top_diag = (
        data[diagnosis_col]
        .value_counts()
        .nlargest(top_n)
        .index
    )

    data["diag_group"] = np.where(
        data[diagnosis_col].isin(top_diag),
        data[diagnosis_col],
        "Other"
    )

    grouped = (
        data
        .groupby(["age_group", sex_col, "diag_group"])
        .size()
        .reset_index(name="count")
    )

    categories = list(top_diag) + ["Other"]

    # ── colors (golden-angle spread) ──────────────────────────────────────────
    def _golden_colors(n):
        hues = [(i * 137.508) % 360 / 360.0 for i in range(n)]
        return [mcolors.hsv_to_rgb((h, 0.65, 0.9)) for h in hues]

    colors = _golden_colors(len(categories))

    # ── plot ──────────────────────────────────────────────────────────────────
    y = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(9, 6))

    for sex, sign in [(female_label, -1), (male_label, 1)]:
        subset = grouped[grouped[sex_col] == sex]
        pivot  = (
            subset
            .pivot(index="age_group", columns="diag_group", values="count")
            .fillna(0)
            .reindex(labels)
        )
        for cat in categories:
            if cat not in pivot.columns:
                pivot[cat] = 0
        pivot = pivot[categories]
        left  = np.zeros(len(labels))

        for i, cat in enumerate(categories):
            values = pivot[cat].values
            ax.barh(
                y,
                sign * values,
                left=sign * left,
                color=colors[i],
                label=cat if sex == male_label else None,
            )
            left += values

    # ── x-axis: no negative tick labels ──────────────────────────────────────
    xticks = ax.get_xticks()
    ax.set_xticks(xticks)
    ax.set_xticklabels([f"{abs(int(x)):,}" for x in xticks])

    ax.set(yticks=y, yticklabels=labels)
    ax.set_xlabel("Number of Patients")

    if show_title:
        ax.set_title(title)

    ax.axvline(0, color="black", linewidth=1)

    # ── sex labels with cond_id counts ───────────────────────────────────────
    ax.text(
        0.25, 0.98,
        f"{female_label.upper()}\n(n={n_female:,})",
        transform=ax.transAxes,
        ha="center", va="top",
        fontsize=11, fontweight="bold", color="black",
    )
    ax.text(
        0.75, 0.98,
        f"{male_label.upper()}\n(n={n_male:,})",
        transform=ax.transAxes,
        ha="center", va="top",
        fontsize=11, fontweight="bold", color="black",
    )

    # ── legend ────────────────────────────────────────────────────────────────
    ax.legend(
        title=f"Top {top_n} Diagnoses + Other",
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
    )

    plt.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()


# ── calls ─────────────────────────────────────────────────────────────────────
# Standalone-Ausführung: python PlotsICDDiagzuAlter.py
# Bei Import durch main_one_path.py wird dieser Block NICHT ausgeführt.

if __name__ == "__main__":
    import matplotlib
    matplotlib.use("Agg")

    data_path = r"C:\Users\boehnesn1\Desktop\Projects\BZKF\Data\all_obds_patients_11.05.26\parquet"

    df_tumore_gesamt = pd.read_parquet(os.path.join(data_path, "df_all_obds_clean_deidentified.parquet"))
    df_tumore_gesamt = df_tumore_gesamt[~(df_tumore_gesamt["icd10_parent_code"].str.contains("D", na=False))]
    df_tumore_gesamt = df_tumore_gesamt[
        ~df_tumore_gesamt[["icd10_parent_code"]].astype(str).apply(lambda s: s.str.contains("C44", na=False)).any(axis=1)
    ]
    icd_lookup_dwh = pd.read_parquet(
        r"C:\Users\boehnesn1\Desktop\Projects\BZKF\ZweitKarzinom\Resources\parquet_files_steven\DWH_ICD_CODE_MAPPING.parquet"
    )
    icd_lookup_dwh_reduced_base_code = icd_lookup_dwh[["ICD3_CODE", "ICD3_NAME"]].drop_duplicates()
    df_tumore_gesamt = df_tumore_gesamt.merge(
        icd_lookup_dwh_reduced_base_code, left_on="icd10_parent_code", right_on="ICD3_CODE", how="left"
    )
    # TODO: ändere auf entity_or_parent Top-20:
    # df_tumore_gesamt = df_tumore_gesamt[
    #     df_tumore_gesamt["entity_or_parent"].isin(
    #         df_tumore_gesamt["entity_or_parent"].value_counts().iloc[:20].index
    #     )
    # ]

    PLOTS         = Path(r"C:\Users\boehnesn1\Desktop\Projects\BZKF\2ndPaper_refactored\Plots2ndPaper_simplified")
    DIR_BUTTERFLY = PLOTS / "Butterfly"
    DIR_BUTTERFLY.mkdir(parents=True, exist_ok=True)

    plot_population_pyramid_topn(
        df_tumore_gesamt,
        age_col="age_at_diagnosis",
        sex_col="gender",
        diagnosis_col="entity_or_parent",
        top_n=20,
        title=(
            "Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\n"
            "Gesamt Population UKer\n"
            "Aufgeteilt nach TOP 15 meist vorkommende C-ICD-Diagnosen"
        ),
        show_title=False,
        output_path=DIR_BUTTERFLY / "butterfly_topn.tiff",
    )

    plot_population_pyramid_from_raw(
        df_tumore_gesamt,
        age_col="age_at_diagnosis",
        sex_col="gender",
        title="Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\nGesamt Population UKer",
        show_title=False,
        output_path=DIR_BUTTERFLY / "butterfly_overall.tiff",
    )

    print("debug")