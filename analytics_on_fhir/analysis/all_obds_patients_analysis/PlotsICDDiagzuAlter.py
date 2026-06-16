import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ── Zentrale Konfiguration aus plot_config.py ─────────────────────────────────
try:
    from plot_config import PLOT_CONFIG, tab20b_colors
except ImportError:
    # Fallback falls plot_config.py nicht erreichbar ist
    PLOT_CONFIG = {
        "dpi": 300, "fontsize_base": 14, "fontsize_axis_label": 16,
        "fontsize_subplot_title": 16, "fontsize_legend": 13,
        "show_titles": False, "show_legend": True, "colormap": "tab20b",
    }
    import matplotlib.cm as _cm
    def tab20b_colors(n):
        cmap = _cm.get_cmap("tab20b")
        if n <= 0:
            return []
        return [cmap(i / max(min(n, 20) - 1, 1)) for i in range(n)]


def _age_bins_18_start(step: int = 5):
    """Altersbrackets: erstes Bracket 18-19, danach step-Schritte ab 20."""
    bins = [18, 20] + list(range(20 + step, 95, step)) + [np.inf]
    labels = []
    for i in range(len(bins) - 1):
        lo = int(bins[i])
        hi = bins[i + 1]
        if np.isinf(hi):
            labels.append(f"{lo}+")
        else:
            labels.append(f"{lo}-{int(hi - 1)}")
    return bins, labels


def plot_population_pyramid_from_raw(
    df,
    age_col,
    sex_col,
    bins=None,
    age_step: int = 5,
    male_label="male",
    female_label="female",
    title="Butterfly Plot",
    show_title: bool | None = None,
    output_path: Path | str | None = None,
):
    data = df.copy()

    if bins is None:
        bins, labels = _age_bins_18_start(age_step)
    else:
        labels = []
        for i in range(len(bins) - 1):
            start = int(bins[i])
            end = bins[i + 1]
            labels.append(f"{start}+" if np.isinf(end) else f"{start}-{int(end - 1)}")

    data["age_group"] = pd.cut(data[age_col], bins=bins, labels=labels, right=False)
    grouped = data.groupby(["age_group", sex_col]).size().unstack(fill_value=0)
    for col in [male_label, female_label]:
        if col not in grouped.columns:
            grouped[col] = 0
    grouped = grouped.reindex(labels)

    ages = grouped.index.astype(str).values
    female = grouped[female_label].values
    male = grouped[male_label].values
    total_female = int(female.sum())
    total_male = int(male.sum())
    y = np.arange(len(ages))

    _cols = tab20b_colors(4)
    fig, ax = plt.subplots(figsize=(9, 7))
    ax.barh(y, -female, color=_cols[0], label=f"{female_label} (n={total_female:,})")
    ax.barh(y, male, color=_cols[2], label=f"{male_label} (n={total_male:,})")
    ax.set(yticks=y, yticklabels=ages)
    ax.set_xlabel("Number of Diagnoses", fontsize=PLOT_CONFIG["fontsize_axis_label"])

    if show_title is None:
        show_title = PLOT_CONFIG["show_titles"]
    if show_title:
        ax.set_title(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"])

    max_val = max(female.max(), male.max()) if len(female) else 1
    ax.set_xlim(-max_val * 1.1, max_val * 1.1)
    xticks = ax.get_xticks()
    ax.set_xticklabels([f"{abs(int(x))}" for x in xticks])
    ax.axvline(0, color="black", linewidth=1)
    if PLOT_CONFIG["show_legend"]:
        ax.legend(fontsize=PLOT_CONFIG["fontsize_legend"])

    plt.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
        print(f"Plot saved to: {output_path}")
    plt.close(fig)


def plot_population_pyramid_topn(
    df,
    age_col,
    sex_col,
    diagnosis_col,
    bins=None,
    age_step: int = 5,
    top_n=10,
    male_label="male",
    female_label="female",
    title="Population Pyramid (Top Diagnoses)",
    show_title: bool | None = None,
    include_other: bool = False,
    output_path: Path | str | None = None,
):
    data = df.copy()

    # ── age groups: erstes Bracket 18-20, danach age_step ────────────────────
    if bins is None:
        bins, labels = _age_bins_18_start(age_step)
    else:
        labels = [
            f"{int(bins[i])}+" if np.isinf(bins[i + 1]) else f"{int(bins[i])}-{int(bins[i + 1] - 1)}"
            for i in range(len(bins) - 1)
        ]

    data["age_group"] = pd.cut(data[age_col], bins=bins, labels=labels, right=False)

    # ── top-N diagnoses ───────────────────────────────────────────────────────
    top_diag = data[diagnosis_col].value_counts().nlargest(top_n).index
    data_top = data[data[diagnosis_col].isin(top_diag)]
    n_female_top = int((data_top[sex_col] == female_label).sum())
    n_male_top = int((data_top[sex_col] == male_label).sum())

    # "Other"-Kategorie optional (default aus, Punkt 10)
    if include_other:
        data["diag_group"] = np.where(
            data[diagnosis_col].isin(top_diag), data[diagnosis_col], "Other"
        )
        categories = list(top_diag) + ["Other"]
    else:
        data = data[data[diagnosis_col].isin(top_diag)].copy()
        data["diag_group"] = data[diagnosis_col]
        categories = list(top_diag)

    grouped = data.groupby(["age_group", sex_col, "diag_group"]).size().reset_index(name="count")

    # ── Farben aus tab20b ─────────────────────────────────────────────────────
    colors = tab20b_colors(len(categories))

    if show_title is None:
        show_title = PLOT_CONFIG["show_titles"]

    # ── plot ──────────────────────────────────────────────────────────────────
    y = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(10, 7))

    for sex, sign in [(female_label, -1), (male_label, 1)]:
        subset = grouped[grouped[sex_col] == sex]
        pivot = (
            subset.pivot(index="age_group", columns="diag_group", values="count")
            .fillna(0)
            .reindex(labels)
        )
        for cat in categories:
            if cat not in pivot.columns:
                pivot[cat] = 0
        pivot = pivot[categories]
        left = np.zeros(len(labels))
        for i, cat in enumerate(categories):
            values = pivot[cat].values
            ax.barh(
                y, sign * values, left=sign * left, color=colors[i],
                label=cat if sex == male_label else None,
            )
            left += values

    xticks = ax.get_xticks()
    ax.set_xticks(xticks)
    ax.set_xticklabels([f"{abs(int(x)):,}" for x in xticks])
    ax.set(yticks=y, yticklabels=labels)
    ax.set_xlabel("Number of Diagnoses", fontsize=PLOT_CONFIG["fontsize_axis_label"])

    if show_title:
        ax.set_title(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"])

    ax.axvline(0, color="black", linewidth=1)
    ax.text(0.20, 0.98, f"{female_label.upper()}\n(n={n_female_top:,})",
            transform=ax.transAxes, ha="center", va="top",
            fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold", color="black")
    ax.text(0.70, 0.98, f"{male_label.upper()}\n(n={n_male_top:,})",
            transform=ax.transAxes, ha="center", va="top",
            fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold", color="black")

    if PLOT_CONFIG["show_legend"]:
        _legtitle = f"Top {top_n} Diagnoses" + (" + Other" if include_other else "")
        ax.legend(title=_legtitle, bbox_to_anchor=(1.05, 1), loc="upper left",
                  fontsize=PLOT_CONFIG["fontsize_legend"])

    plt.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
        print(f"Plot saved to: {output_path}")
    plt.close(fig)


# ── calls ─────────────────────────────────────────────────────────────────────
# Standalone-Ausführung: python PlotsICDDiagzuAlter.py
# Bei Import durch main_one_path.py wird dieser Block NICHT ausgeführt.

if __name__ == "__main__":
    import matplotlib

    matplotlib.use("Agg")

    data_path = r"C:\Users\boehnesn1\Desktop\Projects\BZKF\Data\all_obds_patients_11.05.26\parquet"

    df_tumore_gesamt = pd.read_parquet(
        os.path.join(data_path, "df_all_obds_clean_deidentified.parquet")
    )
    df_tumore_gesamt = df_tumore_gesamt[
        ~(df_tumore_gesamt["icd10_parent_code"].str.contains("D", na=False))
    ]
    df_tumore_gesamt = df_tumore_gesamt[
        ~df_tumore_gesamt[["icd10_parent_code"]]
        .astype(str)
        .apply(lambda s: s.str.contains("C44", na=False))
        .any(axis=1)
    ]
    icd_lookup_dwh = pd.read_parquet(
        r"C:\Users\boehnesn1\Desktop\Projects\BZKF\ZweitKarzinom\Resources\parquet_files_steven\DWH_ICD_CODE_MAPPING.parquet"
    )
    icd_lookup_dwh_reduced_base_code = icd_lookup_dwh[["ICD3_CODE", "ICD3_NAME"]].drop_duplicates()
    df_tumore_gesamt = df_tumore_gesamt.merge(
        icd_lookup_dwh_reduced_base_code,
        left_on="icd10_parent_code",
        right_on="ICD3_CODE",
        how="left",
    )
    # TODO: ändere auf entity_or_parent Top-20:
    # df_tumore_gesamt = df_tumore_gesamt[
    #     df_tumore_gesamt["entity_or_parent"].isin(
    #         df_tumore_gesamt["entity_or_parent"].value_counts().iloc[:20].index
    #     )
    # ]

    PLOTS = Path(
        r"C:\Users\boehnesn1\Desktop\Projects\BZKF\2ndPaper_refactored\Plots2ndPaper_simplified"
    )
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
