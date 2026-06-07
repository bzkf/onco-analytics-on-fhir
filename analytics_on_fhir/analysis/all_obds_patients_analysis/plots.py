"""
plots.py
========
Alle Plot-Funktionen für die BZKF-Auswertung.

Inhaltsübersicht
----------------
A – Altersverteilung
    plot_age_distribution_grouped_bar

B – Therapie-Zeitpunkte & Bias
    plot_therapy_times
    plot_therapy_bias_analysis
    plot_dropout_curve

C – Bestand Therapien
    plot_lorenz_curve
    plot_log_histogram

D – UICC / ECOG Inventar & Verteilung
    plot_uicc_ecog_inventory
    plot_uicc_distribution_grouped_bar
    plot_ecog_distribution_grouped_bar

E – Merge-Delta & Sweep (Panel-Plots)
    plot_merge_panel
    plot_sweep_panel

F – Basis-Plots
    scatterplot
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Union

import matplotlib.cm as cm
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

# ══════════════════════════════════════════════════════════════════════════════
# GETEILTE KONSTANTEN
# ══════════════════════════════════════════════════════════════════════════════

UICC_MAPPING = {
    "0": "0", "0a": "0", "0is": "0",
    "I": "I", "IA": "I", "IA2": "I", "IA3": "I", "IB": "I", "IB2": "I",
    "II": "II", "IIA": "II", "IIB": "II", "IIC": "II",
    "III": "III", "IIIA": "III", "IIIB": "III", "IIIC": "III",
    "IV": "IV", "IVA": "IV", "IVB": "IV", "IVC": "IV",
}
UICC_ORDER  = ["missing", "0", "I", "II", "III", "IV"]
ECOG_ORDER  = ["0", "1", "2", "3", "4"]

UICC_GROUPS = {
    "0":   {"substages": ["0", "0a", "0is"],                        "cmap": "Greys",   "cmap_range": (0.4, 0.75), "header_color": "#555555"},
    "I":   {"substages": ["I", "IA", "IA1", "IA2", "IA3", "IB", "IB1", "IB2", "IC"], "cmap": "Blues",   "cmap_range": (0.35, 0.90), "header_color": "#08519c"},
    "II":  {"substages": ["II", "IIA", "IIA1", "IIA2", "IIB", "IIC"],                 "cmap": "Greens",  "cmap_range": (0.35, 0.90), "header_color": "#006d2c"},
    "III": {"substages": ["III", "IIIA", "IIIB", "IIIC", "IIIC1"],                    "cmap": "Oranges", "cmap_range": (0.35, 0.90), "header_color": "#a63603"},
    "IV":  {"substages": ["IV", "IVA", "IVB", "IVC"],                                 "cmap": "Purples", "cmap_range": (0.35, 0.90), "header_color": "#54278f"},
}
UICC_GROUP_ORDER = ["missing", "0", "I", "II", "III", "IV"]

_DEFAULT_COLORS = ["#4C78A8", "#F58518", "#54A24B", "#B279A2", "#E45756", "#72B7B2"]


# ══════════════════════════════════════════════════════════════════════════════
# PRIVATE HILFSFUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

def _gini(values: np.ndarray) -> float:
    v = np.sort(values[values >= 0].astype(float))
    if v.size == 0 or v.sum() == 0:
        return 0.0
    n   = v.size
    cum = np.cumsum(v)
    return (n + 1 - 2 * np.sum(cum) / cum[-1]) / n


def _therapy_counts_per_cond(
    df: pd.DataFrame, cond_col: str, n_total_patients: int
) -> pd.Series:
    counts = df.groupby(cond_col).size()
    n_without = max(0, n_total_patients - len(counts))
    if n_without > 0:
        zeros  = pd.Series(np.zeros(n_without, dtype=int))
        counts = pd.concat([counts, zeros], ignore_index=True)
    return counts


def _generate_colors(cmap_name: str, n: int, cmap_range: tuple) -> list:
    cmap   = cm.get_cmap(cmap_name)
    values = np.linspace(cmap_range[0], cmap_range[1], max(n, 1))
    return [cmap(v) for v in values]


def _build_grouped_bar(
    agg: pd.DataFrame,
    category_order: list,
    therapies: list,
    color_map: dict,
    xlabel: str,
    title: str,
    save_path: str | None,
    dpi: int,
) -> plt.Figure:
    """Gemeinsame Rendering-Logik für UICC- und ECOG-Grouped-Bar-Charts."""
    therapy_totals = agg.groupby("therapy")["count"].sum().to_dict()

    x_positions = np.arange(len(category_order))
    n           = len(therapies)
    width       = 0.8 / n
    offsets     = np.linspace(-(n - 1) / 2, (n - 1) / 2, n) * width

    fig, ax = plt.subplots(figsize=(14, 6))

    for offset, therapy in zip(offsets, therapies):
        sub    = agg[agg["therapy"] == therapy].set_index("category").reindex(category_order).fillna(0)
        counts = sub["count"].values
        total  = int(therapy_totals.get(therapy, 0))

        bars = ax.bar(
            x_positions + offset, counts, width=width,
            color=color_map[therapy], alpha=0.9,
            label=f"{therapy} (n={total:,})",
        )
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2, height + 0.3,
                    f"{int(height)}", ha="center", fontsize=7,
                )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(category_order, rotation=0, ha="center")
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Number of Condition IDs", fontsize=11)
    if title:
        ax.set_title(title, fontsize=15, fontweight="bold", pad=15)
    ax.legend(frameon=False, title="Therapy", title_fontsize=10)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

    if save_path:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor="white")
        print(f"Plot saved to: {path.resolve()}")
    else:
        plt.show()

    plt.close()
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# A – ALTERSVERTEILUNG
# ══════════════════════════════════════════════════════════════════════════════

def plot_age_distribution_grouped_bar(
    dataframes: dict,
    age_column: str              = "age_at_diagnosis",
    age_bracket: int             = 10,
    title: str | bool | None     = None,
    colors: dict                 = None,
    save_path: str               = None,
    dpi: int                     = 300,
) -> plt.Figure:
    """
    Altersverteilung der Patienten pro Therapietyp als gruppiertes Balkendiagramm.

    Parameters
    ----------
    dataframes  : dict – Therapiename → DataFrame (muss `age_column` enthalten)
    age_column  : Spalte mit dem Alter bei Diagnose
    age_bracket : Breite der Altersgruppe in Jahren
    title       : None = automatisch | str = benutzerdefiniert | False = kein Titel
    colors      : Therapiename → Hex-Farbe (optional)
    save_path   : Speicherpfad; wenn None → plt.show()
    dpi         : Auflösung beim Speichern
    """
    color_map = {
        therapy: (colors or {}).get(therapy, _DEFAULT_COLORS[i % len(_DEFAULT_COLORS)])
        for i, therapy in enumerate(dataframes.keys())
    }

    bins   = list(range(0, 105, age_bracket))
    labels = [f"{b}–{b + age_bracket - 1}" for b in bins[:-1]]
    rows   = []

    for therapy, df in dataframes.items():
        if age_column not in df.columns:
            raise ValueError(f"Spalte '{age_column}' nicht in Therapiegruppe '{therapy}'.")

        ages   = pd.to_numeric(df[age_column], errors="coerce").dropna()
        counts = (
            pd.cut(ages, bins=bins, labels=labels, right=False)
            .value_counts().reindex(labels, fill_value=0).reset_index()
        )
        counts.columns = ["age_bracket", "count"]
        counts["therapy"] = therapy
        rows.append(counts)

    agg         = pd.concat(rows, ignore_index=True)
    therapies   = list(dataframes.keys())
    x_positions = np.arange(len(labels))
    n           = len(therapies)
    width       = 0.8 / n
    offsets     = np.linspace(-(n - 1) / 2, (n - 1) / 2, n) * width

    fig, ax = plt.subplots(figsize=(14, 6))

    for offset, therapy in zip(offsets, therapies):
        sub    = agg[agg["therapy"] == therapy].set_index("age_bracket").reindex(labels).fillna(0)
        counts = sub["count"].values
        bars   = ax.bar(
            x_positions + offset, counts, width=width,
            color=color_map[therapy], alpha=0.9, label=therapy,
        )
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2, height + 0.3,
                    f"{int(height)}", ha="center", fontsize=7,
                )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_xlabel("Age Group", fontsize=11)
    ax.set_ylabel("Number of Condition IDs", fontsize=11)

    if title is not False:
        chart_title = title or f"Age Distribution by Therapy Type ({age_bracket}-Year Groups)"
        ax.set_title(chart_title, fontsize=15, fontweight="bold", pad=15)

    ax.legend(frameon=False)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

    if save_path:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor="white")
        print(f"Plot saved to: {path.resolve()}")
    else:
        plt.show()

    plt.close()
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# B – THERAPIE-ZEITPUNKTE & BIAS
# ══════════════════════════════════════════════════════════════════════════════

def plot_therapy_times(
    firstline_df,
    rel_time_col: str  = "rel_time",
    title: str         = "Verteilung der ersten Therapien",
    therapy_col: str   = None,
    figsize: tuple     = (10, 6),
    save_path: str     = "plots/firstline_therapy/firstline_violinplot.png",
    dpi: int           = 300,
    allow_negative: bool = True,
) -> None:
    """Violin-Plot der Firstline-Therapiezeitpunkte relativ zur Diagnose."""
    negative_firstline = (firstline_df[rel_time_col] < 0).sum()

    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=figsize)

    violin_kwargs = dict(inner="box", cut=0, ax=ax)
    strip_kwargs  = dict(color="black", alpha=0.4, size=3, jitter=True, ax=ax)

    if therapy_col and therapy_col in firstline_df.columns:
        sns.violinplot(data=firstline_df, x=therapy_col, y=rel_time_col, **violin_kwargs)
        sns.stripplot(data=firstline_df, x=therapy_col, y=rel_time_col, **strip_kwargs)
    else:
        sns.violinplot(data=firstline_df, y=rel_time_col, **violin_kwargs)
        sns.stripplot(data=firstline_df, y=rel_time_col, **strip_kwargs)

    ax.axhline(0, color="red", linestyle="--", linewidth=1.5, label="Diagnosezeitpunkt")

    stats_text = (
        f"N Patienten: {len(firstline_df)}\n"
        f"Negative Zeitpunkte: {negative_firstline}\n"
        f"Median: {firstline_df[rel_time_col].median():.2f}\n"
        f"Mittelwert: {firstline_df[rel_time_col].mean():.2f}\n"
        f"Min: {firstline_df[rel_time_col].min():.2f}\n"
        f"Max: {firstline_df[rel_time_col].max():.2f}\n"
        f"allow_negative={allow_negative}"
    )
    ax.text(
        0.98, 0.02, stats_text, transform=ax.transAxes,
        fontsize=10, verticalalignment="bottom", horizontalalignment="right",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85),
    )
    ax.set_title(title, fontsize=14, weight="bold")
    ax.set_ylabel("Relativer Therapiezeitpunkt")
    ax.legend()
    plt.tight_layout()

    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.savefig(save_path, dpi=dpi, bbox_inches="tight", facecolor="white")
        print(f"\nPlot gespeichert unter:\n{save_path.resolve()}")
    except Exception as e:
        print(f"Fehler beim Speichern: {e}")
    plt.close(fig)


def plot_therapy_bias_analysis(
    df,
    diagnosis_year_col: str,
    relative_col: str       = "months_between_asserted_therapy_start_date",
    therapy_type: str       = "",
    title: Optional[str]    = None,
    save_path               = None,
    figsize: tuple          = (14, 4),
    font_family: str        = "DejaVu Sans",
) -> None:

    plt.rcParams["font.family"] = font_family

    data = df.copy()
    data[diagnosis_year_col] = pd.to_numeric(data[diagnosis_year_col], errors="coerce")
    data[relative_col]       = pd.to_numeric(data[relative_col],       errors="coerce")
    data = data.dropna(subset=[diagnosis_year_col, relative_col])

    sorted_years   = sorted(data[diagnosis_year_col].unique())
    year_min       = int(min(sorted_years))
    year_max       = int(max(sorted_years))
    all_years      = list(range(year_min, year_max + 1))
    existing_years = {int(y) for y in sorted_years}
    total_cases    = len(data)

    cases_per_year    = data[diagnosis_year_col].value_counts().sort_index()
    cases_all_line    = [cases_per_year.get(y, float("nan")) for y in all_years]
    cases_all_markers = [cases_per_year.get(y, None) for y in all_years]

    fig, axes = plt.subplots(
        2, 1, figsize=figsize, sharex=True,
        gridspec_kw={"height_ratios": [1, 2]},
    )
    fig.subplots_adjust(hspace=0.35)

    if title:
        fig.suptitle(title, fontsize=11, fontweight="bold", y=1.01)

    # – Subplot 1: Case Count ─────────────────────────────────────────────────
    axes[0].plot(all_years, cases_all_line, linestyle="-", color="steelblue")
    axes[0].plot(
        [y for y, v in zip(all_years, cases_all_markers) if v is not None],
        [v for v in cases_all_markers if v is not None],
        marker="o", linestyle="None", color="steelblue",
    )
    therapy_label = f" – {therapy_type}" if therapy_type else ""
    axes[0].set_title(
        f"Number of {therapy_type or 'Therapy'} Records per First Diagnosis Year"
        f"{therapy_label}  n = {total_cases:,} records",
        fontsize=8, fontweight="bold", pad=3,
    )
    axes[0].set_ylabel("Count", fontsize=8)
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    axes[0].tick_params(labelsize=7)
    axes[0].spines[["top", "right"]].set_visible(False)
    axes[0].grid(axis="y", linestyle="--", alpha=0.35)

    # – Subplot 2: Boxplot ────────────────────────────────────────────────────
    positions_with_data   = [y for y in all_years if y in existing_years]
    grouped_data_filtered = [
        data.loc[data[diagnosis_year_col] == year, relative_col].values
        for year in positions_with_data
    ]
    axes[1].boxplot(
        grouped_data_filtered, positions=positions_with_data,
        widths=0.6, showfliers=True, manage_ticks=False,
    )
    axes[1].set_title(
        "Distribution of the rel. Time Interval between First Diagnosis Year and Therapy Date in Months (Boxplot)",
        fontsize=8, fontweight="bold", pad=3,
    )
    axes[1].set_xlabel("First Diagnosis Year", fontsize=8)
    axes[1].set_ylabel("Months", fontsize=8)
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    axes[1].tick_params(labelsize=7)
    axes[1].spines[["top", "right"]].set_visible(False)
    axes[1].grid(axis="y", linestyle="--", alpha=0.35)

    tick_step  = max(1, (year_max - year_min) // 20)
    tick_years = [y for y in all_years if (y - year_min) % tick_step == 0]
    axes[1].set_xlim(year_min - 0.5, year_max + 0.5)
    axes[1].set_xticks(tick_years)
    axes[1].set_xticklabels([str(y) for y in tick_years], rotation=90, fontsize=7)

    fig.tight_layout(pad=0.4, h_pad=0.5)

    if save_path is not None:
        directory = os.path.dirname(save_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches=None)
        print(f"Plot saved to: {save_path}")

    plt.show()
    plt.close(fig)


def plot_dropout_curve(
    result_df,
    titel: Optional[str]         = None,
    speichern: bool              = False,
    ordner: str                  = "plots",
    dateiname: str               = "dropout_curve.png",
    dpi: int                     = 300,
    font_family: str             = "DejaVu Sans",
    highlight_month: Optional[int] = None,
    log_y: bool                  = False,
):
    """
    Plottet die Dropout-Kurve (Anteil verbleibender cond_ids je Cutoff-Monat).
    Eingabe ist die Ausgabe von `patient_dropout_by_cutoff` aus data_processing.py.
    """
    plt.rcParams["font.family"] = font_family

    n_total  = result_df["patients_remaining"].max()
    pct_vals = result_df["patients_remaining"] / n_total * 100

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(
        result_df["cutoff_month"], pct_vals,
        marker="o", markersize=4, color="#2e6fba", linewidth=1.8, zorder=2,
    )
    ax.set_xlabel("Therapy Start Date - Date of Diagnosis (Month)", fontsize=10)
    ax.set_ylabel("Number of cond_ids (%)", fontsize=10)
    ax.set_xlim(0, result_df["cutoff_month"].max() + 0.5)
    ax.set_ylim(0, 105)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    if log_y:
        ax.set_yscale("log")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    if titel:
        ax.set_title(titel, fontsize=13, fontweight="bold", pad=10)

    if highlight_month is not None:
        hl_row = result_df[result_df["cutoff_month"] == highlight_month]
        if not hl_row.empty:
            hl_x = highlight_month
            hl_y = hl_row["patients_remaining"].values[0] / n_total * 100
            ax.plot([hl_x, hl_x], [0, hl_y], color="red", linestyle="--",
                    linewidth=1.2, alpha=0.7, zorder=3)
            ax.plot([0, hl_x],    [hl_y, hl_y], color="red", linestyle="--",
                    linewidth=1.2, alpha=0.7, zorder=3)
            ax.scatter(hl_x, hl_y, color="red", s=80, zorder=5)
            print(f"  Highlight month={hl_x}  →  "
                  f"{hl_row['patients_remaining'].values[0]:,} cond_ids  ({hl_y:.1f}%)")
        else:
            print(f"[WARNING] highlight_month={highlight_month} nicht in result_df")

    ax.text(
        0.5, -0.10, f"Total cond_ids: {n_total:,}",
        transform=ax.transAxes,
        ha="center", va="top", fontsize=7.5, color="black", style="italic",
    )
    fig.tight_layout()

    if speichern:
        save_path = Path(ordner)
        save_path.mkdir(parents=True, exist_ok=True)
        full_path = save_path / dateiname
        fig.savefig(full_path, dpi=dpi, bbox_inches="tight")
        print(f"Plot gespeichert als: {full_path.resolve()}")

    plt.show()
    plt.close(fig)
    return result_df, fig


# ══════════════════════════════════════════════════════════════════════════════
# C – BESTAND THERAPIEN
# ══════════════════════════════════════════════════════════════════════════════

def plot_lorenz_curve(
    df: pd.DataFrame,
    n_total_patients: int,
    cond_col: str                   = "cond_id",
    cohort_name: str                = "",
    output_path: Optional[Union[str, Path]] = None,
    show: bool                      = True,
) -> None:
    """Lorenz-Kurve der Therapien pro cond_id (mit Gini-Koeffizient)."""
    counts = _therapy_counts_per_cond(df, cond_col, n_total_patients)
    values = np.sort(counts.to_numpy(dtype=float))

    if values.size == 0:
        print("[WARNUNG] Keine Daten für Lorenz-Kurve")
        return

    cum_vals = np.cumsum(values)
    cum_vals = np.insert(cum_vals, 0, 0)
    if cum_vals[-1] > 0:
        cum_vals = cum_vals / cum_vals[-1]
    cum_pop = np.linspace(0, 1, len(cum_vals))
    gini    = _gini(values)

    fig, ax = plt.subplots(figsize=(7, 6.5))
    ax.plot(cum_pop, cum_vals, color="#2e6fba", linewidth=2.2, label="Lorenz-Kurve")
    ax.fill_between(cum_pop, cum_vals, cum_pop, alpha=0.12, color="#2e6fba")
    ax.plot([0, 1], [0, 1], linestyle="--", color="#888888", linewidth=1.2,
            label="Gleichverteilung")
    ax.text(0.05, 0.93, f"Gini = {gini:.3f}", transform=ax.transAxes,
            fontsize=11, fontweight="bold", color="#2e6fba")
    ax.set_xlabel("Kumulativer Anteil der cond_id (aufsteigend sortiert)", fontsize=10)
    ax.set_ylabel("Kumulativer Anteil der Therapien", fontsize=10)
    ax.set_title("Lorenz-Kurve – Therapien pro cond_id", fontsize=13, fontweight="bold", pad=14)
    ax.text(
        0.5, -0.12,
        f"{cohort_name} | cond_ids={counts.size:,} | Therapien={len(df):,}",
        transform=ax.transAxes, ha="center", va="top",
        fontsize=7.5, color="#555555", style="italic",
    )
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
    if show:
        plt.show()
    plt.close(fig)


def plot_log_histogram(
    df: pd.DataFrame,
    n_total_patients: int,
    cond_col: str              = "cond_id",
    cohort_name: str           = "",
    output_path: Optional[Path] = None,
    show: bool                 = True,
    bins: int                  = 60,
    year_col: Optional[str]    = None,
    year_cutoff: Optional[int] = None,
    n_patients_before: Optional[int] = None,
    n_patients_from: Optional[int]   = None,
    title: Optional[str]       = "Distribution of Therapies per cond_id",
    font_family: str           = "DejaVu Sans",
) -> None:
    """
    Log-skaliertes Histogramm der Therapien pro cond_id.
    Optional aufgeteilt nach Jahr (vor/ab year_cutoff).
    """
    plt.rcParams["font.family"] = font_family

    counts = _therapy_counts_per_cond(df, cond_col, n_total_patients)
    if counts.empty:
        print("[WARNING] No data for histogram")
        return

    n_with_therapy = df[cond_col].nunique()
    use_year_split = (year_col is not None) and (year_cutoff is not None)

    if use_year_split:
        df_before = df[df[year_col] < year_cutoff]
        df_from   = df[df[year_col] >= year_cutoff]
        counts_before = _therapy_counts_per_cond(df_before, cond_col, n_patients_before)
        counts_from   = _therapy_counts_per_cond(df_from,   cond_col, n_patients_from)
        fig, (ax_before, ax_from) = plt.subplots(2, 1, figsize=(10, 9), sharex=True)
        subplots = [
            (ax_before, counts_before, f"Before {year_cutoff}", "#2e6fba", df_before, n_patients_before),
            (ax_from,   counts_from,   f"From {year_cutoff}",   "#e07b39", df_from,   n_patients_from),
        ]
    else:
        fig, ax = plt.subplots(figsize=(10, 5))
        subplots = [(ax, counts, "", "#2e6fba", df, n_total_patients)]

    bin_edges = np.linspace(counts.min(), counts.max(), bins + 1)

    for entry in subplots:
        ax, subset, label, color, df_sub, n_pat_sub = entry

        if subset.empty:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                    ha="center", va="center", fontsize=11, color="#888888")
            ax.set_ylabel("Count cond_id (log scale)", fontsize=10)
            if use_year_split:
                ax.set_title(label, fontsize=11, fontweight="bold", pad=8)
            continue

        _, _, patches = ax.hist(
            subset, bins=bin_edges, color=color,
            edgecolor="white", linewidth=0.6, rwidth=0.98,
        )
        for patch in patches:
            height = patch.get_height()
            if height == 0:
                continue
            ax.text(
                patch.get_x() + patch.get_width() / 2, height * 1.15,
                f"{int(height):,}", ha="center", va="bottom",
                fontsize=8, color="#333333", rotation_mode="anchor",
            )

        ax.set_yscale("log")
        ax.set_ylabel("Count cond_id (log scale)", fontsize=10)
        if use_year_split:
            ax.set_title(label, fontsize=11, fontweight="bold", pad=8)

        stats = (
            f"Median={subset.median():.0f}  Mean={subset.mean():.1f}  "
            f"P90={subset.quantile(0.90):.0f}  Max={subset.max():.0f}"
        )
        ax.text(
            0.98, 0.97, stats, transform=ax.transAxes,
            ha="right", va="top", fontsize=8.5,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#cccccc"),
        )
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)

    bottom_ax = subplots[-1][0]
    bottom_ax.set_xlabel("Therapies per cond_id", fontsize=10)
    bottom_ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

    if title:
        fig.suptitle(title, fontsize=13, fontweight="bold", y=1.01)

    footer_kwargs = dict(ha="center", va="top", fontsize=8, color="black", style="italic")
    if use_year_split:
        n_with_before = df_before[cond_col].nunique()
        n_with_from   = df_from[cond_col].nunique()
        ax_before.text(0.5, -0.04,
            f"{cohort_name} | cond_ids={counts_before.size:,} | "
            f"Therapies={len(df_before):,} | cond_ids with therapy={n_with_before:,}",
            transform=ax_before.transAxes, **footer_kwargs)
        ax_from.text(0.5, -0.08,
            f"{cohort_name} | cond_ids={counts_from.size:,} | "
            f"Therapies={len(df_from):,} | Patients with therapy={n_with_from:,} / {n_patients_from:,}",
            transform=ax_from.transAxes, **footer_kwargs)
    else:
        bottom_ax.text(0.5, -0.14,
            f"{cohort_name} | cond_ids={counts.size:,} | "
            f"Therapies={len(df):,} | cond_ids with therapy={n_with_therapy:,} / {n_total_patients:,}",
            transform=bottom_ax.transAxes, **footer_kwargs)

    fig.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
    if show:
        plt.show()
    plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# D – UICC / ECOG INVENTAR & VERTEILUNG
# ══════════════════════════════════════════════════════════════════════════════

def plot_uicc_ecog_inventory(
    df_uicc,
    df_ecog,
    uicc_col: str                    = "uicc_tnm",
    ecog_col: str                    = "ecog_performance_status",
    cond_col: str                    = "cond_id",
    cohort_name: str                 = "",
    n_total_cond_ids: Optional[int]  = None,
    title: Optional[str]             = "UICC & ECOG Inventory",
    output_path: Optional[Path]      = None,
    show: bool                       = True,
    font_family: str                 = "DejaVu Sans",
) -> None:
    """
    Zwei-Panel-Plot: UICC-Stage-Verteilung (gestackte Horizontal-Balken)
    und ECOG-Verteilung nebeneinander.
    """
    plt.rcParams["font.family"] = font_family

    group_colors = {
        main: _generate_colors(cfg["cmap"], len(cfg["substages"]), cfg["cmap_range"])
        for main, cfg in UICC_GROUPS.items()
    }

    def _value_counts(series):
        return (
            series.astype(str).str.strip()
            .value_counts(dropna=False)
            .rename(index={"nan": "missing", "unknown": "missing"})
        )

    # ── UICC ──────────────────────────────────────────────────────────────────
    uicc_vc    = _value_counts(df_uicc[uicc_col])
    uicc_total = int(uicc_vc.sum())
    uicc_data  = {
        main: {sub: int(uicc_vc.get(sub, 0)) for sub in cfg["substages"]}
        for main, cfg in UICC_GROUPS.items()
    }
    uicc_unknown = int(uicc_vc.get("missing", 0))
    uicc_n_known = sum(sum(uicc_data[m].values()) for m in UICC_GROUP_ORDER if m != "missing")
    n_uicc_cond_with_known = (
        df_uicc[df_uicc[uicc_col].astype(str).str.strip().ne("missing")][cond_col].nunique()
    )
    max_known_bar = max(sum(uicc_data[m].values()) for m in UICC_GROUP_ORDER if m != "missing")
    uicc_max_val  = max(uicc_unknown, max_known_bar)

    # ── ECOG ──────────────────────────────────────────────────────────────────
    ECOG_ORDER_INV = ["U", "0", "1", "2", "3", "4"]
    ECOG_COLORS    = {
        "U": "#b0b0b0", "0": "#08519c", "1": "#3182bd",
        "2": "#f5a623", "3": "#e07b39", "4": "#c0392b",
    }
    ecog_vc      = _value_counts(df_ecog[ecog_col])
    ecog_total   = int(ecog_vc.sum())
    ecog_labels  = [v for v in ECOG_ORDER_INV if str(v) in ecog_vc.index]
    ecog_vals    = [int(ecog_vc.get(v, 0)) for v in ecog_labels]
    ecog_colors  = [ECOG_COLORS.get(str(v), "#b0b0b0") for v in ecog_labels]
    ecog_n_known = sum(v for l, v in zip(ecog_labels, ecog_vals) if l != "U")
    n_ecog_cond_with_known = (
        df_ecog[df_ecog[ecog_col].astype(str).str.strip().ne("U")][cond_col].nunique()
    )
    ecog_max = max(ecog_vals) if ecog_vals else 1

    # ── Figure ────────────────────────────────────────────────────────────────
    fig, (ax_uicc, ax_ecog) = plt.subplots(
        1, 2, figsize=(22, 8),
        gridspec_kw={"width_ratios": [3, 1], "wspace": 0.35},
    )

    # UICC gestackte Balken
    for i, main in enumerate(UICC_GROUP_ORDER):
        if main == "missing":
            val = uicc_unknown
            ax_uicc.barh(i, val, color="#b0b0b0", edgecolor="white",
                         linewidth=0.6, height=0.65)
            ax_uicc.text(val + uicc_max_val * 0.004, i,
                         f"{val:,}  ({100*val/uicc_total:.1f}%)",
                         va="center", ha="left", fontsize=8.5, color="#888888")
        else:
            cfg    = UICC_GROUPS[main]
            colors = group_colors[main]
            left   = 0
            for sub, color in zip(cfg["substages"], colors):
                val = uicc_data[main][sub]
                if val == 0:
                    continue
                ax_uicc.barh(i, val, left=left, color=color,
                             edgecolor="white", linewidth=0.6, height=0.65)
                left += val
            row_total = sum(uicc_data[main].values())
            ax_uicc.text(left + uicc_max_val * 0.004, i,
                         f"{row_total:,}  ({100*row_total/uicc_total:.1f}%)",
                         va="center", ha="left", fontsize=8.5, color="#333333")

    ax_uicc.axhline(0.5, color="#aaaaaa", linewidth=1.0, linestyle="--")
    ax_uicc.set_yticks(range(len(UICC_GROUP_ORDER)))
    ax_uicc.set_yticklabels(
        ["Missing" if m == "missing" else f"Stage {m}" for m in UICC_GROUP_ORDER],
        fontsize=10, fontweight="bold",
    )

    x_legend_start = max_known_bar * 1.18
    xlim_total     = max(uicc_max_val * 1.15, x_legend_start * 3.0)
    ax_uicc.set_xlim(0, xlim_total)
    ax_uicc.set_ylim(-0.5, len(UICC_GROUP_ORDER) - 0.5)
    ax_uicc.spines[["top", "right", "bottom"]].set_visible(False)
    ax_uicc.xaxis.set_visible(False)
    ax_uicc.tick_params(left=False)
    for i in range(0, len(UICC_GROUP_ORDER), 2):
        ax_uicc.axhspan(i - 0.45, i + 0.45, color="#f7f7f7", zorder=0)
    ax_uicc.set_title(
        f"UICC Stage Distribution\n"
        f"rows = {uicc_total:,}  |  known entries = {uicc_n_known:,}  |  "
        f"cond_ids with known UICC = {n_uicc_cond_with_known:,}",
        fontsize=10, fontweight="bold", pad=10,
    )

    # Legende UICC
    known_groups   = ["0", "I", "II", "III", "IV"]
    max_subs       = max(len(UICC_GROUPS[m]["substages"]) for m in UICC_GROUPS)
    rows_per_col   = max_subs + 1
    legend_patches = []
    for main in known_groups:
        cfg        = UICC_GROUPS[main]
        colors     = group_colors[main]
        main_total = sum(uicc_data[main].values())
        legend_patches.append(
            mpatches.Patch(color=cfg["header_color"],
                           label=f"$\\bf{{Stage\\ {main}}}$  –  {main_total:,}")
        )
        for sub, color in zip(cfg["substages"], colors):
            val = uicc_data[main][sub]
            legend_patches.append(mpatches.Patch(color=color, label=f"{sub:<7} {val:,}"))
        for _ in range(rows_per_col - 1 - len(cfg["substages"])):
            legend_patches.append(mpatches.Patch(color="none", label=""))

    leg_anchor_x = x_legend_start * 1.05
    leg_anchor_y = (1 + len(UICC_GROUP_ORDER) - 1) / 2 + 0.7
    leg = ax_uicc.legend(
        handles=legend_patches,
        ncol=len(known_groups),
        loc="center left",
        bbox_to_anchor=(leg_anchor_x, leg_anchor_y),
        bbox_transform=ax_uicc.transData,
        framealpha=0.95, edgecolor="#cccccc",
        fontsize=8.5, title="Sub-stage composition  •  n cond_ids",
        title_fontsize=9.5, handlelength=1.1, handleheight=1.1,
        borderpad=0.9, labelspacing=0.45, columnspacing=2.5,
    )
    for handle, text in zip(leg.legend_handles, leg.get_texts()):
        if text.get_text() == "":
            handle.set_visible(False)
            text.set_visible(False)

    # ECOG Balken
    bars = ax_ecog.barh(
        range(len(ecog_labels)), ecog_vals, color=ecog_colors,
        edgecolor="white", linewidth=0.6, height=0.60,
    )
    for bar, val, lbl in zip(bars, ecog_vals, ecog_labels):
        ax_ecog.text(
            bar.get_width() + ecog_max * 0.012,
            bar.get_y() + bar.get_height() / 2,
            f"{val:,}  ({100*val/ecog_total:.1f}%)",
            va="center", ha="left", fontsize=8.5,
            color="#888888" if lbl == "U" else "#333333",
        )
    ax_ecog.set_yticks(range(len(ecog_labels)))
    ax_ecog.set_yticklabels(
        ["Unknown" if l == "U" else f"ECOG {l}" for l in ecog_labels],
        fontsize=10, fontweight="bold",
    )
    ax_ecog.set_xlim(0, ecog_max * 1.55)
    ax_ecog.spines[["top", "right", "bottom"]].set_visible(False)
    ax_ecog.xaxis.set_visible(False)
    ax_ecog.tick_params(left=False)
    ax_ecog.axhline(0.5, color="#aaaaaa", linewidth=1.0, linestyle="--")
    for i in range(0, len(ecog_labels), 2):
        ax_ecog.axhspan(i - 0.45, i + 0.45, color="#f7f7f7", zorder=0)
    ax_ecog.set_title(
        f"ECOG Performance Status\n"
        f"rows = {ecog_total:,}  |  known entries = {ecog_n_known:,}  |  "
        f"cond_ids with known ECOG = {n_ecog_cond_with_known:,}",
        fontsize=10, fontweight="bold", pad=10,
    )

    if title:
        sup = f"{cohort_name} – {title}" if cohort_name else title
        if n_total_cond_ids:
            sup += f"\n(Cohort: {n_total_cond_ids:,} cond_ids total)"
        fig.suptitle(sup, fontsize=13, fontweight="bold", y=1.01)

    fig.tight_layout()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Plot saved to: {output_path}")
    if show:
        plt.show()
    plt.close(fig)


def plot_uicc_distribution_grouped_bar(
    dataframes: dict,
    uicc_column: str = "uicc_tnm",
    title: str       = None,
    colors: dict     = None,
    save_path: str   = None,
    dpi: int         = 300,
) -> plt.Figure:
    """
    Gruppiertes Balkendiagramm der UICC-Stage-Verteilung pro Therapietyp.
    Granulare Sub-Stufen (IA, IIB, …) werden auf kanonische Stufen reduziert.
    """
    color_map = {
        therapy: (colors or {}).get(therapy, _DEFAULT_COLORS[i % len(_DEFAULT_COLORS)])
        for i, therapy in enumerate(dataframes.keys())
    }
    rows = []
    for therapy, df in dataframes.items():
        if uicc_column not in df.columns:
            raise ValueError(f"Spalte '{uicc_column}' nicht in '{therapy}'.")
        raw    = df[uicc_column].astype(str).str.strip()
        mapped = raw.map(lambda v: UICC_MAPPING.get(v, "missing"))
        counts = (
            mapped.value_counts()
            .reindex(UICC_ORDER, fill_value=0)
            .reset_index()
        )
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)

    agg = pd.concat(rows, ignore_index=True)
    return _build_grouped_bar(
        agg=agg, category_order=UICC_ORDER,
        therapies=list(dataframes.keys()),
        color_map=color_map, xlabel="UICC Stage",
        title=title, save_path=save_path, dpi=dpi,
    )


def plot_ecog_distribution_grouped_bar(
    dataframes: dict,
    ecog_column: str = "ecog_performance_status",
    title: str       = None,
    colors: dict     = None,
    save_path: str   = None,
    dpi: int         = 300,
) -> plt.Figure:
    """
    Gruppiertes Balkendiagramm der ECOG-Verteilung pro Therapietyp.
    Bekannte ECOG-Werte (0–4) zuerst, dann unbekannte.
    """
    color_map = {
        therapy: (colors or {}).get(therapy, _DEFAULT_COLORS[i % len(_DEFAULT_COLORS)])
        for i, therapy in enumerate(dataframes.keys())
    }
    rows       = []
    all_values = set()

    for therapy, df in dataframes.items():
        if ecog_column not in df.columns:
            raise ValueError(f"Spalte '{ecog_column}' nicht in '{therapy}'.")
        raw = df[ecog_column].astype(str).str.strip()
        all_values.update(raw.unique())
        counts = raw.value_counts().reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)

    known          = sorted([v for v in all_values if v in ECOG_ORDER], key=lambda x: int(x))
    unknown        = sorted([v for v in all_values if v not in ECOG_ORDER])
    category_order = known + unknown
    agg            = pd.concat(rows, ignore_index=True)

    return _build_grouped_bar(
        agg=agg, category_order=category_order,
        therapies=list(dataframes.keys()),
        color_map=color_map, xlabel="ECOG Performance Status",
        title=title, save_path=save_path, dpi=dpi,
    )


# ══════════════════════════════════════════════════════════════════════════════
# E – MERGE-DELTA & SWEEP (PANEL-PLOTS)
# ══════════════════════════════════════════════════════════════════════════════

def plot_merge_panel(
    panel_entries: list,
    log_y: bool          = True,
    bins: int            = 30,
    save_path: Optional[str] = None,
    file_name: str       = "panel_months_diff.tiff",
    dpi: int             = 300,
    font_family: str     = "DejaVu Sans",
) -> None:
    """
    3×2 Panel-Plot der months_diff-Histogramme aller Merge-Paare.

    panel_entries: Liste von Dicts mit keys:
        df, title, stats, staging_label
    """
    plt.rcParams["font.family"] = font_family

    fig, axes = plt.subplots(3, 2, figsize=(12, 10))
    fig.subplots_adjust(hspace=0.60, wspace=0.30)

    for idx, entry in enumerate(panel_entries):
        row = idx // 2
        col = idx  % 2
        ax  = axes[row][col]

        data          = entry["df"]["months_diff"].dropna()
        stats         = entry.get("stats", {})
        staging_label = entry.get("staging_label", "Staging")

        ax.hist(data, bins=bins, color="#2e6fba", edgecolor="white", linewidth=0.4)
        if log_y:
            ax.set_yscale("log")
        ax.set_title(entry["title"], fontsize=9, fontweight="bold", pad=5)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        ax.tick_params(labelsize=8)
        if row == 2:
            ax.set_xlabel("Delta (Months)", fontsize=8)
        if col == 0:
            ax.set_ylabel("Count (log)" if log_y else "Count", fontsize=8)

        if stats:
            ax.text(
                0.98, 0.97,
                f"Therapy in     : {stats['n_therapy']:>7,}\n"
                f"{staging_label:<14} : {stats['n_staging']:>7,}\n"
                f"Matched        : {stats['n_matched']:>7,}\n"
                f"Unmatched      : {stats['n_unmatched']:>7,}",
                transform=ax.transAxes,
                ha="right", va="top", fontsize=7, fontfamily="monospace",
                bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="#cccccc"),
            )

    fig.suptitle("Time Delta: Nearest Staging before Therapy",
                 fontsize=12, fontweight="bold", y=1.01)
    fig.tight_layout()

    if save_path is not None:
        full_path = Path(save_path) / file_name
        Path(save_path).mkdir(parents=True, exist_ok=True)
        fig.savefig(full_path, dpi=dpi, bbox_inches="tight")
        print(f"Panel gespeichert: {full_path.resolve()}")

    plt.show()
    plt.close(fig)


def plot_sweep_panel(
    sweeps: list,
    highlight_tolerance: Optional[int] = None,
    save_plot: bool   = False,
    save_dir: str     = "plots",
    file_name: str    = "sweep_panel.tiff",
    dpi: int          = 300,
    font_family: str  = "DejaVu Sans",
) -> None:
    """
    3×2 Panel-Plot der Toleranz-Sweep-Kurven für alle Therapie-Staging-Paare.

    sweeps: Liste von Dicts mit keys:
        df (Ausgabe von sweep_tolerance_fast), therapy, pair, highlight, n_total
    """
    plt.rcParams["font.family"] = font_family

    PAIR_COLORS = {"ECOG": "#2e6fba", "UICC": "#2e8b57"}

    fig, axes = plt.subplots(3, 2, figsize=(10, 12), sharex=True, sharey=True)
    fig.subplots_adjust(hspace=0.38, wspace=0.15)

    for idx, entry in enumerate(sweeps):
        row = idx // 2
        col = idx  % 2
        ax  = axes[row][col]

        df      = entry["df"]
        therapy = entry["therapy"]
        pair    = entry["pair"]
        hl      = entry.get("highlight", highlight_tolerance)
        n_total = entry.get("n_total", None)
        color   = PAIR_COLORS.get(pair, "#2e6fba")

        tolerances = df["tolerance"].tolist()

        ax.plot(df["tolerance"], df["pct_cond_ids"],
                marker="o", markersize=3, color=color, linewidth=1.6, zorder=2)
        ax.set_ylim(0, 105)
        ax.set_xlim(0, max(tolerances) + 0.5)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)

        if hl is not None:
            hl_row = df[df["tolerance"] == hl]
            if not hl_row.empty:
                hl_y = hl_row["pct_cond_ids"].values[0]
                ax.plot([hl, hl],  [0, hl_y], color="red", linestyle="--",
                        linewidth=1.0, alpha=0.7, zorder=3)
                ax.plot([0, hl],   [hl_y, hl_y], color="red", linestyle="--",
                        linewidth=1.0, alpha=0.7, zorder=3)
                ax.scatter(hl, hl_y, color="red", s=50, zorder=5)

        n_str = f"  (n={n_total:,})" if n_total else ""
        ax.set_title(f"{therapy}  ×  {pair}{n_str}", fontsize=9, fontweight="bold", pad=5)

        if row == 2:
            ax.set_xlabel("Tolerance (months)", fontsize=8)
        if col == 0:
            ax.set_ylabel("Matched cond_ids (%)", fontsize=8)
        ax.tick_params(labelsize=8)

    fig.suptitle("Tolerance Sweep — Match Rate by Therapy × Staging",
                 fontsize=11, fontweight="bold", y=1.01)

    if save_plot:
        save_path = Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)
        full_path = save_path / file_name
        fig.savefig(full_path, dpi=dpi, bbox_inches="tight")
        print(f"Panel saved to: {full_path.resolve()}")

    plt.show()
    plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# F – BASIS-PLOTS
# ══════════════════════════════════════════════════════════════════════════════

def scatterplot(
    x, y,
    titel: str       = "Scatterplot",
    x_label: str     = "X-Achse",
    y_label: str     = "Y-Achse",
    farbe: str       = "blue",
    punktgroesse: int = 50,
    speichern: bool  = False,
    dateiname: str   = "scatterplot.png",
    ordner: str      = "plots",
    dpi: int         = 300,
) -> None:
    """Einfacher Scatterplot mit optionaler Speicherfunktion."""
    plt.figure(figsize=(8, 6))
    plt.scatter(x, y, c=farbe, s=punktgroesse)
    plt.title(titel)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.grid(True)

    if speichern:
        if not os.path.exists(ordner):
            os.makedirs(ordner)
            print(f"Ordner erstellt: {ordner}")
        pfad = os.path.join(ordner, dateiname)
        plt.savefig(pfad, dpi=dpi, bbox_inches="tight")
        print(f"Plot gespeichert als: {pfad}")
