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

# ── Zentrale Konfiguration aus plot_config.py ─────────────────────────────────
from plot_config import (
    PLOT_CONFIG,
    UICC_MAPPING,
    UICC_ORDER,
    ECOG_ORDER,
    UICC_GROUPS,
    UICC_GROUP_ORDER,
    tab20b_colors,
    map_uicc_to_main_stage,
    map_ecog_value,
    apply_plot_config,
)

# ══════════════════════════════════════════════════════════════════════════════
# GETEILTE KONSTANTEN – jetzt aus plot_config.py importiert (siehe oben)
# ══════════════════════════════════════════════════════════════════════════════
# Hinweis: map_uicc_to_main_stage und map_ecog_value werden aus plot_config.py
# importiert (siehe Import-Block oben) und NICHT hier neu definiert.

# ══════════════════════════════════════════════════════════════════════════════
# PRIVATE HILFSFUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════


def _gini(values: np.ndarray) -> float:
    v = np.sort(values[values >= 0].astype(float))
    if v.size == 0 or v.sum() == 0:
        return 0.0
    n = v.size
    cum = np.cumsum(v)
    return (n + 1 - 2 * np.sum(cum) / cum[-1]) / n


def _therapy_counts_per_cond(df: pd.DataFrame, cond_col: str, n_total_patients: int) -> pd.Series:
    counts = df.groupby(cond_col).size()
    n_without = max(0, n_total_patients - len(counts))
    if n_without > 0:
        zeros = pd.Series(np.zeros(n_without, dtype=int))
        counts = pd.concat([counts, zeros], ignore_index=True)
    return counts


def _generate_colors(cmap_name: str, n: int, cmap_range: tuple) -> list:
    cmap = cm.get_cmap(cmap_name)
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
    show_bar_numbers: bool | None = None,
    relative: bool = False,
) -> plt.Figure:
    """Gemeinsame Rendering-Logik für UICC- und ECOG-Grouped-Bar-Charts.
    Balken sind mittig über den Ticks ausgerichtet (Punkt 9). Zahlen über den
    Balken optional (global via PLOT_CONFIG['show_bar_numbers'])."""
    if show_bar_numbers is None:
        show_bar_numbers = PLOT_CONFIG["show_bar_numbers"]
    therapy_totals = agg.groupby("therapy")["count"].sum().to_dict()
    x_positions = np.arange(len(category_order))
    n = len(therapies)
    # Balkenbreite + zentrierte Offsets: garantiert mittige Ausrichtung über Ticks
    group_width = 0.8
    width = group_width / max(n, 1)
    offsets = (np.arange(n) - (n - 1) / 2) * width
    fig, ax = plt.subplots(figsize=(14, 7))
    for offset, therapy in zip(offsets, therapies):
        sub = agg[agg["therapy"] == therapy].set_index("category").reindex(category_order).fillna(0)
        counts = sub["count"].values.astype(float)
        total = int(therapy_totals.get(therapy, 0))
        if relative and total > 0:
            counts = 100.0 * counts / total
        bars = ax.bar(
            x_positions + offset,
            counts,
            width=width,
            color=color_map[therapy],
            alpha=0.9,
            label=f"{therapy} (n={total:,})",
        )
        if show_bar_numbers:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        height,
                        f"{height:.0f}" if not relative else f"{height:.1f}%",
                        ha="center", va="bottom",
                        fontsize=PLOT_CONFIG["fontsize_bar_label"],
                    )
    ax.set_xticks(x_positions)
    ax.set_xticklabels(category_order, rotation=0, ha="center")
    ax.set_xlabel(xlabel, fontsize=PLOT_CONFIG["fontsize_axis_label"])
    ax.set_ylabel("Share within therapy (%)" if relative else "Number of Condition IDs",
                  fontsize=PLOT_CONFIG["fontsize_axis_label"])
    # Plottitel nur wenn global erlaubt UND ein Titel übergeben wurde
    if title and PLOT_CONFIG["show_titles"]:
        ax.set_title(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=15)
    if PLOT_CONFIG["show_legend"]:
        ax.legend(frameon=False, title="Therapy", title_fontsize=PLOT_CONFIG["fontsize_legend"])
    if not relative:
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    # x-Limits eng setzen: erster/letzter Balken nicht am Rand abgeschnitten
    ax.set_xlim(-0.5, len(category_order) - 0.5)
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
    age_column: str = "age_at_diagnosis",
    age_bracket: int = 10,
    title: str | bool | None = None,
    colors: dict = None,
    save_path: str = None,
    dpi: int = 300,
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
    therapies = list(dataframes.keys())
    color_map = {t: c for t, c in zip(therapies, tab20b_colors(len(therapies)))}
    if colors:
        color_map.update(colors)
    # Erstes Bracket hardcoded 18-20, danach age_bracket-Schritte (Punkt 11)
    bins = [18, 21] + list(range(21 + (age_bracket - 1), 105, age_bracket))
    if bins[-1] < 105:
        bins.append(105)
    labels = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1] - 1
        labels.append(f"{lo}–{hi}")
    rows = []
    for therapy, df in dataframes.items():
        if age_column not in df.columns:
            raise ValueError(f"Spalte '{age_column}' nicht in Therapiegruppe '{therapy}'.")
        ages = pd.to_numeric(df[age_column], errors="coerce").dropna()
        counts = (
            pd.cut(ages, bins=bins, labels=labels, right=False)
            .value_counts()
            .reindex(labels, fill_value=0)
            .reset_index()
        )
        counts.columns = ["age_bracket", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
    agg = pd.concat(rows, ignore_index=True)
    x_positions = np.arange(len(labels))
    n = len(therapies)
    group_width = 0.8
    width = group_width / max(n, 1)
    offsets = (np.arange(n) - (n - 1) / 2) * width
    show_bar_numbers = PLOT_CONFIG["show_bar_numbers"]
    fig, ax = plt.subplots(figsize=(14, 7))
    for offset, therapy in zip(offsets, therapies):
        sub = agg[agg["therapy"] == therapy].set_index("age_bracket").reindex(labels).fillna(0)
        counts = sub["count"].values
        bars = ax.bar(
            x_positions + offset,
            counts,
            width=width,
            color=color_map[therapy],
            alpha=0.9,
            label=therapy,
        )
        if show_bar_numbers:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, height,
                            f"{int(height)}", ha="center", va="bottom",
                            fontsize=PLOT_CONFIG["fontsize_bar_label"])
    ax.set_xticks(x_positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_xlabel("Age Group", fontsize=PLOT_CONFIG["fontsize_axis_label"])
    ax.set_ylabel("Number of Condition IDs", fontsize=PLOT_CONFIG["fontsize_axis_label"])
    if title is not False and PLOT_CONFIG["show_titles"]:
        chart_title = title or f"Age Distribution by Therapy Type ({age_bracket}-Year Groups)"
        ax.set_title(chart_title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=15)
    if PLOT_CONFIG["show_legend"]:
        ax.legend(frameon=False)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_xlim(-0.5, len(labels) - 0.5)
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
    rel_time_col: str = "rel_time",
    title: str = "Verteilung der ersten Therapien",
    therapy_col: str = None,
    figsize: tuple = (10, 6),
    save_path: str = "plots/firstline_therapy/firstline_violinplot.png",
    dpi: int = 300,
    allow_negative: bool = True,
) -> None:
    """Violin-Plot der Firstline-Therapiezeitpunkte relativ zur Diagnose."""
    negative_firstline = (firstline_df[rel_time_col] < 0).sum()
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=figsize)
    violin_kwargs = dict(inner="box", cut=0, ax=ax)
    strip_kwargs = dict(color="black", alpha=0.4, size=3, jitter=True, ax=ax)
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
        0.98,
        0.02,
        stats_text,
        transform=ax.transAxes,
        fontsize=PLOT_CONFIG["fontsize_annotation"],
        verticalalignment="bottom",
        horizontalalignment="right",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85),
    )
    if title and PLOT_CONFIG["show_titles"]:
        ax.set_title(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], weight="bold")
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
    relative_col: str = "months_between_asserted_therapy_start_date",
    therapy_type: str = "",
    title: Optional[str] = None,
    save_path=None,
    figsize: tuple = (14, 4),
    font_family: str = "DejaVu Sans",
) -> None:
    plt.rcParams["font.family"] = font_family
    data = df.copy()
    data[diagnosis_year_col] = pd.to_numeric(data[diagnosis_year_col], errors="coerce")
    data[relative_col] = pd.to_numeric(data[relative_col], errors="coerce")
    data = data.dropna(subset=[diagnosis_year_col, relative_col])
    sorted_years = sorted(data[diagnosis_year_col].unique())
    year_min = int(min(sorted_years))
    year_max = int(max(sorted_years))
    all_years = list(range(year_min, year_max + 1))
    existing_years = {int(y) for y in sorted_years}
    total_cases = len(data)
    cases_per_year = data[diagnosis_year_col].value_counts().sort_index()
    cases_all_line = [cases_per_year.get(y, float("nan")) for y in all_years]
    cases_all_markers = [cases_per_year.get(y, None) for y in all_years]
    fig, axes = plt.subplots(
        2,
        1,
        figsize=figsize,
        sharex=True,
        gridspec_kw={"height_ratios": [1, 2]},
    )
    fig.subplots_adjust(hspace=0.35)
    if title and PLOT_CONFIG["show_titles"]:
        fig.suptitle(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.01)
    # – Subplot 1: Case Count ─────────────────────────────────────────────────
    axes[0].plot(all_years, cases_all_line, linestyle="-", color="steelblue")
    axes[0].plot(
        [y for y, v in zip(all_years, cases_all_markers) if v is not None],
        [v for v in cases_all_markers if v is not None],
        marker="o",
        linestyle="None",
        color="steelblue",
    )
    therapy_label = f" – {therapy_type}" if therapy_type else ""
    axes[0].set_title(
        f"Number of {therapy_type or 'Therapy'} Records per First Diagnosis Year"
        f"{therapy_label}  n = {total_cases:,} records",
        fontsize=PLOT_CONFIG["fontsize_annotation_small"],
        fontweight="bold",
        pad=3,
    )
    axes[0].set_ylabel("Count", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    axes[0].tick_params(labelsize=7)
    axes[0].spines[["top", "right"]].set_visible(False)
    axes[0].grid(axis="y", linestyle="--", alpha=0.35)
    # – Subplot 2: Boxplot ────────────────────────────────────────────────────
    positions_with_data = [y for y in all_years if y in existing_years]
    grouped_data_filtered = [
        data.loc[data[diagnosis_year_col] == year, relative_col].values
        for year in positions_with_data
    ]
    axes[1].boxplot(
        grouped_data_filtered,
        positions=positions_with_data,
        widths=0.6,
        showfliers=True,
        manage_ticks=False,
    )
    axes[1].set_title(
        "Distribution of the rel. Time Interval between First Diagnosis Year and Therapy Date in Months (Boxplot)",
        fontsize=PLOT_CONFIG["fontsize_annotation_small"],
        fontweight="bold",
        pad=3,
    )
    axes[1].set_xlabel("First Diagnosis Year", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
    axes[1].set_ylabel("Months", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    axes[1].tick_params(labelsize=7)
    axes[1].spines[["top", "right"]].set_visible(False)
    axes[1].grid(axis="y", linestyle="--", alpha=0.35)
    tick_step = max(1, (year_max - year_min) // 20)
    tick_years = [y for y in all_years if (y - year_min) % tick_step == 0]
    axes[1].set_xlim(year_min - 0.5, year_max + 0.5)
    axes[1].set_xticks(tick_years)
    axes[1].set_xticklabels([str(y) for y in tick_years], rotation=90, fontsize=PLOT_CONFIG["fontsize_annotation_tiny"])
    fig.tight_layout(pad=0.4, h_pad=0.5)
    if save_path is not None:
        directory = os.path.dirname(save_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        fig.savefig(save_path, dpi=PLOT_CONFIG["dpi"], bbox_inches=None)
        print(f"Plot saved to: {save_path}")
    plt.show()
    plt.close(fig)


def plot_dropout_curve(
    result_df,
    titel: Optional[str] = None,
    speichern: bool = False,
    ordner: str = "plots",
    dateiname: str = "dropout_curve.png",
    dpi: int = 300,
    font_family: str = "DejaVu Sans",
    highlight_month: Optional[int] = None,
    log_y: bool = False,
):
    """
    Plottet die Dropout-Kurve (Anteil verbleibender cond_ids je Cutoff-Monat).
    Eingabe ist die Ausgabe von `patient_dropout_by_cutoff` aus data_processing.py.
    """
    plt.rcParams["font.family"] = font_family
    n_total = result_df["patients_remaining"].max()
    pct_vals = result_df["patients_remaining"] / n_total * 100
    fig, ax = plt.subplots(figsize=(10, 6))
    primary_color = tab20b_colors(1)[0]
    ax.plot(
        result_df["cutoff_month"],
        pct_vals,
        marker="o",
        markersize=4,
        color=primary_color,
        linewidth=1.8,
        zorder=2,
    )
    ax.set_xlabel("Therapy Start Date - Date of Diagnosis (Month)", fontsize=PLOT_CONFIG["fontsize_annotation"])
    ax.set_ylabel("Number of cond_ids (%)", fontsize=PLOT_CONFIG["fontsize_annotation"])
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
            ax.plot(
                [hl_x, hl_x],
                [0, hl_y],
                color="red",
                linestyle="--",
                linewidth=1.2,
                alpha=0.7,
                zorder=3,
            )
            ax.plot(
                [0, hl_x],
                [hl_y, hl_y],
                color="red",
                linestyle="--",
                linewidth=1.2,
                alpha=0.7,
                zorder=3,
            )
            ax.scatter(hl_x, hl_y, color="red", s=80, zorder=5)
            print(
                f"  Highlight month={hl_x}  →  "
                f"{hl_row['patients_remaining'].values[0]:,} cond_ids  ({hl_y:.1f}%)"
            )
        else:
            print(f"[WARNING] highlight_month={highlight_month} nicht in result_df")
    ax.text(
        0.5,
        -0.10,
        f"Total cond_ids: {n_total:,}",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=PLOT_CONFIG["fontsize_annotation_tiny"],
        color="black",
        style="italic",
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
    cond_col: str = "cond_id",
    cohort_name: str = "",
    output_path: Optional[Union[str, Path]] = None,
    show: bool = True,
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
    gini = _gini(values)
    fig, ax = plt.subplots(figsize=(7, 6.5))
    primary_color = tab20b_colors(1)[0]
    ax.plot(cum_pop, cum_vals, color=primary_color, linewidth=2.2, label="Lorenz-Kurve")
    ax.fill_between(cum_pop, cum_vals, cum_pop, alpha=0.12, color=primary_color)
    ax.plot(
        [0, 1], [0, 1], linestyle="--", color="#999999", linewidth=1.2, label="Gleichverteilung"
    )
    ax.text(
        0.05,
        0.93,
        f"Gini = {gini:.3f}",
        transform=ax.transAxes,
        fontsize=PLOT_CONFIG["fontsize_annotation_large"],
        fontweight="bold",
        color=primary_color,
    )
    ax.set_xlabel("Kumulativer Anteil der cond_id (aufsteigend sortiert)", fontsize=PLOT_CONFIG["fontsize_annotation"])
    ax.set_ylabel("Kumulativer Anteil der Therapien", fontsize=PLOT_CONFIG["fontsize_annotation"])
    if PLOT_CONFIG["show_titles"]:
        ax.set_title("Lorenz-Kurve – Therapien pro cond_id", fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=14)
    ax.text(
        0.5,
        -0.12,
        f"{cohort_name} | cond_ids={counts.size:,} | Therapien={len(df):,}",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=PLOT_CONFIG["fontsize_annotation_tiny"],
        color="#555555",
        style="italic",
    )
    ax.legend(fontsize=PLOT_CONFIG["fontsize_legend"])
    ax.spines[["top", "right"]].set_visible(False)
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
    if show:
        plt.show()
    plt.close(fig)


def plot_log_histogram(
    df: pd.DataFrame,
    n_total_patients: int,
    cond_col: str = "cond_id",
    cohort_name: str = "",
    output_path: Optional[Path] = None,
    show: bool = True,
    bins: int = 60,
    year_col: Optional[str] = None,
    year_cutoff: Optional[int] = None,
    n_patients_before: Optional[int] = None,
    n_patients_from: Optional[int] = None,
    title: Optional[str] = "Distribution of Therapies per cond_id",
    font_family: str = "DejaVu Sans",
    max_therapies: Optional[int] = 40,
) -> None:
    """
    Log-skaliertes Histogramm der Therapien pro cond_id.
    Optional aufgeteilt nach Jahr (vor/ab year_cutoff).

    max_therapies : cond_ids mit mehr als so vielen Therapien werden als
                    Ausreißer entfernt (Punkt 6, default 40). Anzahl wird
                    im sprechenden Printout ausgegeben.
    """
    plt.rcParams["font.family"] = font_family
    counts = _therapy_counts_per_cond(df, cond_col, n_total_patients)
    if counts.empty:
        print("[WARNING] No data for histogram")
        return
    # ── Ausreißer entfernen (Punkt 6) ────────────────────────────────────────
    if max_therapies is not None:
        n_outliers = int((counts > max_therapies).sum())
        if n_outliers > 0:
            print(f"    [log_histo] {n_outliers} Ausreißer-cond_ids mit > {max_therapies} "
                  f"Therapien entfernt (max war {int(counts.max())}).")
        counts = counts[counts <= max_therapies]
    n_with_therapy = df[cond_col].nunique()
    use_year_split = (year_col is not None) and (year_cutoff is not None)
    if use_year_split:
        df_before = df[df[year_col] < year_cutoff]
        df_from = df[df[year_col] >= year_cutoff]
        counts_before = _therapy_counts_per_cond(df_before, cond_col, n_patients_before)
        counts_from = _therapy_counts_per_cond(df_from, cond_col, n_patients_from)
        if max_therapies is not None:
            counts_before = counts_before[counts_before <= max_therapies]
            counts_from = counts_from[counts_from <= max_therapies]
        fig, (ax_before, ax_from) = plt.subplots(2, 1, figsize=(10, 9), sharex=True)
        hist_colors = tab20b_colors(2)  # Zwei Farben für Before/From
        subplots = [
            (
                ax_before,
                counts_before,
                f"Before {year_cutoff}",
                hist_colors[0],
                df_before,
                n_patients_before,
            ),
            (ax_from, counts_from, f"From {year_cutoff}", hist_colors[1], df_from, n_patients_from),
        ]
    else:
        fig, ax = plt.subplots(figsize=(10, 5))
        hist_color = tab20b_colors(1)[0]
        subplots = [(ax, counts, "", hist_color, df, n_total_patients)]
    bin_edges = np.linspace(counts.min(), counts.max(), bins + 1)
    for entry in subplots:
        ax, subset, label, color, df_sub, n_pat_sub = entry
        if subset.empty:
            ax.text(
                0.5,
                0.5,
                "No data",
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=PLOT_CONFIG["fontsize_annotation_large"],
                color="#888888",
            )
            ax.set_ylabel("Count cond_id (log scale)", fontsize=PLOT_CONFIG["fontsize_annotation"])
            if use_year_split:
                ax.set_title(label, fontsize=PLOT_CONFIG["fontsize_annotation_large"], fontweight="bold", pad=8)
            continue
        _, _, patches = ax.hist(
            subset,
            bins=bin_edges,
            color=color,
            edgecolor="white",
            linewidth=0.6,
            rwidth=0.98,
        )
        # Zahlen über den Balken entfernt (Punkt 6)
        ax.set_yscale("log")
        ax.set_ylabel("Count cond_id (log scale)", fontsize=PLOT_CONFIG["fontsize_annotation"])
        if use_year_split:
            ax.set_title(label, fontsize=PLOT_CONFIG["fontsize_annotation_large"], fontweight="bold", pad=8)
        stats = (
            f"Median={subset.median():.0f}  Mean={subset.mean():.1f}  "
            f"P90={subset.quantile(0.90):.0f}  Max={subset.max():.0f}"
        )
        ax.text(
            0.98,
            0.97,
            stats,
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=PLOT_CONFIG["fontsize_annotation_small"],
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#cccccc"),
        )
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
    bottom_ax = subplots[-1][0]
    bottom_ax.set_xlabel("Therapies per cond_id", fontsize=PLOT_CONFIG["fontsize_annotation"])
    bottom_ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    if title and PLOT_CONFIG["show_titles"]:
        fig.suptitle(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.01)
    footer_kwargs = dict(ha="center", va="top", fontsize=PLOT_CONFIG["fontsize_annotation_small"], color="black", style="italic")
    if use_year_split:
        n_with_before = df_before[cond_col].nunique()
        n_with_from = df_from[cond_col].nunique()
        ax_before.text(
            0.5,
            -0.04,
            f"{cohort_name} | cond_ids={counts_before.size:,} | "
            f"Therapies={len(df_before):,} | cond_ids with therapy={n_with_before:,}",
            transform=ax_before.transAxes,
            **footer_kwargs,
        )
        ax_from.text(
            0.5,
            -0.08,
            f"{cohort_name} | cond_ids={counts_from.size:,} | "
            f"Therapies={len(df_from):,} | Patients with therapy={n_with_from:,} / {n_patients_from:,}",
            transform=ax_from.transAxes,
            **footer_kwargs,
        )
    else:
        bottom_ax.text(
            0.5,
            -0.14,
            f"{cohort_name} | cond_ids={counts.size:,} | "
            f"Therapies={len(df):,} | cond_ids with therapy={n_with_therapy:,} / {n_total_patients:,}",
            transform=bottom_ax.transAxes,
            **footer_kwargs,
        )
    fig.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
    if show:
        plt.show()
    plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# D – UICC / ECOG INVENTAR & VERTEILUNG
# ══════════════════════════════════════════════════════════════════════════════


def plot_uicc_ecog_inventory(
    df_uicc,
    df_ecog,
    df_uicc_must=None,
    uicc_col: str = "uicc_tnm",
    ecog_col: str = "ecog_performance_status",
    cond_col: str = "cond_id",
    cohort_name: str = "",
    n_total_cond_ids: Optional[int] = None,
    title: Optional[str] = "UICC & ECOG Inventory",
    output_path: Optional[Path] = None,
    show: bool = True,
    font_family: str = "DejaVu Sans",
    show_legend: bool | None = None,
    show_substages: bool = False,
    label_obds: str = "before MUST",
    label_must: str = "after MUST",
) -> None:
    """
    Zwei-Panel-Plot: UICC-Stage-Verteilung (links) + ECOG-Verteilung (rechts).

    UICC-Panel (Punkt 5):
      • Alle Sub-Stufen werden auf die Hauptstufe (0/I/II/III/IV) gemappt.
      • Wird df_uicc_must übergeben, bekommt jede Stage ZWEI Balken:
        vor MUST (oBDS) und nach MUST. Balkenbreite halbiert + kleiner Abstand.
      • Gesamtzahl der Stage steht NEBEN dem Balken (für MUST-Balken mit
        Zusatz 'after MUST'). Das ist der Fix für den alten Bug, der nur die
        letzte Sub-Stufe statt der Stage-Summe angezeigt hat.
      • Legende optional (default aus, da Zahlen am Balken stehen).
      • Sub-Stufen-Unterteilung optional (show_substages, default aus).
    """
    if show_legend is None:
        show_legend = PLOT_CONFIG["show_legend"]
    plt.rcParams["font.family"] = font_family

    stages = ["missing", "0", "I", "II", "III", "IV"]
    stage_labels = ["Missing" if s == "missing" else f"Stage {s}" for s in stages]

    def _main_counts(df):
        """Hauptstufen-Zählung (cond_id-Zeilen) als dict stage→count."""
        mapped = df[uicc_col].map(map_uicc_to_main_stage)
        vc = mapped.value_counts()
        return {s: int(vc.get(s, 0)) for s in stages}

    uicc_obds = _main_counts(df_uicc)
    uicc_obds_total = sum(uicc_obds.values())
    has_must = df_uicc_must is not None
    if has_must:
        uicc_must = _main_counts(df_uicc_must)
        uicc_must_total = sum(uicc_must.values())

    # ── ECOG (unverändert, Hauptlogik) ──────────────────────────────────────
    ECOG_ORDER_INV = ["U", "0", "1", "2", "3", "4"]
    ecog_mapped = df_ecog[ecog_col].map(map_ecog_value)
    ecog_vc = ecog_mapped.value_counts()
    ecog_labels = [v for v in ECOG_ORDER_INV if ecog_vc.get(v, 0) > 0]
    ecog_vals = [int(ecog_vc.get(v, 0)) for v in ecog_labels]
    ecog_total = int(sum(ecog_vals))
    ecog_cols = tab20b_colors(len(ecog_labels))
    ecog_n_known = sum(v for l, v in zip(ecog_labels, ecog_vals) if l != "U")
    ecog_max = max(ecog_vals) if ecog_vals else 1

    # ── Figure: schmaleres wspace (Punkt 5: Whitespace verkleinern) ─────────
    fig, (ax_uicc, ax_ecog) = plt.subplots(
        1, 2, figsize=(20, 8),
        gridspec_kw={"width_ratios": [3, 1], "wspace": 0.18},
    )

    y = np.arange(len(stages))
    tab = tab20b_colors(4)
    color_obds, color_must = tab[0], tab[2]

    if has_must:
        # zwei Balken je Stage: Breite halbiert, kleiner Abstand
        bar_h = 0.34
        gap = 0.04
        obds_vals = [uicc_obds[s] for s in stages]
        must_vals = [uicc_must[s] for s in stages]
        uicc_max_val = max(max(obds_vals), max(must_vals), 1)
        ax_uicc.barh(y + (bar_h/2 + gap/2), obds_vals, height=bar_h,
                     color=color_obds, edgecolor="white", linewidth=0.6, label=label_obds)
        ax_uicc.barh(y - (bar_h/2 + gap/2), must_vals, height=bar_h,
                     color=color_must, edgecolor="white", linewidth=0.6, label=label_must)
        # Gesamtzahl je Stage NEBEN dem Balken
        for yi, s in zip(y, stages):
            vo, vm = uicc_obds[s], uicc_must[s]
            ax_uicc.text(vo + uicc_max_val*0.004, yi + (bar_h/2 + gap/2),
                         f"{vo:,}", va="center", ha="left",
                         fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")
            ax_uicc.text(vm + uicc_max_val*0.004, yi - (bar_h/2 + gap/2),
                         f"{vm:,}, {label_must}", va="center", ha="left",
                         fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")
    else:
        bar_h = 0.6
        obds_vals = [uicc_obds[s] for s in stages]
        uicc_max_val = max(max(obds_vals), 1)
        ax_uicc.barh(y, obds_vals, height=bar_h,
                     color=color_obds, edgecolor="white", linewidth=0.6, label=label_obds)
        for yi, s in zip(y, stages):
            vo = uicc_obds[s]
            ax_uicc.text(vo + uicc_max_val*0.004, yi, f"{vo:,}", va="center", ha="left",
                         fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")

    ax_uicc.set_yticks(y)
    ax_uicc.set_yticklabels(stage_labels, fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold")
    ax_uicc.set_xlim(0, uicc_max_val * 1.18)
    ax_uicc.set_ylim(-0.6, len(stages) - 0.4)
    ax_uicc.spines[["top", "right", "bottom"]].set_visible(False)
    ax_uicc.xaxis.set_visible(False)
    ax_uicc.tick_params(left=False)
    ax_uicc.set_title(
        f"UICC Stage Distribution\nrows oBDS = {uicc_obds_total:,}"
        + (f"  |  rows MUST = {uicc_must_total:,}" if has_must else ""),
        fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=10,
    )
    if show_legend and has_must:
        ax_uicc.legend(frameon=False, fontsize=PLOT_CONFIG["fontsize_legend"], loc="lower right")

    # ── ECOG Balken ──────────────────────────────────────────────────────────
    bars = ax_ecog.barh(range(len(ecog_labels)), ecog_vals, color=ecog_cols,
                        edgecolor="white", linewidth=0.6, height=0.6)
    for bar, val, lbl in zip(bars, ecog_vals, ecog_labels):
        ax_ecog.text(bar.get_width() + ecog_max*0.012, bar.get_y() + bar.get_height()/2,
                     f"{val:,}  ({100*val/ecog_total:.1f}%)" if ecog_total else f"{val:,}",
                     va="center", ha="left", fontsize=PLOT_CONFIG["fontsize_bar_label"],
                     color="#888888" if lbl == "U" else "#333333")
    ax_ecog.set_yticks(range(len(ecog_labels)))
    ax_ecog.set_yticklabels(["Unknown" if l == "U" else f"ECOG {l}" for l in ecog_labels],
                            fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold")
    ax_ecog.set_xlim(0, ecog_max * 1.55)
    ax_ecog.spines[["top", "right", "bottom"]].set_visible(False)
    ax_ecog.xaxis.set_visible(False)
    ax_ecog.tick_params(left=False)
    ax_ecog.set_title(
        f"ECOG Performance Status\nrows = {ecog_total:,}  |  known = {ecog_n_known:,}",
        fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=10,
    )

    # Plot-Suptitel nur wenn global erlaubt
    if title and PLOT_CONFIG["show_titles"]:
        sup = f"{cohort_name} – {title}" if cohort_name else title
        if n_total_cond_ids:
            sup += f"\n(Cohort: {n_total_cond_ids:,} cond_ids total)"
        fig.suptitle(sup, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.01)
    fig.tight_layout()
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
        print(f"Plot saved to: {output_path}")
    if show:
        plt.show()
    plt.close(fig)


def plot_uicc_distribution_grouped_bar(
    dataframes: dict,
    uicc_column: str = "uicc_tnm",
    title: str = None,
    colors: dict = None,
    save_path: str = None,
    dpi: int = None,
    exclude_missing: bool = True,
    exclude_zero: bool = True,
    relative: bool = True,
) -> plt.Figure:
    """
    Gruppiertes Balkendiagramm der UICC-Stage-Verteilung pro Therapietyp.
    Alle Sub-Stufen werden auf die Hauptstufe (0/I/II/III/IV) gemappt.

    exclude_missing : 'missing'/Unknown ausschließen (default True)
    exclude_zero    : Stage '0' ausschließen (default True)
    relative        : Anteil je Therapie in % statt absoluter Zahlen.
                      n bezieht sich auf das, was nach Ausschluss übrig bleibt.
    """
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    therapies = list(dataframes.keys())
    color_map = {t: c for t, c in zip(therapies, tab20b_colors(len(therapies)))}
    if colors:
        color_map.update(colors)

    order = [s for s in UICC_ORDER]
    if exclude_missing and "missing" in order:
        order.remove("missing")
    if exclude_zero and "0" in order:
        order.remove("0")

    rows = []
    for therapy, df in dataframes.items():
        if uicc_column not in df.columns:
            raise ValueError(f"Spalte '{uicc_column}' nicht in '{therapy}'.")
        mapped = df[uicc_column].map(map_uicc_to_main_stage)
        mapped = mapped[mapped.isin(order)]  # nur behaltene Stufen → n schrumpft
        counts = mapped.value_counts().reindex(order, fill_value=0).reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
        print(f"    [UICC-Dist] {therapy}: n={int(counts['count'].sum()):,} "
              f"(nach Ausschluss missing={exclude_missing}, 0={exclude_zero})")
    agg = pd.concat(rows, ignore_index=True)
    return _build_grouped_bar(
        agg=agg,
        category_order=order,
        therapies=therapies,
        color_map=color_map,
        xlabel="UICC Stage",
        title=title,
        save_path=save_path,
        dpi=dpi,
        relative=relative,
    )


def plot_ecog_distribution_grouped_bar(
    dataframes: dict,
    ecog_column: str = "ecog_performance_status",
    title: str = None,
    colors: dict = None,
    save_path: str = None,
    dpi: int = None,
    exclude_unknown: bool = True,
    relative: bool = True,
) -> plt.Figure:
    """
    Gruppiertes Balkendiagramm der ECOG-Verteilung pro Therapietyp.
    Werte werden auf 0–4 normalisiert; 'U'/Unknown optional ausgeschlossen.

    exclude_unknown : U/Unknown ausschließen (default True)
    relative        : Anteil je Therapie in % (n nach Ausschluss).
    """
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    therapies = list(dataframes.keys())
    color_map = {t: c for t, c in zip(therapies, tab20b_colors(len(therapies)))}
    if colors:
        color_map.update(colors)

    order = list(ECOG_ORDER)
    if not exclude_unknown:
        order = order + ["U"]

    rows = []
    for therapy, df in dataframes.items():
        if ecog_column not in df.columns:
            raise ValueError(f"Spalte '{ecog_column}' nicht in '{therapy}'.")
        mapped = df[ecog_column].map(map_ecog_value)
        mapped = mapped[mapped.isin(order)]
        counts = mapped.value_counts().reindex(order, fill_value=0).reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
        print(f"    [ECOG-Dist] {therapy}: n={int(counts['count'].sum()):,} "
              f"(nach Ausschluss U={exclude_unknown})")
    agg = pd.concat(rows, ignore_index=True)
    return _build_grouped_bar(
        agg=agg,
        category_order=order,
        therapies=therapies,
        color_map=color_map,
        xlabel="ECOG Performance Status",
        title=title,
        save_path=save_path,
        dpi=dpi,
        relative=relative,
    )


def plot_gender_stage_grouped_bar(
    crosstab: pd.DataFrame,
    xlabel: str = "Stage",
    title: str = None,
    save_path: str = None,
    dpi: int = None,
    relative: bool = True,
    genders: list | None = None,
) -> plt.Figure:
    """
    Gruppierte Balken-„Tabelle": Stages auf der X-Achse, je Geschlecht ein Balken
    (Punkt 9). Erwartet eine Crosstab (Index = Geschlecht, Spalten = Stages).

    relative : Anteil je Geschlecht in % (Zeilen-%), default True.
    genders  : Reihenfolge/Auswahl der Geschlechter (z.B. ['female','male']).
               'unknown' wird – falls vorhanden – standardmäßig entfernt.
    """
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    ct = crosstab.copy()
    # 'unknown' raus (Entscheidung: "Ja werf raus")
    ct = ct.drop(index=[g for g in ct.index if str(g).lower() in ("unknown", "u", "nan")],
                 errors="ignore")
    if genders is None:
        genders = [g for g in ["female", "male"] if g in ct.index]
        genders += [g for g in ct.index if g not in genders]
    genders = [g for g in genders if g in ct.index]

    stages = list(ct.columns)
    x = np.arange(len(stages))
    n = len(genders)
    group_width = 0.8
    width = group_width / max(n, 1)
    offsets = (np.arange(n) - (n - 1) / 2) * width
    colors = {g: c for g, c in zip(genders, tab20b_colors(max(n, 1)))}
    show_bar_numbers = PLOT_CONFIG["show_bar_numbers"]

    fig, ax = plt.subplots(figsize=(12, 7))
    for offset, g in zip(offsets, genders):
        vals = ct.loc[g, stages].values.astype(float)
        total = vals.sum()
        if relative and total > 0:
            vals = 100.0 * vals / total
        bars = ax.bar(x + offset, vals, width=width, color=colors[g], alpha=0.9,
                      label=f"{g} (n={int(ct.loc[g, stages].sum()):,})")
        if show_bar_numbers:
            for bar in bars:
                h = bar.get_height()
                if h > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, h,
                            f"{h:.1f}%" if relative else f"{h:.0f}",
                            ha="center", va="bottom", fontsize=PLOT_CONFIG["fontsize_bar_label"])
    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in stages])
    ax.set_xlabel(xlabel, fontsize=PLOT_CONFIG["fontsize_axis_label"])
    ax.set_ylabel("Share within gender (%)" if relative else "Count",
                  fontsize=PLOT_CONFIG["fontsize_axis_label"])
    if title and PLOT_CONFIG["show_titles"]:
        ax.set_title(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=15)
    if PLOT_CONFIG["show_legend"]:
        ax.legend(frameon=False, title="Gender", title_fontsize=PLOT_CONFIG["fontsize_legend"])
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_xlim(-0.5, len(stages) - 0.5)
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
# E – MERGE-DELTA & SWEEP (PANEL-PLOTS)
# ══════════════════════════════════════════════════════════════════════════════


def plot_merge_panel(
    panel_entries: list,
    log_y: bool = True,
    bins: int = 30,
    save_path: Optional[str] = None,
    file_name: str = "panel_months_diff.tiff",
    dpi: int = 300,
    font_family: str = "DejaVu Sans",
) -> None:
    """
    3×2 Panel-Plot der months_diff-Histogramme aller Merge-Paare.
    panel_entries: Liste von Dicts mit keys:
        df, title, stats, staging_label
    """
    plt.rcParams["font.family"] = font_family
    fig, axes = plt.subplots(3, 2, figsize=(12, 10))
    fig.subplots_adjust(hspace=0.60, wspace=0.30)
    hist_color = tab20b_colors(1)[0]
    for idx, entry in enumerate(panel_entries):
        row = idx // 2
        col = idx % 2
        ax = axes[row][col]
        data = entry["df"]["months_diff"].dropna()
        stats = entry.get("stats", {})
        staging_label = entry.get("staging_label", "Staging")
        ax.hist(data, bins=bins, color=hist_color, edgecolor="white", linewidth=0.4)
        if log_y:
            ax.set_yscale("log")
        ax.set_title(entry["title"], fontsize=PLOT_CONFIG["fontsize_legend"], fontweight="bold", pad=5)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        ax.tick_params(labelsize=8)
        if row == 2:
            ax.set_xlabel("Delta (Months)", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        if col == 0:
            ax.set_ylabel("Count (log)" if log_y else "Count", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        if stats:
            ax.text(
                0.98,
                0.97,
                f"Therapy in     : {stats['n_therapy']:>7,}\n"
                f"{staging_label:<14} : {stats['n_staging']:>7,}\n"
                f"Matched        : {stats['n_matched']:>7,}\n"
                f"Unmatched      : {stats['n_unmatched']:>7,}",
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=PLOT_CONFIG["fontsize_annotation_tiny"],
                fontfamily="monospace",
                bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="#cccccc"),
            )
    if PLOT_CONFIG["show_titles"]:
        fig.suptitle(
            "Time Delta: Nearest Staging before Therapy",
            fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.01
        )
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
    save_plot: bool = False,
    save_dir: str = "plots",
    file_name: str = "sweep_panel.tiff",
    dpi: int = 300,
    font_family: str = "DejaVu Sans",
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
        col = idx % 2
        ax = axes[row][col]
        df = entry["df"]
        therapy = entry["therapy"]
        pair = entry["pair"]
        hl = entry.get("highlight", highlight_tolerance)
        n_total = entry.get("n_total", None)
        color = PAIR_COLORS.get(pair, "#2e6fba")
        tolerances = df["tolerance"].tolist()
        ax.plot(
            df["tolerance"],
            df["pct_cond_ids"],
            marker="o",
            markersize=3,
            color=color,
            linewidth=1.6,
            zorder=2,
        )
        ax.set_ylim(0, 105)
        ax.set_xlim(0, max(tolerances) + 0.5)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        if hl is not None:
            hl_row = df[df["tolerance"] == hl]
            if not hl_row.empty:
                hl_y = hl_row["pct_cond_ids"].values[0]
                ax.plot(
                    [hl, hl],
                    [0, hl_y],
                    color="red",
                    linestyle="--",
                    linewidth=1.0,
                    alpha=0.7,
                    zorder=3,
                )
                ax.plot(
                    [0, hl],
                    [hl_y, hl_y],
                    color="red",
                    linestyle="--",
                    linewidth=1.0,
                    alpha=0.7,
                    zorder=3,
                )
                ax.scatter(hl, hl_y, color="red", s=50, zorder=5)
        n_str = f"  (n={n_total:,})" if n_total else ""
        ax.set_title(f"{therapy}  ×  {pair}{n_str}", fontsize=PLOT_CONFIG["fontsize_legend"], fontweight="bold", pad=5)
        if row == 2:
            ax.set_xlabel("Tolerance (months)", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        if col == 0:
            ax.set_ylabel("Matched cond_ids (%)", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        ax.tick_params(labelsize=8)
    if PLOT_CONFIG["show_titles"]:
        fig.suptitle(
            "Tolerance Sweep — Match Rate by Therapy × Staging",
            fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.01
        )
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
    x,
    y,
    titel: str = "Scatterplot",
    x_label: str = "X-Achse",
    y_label: str = "Y-Achse",
    farbe: str = "blue",
    punktgroesse: int = 50,
    speichern: bool = False,
    dateiname: str = "scatterplot.png",
    ordner: str = "plots",
    dpi: int = 300,
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
