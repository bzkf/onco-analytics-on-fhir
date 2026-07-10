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
# PRINTOUT-STEUERUNG  (relevante Paper-Plots vs. übrige Plots)
# ══════════════════════════════════════════════════════════════════════════════
# Die Paper-relevanten Plotfunktionen geben sprechende Diagnostik aus, damit jede
# interne Filterung (Stolperfallen!) im Konsolen-Log nachvollziehbar ist. Alle
# übrigen Plots (nicht im Paper) sind standardmäßig STUMM, lassen sich aber durch
# Umschalten von VERBOSE_OTHER_PLOTS=True bei Bedarf wieder einschalten.
# Jede Diagnostik-Zeile trägt ein [funktionsname]-Präfix, sodass klar ist, AUS
# WELCHER Plot-Funktion sie stammt. Echte Warnungen/Fehler werden IMMER gedruckt.
VERBOSE_PAPER_PLOTS = True    # Paper-relevante Plots (Fig 1, 2, 3, 6, 7, 12, …)
VERBOSE_OTHER_PLOTS = False   # übrige Plots (Violin, Bias, Dropout, Lorenz, Gender, Scatter)


def _say(msg: str, *, paper: bool = True) -> None:
    """
    Schaltbare Diagnostik-Ausgabe einer Plot-Funktion.

    paper=True  → nur drucken, wenn VERBOSE_PAPER_PLOTS gesetzt ist
    paper=False → nur drucken, wenn VERBOSE_OTHER_PLOTS gesetzt ist

    Für Warnungen/Fehler NICHT verwenden – die sollen immer sichtbar sein
    (direktes print()).
    """
    if (paper and VERBOSE_PAPER_PLOTS) or (not paper and VERBOSE_OTHER_PLOTS):
        print(msg)


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
    totals: dict | None = None,
) -> plt.Figure:
    """Gemeinsame Rendering-Logik für UICC- und ECOG-Grouped-Bar-Charts.
    Balken sind mittig über den Ticks ausgerichtet (Punkt 9). Zahlen über den
    Balken optional (global via PLOT_CONFIG['show_bar_numbers']).

    totals : dict | None
        Therapiename → Gesamtzahl der Therapien dieses Typs (vor Matching/Cutoff).
        - totals=None  (Default): 100% = gematchte/auswertbare Therapien (altes
          Verhalten); Legende zeigt 'n=<matched>'.
        - totals gesetzt: 100% = ALLE Therapien des Typs. Die Balken zeigen damit,
          welcher Anteil der Therapien eine nutzbare UICC/ECOG besitzt. Legende
          zeigt 'n=<matched> / <total> total (<coverage>%)'.
    """
    if show_bar_numbers is None:
        show_bar_numbers = PLOT_CONFIG["show_bar_numbers"]
    # Gematchte/auswertbare Anzahl je Therapie = Summe der dargestellten Kategorien
    matched_totals = agg.groupby("therapy")["count"].sum().to_dict()
    use_totals = totals is not None
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
        matched = int(matched_totals.get(therapy, 0))
        # Nenner für die %-Berechnung: Gesamtzahl (falls übergeben) statt nur gematchte
        denom = int(totals.get(therapy, 0)) if use_totals else matched
        if relative and denom > 0:
            counts = 100.0 * counts / denom
        if use_totals:
            cov = 100.0 * matched / denom if denom > 0 else 0.0
            _label = f"{therapy} (n={matched:,} / {denom:,} total, {cov:.1f}%)"
        else:
            _label = f"{therapy} (n={matched:,})"
        bars = ax.bar(
            x_positions + offset,
            counts,
            width=width,
            color=color_map[therapy],
            alpha=0.9,
            label=_label,
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
    if relative:
        _ylabel = "Share of all therapies (%)" if use_totals else "Share within therapy (%)"
    else:
        _ylabel = "Number of Condition IDs"
    ax.set_ylabel(_ylabel, fontsize=PLOT_CONFIG["fontsize_axis_label"])
    # Plottitel nur wenn global erlaubt UND ein Titel übergeben wurde
    if title and PLOT_CONFIG["show_titles"]:
        ax.set_title(title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=15)
    if PLOT_CONFIG["show_legend"]:
        ax.legend(frameon=False, title="Therapy", title_fontsize=PLOT_CONFIG["fontsize_legend"])
    if not relative:
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    ax.set_ylim(0,100)
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
        _say(f"[grouped_bar] gespeichert: {path.resolve()}")
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
        show_bar_numbers: bool = None,
        bar_label_rotation: int = 90,
        color_offset: int = 4,
        color_shade: int = 1,
        show_total_in_legend: bool = True,
        k_anonymity: int = 3,
) -> plt.Figure:
    """
    Altersverteilung der Patienten pro Therapietyp als gruppiertes Balkendiagramm.

    Mit k-Anonymität: Age Brackets mit ≤ k_anonymity Samples werden gefiltert.

    Parameters
    ----------
    dataframes  : dict – Therapiename → DataFrame (muss `age_column` enthalten)
    age_column  : Spalte mit dem Alter bei Diagnose
    age_bracket : Breite der Altersgruppe in Jahren (z.B. 5 oder 10)
    title       : None = automatisch | str = benutzerdefiniert | False = kein Titel
    colors      : Therapiename → Hex-Farbe (optional, überschreibt automatische Zuordnung)
    save_path   : Speicherpfad; wenn None → plt.show()
    dpi         : Auflösung beim Speichern
    show_bar_numbers   : True/False um Anzeige der Balkenwerte zu erzwingen;
                         None = PLOT_CONFIG["show_bar_numbers"] verwenden
    bar_label_rotation : Rotation der Balkenbeschriftung in Grad (z.B. 90 = vertikal)
    color_offset        : Schrittweite innerhalb der tab20b-Palette zwischen den
                          Therapiegruppen, damit jede Gruppe eine eigene Farbfamilie
                          erhält (tab20b hat 5 Farbfamilien à 4 Schattierungen,
                          Index-Schritt von 4 wechselt also die Farbfamilie)
    color_shade         : Welche der 4 Schattierungen pro Familie genutzt wird
                          (0 = dunkelste/gedeckteste, 3 = hellste; 1 = kräftigste
                          mittlere Schattierung, wirkt am "vibrantesten")
    show_total_in_legend : Wenn True, wird in der Legende hinter jedem Therapienamen
                          die Gesamtanzahl an Condition IDs angezeigt (z.B.
                          "Surgical Procedure (n=12 345)")
    k_anonymity : Minimum Sample Size pro Age Bracket. Brackets mit ≤ k_anonymity
                  Samples werden gefiltert (Standard: 3)
    """
    therapies = list(dataframes.keys())

    # ── Bins: erstes Bracket 18–19, danach age_bracket-Schritte ─────────────
    bins = [18, 20] + list(range(20 + age_bracket, 105, age_bracket))
    if bins[-1] < 105:
        bins.append(105)
    labels = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1] - 1
        labels.append(f"{lo}–{hi}")

    # ── Farben: pro Therapie eine eigene Farbfamilie aus tab20b ─────────────
    tab20b = plt.get_cmap("tab20b").colors  # 20 Farben, 5 Familien à 4 Schattierungen
    color_map = {
        t: tab20b[(i * color_offset + color_shade) % len(tab20b)]
        for i, t in enumerate(therapies)
    }
    if colors:
        color_map.update(colors)

    rows = []
    _say(
        f"[age_dist] Altersverteilung je Therapietyp  |  Bracket={age_bracket}J  "
        f"|  Bins {bins[0]}–{bins[-1]} (erstes Bracket 18–19)  |  k-Anonymität={k_anonymity}"
    )
    for therapy, df in dataframes.items():
        if age_column not in df.columns:
            raise ValueError(f"Spalte '{age_column}' nicht in Therapiegruppe '{therapy}'.")
        _n_in = len(df)
        ages = pd.to_numeric(df[age_column], errors="coerce").dropna()
        _n_valid = len(ages)
        _n_drop = _n_in - _n_valid
        # Werte außerhalb der Bins (z.B. <18 oder >=105) fallen bei pd.cut auf NaN
        _n_outside = int(((ages < bins[0]) | (ages >= bins[-1])).sum())
        _say(
            f"[age_dist]   {therapy:<22}: {_n_in:>8,} Zeilen rein  →  {_n_valid:>8,} mit gültigem Alter "
            f"({_n_drop:,} ohne Alter verworfen; {_n_outside:,} außerhalb {bins[0]}–{bins[-1]})"
        )
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

    # ─── K-ANONYMITY: Filtere Age Brackets mit ≤ k_anonymity Samples ───────
    # Bestimme, welche Age Brackets die Schwelle unterschreiten (über alle Therapien)
    bracket_totals = agg.groupby("age_bracket")["count"].sum()
    filtered_brackets = bracket_totals[bracket_totals <= k_anonymity].index.tolist()

    if filtered_brackets:
        _say(f"[age_dist] k-Anonymität: Folgende Brackets werden gefiltert (≤{k_anonymity} Samples):")
        for bracket in filtered_brackets:
            total = bracket_totals[bracket]
            _say(f"[age_dist]   → {bracket}: {int(total)} Samples")

        # Entferne gefilterte Brackets aus dem Aggregat
        agg = agg[~agg["age_bracket"].isin(filtered_brackets)].reset_index(drop=True)

    # Aktualisiere Labels und Positionen für die Visualisierung
    remaining_labels = [l for l in labels if l not in filtered_brackets]

    if len(remaining_labels) == 0:
        _say("[age_dist] ⚠️  WARNUNG: Alle Age Brackets unterschreiten die k-Anonymität-Schwelle!")
        remaining_labels = labels  # Fallback

    x_positions = np.arange(len(remaining_labels))
    n = len(therapies)
    group_width = 0.8
    width = group_width / max(n, 1)
    offsets = (np.arange(n) - (n - 1) / 2) * width

    if show_bar_numbers is None:
        show_bar_numbers = PLOT_CONFIG["show_bar_numbers"]

    fig, ax = plt.subplots(figsize=(14, 7))
    totals = agg.groupby("therapy")["count"].sum().to_dict()
    legend_labels = {
        t: f"{t} (n={int(totals.get(t, 0)):,})".replace(",", " ") if show_total_in_legend else t
        for t in therapies
    }
    for offset, therapy in zip(offsets, therapies):
        sub = agg[agg["therapy"] == therapy].set_index("age_bracket").reindex(remaining_labels).fillna(0)
        counts = sub["count"].values
        bars = ax.bar(
            x_positions + offset,
            counts,
            width=width,
            color=color_map[therapy],
            alpha=0.9,
            label=legend_labels[therapy],
        )
        if show_bar_numbers:
            ymax_for_pad = agg["count"].max() if not agg.empty else 0
            label_pad = ymax_for_pad * 0.015
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        height + label_pad,
                        f"{int(height)}",
                        ha="center",
                        va="bottom",
                        rotation=bar_label_rotation,
                        fontsize=PLOT_CONFIG["fontsize_bar_label"],
                    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(remaining_labels, rotation=45, ha="right")
    ax.set_xlabel("Age Group", fontsize=PLOT_CONFIG["fontsize_axis_label"])
    ax.set_ylabel("Therapy Records", fontsize=PLOT_CONFIG["fontsize_axis_label"])
    if title is not False and PLOT_CONFIG["show_titles"]:
        chart_title = title or f"Age Distribution by Therapy Type ({age_bracket}-Year Groups)"
        if k_anonymity:
            chart_title += f" [k-anonymity: k≥{k_anonymity}]"
        ax.set_title(chart_title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=15)
    if PLOT_CONFIG["show_legend"]:
        ax.legend(frameon=False)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_xlim(-0.5, len(remaining_labels) - 0.5)

    # Etwas mehr Platz nach oben, da die Labels jetzt vertikal über den Balken stehen
    if show_bar_numbers and bar_label_rotation in (90, 270):
        ymin, ymax = ax.get_ylim()
        ax.set_ylim(ymin, ymax * 1.15)

    fig.tight_layout()
    if save_path:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor="white")
        _say(f"[age_dist] gespeichert: {path.resolve()}")
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
        _say(f"[therapy_times] gespeichert: {save_path.resolve()}", paper=False)
    except Exception as e:
        print(f"[therapy_times] Fehler beim Speichern: {e}")
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
        _say(f"[therapy_bias] gespeichert: {save_path}", paper=False)
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
            _say(
                f"[dropout] Highlight month={hl_x}  →  "
                f"{hl_row['patients_remaining'].values[0]:,} cond_ids  ({hl_y:.1f}%)",
                paper=False,
            )
        else:
            print(f"[dropout][WARNING] highlight_month={highlight_month} nicht in result_df")
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
        _say(f"[dropout] gespeichert: {full_path.resolve()}", paper=False)
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
        print("[lorenz][WARNUNG] Keine Daten für Lorenz-Kurve")
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
        print("[log_histo][WARNING] Keine Daten für Histogramm")
        return
    _n_with_therapy_pre = df[cond_col].nunique()
    _say(
        f"[log_histo] {cohort_name or 'Kohorte'}: {len(counts):,} cond_ids im Nenner "
        f"({_n_with_therapy_pre:,} mit ≥1 Therapie; "
        f"{len(counts) - _n_with_therapy_pre:,} mit 0 Therapien als Balken bei x=0 ergänzt)"
    )
    # ── Ausreißer entfernen (Punkt 6) ────────────────────────────────────────
    # WICHTIG für die Caption: cond_ids mit sehr vielen Therapien werden gekappt.
    if max_therapies is not None:
        n_outliers = int((counts > max_therapies).sum())
        _say(
            f"[log_histo]   Ausreißer-Cap max_therapies={max_therapies}: "
            f"{n_outliers:,} cond_ids mit > {max_therapies} Therapien entfernt "
            f"(Maximum war {int(counts.max())} Therapien/cond_id)."
        )
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

    # ── Integer-basierte, exakt zentrierte Bins (FIX) ───────────────────────
    # Statt linspace über min/max (führt zu Drift bei diskreten Werten) nutzen
    # wir ganzzahlige Werte und zentrieren jeden Balken exakt auf seinen Tick.
    max_val = int(counts.max())
    int_values = np.arange(0, max_val + 1)  # 0, 1, 2, ..., max_val
    bin_width = 0.8  # Balkenbreite < 1, damit etwas Lücke zum Nachbar-Tick bleibt
    bin_edges = np.arange(-bin_width / 2, max_val + 1, 1.0)
    # bin_edges liegen jetzt bei -0.5, 0.5, 1.5, ... -> jeder Bin ist exakt 1
    # breit und zentriert auf den jeweiligen Integer-Wert.

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

        # Zählungen pro Integer-Wert (statt freiem Histogramm) -> garantiert
        # konstanten Abstand und exakte Zentrierung über den Ticks.
        value_counts = subset.value_counts().reindex(int_values, fill_value=0)

        bars = ax.bar(
            int_values,
            value_counts.values,
            width=bin_width,
            align="center",
            color=color,
            edgecolor="white",
            linewidth=0.6,
        )

        # ── Zahlen über den Balken (FIX) ─────────────────────────────────────
        for rect, val in zip(bars, value_counts.values):
            if val > 0:
                ax.text(
                    rect.get_x() + rect.get_width() / 2,
                    val * 1.15,  # leicht über dem Balken, log-skaliert
                    f"{int(val):,}",
                    ha="center",
                    va="bottom",
                    fontsize=PLOT_CONFIG["fontsize_annotation_small"],
                    rotation=0,
                )

        ax.set_yscale("log")
        ax.set_ylim(bottom=0.8)  # etwas Luft unten, damit kleine Balken sichtbar bleiben
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

        # ── Ticks exakt auf Integer-Werten, damit Balken == Tick ────────────
        ax.set_xticks(int_values)
        ax.set_xlim(-bin_width, max_val + bin_width)

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
def plot_uicc_ecog_inventory_coverage(
    df_uicc,
    df_ecog=None,
    uicc_col: str = "uicc_tnm",
    ecog_col: str = "ecog_performance_status",
    cond_col: str = "cond_id",
    n_total_cond_ids: Optional[int] = None,
    cohort_name: str = "",
    title: Optional[str] = "UICC & ECOG Inventory",
    output_path: Optional[Path] = None,
    show: bool = True,
    font_family: str = "DejaVu Sans",
    show_legend: bool | None = None,
    show_ecog: bool = True,
    show_known_label: bool = True,   # nur fuer Signatur-Kompatibilitaet, hier ungenutzt
    color_unusable: str = "#bdbdbd",
    color_usable: Optional[str] = None,
) -> dict:
    """
    Variante von plot_uicc_ecog_inventory – ZWEIGETEILT:

      OBEN  (ZEILEN-Ebene)  : UICC-Stufen 0–IV bzw. ECOG 0–4 als absolute Anzahl
                              NUTZBARER Staging-/ECOG-ZEILEN. Identisch zur alten
                              Funktion, nur OHNE die Missing/Unknown-Zeile.
      UNTEN (COND_ID-Ebene) : je Modalitaet EIN 100%-Balken ueber die KOHORTE:
                                - "usable"     = cond_ids mit >=1 Eintrag != missing/U
                                - "no staging" = alle uebrigen Kohorten-cond_ids,
                                                 d.h. nur-missing/U UND cond_ids ganz
                                                 ohne Eintrag (durch das dropna auf das
                                                 Datum verloren gegangen).

    Damit beantwortet der untere Balken die eigentliche Coverage-Frage auf
    Patienten-/cond_id-Ebene, waehrend die oberen Balken weiter die nutzbaren
    Messwerte (Zeilen) zeigen.

    Nenner (100%) der unteren Balken = n_total_cond_ids (Kohorten-cond_ids, also
    cond_ids_gk.nunique()). Fehlt n_total_cond_ids, wird der untere Balken
    uebersprungen (mit Warnung).

    Hinweis: Diese Funktion MUSS vor der STAGING-BEREINIGUNG aufgerufen werden,
    solange df_uicc/df_ecog noch 'missing'/'U' enthalten – nur dann ist die
    cond_id-Coverage gegen die volle Kohorte korrekt.
    """
    from matplotlib.colors import to_rgb

    def _text_on(color: object) -> str:
        """Dunkler oder weisser Text je nach Helligkeit der Balkenfarbe."""
        r, g, b = to_rgb(color)
        return "#222222" if (0.299 * r + 0.587 * g + 0.114 * b) > 0.6 else "white"

    if show_legend is None:
        show_legend = PLOT_CONFIG["show_legend"]
    plt.rcParams["font.family"] = font_family

    if show_ecog and df_ecog is None:
        print("[uicc_ecog_inventory_coverage][WARNUNG] show_ecog=True aber df_ecog=None -> ECOG deaktiviert.")
        show_ecog = False

    have_total = (n_total_cond_ids is not None) and (n_total_cond_ids > 0)

    # ── UICC: nutzbare Zeilen je Hauptstufe + cond_id-Coverage ───────────────
    stages = ["0", "I", "II", "III", "IV"]
    stage_labels = [f"Stage {s}" for s in stages]
    uicc_mapped = df_uicc[uicc_col].map(map_uicc_to_main_stage)
    uicc_stage_rows = {s: int((uicc_mapped == s).sum()) for s in stages}
    uicc_rows_usable = sum(uicc_stage_rows.values())
    uicc_usable_cond = int(df_uicc.loc[uicc_mapped.isin(stages), cond_col].nunique())
    uicc_nostage_cond = (n_total_cond_ids - uicc_usable_cond) if have_total else None

    # ── ECOG: analog ─────────────────────────────────────────────────────────
    ecog_level_rows, ecog_rows_usable, ecog_usable_cond, ecog_nostage_cond = {}, 0, 0, None
    if show_ecog:
        ecog_levels = ["0", "1", "2", "3", "4"]
        ecog_mapped = df_ecog[ecog_col].map(map_ecog_value)
        ecog_level_rows = {v: int((ecog_mapped == v).sum()) for v in ecog_levels}
        ecog_rows_usable = sum(ecog_level_rows.values())
        ecog_usable_cond = int(df_ecog.loc[ecog_mapped.isin(ecog_levels), cond_col].nunique())
        ecog_nostage_cond = (n_total_cond_ids - ecog_usable_cond) if have_total else None

    # ── Sprechende Diagnostik ────────────────────────────────────────────────
    _say(f"[uicc_ecog_inventory_coverage] {cohort_name or 'Kohorte'}  |  "
         f"Datei={Path(output_path).name if output_path else '-'}")
    _say("[uicc_ecog_inventory_coverage]   UICC nutzbare Zeilen: "
         f"{uicc_rows_usable:,}  (" + ", ".join(f"{s}={uicc_stage_rows[s]:,}" for s in stages) + ")")
    if have_total:
        _say("[uicc_ecog_inventory_coverage]   UICC cond_id-Coverage: "
             f"usable={uicc_usable_cond:,}  no-staging={uicc_nostage_cond:,}  / N={n_total_cond_ids:,} "
             f"({100 * uicc_usable_cond / n_total_cond_ids:.1f}% usable)")
    if show_ecog:
        _say("[uicc_ecog_inventory_coverage]   ECOG nutzbare Zeilen: "
             f"{ecog_rows_usable:,}  (" + ", ".join(f"{v}={ecog_level_rows[v]:,}" for v in ecog_levels) + ")")
        if have_total:
            _say("[uicc_ecog_inventory_coverage]   ECOG cond_id-Coverage: "
                 f"usable={ecog_usable_cond:,}  no-staging={ecog_nostage_cond:,}  / N={n_total_cond_ids:,} "
                 f"({100 * ecog_usable_cond / n_total_cond_ids:.1f}% usable)")

    # ── Layout: 2x2 (oben Stufen / unten Coverage), bzw. 2x1 ohne ECOG ───────
    FIG_WIDTH, FIG_HEIGHT = 12.5, 8.5
    if show_ecog:
        fig = plt.figure(figsize=(FIG_WIDTH, FIG_HEIGHT))
        gs = fig.add_gridspec(2, 2, width_ratios=[3, 1], height_ratios=[4, 1],
                              wspace=0.45, hspace=0.55)
        ax_uicc = fig.add_subplot(gs[0, 0])
        ax_ecog = fig.add_subplot(gs[0, 1])
        ax_uicc_cov = fig.add_subplot(gs[1, 0])
        ax_ecog_cov = fig.add_subplot(gs[1, 1])
    else:
        fig = plt.figure(figsize=(FIG_WIDTH * 0.72, FIG_HEIGHT))
        gs = fig.add_gridspec(2, 1, height_ratios=[4, 1], hspace=0.55)
        ax_uicc = fig.add_subplot(gs[0, 0])
        ax_uicc_cov = fig.add_subplot(gs[1, 0])
        ax_ecog = ax_ecog_cov = None

    uicc_bar_color = tab20b_colors(6)[0]
    ecog_bar_color = tab20b_colors(6)[2]

    # ── obere Stufen-Balken (ZEILEN-Ebene) ───────────────────────────────────
    def _stage_panel(ax, order, labels, rowmap, total_usable, bar_color, pad_factor, panel_title):
        y = np.arange(len(order))
        vals = [rowmap[k] for k in order]
        vmax = max(vals) if any(vals) else 1
        ax.barh(y, vals, height=0.6, color=bar_color, edgecolor="white", linewidth=0.6)
        for yi, k in zip(y, order):
            v = rowmap[k]
            pct = f"  ({100 * v / total_usable:.1f}%)" if total_usable else ""
            ax.text(v + vmax * 0.012, yi, f"{v:,}{pct}", va="center", ha="left",
                    fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")
        ax.set_yticks(y)
        ax.set_yticklabels(labels, fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold")
        ax.set_xlim(0, vmax * pad_factor)
        ax.set_ylim(-0.6, len(order) - 0.4)
        ax.spines[["top", "right", "bottom"]].set_visible(False)
        ax.xaxis.set_visible(False)
        ax.tick_params(left=False)
        ax.set_title(panel_title, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=10)

    _stage_panel(ax_uicc, stages, stage_labels, uicc_stage_rows, uicc_rows_usable,
                 uicc_bar_color, 1.20,
                 f"UICC Stage Distribution (usable rows)\nrows = {uicc_rows_usable:,}")
    if show_ecog:
        _stage_panel(ax_ecog, ecog_levels, [f"ECOG {v}" for v in ecog_levels], ecog_level_rows,
                     ecog_rows_usable, ecog_bar_color, 1.55,
                     f"ECOG Performance Status (usable rows)\nrows = {ecog_rows_usable:,}")

    # ── untere 100%-Coverage-Balken (COND_ID-Ebene) ──────────────────────────
    def _draw_coverage(ax, usable, nostage, modality, bar_color):
        total = usable + nostage
        if total <= 0:
            ax.axis("off")
            return
        pu, pn = 100 * usable / total, 100 * nostage / total
        cu = color_usable or bar_color
        ax.barh([0], [pu], height=0.55, color=cu, edgecolor="white", linewidth=0.8)
        ax.barh([0], [pn], left=[pu], height=0.55, color=color_unusable, edgecolor="white", linewidth=0.8)
        # usable-Label: in das Segment, wenn breit genug, sonst oberhalb
        if pu >= 11:
            ax.text(pu / 2, 0, f"{usable:,}\n{pu:.1f}%", va="center", ha="center",
                    color=_text_on(cu), fontweight="bold", fontsize=PLOT_CONFIG["fontsize_bar_label"])
        else:
            ax.text(0, 0.45, f"{usable:,} ({pu:.1f}%)", va="bottom", ha="left",
                    color="#333333", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        # no-staging-Label: analog
        if pn >= 11:
            ax.text(pu + pn / 2, 0, f"{nostage:,}\n{pn:.1f}%", va="center", ha="center",
                    color=_text_on(color_unusable), fontweight="bold", fontsize=PLOT_CONFIG["fontsize_bar_label"])
        else:
            ax.text(100, 0.45, f"{nostage:,} ({pn:.1f}%)", va="bottom", ha="right",
                    color="#333333", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        ax.set_xlim(0, 100)
        ax.set_ylim(-0.6, 0.85)
        ax.set_yticks([0])
        ax.set_yticklabels([modality], fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(left=False)
        ax.set_xlabel("Share of cohort cond_ids (%)", fontsize=PLOT_CONFIG["fontsize_annotation_small"])
        ax.set_title(f"Cohort coverage: usable vs. no usable staging  (cond_ids, N={total:,})",
                     fontsize=PLOT_CONFIG["fontsize_annotation_small"], fontweight="bold", pad=6)

    if have_total:
        _draw_coverage(ax_uicc_cov, uicc_usable_cond, uicc_nostage_cond, "UICC", uicc_bar_color)
        if show_ecog:
            _draw_coverage(ax_ecog_cov, ecog_usable_cond, ecog_nostage_cond, "ECOG", ecog_bar_color)
    else:
        ax_uicc_cov.axis("off")
        if show_ecog:
            ax_ecog_cov.axis("off")
        print("[uicc_ecog_inventory_coverage][WARNUNG] n_total_cond_ids fehlt -> cond_id-Coverage uebersprungen.")

    if title and PLOT_CONFIG["show_titles"]:
        sup = f"{cohort_name} - {title}" if cohort_name else title
        fig.suptitle(sup, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.02)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
        _say(f"[uicc_ecog_inventory_coverage] gespeichert: {output_path}")
    if show:
        plt.show()
    plt.close(fig)

    return {
        "uicc": {"stage_rows": uicc_stage_rows, "usable_rows": uicc_rows_usable,
                 "usable_cond_ids": uicc_usable_cond, "nostage_cond_ids": uicc_nostage_cond},
        "ecog": ({"level_rows": ecog_level_rows, "usable_rows": ecog_rows_usable,
                  "usable_cond_ids": ecog_usable_cond, "nostage_cond_ids": ecog_nostage_cond}
                 if show_ecog else None),
        "n_total_cond_ids": n_total_cond_ids,
    }

def plot_uicc_ecog_inventory(
    df_uicc,
    df_ecog=None,
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
    show_ecog: bool = True,
    show_known_label: bool = True,
    label_obds: str = "before MUST",
    label_must: str = "after MUST",
) -> None:
    if show_legend is None:
        show_legend = PLOT_CONFIG["show_legend"]
    plt.rcParams["font.family"] = font_family

    stages = ["missing", "0", "I", "II", "III", "IV"]
    stage_labels = ["Missing" if s == "missing" else f"Stage {s}" for s in stages]

    def _main_counts(df):
        mapped = df[uicc_col].map(map_uicc_to_main_stage)
        vc = mapped.value_counts()
        return {s: int(vc.get(s, 0)) for s in stages}

    uicc_obds = _main_counts(df_uicc)
    uicc_obds_total = sum(uicc_obds.values())
    has_must = df_uicc_must is not None
    if has_must:
        uicc_must = _main_counts(df_uicc_must)
        uicc_must_total = sum(uicc_must.values())

    if show_ecog:
        ECOG_ORDER_INV = ["U", "0", "1", "2", "3", "4"]
        ecog_mapped = df_ecog[ecog_col].map(map_ecog_value)
        ecog_vc = ecog_mapped.value_counts()
        ecog_labels = [v for v in ECOG_ORDER_INV if ecog_vc.get(v, 0) > 0]
        ecog_vals = [int(ecog_vc.get(v, 0)) for v in ecog_labels]
        ecog_total = int(sum(ecog_vals))
        ecog_cols = tab20b_colors(len(ecog_labels))
        ecog_n_known = sum(v for l, v in zip(ecog_labels, ecog_vals) if l != "U")
        ecog_max = max(ecog_vals) if ecog_vals else 1

    # ── Sprechende Diagnostik (Fig 2) ─────────────────────────────────────────
    _say(
        f"[uicc_ecog_inventory] {cohort_name or 'Kohorte'}  |  "
        f"Datei={Path(output_path).name if output_path else '–'}"
    )
    _say(
        "[uicc_ecog_inventory]   UICC (oBDS): "
        f"{uicc_obds_total:,} Zeilen → Hauptstufen "
        + ", ".join(f"{s}={uicc_obds[s]:,}" for s in stages)
    )
    if has_must:
        _say(
            "[uicc_ecog_inventory]   UICC (MUST): "
            f"{uicc_must_total:,} Zeilen → Hauptstufen "
            + ", ".join(f"{s}={uicc_must[s]:,}" for s in stages)
        )
    if show_ecog:
        _say(
            "[uicc_ecog_inventory]   ECOG: "
            f"{ecog_total:,} Zeilen  (bekannt 0–4: {ecog_n_known:,};  "
            + ", ".join(f"{l}={v:,}" for l, v in zip(ecog_labels, ecog_vals))
            + ")"
        )

    FIG_WIDTH = 12.5
    FIG_HEIGHT = 8

    if show_ecog:
        fig, (ax_uicc, ax_ecog) = plt.subplots(
            1, 2, figsize=(FIG_WIDTH, FIG_HEIGHT),
            gridspec_kw={"width_ratios": [3, 1], "wspace": 0.45},
        )
    else:
        fig, ax_uicc = plt.subplots(1, 1, figsize=(FIG_WIDTH, FIG_HEIGHT))

    y = np.arange(len(stages))
    tab = tab20b_colors(4)
    color_obds, color_must = tab[0], tab[2]

    if has_must:
        bar_h = 0.34
        gap = 0.04
        obds_vals = [uicc_obds[s] for s in stages]
        must_vals = [uicc_must[s] for s in stages]
        uicc_max_val = max(max(obds_vals), max(must_vals), 1)
        ax_uicc.barh(y + (bar_h/2 + gap/2), obds_vals, height=bar_h,
                     color=color_obds, edgecolor="white", linewidth=0.6, label=label_obds)
        ax_uicc.barh(y - (bar_h/2 + gap/2), must_vals, height=bar_h,
                     color=color_must, edgecolor="white", linewidth=0.6, label=label_must)
        for yi, s in zip(y, stages):
            vo, vm = uicc_obds[s], uicc_must[s]
            pct_o = f"  ({100*vo/uicc_obds_total:.1f}%)" if uicc_obds_total else ""
            pct_m = f"  ({100*vm/uicc_must_total:.1f}%)" if uicc_must_total else ""
            ax_uicc.text(vo + uicc_max_val*0.004, yi + (bar_h/2 + gap/2),
                         f"{vo:,}{pct_o}", va="center", ha="left",
                         fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")
            ax_uicc.text(vm + uicc_max_val*0.004, yi - (bar_h/2 + gap/2),
                         f"{vm:,}{pct_m}, {label_must}", va="center", ha="left",
                         fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")
    else:
        bar_h = 0.6
        obds_vals = [uicc_obds[s] for s in stages]
        uicc_max_val = max(max(obds_vals), 1)
        ax_uicc.barh(y, obds_vals, height=bar_h,
                     color=color_obds, edgecolor="white", linewidth=0.6, label=label_obds)
        for yi, s in zip(y, stages):
            vo = uicc_obds[s]
            pct_o = f"  ({100*vo/uicc_obds_total:.1f}%)" if uicc_obds_total else ""
            ax_uicc.text(vo + uicc_max_val*0.004, yi, f"{vo:,}{pct_o}", va="center", ha="left",
                         fontsize=PLOT_CONFIG["fontsize_bar_label"], color="#333333")

    ax_uicc.set_yticks(y)
    ax_uicc.set_yticklabels(stage_labels, fontsize=PLOT_CONFIG["fontsize_base"], fontweight="bold")
    ax_uicc.set_xlim(0, uicc_max_val * 1.18)
    ax_uicc.set_ylim(-0.6, len(stages) - 0.4)
    ax_uicc.spines[["top", "right", "bottom"]].set_visible(False)
    ax_uicc.xaxis.set_visible(False)
    ax_uicc.tick_params(left=False)
    ax_uicc.set_title(
        f"UICC Stage Distribution\nrows = {uicc_obds_total:,}"
        + (f"  |  rows MUST = {uicc_must_total:,}" if has_must else ""),
        fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=10,
    )
    if show_legend and has_must:
        ax_uicc.legend(frameon=False, fontsize=PLOT_CONFIG["fontsize_legend"], loc="lower right")

    if show_ecog:
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
        known_part = f"  |  known = {ecog_n_known:,}" if show_known_label else ""
        ax_ecog.set_title(
            f"ECOG Performance Status\nrows = {ecog_total:,}{known_part}",
            fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", pad=10,
        )

    if title and PLOT_CONFIG["show_titles"]:
        sup = f"{cohort_name} – {title}" if cohort_name else title
        if n_total_cond_ids:
            sup += f"\n(Cohort: {n_total_cond_ids:,} cond_ids total)"
        fig.suptitle(sup, fontsize=PLOT_CONFIG["fontsize_subplot_title"], fontweight="bold", y=1.02)
    fig.tight_layout()
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
        _say(f"[uicc_ecog_inventory] gespeichert: {output_path}")
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
    totals: dict | None = None,
    color_offset: int = 4,
    color_shade: int = 1,
) -> plt.Figure:
    """
    Gruppiertes Balkendiagramm der UICC-Stage-Verteilung pro Therapietyp.
    Alle Sub-Stufen werden auf die Hauptstufe (0/I/II/III/IV) gemappt.

    exclude_missing : 'missing'/Unknown ausschließen (default True)
    exclude_zero    : Stage '0' ausschließen (default True)
    relative        : Anteil je Therapie in % statt absoluter Zahlen.
                      n bezieht sich auf das, was nach Ausschluss übrig bleibt.
    totals          : dict | None – Therapiename → Gesamtzahl der Therapien
                      dieses Typs (vor Matching/Cutoff). Wenn gesetzt, ist
                      100% = ALLE Therapien (nicht nur die gematchten); die
                      Balken zeigen den Anteil mit nutzbarer UICC, die Legende
                      'matched / total (coverage%)'. Default None → altes Verhalten.
    color_offset    : Schrittweite innerhalb der tab20b-Palette zwischen den
                      Therapiegruppen (siehe plot_age_distribution_grouped_bar).
    color_shade     : Welche der 4 Schattierungen pro Familie genutzt wird.
    """
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    therapies = list(dataframes.keys())
    tab20b = plt.get_cmap("tab20b").colors
    color_map = {
        t: tab20b[(i * color_offset + color_shade) % len(tab20b)]
        for i, t in enumerate(therapies)
    }
    if colors:
        color_map.update(colors)

    order = [s for s in UICC_ORDER]
    if exclude_missing and "missing" in order:
        order.remove("missing")
    if exclude_zero and "0" in order:
        order.remove("0")

    rows = []
    _say(
        f"[uicc_dist] UICC-Verteilung je Therapie  |  Ausschluss: "
        f"missing={exclude_missing}, Stage0={exclude_zero}  |  relativ={relative}"
    )
    for therapy, df in dataframes.items():
        if uicc_column not in df.columns:
            raise ValueError(f"Spalte '{uicc_column}' nicht in '{therapy}'.")
        mapped_all = df[uicc_column].map(map_uicc_to_main_stage)
        _n_all = len(mapped_all)
        _n_missing = int((mapped_all == "missing").sum())
        _n_zero = int((mapped_all == "0").sum())
        mapped = mapped_all[mapped_all.isin(order)]  # nur behaltene Hauptstufen → n schrumpft
        _n_excluded = _n_all - len(mapped)
        counts = mapped.value_counts().reindex(order, fill_value=0).reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
        _matched = int(counts["count"].sum())
        if totals is not None and totals.get(therapy, 0) > 0:
            _cov = 100.0 * _matched / totals[therapy]
            _say(
                f"[uicc_dist]   {therapy:<22}: dargestellt={_matched:,} / total={totals[therapy]:,} "
                f"({_cov:.1f}% mit nutzbarer UICC)  |  ausgeschlossen={_n_excluded:,} "
                f"(missing={_n_missing:,}, Stage0={_n_zero:,})"
            )
        else:
            _say(
                f"[uicc_dist]   {therapy:<22}: dargestellt n={_matched:,}  |  ausgeschlossen="
                f"{_n_excluded:,} (missing={_n_missing:,}, Stage0={_n_zero:,})"
            )
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
        totals=totals,
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
    totals: dict | None = None,
    color_offset: int = 4,
    color_shade: int = 1,
) -> plt.Figure:
    """
    Gruppiertes Balkendiagramm der ECOG-Verteilung pro Therapietyp.
    Werte werden auf 0–4 normalisiert; 'U'/Unknown optional ausgeschlossen.

    exclude_unknown : U/Unknown ausschließen (default True)
    relative        : Anteil je Therapie in % (n nach Ausschluss).
    totals          : dict | None – Therapiename → Gesamtzahl der Therapien
                      dieses Typs (vor Matching/Cutoff). Wenn gesetzt, ist
                      100% = ALLE Therapien (nicht nur die gematchten); die
                      Balken zeigen den Anteil mit nutzbarem ECOG, die Legende
                      'matched / total (coverage%)'. Default None → altes Verhalten.
    color_offset    : Schrittweite innerhalb der tab20b-Palette zwischen den
                      Therapiegruppen (siehe plot_age_distribution_grouped_bar).
    color_shade     : Welche der 4 Schattierungen pro Familie genutzt wird.
    """
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    therapies = list(dataframes.keys())
    tab20b = plt.get_cmap("tab20b").colors
    color_map = {
        t: tab20b[(i * color_offset + color_shade) % len(tab20b)]
        for i, t in enumerate(therapies)
    }
    if colors:
        color_map.update(colors)

    order = list(ECOG_ORDER)
    if not exclude_unknown:
        order = order + ["U"]

    rows = []
    _say(
        f"[ecog_dist] ECOG-Verteilung je Therapie  |  Ausschluss: U(unbekannt)="
        f"{exclude_unknown}  |  relativ={relative}"
    )
    for therapy, df in dataframes.items():
        if ecog_column not in df.columns:
            raise ValueError(f"Spalte '{ecog_column}' nicht in '{therapy}'.")
        mapped_all = df[ecog_column].map(map_ecog_value)
        _n_all = len(mapped_all)
        _n_u = int((mapped_all == "U").sum())
        mapped = mapped_all[mapped_all.isin(order)]
        _n_excluded = _n_all - len(mapped)
        counts = mapped.value_counts().reindex(order, fill_value=0).reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
        _matched = int(counts["count"].sum())
        if totals is not None and totals.get(therapy, 0) > 0:
            _cov = 100.0 * _matched / totals[therapy]
            _say(
                f"[ecog_dist]   {therapy:<22}: dargestellt={_matched:,} / total={totals[therapy]:,} "
                f"({_cov:.1f}% mit nutzbarem ECOG)  |  ausgeschlossen U={_n_u:,}"
            )
        else:
            _say(
                f"[ecog_dist]   {therapy:<22}: dargestellt n={_matched:,}  |  "
                f"ausgeschlossen U={_n_u:,}"
            )
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
        totals=totals,
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
        _say(f"[gender_stage] gespeichert: {path.resolve()}", paper=False)
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
    _say(
        f"[merge_panel] {file_name}: {len(panel_entries)} Histogramme "
        f"(months_diff je Therapie×Staging, log_y={log_y})"
    )
    for idx, entry in enumerate(panel_entries):
        row = idx // 2
        col = idx % 2
        ax = axes[row][col]
        data = entry["df"]["months_diff"].dropna()
        stats = entry.get("stats", {})
        staging_label = entry.get("staging_label", "Staging")
        if stats:
            _pct = 100 * stats.get("n_matched", 0) / stats["n_therapy"] if stats.get("n_therapy") else 0
            _say(
                f"[merge_panel]   {entry['title']:<30}: "
                f"{stats.get('n_matched', 0):>7,}/{stats.get('n_therapy', 0):>7,} gematcht "
                f"({_pct:4.1f}%)  |  {len(data):>7,} Paare mit Δ-Wert"
            )

        # ── Bin-Kanten so verschieben, dass jeder Bin auf seinem Tick
        # zentriert liegt (FIX). Anzahl der Bins (=bins) bleibt unverändert,
        # nur die Breite wird gleichverteilt und der Start um eine
        # halbe Bin-Breite nach links versetzt.
        data_min, data_max = data.min(), data.max()
        bin_width = (data_max - data_min) / bins
        bin_edges = np.linspace(
            data_min - bin_width / 2,
            data_max + bin_width / 2,
            bins + 1,
        )

        ax.hist(data, bins=bin_edges, color=hist_color, edgecolor="white", linewidth=0.4)
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
        _say(f"[merge_panel] gespeichert: {full_path.resolve()}")
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
    fig, axes = plt.subplots(3, 2, figsize=(10, 14), sharex=True, sharey=True)
    fig.subplots_adjust(hspace=0.50, wspace=0.25)
    _say(
        f"[sweep_panel] {file_name}: {len(sweeps)} Toleranz-Sweeps "
        f"(% gematchte cond_ids je Therapie×Staging über Toleranzfenster)"
    )
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
                _say(
                    f"[sweep_panel]   {therapy:<16} × {pair:<14}: bei Toleranz {hl} Monaten "
                    f"bleiben {hl_y:4.1f}% der cond_ids"
                    + (f" (von n={n_total:,})" if n_total else "")
                )
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
        _say(f"[sweep_panel] gespeichert: {full_path.resolve()}")
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
            _say(f"[scatterplot] Ordner erstellt: {ordner}", paper=False)
        pfad = os.path.join(ordner, dateiname)
        plt.savefig(pfad, dpi=dpi, bbox_inches="tight")
        _say(f"[scatterplot] gespeichert: {pfad}", paper=False)
