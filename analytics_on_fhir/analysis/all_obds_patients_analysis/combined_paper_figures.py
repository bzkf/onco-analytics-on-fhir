"""
combined_paper_figures.py
==========================
Kombinierte Multi-Panel-Figures fürs Paper.

TEIL 1 – ECOG/UICC-Panel
------------------------
Zusammengesetzt aus den bereits in plots.py vorhandenen Einzel-Plot-Funktionen
(plot_ecog_distribution_grouped_bar, plot_uicc_distribution_grouped_bar). Die
Render-Logik (Balken, Farben, Grid, Spines, %-Berechnung, totals-Coverage) ist
1:1 aus plots.py._build_grouped_bar übernommen – NICHT verändert. Neu/anpassbar:
  1) Subplot-Orientierung/-Parameter: nrows, ncols, figsize, sharex, sharey,
     hspace, wspace
  2) Schriftgrößen: fontsize_axis_label, fontsize_tick_label,
     fontsize_subplot_title, fontsize_legend, fontsize_bar_label

Reihenfolge: A) ECOG  B) UICC (oBDS)  C) UICC (MUST)

TEIL 2 – ICD-Code Top20-Panel (vor/nach Core-Comorbidity-Filterung)
--------------------------------------------------------------------
Zusammengesetzt aus SecondPaper_NebenDiagnosen_Plots.plot_top_bar. Die
Render-Logik (Balken, Value-Labels, Kontextzeile, Grid, Spines) ist 1:1 aus
plot_top_bar übernommen. Der "Vor/Nach"-Split entspricht exakt dem
Core-Comorbidity-Filter aus run_nebendiagnosen_report (Abschnitt B):
  - VOR Filterung  = Standard-Set (alle Codes, kein Filter)
  - NACH Filterung = Core-Comorbidity-Set (In_Core_Comorbidity_Analysis=="Ja")
Neu/anpassbar: Subplot-Orientierung + Schriftgrößen (Bar-Label, Tick-Label,
Achsen-Label, Titel, Kontextzeile) – alles andere = Original-Default.

Alle übrigen Parameter verwenden exakt dieselben Defaults wie in den
Original-Modulen.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── Zentrale Konfiguration & Mapping-Helfer – identisch zu plots.py ───────────
from plot_config import (
    PLOT_CONFIG,
    UICC_ORDER,
    ECOG_ORDER,
    map_uicc_to_main_stage,
    map_ecog_value,
    tab20b_colors,
)

# ── Nebendiagnosen-Helfer – identisch zu SecondPaper_NebenDiagnosen_Plots.py ──
from SecondPaper_NebenDiagnosen_Plots import (
    LevelConfig,
    _prepare_counts,
    _truncate,
    _fmt,
    _context_line,
)


# ══════════════════════════════════════════════════════════════════════════════
# Interne Helfer (1:1 Logik aus plots.py, nur auf übergebene `ax` statt eigener
# Figure umgestellt, damit mehrere Panels in EINER Figure landen können)
# ══════════════════════════════════════════════════════════════════════════════


def _make_color_map(therapies: list, color_offset: int = 4, color_shade: int = 1) -> dict:
    """Identisch zur Farblogik in plot_uicc/ecog_distribution_grouped_bar."""
    tab20b = plt.get_cmap("tab20b").colors
    return {
        t: tab20b[(i * color_offset + color_shade) % len(tab20b)]
        for i, t in enumerate(therapies)
    }


def _aggregate_ecog(
    dataframes: dict,
    ecog_column: str = "ecog_performance_status",
    exclude_unknown: bool = True,
) -> tuple[pd.DataFrame, list, list]:
    """Identische Aggregation wie in plots.plot_ecog_distribution_grouped_bar."""
    order = list(ECOG_ORDER)
    if not exclude_unknown:
        order = order + ["U"]

    rows = []
    for therapy, df in dataframes.items():
        if ecog_column not in df.columns:
            raise ValueError(f"Spalte '{ecog_column}' nicht in '{therapy}'.")
        mapped_all = df[ecog_column].map(map_ecog_value)
        mapped = mapped_all[mapped_all.isin(order)]
        counts = mapped.value_counts().reindex(order, fill_value=0).reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
    agg = pd.concat(rows, ignore_index=True)
    return agg, order, list(dataframes.keys())


def _aggregate_uicc(
    dataframes: dict,
    uicc_column: str = "uicc_tnm",
    exclude_missing: bool = True,
    exclude_zero: bool = True,
) -> tuple[pd.DataFrame, list, list]:
    """Identische Aggregation wie in plots.plot_uicc_distribution_grouped_bar."""
    order = [s for s in UICC_ORDER]
    if exclude_missing and "missing" in order:
        order.remove("missing")
    if exclude_zero and "0" in order:
        order.remove("0")

    rows = []
    for therapy, df in dataframes.items():
        if uicc_column not in df.columns:
            raise ValueError(f"Spalte '{uicc_column}' nicht in '{therapy}'.")
        mapped_all = df[uicc_column].map(map_uicc_to_main_stage)
        mapped = mapped_all[mapped_all.isin(order)]
        counts = mapped.value_counts().reindex(order, fill_value=0).reset_index()
        counts.columns = ["category", "count"]
        counts["therapy"] = therapy
        rows.append(counts)
    agg = pd.concat(rows, ignore_index=True)
    return agg, order, list(dataframes.keys())


def _render_grouped_bar_panel(
    ax,
    agg: pd.DataFrame,
    category_order: list,
    therapies: list,
    color_map: dict,
    xlabel: str,
    title: str | None,
    relative: bool,
    totals: dict | None,
    *,
    fontsize_axis_label: float,
    fontsize_tick_label: float | None,
    fontsize_subplot_title: float,
    fontsize_legend: float,
    fontsize_legend_entries: float | None,
    fontsize_bar_label: float,
    show_bar_numbers: bool,
    show_legend: bool,
    show_titles: bool,
) -> None:
    """1:1 Rendering-Logik aus plots.py._build_grouped_bar, nur parametrisiert
    auf eine übergebene `ax` (statt eigener fig/ax) + adjustierbare Schriftgrößen.
    """
    matched_totals = agg.groupby("therapy")["count"].sum().to_dict()
    use_totals = totals is not None
    x_positions = np.arange(len(category_order))
    n = len(therapies)
    group_width = 0.8
    width = group_width / max(n, 1)
    offsets = (np.arange(n) - (n - 1) / 2) * width

    for offset, therapy in zip(offsets, therapies):
        sub = agg[agg["therapy"] == therapy].set_index("category").reindex(category_order).fillna(0)
        counts = sub["count"].values.astype(float)
        matched = int(matched_totals.get(therapy, 0))
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
                        fontsize=fontsize_bar_label,
                    )

    ax.set_xticks(x_positions)
    _tick_kwargs = {"rotation": 0, "ha": "center"}
    if fontsize_tick_label is not None:
        _tick_kwargs["fontsize"] = fontsize_tick_label
    ax.set_xticklabels(category_order, **_tick_kwargs)
    if fontsize_tick_label is not None:
        ax.tick_params(axis="y", labelsize=fontsize_tick_label)

    ax.set_xlabel(xlabel, fontsize=fontsize_axis_label)
    if relative:
        _ylabel = "Share of all therapies (%)" if use_totals else "Share within therapy (%)"
    else:
        _ylabel = "Number of Condition IDs"
    ax.set_ylabel(_ylabel, fontsize=fontsize_axis_label)

    if title and show_titles:
        ax.set_title(title, fontsize=fontsize_subplot_title, fontweight="bold", pad=15)

    if show_legend:
        _legend_kwargs = dict(frameon=False, title="Therapy", title_fontsize=fontsize_legend)
        if fontsize_legend_entries is not None:
            _legend_kwargs["fontsize"] = fontsize_legend_entries
        ax.legend(**_legend_kwargs)

    if not relative:
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    ax.set_ylim(0, 100)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_xlim(-0.5, len(category_order) - 0.5)


# ══════════════════════════════════════════════════════════════════════════════
# Öffentliche Funktion: kombiniertes ECOG/UICC(oBDS)/UICC(MUST)-Panel
# ══════════════════════════════════════════════════════════════════════════════


def create_ecog_uicc_panel(
    df_ecog: dict,
    df_uicc_obds: dict,
    df_uicc_must: dict,
    totals: dict | None = None,
    *,
    # ── Fachliche Defaultparameter (identisch zu plots.py) ───────────────────
    ecog_column: str = "ecog_performance_status",
    uicc_column: str = "uicc_tnm",
    exclude_unknown: bool = True,
    exclude_missing: bool = True,
    exclude_zero: bool = True,
    relative: bool = True,
    color_offset: int = 4,
    color_shade: int = 1,
    titles: tuple = (None, None, None),
    # ── Subplot-Orientierung / -Parameter ────────────────────────────────────
    nrows: int = 3,
    ncols: int = 1,
    figsize: tuple | None = None,
    sharex: bool = False,
    sharey: bool = False,
    hspace: float = 0.45,
    wspace: float = 0.25,
    # ── Schriftgrößen (None = Default aus PLOT_CONFIG, wie im Original) ─────
    fontsize_axis_label: float | None = None,
    fontsize_tick_label: float | None = None,
    fontsize_subplot_title: float | None = None,
    fontsize_legend: float | None = None,
    fontsize_legend_entries: float | None = None,  # NEU, optional (s. Hinweis unten)
    fontsize_bar_label: float | None = None,
    # ── Sonstiges ─────────────────────────────────────────────────────────────
    show_bar_numbers: bool | None = None,
    show_legend: bool | None = None,
    show_titles: bool | None = None,
    dpi: int | None = None,
    bbox_inches: str | None = None,
    save_path: str | None = None,
) -> plt.Figure:
    """
    Fügt ECOG-Verteilung, UICC-Verteilung (oBDS) und UICC-Verteilung (MUST)
    je Therapietyp als Subplots in GENAU dieser Reihenfolge zu einer Figure
    zusammen (A=ECOG, B=UICC-oBDS, C=UICC-MUST).

    df_ecog / df_uicc_obds / df_uicc_must:
        dict {"Surgical Procedure": df, "Systemic Therapy": df, "Radiation Therapy": df}
        – exakt wie bei den bisherigen Einzel-Funktionsaufrufen in main_sprechend.py.
    totals:
        Wie in plots.py – Therapiename → Gesamtzahl (vor Matching/Cutoff).
        Gleicher `totals`-dict (_therapy_totals) wird für alle drei Panels verwendet.

    Subplot-Orientierung:
        nrows/ncols/figsize steuern die Anordnung. Default = 3 Zeilen × 1 Spalte
        (vertikal gestapelt, wie ein klassisches Paper-Panel A/B/C). Für ein
        horizontales Layout z.B. nrows=1, ncols=3 übergeben.

    Schriftgrößen:
        None → Default aus PLOT_CONFIG (identisch zum bisherigen Verhalten der
        Einzel-Plots). Bei Bedarf gezielt überschreiben, z.B. bei 3x1-Stapelung
        oft sinnvoll etwas kleiner als die Einzel-Plot-Defaults.

    Hinweis fontsize_legend_entries:
        Im Original (plots.py._build_grouped_bar) wird nur die Legenden-
        ÜBERSCHRIFT über PLOT_CONFIG['fontsize_legend'] skaliert, die
        Eintrags-Labels selbst nutzen den matplotlib-Default. Hier zusätzlich
        optional (Default None = unverändertes Originalverhalten), da bei
        kleineren, gestapelten Subplots die Legenden-Einträge sonst schnell zu
        groß wirken.

    WICHTIG – figsize und bbox_inches:
        Die tatsächliche Pixelgröße wird als Pixel = figsize_inches × dpi berechnet.
        - bbox_inches="tight" (plots.py-Standard): Beschneidet die Figure auf den
          tatsächlich verwendeten Inhaltsbereich – figsize wird quasi ignoriert.
        - bbox_inches=None (Standard hier): figsize wird exakt respektiert.
    """
    # ── Defaults aus PLOT_CONFIG auflösen (wie im Original) ──────────────────
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    if fontsize_axis_label is None:
        fontsize_axis_label = PLOT_CONFIG["fontsize_axis_label"]
    if fontsize_subplot_title is None:
        fontsize_subplot_title = PLOT_CONFIG["fontsize_subplot_title"]
    if fontsize_legend is None:
        fontsize_legend = PLOT_CONFIG["fontsize_legend"]
    if fontsize_bar_label is None:
        fontsize_bar_label = PLOT_CONFIG["fontsize_bar_label"]
    if show_bar_numbers is None:
        show_bar_numbers = PLOT_CONFIG["show_bar_numbers"]
    if show_legend is None:
        show_legend = PLOT_CONFIG["show_legend"]
    if show_titles is None:
        show_titles = PLOT_CONFIG["show_titles"]
    # fontsize_tick_label: kein PLOT_CONFIG-Key im Original -> bleibt None = mpl-Default

    if figsize is None:
        figsize = (14 * ncols, 7 * nrows)

    therapies = list(df_ecog.keys())
    color_map = _make_color_map(therapies, color_offset=color_offset, color_shade=color_shade)

    agg_ecog, order_ecog, _ = _aggregate_ecog(df_ecog, ecog_column, exclude_unknown)
    agg_uicc_obds, order_uicc, _ = _aggregate_uicc(df_uicc_obds, uicc_column, exclude_missing, exclude_zero)
    agg_uicc_must, _, _ = _aggregate_uicc(df_uicc_must, uicc_column, exclude_missing, exclude_zero)

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, sharex=sharex, sharey=sharey)
    axes_flat = np.atleast_1d(axes).reshape(-1)

    # ── GENAU diese Reihenfolge: ECOG -> UICC(oBDS) -> UICC(MUST) ────────────
    panels = [
        (agg_ecog, order_ecog, "ECOG Performance Status", titles[0]),
        (agg_uicc_obds, order_uicc, "UICC Stage", titles[1]),
        (agg_uicc_must, order_uicc, "UICC Stage", titles[2]),
    ]

    for ax, (agg, order, xlabel, title) in zip(axes_flat, panels):
        _render_grouped_bar_panel(
            ax=ax,
            agg=agg,
            category_order=order,
            therapies=therapies,
            color_map=color_map,
            xlabel=xlabel,
            title=title,
            relative=relative,
            totals=totals,
            fontsize_axis_label=fontsize_axis_label,
            fontsize_tick_label=fontsize_tick_label,
            fontsize_subplot_title=fontsize_subplot_title,
            fontsize_legend=fontsize_legend,
            fontsize_legend_entries=fontsize_legend_entries,
            fontsize_bar_label=fontsize_bar_label,
            show_bar_numbers=show_bar_numbers,
            show_legend=show_legend,
            show_titles=show_titles,
        )

    # ungenutzte Achsen ausblenden, falls nrows*ncols > 3
    for ax in axes_flat[len(panels):]:
        ax.set_visible(False)

    fig.subplots_adjust(hspace=hspace, wspace=wspace)

    # tight_layout nur, wenn bbox_inches="tight" – sonst können sich zwei
    # Layout-Optimierer ins Gehege kommen
    if bbox_inches == "tight":
        fig.tight_layout()

    if save_path:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Default: bbox_inches=None respektiert figsize exakt;
        # bbox_inches="tight" schneidet auf Inhalt zu (plots.py-Verhalten)
        fig.savefig(path, dpi=dpi, bbox_inches=bbox_inches, facecolor="white")
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# TEIL 2 – ICD-Code Top20-Panel: vor / nach Core-Comorbidity-Filterung
# Native Lösung, 1:1 aus SecondPaper_NebenDiagnosen_Plots.plot_top_bar
# übernommen (Balken, Value-Labels, Kontextzeile, Grid, Spines), nur auf eine
# übergebene `ax` umgestellt + Schriftgrößen adjustierbar.
# ══════════════════════════════════════════════════════════════════════════════


def _render_top_bar_panel(
    ax,
    df: pd.DataFrame,
    patient_col: str,
    level: "LevelConfig",
    cohort_name: str,
    mode: str,
    top_n: int | None,
    *,
    fontsize_bar_label: float,
    fontsize_tick_label: float,
    fontsize_axis_label: float,
    fontsize_subplot_title: float,
    fontsize_context: float,
    show_title: bool,
    show_context_line: bool,
    context_line_y: float = -0.18,
    bar_color=None,
    panel_title: str | None = None,
) -> pd.DataFrame:
    """1:1 Rendering-Logik aus SecondPaper_NebenDiagnosen_Plots.plot_top_bar,
    nur parametrisiert auf eine übergebene `ax` (statt eigener fig/ax) +
    adjustierbare Schriftgrößen. Gibt die geplottete 'top'-DataFrame zurück
    (wie im Original)."""
    counts = _prepare_counts(df, patient_col, level, mode)
    top = counts.head(top_n).copy() if top_n else counts.copy()
    top["label"] = top[level.column].apply(_truncate)

    n_patients = df[patient_col].nunique()
    n_rows = len(df.dropna(subset=[level.column]))
    n_cats = counts[level.column].nunique()
    n_shown = len(top)
    xlabel = "No. of diagnosis rows" if mode == "all" else "No. of patients"
    title_pfx = f"Top {n_shown}" if top_n else "All categories"

    if bar_color is None:
        bar_color = tab20b_colors(1)[0]

    bars = ax.barh(
        list(range(n_shown)),
        top["count"].values[::-1],
        color=bar_color, edgecolor="white", linewidth=0.4,
    )
    for bar, val in zip(bars, top["count"].values[::-1]):
        ax.text(
            bar.get_width() + counts["count"].max() * 0.005,
            bar.get_y() + bar.get_height() / 2,
            _fmt(val),
            va="center", ha="left", fontsize=fontsize_bar_label, color="#333333",
        )

    ax.set_yticks(list(range(n_shown)))
    ax.set_yticklabels(top["label"].values[::-1], fontsize=fontsize_tick_label)
    ax.set_xlabel(xlabel, fontsize=fontsize_axis_label)
    ax.set_ylabel(level.display_name, fontsize=fontsize_axis_label)
    if show_title:
        _title = panel_title or f"{title_pfx} most frequent categories – {level.display_name}"
        ax.set_title(_title, fontsize=fontsize_subplot_title, fontweight="bold", pad=14)
    if show_context_line:
        ax.text(
            0.5, context_line_y,
            _context_line(cohort_name, n_patients, n_rows, level, n_cats, mode),
            transform=ax.transAxes,
            ha="center", va="top", fontsize=fontsize_context, color="#555555", style="italic",
        )
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt(x)))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    return top


def create_icd_code_filter_panel(
    df_conditions: pd.DataFrame,
    patient_col: str,
    icd_nebendiagnosen_einteilung: pd.DataFrame,
    *,
    # ── Fachliche Defaultparameter (identisch zu SecondPaper_NebenDiagnosen_Plots.py) ──
    level: "LevelConfig | None" = None,
    cohort_name: str = "Top20 Cohort",
    mode: str = "unique",  # unique = 1x pro (Patient, Code) – vermeidet Mehrfachzählung
    top_n: int | None = 20,
    icd_code_col_in_df: str = "icd_code",
    icd_code_col_in_mapping: str = "ICD_Code",
    # ── Subplot-Orientierung / -Parameter ────────────────────────────────────
    nrows: int = 2,
    ncols: int = 1,
    figsize: tuple | None = None,
    sharex: bool = False,
    sharey: bool = False,
    hspace: float = 0.6,
    wspace: float = 0.3,
    # ── Schriftgrößen (None = Default aus PLOT_CONFIG, wie im Original) ─────
    fontsize_bar_label: float | None = None,
    fontsize_tick_label: float | None = None,
    fontsize_axis_label: float | None = None,
    fontsize_subplot_title: float | None = None,
    fontsize_context: float | None = None,
    # ── Sonstiges ─────────────────────────────────────────────────────────────
    panel_titles: tuple = (None, None),
    bar_colors: tuple = (None, None),
    show_titles: bool = True,
    show_context_line: bool = True,
    context_line_y: float = -0.18,  # Abstand Fußzeile <-> x-Achse (negativ = weiter unten)
    dpi: int | None = None,
    bbox_inches: str | None = None,
    save_path: str | None = None,
) -> tuple[plt.Figure, list]:
    """
    Fügt den Top-N-Bar-Chart für ein ICD-Level VOR und NACH der
    Core-Comorbidity-Filterung als Subplots in EINER Figure zusammen
    (Reihenfolge: A = vor Filterung, B = nach Filterung).

    Der Filter-Split ist 1:1 aus run_nebendiagnosen_report (Abschnitt B)
    übernommen:
      - VOR Filterung  : df_conditions unverändert (Standard-Set, alle Codes)
      - NACH Filterung : nur Zeilen, deren icd_code in
                          icd_nebendiagnosen_einteilung als
                          In_Core_Comorbidity_Analysis == "Ja" markiert ist

    level:
        Default None → LevelConfig("icd_code", "icd_code", "Full ICD code")
        (genau die von dir gewünschte Konfiguration).

    mode:
        Default "unique" (1x pro Patient+Code gezählt – vermeidet
        Mehrfachzählung bei mehreren Diagnose-Einträgen desselben Codes für
        denselben Patienten). "all" würde jede Zeile einzeln zählen.

    Rückgabe: (fig, [top_vor, top_nach]) – die beiden geplotteten
    Top-N-DataFrames, analog zum Rückgabewert von plot_top_bar.
    """
    if level is None:
        level = LevelConfig("icd_code", "icd_code", "Full ICD code")

    # ── Defaults aus PLOT_CONFIG auflösen (wie im Original plot_top_bar) ─────
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]
    if fontsize_bar_label is None:
        fontsize_bar_label = PLOT_CONFIG["fontsize_annotation_small"]
    if fontsize_tick_label is None:
        fontsize_tick_label = PLOT_CONFIG["fontsize_legend"]
    if fontsize_axis_label is None:
        fontsize_axis_label = PLOT_CONFIG["fontsize_annotation"]
    if fontsize_subplot_title is None:
        fontsize_subplot_title = PLOT_CONFIG["fontsize_subplot_title"]
    if fontsize_context is None:
        fontsize_context = PLOT_CONFIG["fontsize_annotation_tiny"]

    # ── VOR Filterung: Standard-Set (alle Codes) ──────────────────────────────
    df_before = df_conditions

    # ── NACH Filterung: Core-Comorbidity-Set – identische Logik wie in
    #    run_nebendiagnosen_report, Abschnitt B ────────────────────────────────
    core_codes = set(
        icd_nebendiagnosen_einteilung.loc[
            icd_nebendiagnosen_einteilung["In_Core_Comorbidity_Analysis"] == "Ja",
            icd_code_col_in_mapping,
        ]
    )
    df_after = df_conditions[df_conditions[icd_code_col_in_df].isin(core_codes)].copy()

    if figsize is None:
        _height_per_panel = max(6, (top_n or 20) * 0.45)
        figsize = (12 * ncols, _height_per_panel * nrows)

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, sharex=sharex, sharey=sharey)
    axes_flat = np.atleast_1d(axes).reshape(-1)

    default_titles = (
        f"Vor Filterung (alle Codes) – {level.display_name}",
        f"Nach Filterung (Core Comorbidity) – {level.display_name}",
    )
    _panel_titles = [t if t else d for t, d in zip(panel_titles, default_titles)]

    # ── GENAU diese Reihenfolge: vor Filterung -> nach Filterung ─────────────
    panels_data = [df_before, df_after]

    tops = []
    for ax, df_x, ptitle, color in zip(axes_flat, panels_data, _panel_titles, bar_colors):
        top = _render_top_bar_panel(
            ax=ax,
            df=df_x,
            patient_col=patient_col,
            level=level,
            cohort_name=cohort_name,
            mode=mode,
            top_n=top_n,
            fontsize_bar_label=fontsize_bar_label,
            fontsize_tick_label=fontsize_tick_label,
            fontsize_axis_label=fontsize_axis_label,
            fontsize_subplot_title=fontsize_subplot_title,
            fontsize_context=fontsize_context,
            show_title=show_titles,
            show_context_line=show_context_line,
            context_line_y=context_line_y,
            bar_color=color,
            panel_title=ptitle,
        )
        tops.append(top)

    # ungenutzte Achsen ausblenden, falls nrows*ncols > 2
    for ax in axes_flat[len(panels_data):]:
        ax.set_visible(False)

    # ── Layout: IMMER erst tight_layout (reserviert automatisch genug Platz
    #    für Titel, Achsenlabels UND die Fußzeile/Kontextzeile unterhalb der
    #    x-Achse – genau wie im Original _savefig), danach optional per
    #    hspace/wspace zusätzlichen Abstand zwischen den Panels einstellen.
    fig.tight_layout()
    fig.subplots_adjust(hspace=hspace, wspace=wspace)

    if save_path:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches=bbox_inches, facecolor="white")
    return fig, tops


# ══════════════════════════════════════════════════════════════════════════════
# Alternative (Bild-Komposition): Panel aus bereits gerenderten PNGs
# zusammensetzen – nur falls das Re-Rendering oben (TEIL 2) NICHT gewünscht ist
# (z.B. um exakt die bereits produzierten PNGs aus run_nebendiagnosen_report
# wiederzuverwenden, ohne die Berechnung erneut laufen zu lassen).
# ══════════════════════════════════════════════════════════════════════════════


def compose_prerendered_png_panel(
    image_paths: list,
    *,
    nrows: int | None = None,
    ncols: int | None = None,
    figsize: tuple | None = None,
    panel_labels: list | None = None,
    fontsize_panel_label: float = 14,
    hspace: float = 0.05,
    wspace: float = 0.05,
    dpi: int | None = None,
    bbox_inches: str | None = None,
    save_path: str | None = None,
) -> plt.Figure:
    """
    Fügt bereits gerenderte PNG-Dateien (z.B. Top20-ICD-Code-Bar-Charts aus
    run_nebendiagnosen_report) als Subplots zu EINER Figure zusammen.

    WICHTIG: Reine Bild-Komposition (kein Re-Rendering der Original-Charts).
    Die Schriftgrößen INNERHALB der Original-PNGs sind bereits als Pixel fixiert
    und können hier NICHT mehr verändert werden – `fontsize_panel_label` steuert
    ausschließlich optionale zusätzliche Panel-Beschriftungen (z.B. "A)", "B)"),
    die zusätzlich eingeblendet werden.

    bbox_inches:
        Default None: figsize wird exakt respektiert.
        "tight": schneidet auf Inhaltsbereich zu (plots.py-Verhalten).
    """
    import matplotlib.image as mpimg

    n = len(image_paths)
    if nrows is None and ncols is None:
        nrows, ncols = n, 1
    elif nrows is None:
        nrows = int(np.ceil(n / ncols))
    elif ncols is None:
        ncols = int(np.ceil(n / nrows))
    if figsize is None:
        figsize = (8 * ncols, 6 * nrows)
    if dpi is None:
        dpi = PLOT_CONFIG["dpi"]

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    axes_flat = np.atleast_1d(axes).reshape(-1)

    for i, ax in enumerate(axes_flat):
        if i < n:
            img = mpimg.imread(image_paths[i])
            ax.imshow(img)
            ax.axis("off")
            if panel_labels and i < len(panel_labels):
                ax.text(
                    0.01, 0.99, panel_labels[i], transform=ax.transAxes,
                    fontsize=fontsize_panel_label, fontweight="bold",
                    ha="left", va="top",
                )
        else:
            ax.set_visible(False)

    fig.subplots_adjust(hspace=hspace, wspace=wspace)

    # tight_layout nur, wenn bbox_inches="tight"
    if bbox_inches == "tight":
        fig.tight_layout()

    if save_path:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches=bbox_inches, facecolor="white")
    return fig
