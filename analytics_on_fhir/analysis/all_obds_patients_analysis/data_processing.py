"""
data_processing.py
==================
Alle Datentransformations- und Merge-Funktionen für die BZKF-Auswertung.

Inhaltsübersicht
----------------
A – Therapieanalyse
    analyze_therapy_patterns         Therapiesequenzen pro cond_id analysieren
    patient_dropout_by_cutoff        Dropout-Tabelle berechnen (kein Plot)

B – Nearest-Date-Merging  (Therapie × Staging)
    merge_with_nearest_date_matching  Einzelner Merge mit optionalem Histogramm
    sweep_tolerance_fast              Toleranz-Sweep + zugehöriger Plot
    merge_and_plot_ecog_uicc_proximity  ECOG × UICC Nähe + Histogramm/Bubble
    [Hinweis: Die drei Merge-Funktionen enthalten optionale Visualisierungen,
     da Merge-Ergebnis und Plot eng verzahnt sind.]
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
# A – THERAPIEANALYSE
# ══════════════════════════════════════════════════════════════════════════════

def analyze_therapy_patterns(
    df_erstlinie_finding: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analysiert Therapiesequenzen pro condition_id_hash.

    Returns
    -------
    df_patient_level : pd.DataFrame
        Eine Zeile pro condition_id_hash mit Therapieformen,
        Erstlinientherapie, Reihenfolge und Zeitabständen.

    df_pattern_summary : pd.DataFrame
        Aggregierte Übersicht über Therapiekombinationen und Sequenzen.
    """
    therapy_order = ["systemtherapie", "radiotherapie", "op"]

    df_min = (
        df_erstlinie_finding
        .groupby(["condition_id_hash", "therapy_typ"])
        ["months_between_asserted_therapy_start_date"]
        .min()
        .unstack()
    )

    for col in therapy_order:
        if col not in df_min.columns:
            df_min[col] = np.nan

    df_min["has_system"] = df_min["systemtherapie"].notna()
    df_min["has_radio"]  = df_min["radiotherapie"].notna()
    df_min["has_op"]     = df_min["op"].notna()
    df_min["n_therapy_types"] = (
        df_min[["has_system", "has_radio", "has_op"]].sum(axis=1)
    )

    def _build_combination(row):
        combis = []
        if row["has_system"]: combis.append("system")
        if row["has_radio"]:  combis.append("radio")
        if row["has_op"]:     combis.append("op")
        return " + ".join(combis)

    df_min["therapy_combination"] = df_min.apply(_build_combination, axis=1)

    therapy_cols = ["systemtherapie", "radiotherapie", "op"]
    df_min["first_therapy"] = df_min[therapy_cols].idxmin(axis=1)

    def _build_sequence(row):
        entries = [
            (t, row[t]) for t in therapy_cols if pd.notna(row[t])
        ]
        entries.sort(key=lambda x: x[1])
        return " -> ".join(e[0] for e in entries)

    df_min["therapy_sequence"]      = df_min.apply(_build_sequence, axis=1)
    df_min["delta_system_to_radio"] = df_min["radiotherapie"] - df_min["systemtherapie"]
    df_min["delta_op_to_radio"]     = df_min["radiotherapie"] - df_min["op"]
    df_min["delta_system_to_op"]    = df_min["op"]           - df_min["systemtherapie"]

    df_min["radio_after_all"] = (
        (~df_min["has_system"] | (df_min["radiotherapie"] > df_min["systemtherapie"]))
        &
        (~df_min["has_op"]     | (df_min["radiotherapie"] > df_min["op"]))
    )
    df_min["system_is_first"] = df_min["first_therapy"] == "systemtherapie"

    df_pattern_summary = (
        df_min.groupby(
            ["therapy_combination", "therapy_sequence", "first_therapy"]
        )
        .size()
        .reset_index(name="n_conditions")
        .sort_values("n_conditions", ascending=False)
    )

    return df_min.reset_index(), df_pattern_summary


def patient_dropout_by_cutoff(
    df,
    patient_col: str = "patient_id",
    time_col: str    = "months_since_diagnosis",
    max_month: int   = None,
) -> pd.DataFrame:
    """
    Berechnet pro Cutoff-Monat, wie viele Patienten ihre erste Therapie
    innerhalb dieses Fensters hatten (= 'remaining').

    Returns
    -------
    pd.DataFrame mit Spalten: cutoff_month, patients_remaining, patients_lost
    """
    df_max = df.groupby(patient_col)[time_col].max().reset_index()

    if max_month is None:
        max_month = int(df_max[time_col].max())

    records = [
        {
            "cutoff_month":       m,
            "patients_remaining": int((df_max[time_col] <= m).sum()),
        }
        for m in range(0, max_month + 1)
    ]

    result_df = pd.DataFrame(records)
    result_df["patients_lost"] = (
        result_df["patients_remaining"]
        .shift(1)
        .sub(result_df["patients_remaining"])
        .fillna(0)
        .astype(int)
    )
    return result_df


# ══════════════════════════════════════════════════════════════════════════════
# B – NEAREST-DATE-MERGING
# ══════════════════════════════════════════════════════════════════════════════

def merge_with_nearest_date_matching(
    df_system_therapies,
    df_leistungszustand,
    id_col: str               = "condition_id_hash",
    therapy_time_col: str     = "months_between_asserted_therapy_start_date",
    lz_time_col: str          = "months_between_asserted_effective_dateTime",
    direction: str            = "backward",
    tolerance                 = None,
    plot: bool                = False,
    log_y: bool               = False,
    plot_title: str           = "",
    save_path: str            = None,
    save_name: str            = "plot.png",
    font_family: str          = "DejaVu Sans",
):
    """
    Verbindet Therapie- und Staging-Einträge per nearest-date-Merge.

    Für jede Therapie-Zeile wird der zeitlich nächste (zurückliegende)
    Staging-Eintrag desselben condition_id_hash gesucht.

    Returns
    -------
    (df_merged, stats) : tuple[pd.DataFrame, dict]
        df_merged enthält alle Therapie-Spalten plus die gematchten
        Staging-Spalten sowie 'months_diff'.
        stats enthält n_therapy, n_staging, n_matched, n_unmatched.
    """
    plt.rcParams["font.family"] = font_family

    df_left  = df_system_therapies.copy()
    df_right = df_leistungszustand.copy()

    df_left[therapy_time_col] = pd.to_numeric(df_left[therapy_time_col],  errors="coerce")
    df_right[lz_time_col]     = pd.to_numeric(df_right[lz_time_col],      errors="coerce")

    n_before   = len(df_left)
    df_left    = df_left.dropna(subset=[therapy_time_col])
    df_right   = df_right.dropna(subset=[lz_time_col])
    n_dropped  = n_before - len(df_left)
    if n_dropped > 0:
        print(f"[INFO] {n_dropped:,} Zeilen wegen NaN in '{therapy_time_col}' entfernt")

    df_left[therapy_time_col] = df_left[therapy_time_col].astype("float64")
    df_right[lz_time_col]     = df_right[lz_time_col].astype("float64")
    df_left  = df_left.sort_values(therapy_time_col).reset_index(drop=True)
    df_right = df_right.sort_values(lz_time_col).reset_index(drop=True)

    df_merged = pd.merge_asof(
        df_left, df_right,
        left_on=therapy_time_col, right_on=lz_time_col,
        by=id_col,
        direction=direction, tolerance=tolerance,
        suffixes=("", "_lz"),
    )

    df_merged["months_diff"] = df_merged[therapy_time_col] - df_merged[lz_time_col]

    n_matched   = df_merged["months_diff"].notna().sum()
    n_unmatched = df_merged["months_diff"].isna().sum()
    print(f"  Matched   : {n_matched:,}  ({100*n_matched/len(df_merged):.1f}%)")
    print(f"  Unmatched : {n_unmatched:,}  ({100*n_unmatched/len(df_merged):.1f}%)")
    print(df_merged["months_diff"].describe().to_string())

    stats = {
        "n_therapy":   len(df_left),
        "n_staging":   len(df_right),
        "n_matched":   n_matched,
        "n_unmatched": n_unmatched,
    }

    # ── Optionaler Plot ───────────────────────────────────────────────────────
    if plot:
        data = df_merged["months_diff"].dropna()
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(data, bins=30, color="#2e6fba", edgecolor="white", linewidth=0.4)
        if log_y:
            ax.set_yscale("log")
        ax.set_xlabel("Delta (Months)", fontsize=10)
        ax.set_ylabel("Count" if not log_y else "Count (log scale)", fontsize=10)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{int(x):,}")
        )
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        if plot_title:
            ax.set_title(plot_title, fontsize=13, fontweight="bold", pad=10)
        ax.text(
            0.98, 0.97,
            f"Therapy in  :  {stats['n_therapy']:>8,}\n"
            f"Staging in  :  {stats['n_staging']:>8,}\n"
            f"Matched     :  {stats['n_matched']:>8,}\n"
            f"Unmatched   :  {stats['n_unmatched']:>8,}",
            transform=ax.transAxes,
            ha="right", va="top", fontsize=8.5,
            fontfamily="monospace",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="#cccccc"),
        )
        fig.tight_layout()
        if save_path is not None:
            Path(save_path).mkdir(parents=True, exist_ok=True)
            full_path = Path(save_path) / save_name
            fig.savefig(full_path, dpi=300, bbox_inches="tight")
            print(f"Plot gespeichert unter: {full_path.resolve()}")
        plt.show()
        plt.close(fig)

    return df_merged, stats


def sweep_tolerance_fast(
    df_system_therapies,
    df_leistungszustand,
    tolerances,
    id_col: str               = "condition_id_hash",
    therapy_time_col: str     = "months_between_asserted_therapy_start_date",
    lz_time_col: str          = "months_between_asserted_effective_dateTime",
    direction: str            = "backward",
    log_y: bool               = False,
    save_plot: bool           = False,
    save_dir: str             = "plots",
    file_name: str            = "tolerance_sweep.png",
    dpi: int                  = 300,
    title: Optional[str]      = "",
    font_family: str          = "DejaVu Sans",
    highlight_tolerance: Optional[int] = None,
) -> pd.DataFrame:
    """
    Sweept Toleranzwerte für nearest-date-Merge und plottet die Match-Rate.

    Berechnet für jede Toleranz in `tolerances`, wie viele cond_ids einen
    Staging-Eintrag innerhalb des Zeitfensters haben.

    Returns
    -------
    pd.DataFrame mit Spalten: tolerance, n_cond_ids, pct_cond_ids
    """
    plt.rcParams["font.family"] = font_family

    # ── Ausgangszahlen ────────────────────────────────────────────────────────
    n_therapies_raw = len(df_system_therapies)
    n_patients_raw  = df_system_therapies[id_col].nunique()
    n_lz_raw        = len(df_leistungszustand)

    df_left  = df_system_therapies.copy()
    df_right = df_leistungszustand.copy()

    df_left[therapy_time_col] = pd.to_numeric(df_left[therapy_time_col],  errors="coerce")
    df_right[lz_time_col]     = pd.to_numeric(df_right[lz_time_col],      errors="coerce")

    nan_left  = df_left[therapy_time_col].isna().sum()
    df_left   = df_left.dropna(subset=[therapy_time_col])
    nan_right = df_right[lz_time_col].isna().sum()
    df_right  = df_right.dropna(subset=[lz_time_col])

    n_therapies_clean = len(df_left)
    n_patients_clean  = df_left[id_col].nunique()
    n_lz_clean        = len(df_right)

    print("=" * 60)
    print("PRE-MERGE CLEANUP")
    print("=" * 60)
    print(f"  Therapy rows  :  {n_therapies_raw:>8,}  →  {n_therapies_clean:>8,}"
          f"  (dropped {nan_left:,} NaN in '{therapy_time_col}')")
    print(f"  Therapy cond  :  {n_patients_raw:>8,}  →  {n_patients_clean:>8,}"
          f"  ({n_patients_raw - n_patients_clean:,} cond_ids lost)")
    print(f"  LZ rows       :  {n_lz_raw:>8,}  →  {n_lz_clean:>8,}"
          f"  (dropped {nan_right:,} NaN in '{lz_time_col}')")
    print("=" * 60)

    df_left[therapy_time_col] = df_left[therapy_time_col].astype("float64")
    df_right[lz_time_col]     = df_right[lz_time_col].astype("float64")
    df_left  = df_left.sort_values(therapy_time_col).reset_index(drop=True)
    df_right = df_right.sort_values(lz_time_col).reset_index(drop=True)

    # ── Einmal mergen (kein Toleranz-Loop) ───────────────────────────────────
    print("Merging (no tolerance)...")
    df_merged_full = pd.merge_asof(
        df_left, df_right,
        left_on=therapy_time_col, right_on=lz_time_col,
        by=id_col, direction=direction,
        suffixes=("_x", "_y"),
    )
    df_merged_full["_abs_diff"] = (
        df_merged_full[therapy_time_col] - df_merged_full[lz_time_col]
    ).abs()

    n_no_match    = df_merged_full[lz_time_col].isna().sum()
    n_matched_any = len(df_merged_full) - n_no_match
    print(f"  Merge result  :  {len(df_merged_full):,} rows total")
    print(f"  With LZ match :  {n_matched_any:,}  ({100*n_matched_any/len(df_merged_full):.1f}%)")
    print(f"  No LZ match   :  {n_no_match:,}  ({100*n_no_match/len(df_merged_full):.1f}%)")
    print("=" * 60)

    # ── Sweep ─────────────────────────────────────────────────────────────────
    tolerances = list(tolerances)
    print(f"Sweeping {len(tolerances)} tolerances...")

    records = []
    for tol in tolerances:
        matched = df_merged_full[
            df_merged_full["_abs_diff"] <= tol
        ].dropna(subset=[lz_time_col])
        n_cond_matched = matched[id_col].nunique()
        records.append({
            "tolerance":    tol,
            "n_cond_ids":   n_cond_matched,
            "pct_cond_ids": n_cond_matched / n_patients_clean * 100,
        })

    df_sweep = pd.DataFrame(records)
    print("Sweep done.")
    print("=" * 60)

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(
        df_sweep["tolerance"], df_sweep["pct_cond_ids"],
        marker="o", markersize=4,
        color="#2e6fba", linewidth=1.8, zorder=2,
    )
    ax.set_xlabel("Tolerance (months)", fontsize=10)
    ax.set_ylabel("Matched cond_ids (%)", fontsize=10)
    ax.set_ylim(0, 105)
    ax.set_xlim(0, max(tolerances) + 0.5)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x:.0f}%")
    )
    if log_y:
        ax.set_yscale("log")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)

    if highlight_tolerance is not None:
        row = df_sweep[df_sweep["tolerance"] == highlight_tolerance]
        if not row.empty:
            hl_x = highlight_tolerance
            hl_y = row["pct_cond_ids"].values[0]
            ax.plot([hl_x, hl_x], [0, hl_y],
                    color="red", linestyle="--", linewidth=1.2, alpha=0.7, zorder=3)
            ax.plot([0, hl_x],    [hl_y, hl_y],
                    color="red", linestyle="--", linewidth=1.2, alpha=0.7, zorder=3)
            ax.scatter(hl_x, hl_y, color="red", s=80, zorder=5)
            print(f"  Highlight tol={hl_x}  →  "
                  f"{row['n_cond_ids'].values[0]:,} cond_ids  ({hl_y:.1f}%)")
        else:
            print(f"[WARNING] highlight_tolerance={highlight_tolerance} nicht in tolerances")

    ax.text(
        0.5, -0.10, f"Total therapy rows: {n_therapies_clean:,}",
        transform=ax.transAxes,
        ha="center", va="top", fontsize=7.5, color="black", style="italic",
    )
    if title:
        ax.set_title(title, fontsize=13, fontweight="bold", pad=10)

    fig.tight_layout()

    if save_plot:
        save_path = Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)
        full_path = save_path / file_name
        fig.savefig(full_path, dpi=dpi, bbox_inches="tight")
        print(f"Plot saved to: {full_path.resolve()}")

    plt.show()
    plt.close(fig)
    return df_sweep


def merge_and_plot_ecog_uicc_proximity(
    df_leistungszustand,
    df_uicc,
    id_col: str          = "condition_id_hash",
    ecog_time_col: str   = "months_between_asserted_effective_dateTime",
    uicc_time_col: str   = "months_between_asserted_uicc_tnm_date",
    ecog_cat_col: str    = "ecog_performance_status",
    uicc_cat_col: str    = "uicc_tnm",
    max_months: Optional[float] = None,
    bins: int            = 30,
    save_path: Optional[str]  = None,
    file_name_dist: str  = "ecog_uicc_distribution.tiff",
    file_name_bubble: str = "ecog_uicc_bubble.tiff",
    dpi: int             = 300,
    font_family: str     = "DejaVu Sans",
) -> pd.DataFrame:
    """
    Merged ECOG und UICC per nearest-date und erstellt zwei Plots:
      1. Histogramm der Zeitdifferenz + Scatter (ECOG-Zeit vs. UICC-Zeit)
      2. Bubble-Plot der Kategorie-Kookkurrenz (ECOG × UICC)

    Returns
    -------
    pd.DataFrame – bestes ECOG-UICC-Paar pro cond_id
    """
    plt.rcParams["font.family"] = font_family

    # ── UICC-Konstanten (lokal, da nur für diesen Plot benötigt) ─────────────
    UICC_MAPPING = {
        "0": "0", "0a": "0", "0is": "0",
        "I": "I", "IA": "I", "IA1": "I", "IA2": "I", "IA3": "I",
        "IB": "I", "IB1": "I", "IB2": "I", "IC": "I",
        "II": "II", "IIA": "II", "IIA1": "II", "IIA2": "II",
        "IIB": "II", "IIC": "II",
        "III": "III", "IIIA": "III", "IIIB": "III",
        "IIIC": "III", "IIIC1": "III",
        "IV": "IV", "IVA": "IV", "IVB": "IV", "IVC": "IV",
    }
    UICC_ORDER_MAIN = ["unknown", "0", "I", "II", "III", "IV"]
    ECOG_ORDER      = ["U", "0", "1", "2", "3", "4"]
    UICC_COLORS = {
        "unknown": "#b0b0b0", "0": "#555555", "I": "#08519c",
        "II": "#006d2c", "III": "#a63603", "IV": "#54278f",
    }

    # ── Daten vorbereiten ─────────────────────────────────────────────────────
    df_left  = df_leistungszustand.copy()
    df_right = df_uicc.copy()

    df_left[ecog_time_col]  = pd.to_numeric(df_left[ecog_time_col],  errors="coerce")
    df_right[uicc_time_col] = pd.to_numeric(df_right[uicc_time_col], errors="coerce")

    nan_left  = df_left[ecog_time_col].isna().sum()
    nan_right = df_right[uicc_time_col].isna().sum()
    df_left   = df_left.dropna(subset=[ecog_time_col])
    df_right  = df_right.dropna(subset=[uicc_time_col])

    df_left[ecog_time_col]  = df_left[ecog_time_col].astype("float64")
    df_right[uicc_time_col] = df_right[uicc_time_col].astype("float64")
    df_left  = df_left.sort_values(ecog_time_col).reset_index(drop=True)
    df_right = df_right.sort_values(uicc_time_col).reset_index(drop=True)

    n_ecog = len(df_left)
    n_uicc = len(df_right)

    print("=" * 60)
    print("ECOG × UICC PROXIMITY MERGE")
    print("=" * 60)
    print(f"  ECOG rows    :  {n_ecog:,}  (dropped {nan_left:,} NaN)")
    print(f"  UICC rows    :  {n_uicc:,}  (dropped {nan_right:,} NaN)")
    print(f"  Max tolerance:  {'no limit' if max_months is None else f'{max_months} months'}")

    df_merged = pd.merge_asof(
        df_left, df_right,
        left_on=ecog_time_col, right_on=uicc_time_col,
        by=id_col, direction="nearest",
        suffixes=("", "_uicc"),
    )
    df_merged["_abs_diff"] = (df_merged[ecog_time_col] - df_merged[uicc_time_col]).abs()

    df_matched = df_merged.dropna(subset=[uicc_time_col]).copy()
    if max_months is not None:
        df_matched = df_matched[df_matched["_abs_diff"] <= max_months]

    df_best = (
        df_matched.sort_values("_abs_diff")
        .groupby(id_col, as_index=False).first()
    )

    n_matched   = len(df_best)
    n_cond_ids  = df_best[id_col].nunique()
    n_unmatched = n_ecog - n_matched

    print(f"  Matched pairs          : {n_matched:,}  ({100*n_matched/n_ecog:.1f}%)")
    print(f"  Unique cond_ids matched: {n_cond_ids:,}")
    print(f"  Unmatched ECOG rows    : {n_unmatched:,}")
    print(f"  Median abs diff        : {df_best['_abs_diff'].median():.2f} months")
    print("=" * 60)

    tol_label = "no tolerance" if max_months is None else f"max {max_months}m"

    # ── Plot 1: Histogramm + Scatter ──────────────────────────────────────────
    fig1, (ax_hist, ax_scatter) = plt.subplots(
        1, 2, figsize=(14, 5),
        gridspec_kw={"width_ratios": [1, 1], "wspace": 0.35},
    )

    ax_hist.hist(
        df_best["_abs_diff"], bins=bins, color="#2e6fba",
        edgecolor="white", linewidth=0.4,
    )
    ax_hist.set_yscale("log")
    ax_hist.set_xlabel("Absolute Time Difference (Months)", fontsize=10)
    ax_hist.set_ylabel("Count (log scale)", fontsize=10)
    ax_hist.set_title(
        f"ECOG × UICC — Time Difference Distribution\n(nearest pair per cond_id, {tol_label})",
        fontsize=10, fontweight="bold", pad=8,
    )
    ax_hist.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax_hist.spines[["top", "right"]].set_visible(False)
    ax_hist.grid(axis="y", linestyle="--", alpha=0.35)
    ax_hist.text(
        0.98, 0.97,
        f"ECOG in        : {n_ecog:>7,}\n"
        f"UICC in        : {n_uicc:>7,}\n"
        f"Matched        : {n_matched:>7,}\n"
        f"Unmatched      : {n_unmatched:>7,}\n"
        f"Unique cond_ids: {n_cond_ids:>7,}",
        transform=ax_hist.transAxes,
        ha="right", va="top", fontsize=8, fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="#cccccc"),
    )

    ax_scatter.scatter(
        df_best[ecog_time_col], df_best[uicc_time_col],
        alpha=0.35, s=8, color="#e07b39", linewidths=0,
    )
    lim_min = min(df_best[ecog_time_col].min(), df_best[uicc_time_col].min())
    lim_max = max(df_best[ecog_time_col].max(), df_best[uicc_time_col].max())
    ax_scatter.plot(
        [lim_min, lim_max], [lim_min, lim_max],
        color="#aaaaaa", linestyle="--", linewidth=1.0, label="ECOG = UICC",
    )
    ax_scatter.set_xlabel("ECOG Date (months since diagnosis)", fontsize=10)
    ax_scatter.set_ylabel("UICC Date (months since diagnosis)", fontsize=10)
    ax_scatter.set_title(
        f"ECOG × UICC — Timing Scatter\n(nearest pair per cond_id, {tol_label})",
        fontsize=10, fontweight="bold", pad=8,
    )
    ax_scatter.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax_scatter.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax_scatter.spines[["top", "right"]].set_visible(False)
    ax_scatter.grid(axis="both", linestyle="--", alpha=0.25)
    ax_scatter.legend(fontsize=8, framealpha=0.85)

    fig1.text(
        0.5, -0.03,
        f"ECOG in = {n_ecog:,}  |  UICC in = {n_uicc:,}  |  "
        f"Matched = {n_matched:,}  |  {tol_label}",
        ha="center", fontsize=7.5, color="black", style="italic",
    )
    fig1.tight_layout()

    if save_path is not None:
        Path(save_path).mkdir(parents=True, exist_ok=True)
        fig1.savefig(Path(save_path) / file_name_dist, dpi=dpi, bbox_inches="tight")
        print(f"Plot 1 saved to: {Path(save_path) / file_name_dist}")
    plt.show()
    plt.close(fig1)

    # ── Plot 2: Bubble-Plot ───────────────────────────────────────────────────
    df_plot = df_best.copy()
    df_plot[ecog_cat_col] = (
        df_plot[ecog_cat_col].astype(str).str.strip().replace({"nan": "U", "": "U"})
    )
    df_plot[uicc_cat_col] = df_plot[uicc_cat_col].astype(str).str.strip()
    df_plot["uicc_main"]  = df_plot[uicc_cat_col].map(UICC_MAPPING).fillna("unknown")

    df_plot_filtered = df_plot[
        df_plot[ecog_cat_col].isin(ECOG_ORDER) &
        df_plot["uicc_main"].isin(UICC_ORDER_MAIN)
    ]

    ct           = pd.crosstab(df_plot_filtered[ecog_cat_col], df_plot_filtered["uicc_main"])
    ecog_present = [e for e in ECOG_ORDER      if e in ct.index]
    uicc_present = [u for u in UICC_ORDER_MAIN if u in ct.columns]
    ct           = ct.reindex(index=ecog_present, columns=uicc_present).fillna(0)

    x_vals, y_vals, counts = [], [], []
    for xi, uicc in enumerate(uicc_present):
        for yi, ecog in enumerate(ecog_present):
            val = int(ct.loc[ecog, uicc])
            if val > 0:
                x_vals.append(xi); y_vals.append(yi); counts.append(val)

    max_count    = max(counts) if counts else 1
    bubble_sizes = [max(30, (np.sqrt(c) / np.sqrt(max_count)) * 2500) for c in counts]
    bubble_colors = [UICC_COLORS[uicc_present[xi]] for xi in x_vals]

    fig2, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(
        x_vals, y_vals, s=bubble_sizes, c=bubble_colors,
        alpha=0.80, edgecolors="#444444", linewidths=0.5, zorder=3,
    )
    for xi, yi, c in zip(x_vals, y_vals, counts):
        ax.text(xi, yi, f"{int(c):,}", ha="center", va="center", fontsize=8,
                color="white" if c / max_count > 0.35 else "#222222", fontweight="bold")

    ax.set_xticks(range(len(uicc_present)))
    ax.set_xticklabels(
        ["Missing" if u in ("unknown", "missing") else f"Stage {u}" for u in uicc_present], fontsize=10
    )
    ax.set_yticks(range(len(ecog_present)))
    ax.set_yticklabels(
        ["Unknown" if e == "U" else f"ECOG {e}" for e in ecog_present], fontsize=10
    )
    ax.set_xlabel("UICC Stage", fontsize=10)
    ax.set_ylabel("ECOG Performance Status", fontsize=10)
    ax.set_title(
        f"ECOG × UICC — Category Co-occurrence\n"
        f"(nearest pair per cond_id, {tol_label}  |  n = {len(df_plot_filtered):,} pairs)",
        fontsize=10, fontweight="bold", pad=10,
    )
    ax.set_xlim(-0.7, len(uicc_present) - 0.3)
    ax.set_ylim(-0.7, len(ecog_present) - 0.3)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(linestyle="--", alpha=0.25, zorder=0)

    substage_counts = df_best[uicc_cat_col].astype(str).str.strip().value_counts()
    substage_map = {}
    for sub, main in UICC_MAPPING.items():
        if sub in substage_counts.index and substage_counts[sub] > 0:
            substage_map.setdefault(main, []).append(f"{sub}({substage_counts[sub]:,})")

    unknown_count  = int((~df_best[uicc_cat_col].astype(str).str.strip()
                          .isin(UICC_MAPPING.keys())).sum())
    legend_handles = []

    if "unknown" in uicc_present:
        legend_handles.append(
            mpatches.Patch(color=UICC_COLORS["unknown"], label=f"unknown  ({unknown_count:,})")
        )
    for main in [u for u in uicc_present if u != "unknown"]:
        subs   = substage_map.get(main, [])
        chunks = [subs[i:i + 4] for i in range(0, len(subs), 4)]
        label  = f"Stage {main}:\n" + "\n".join("  " + ",  ".join(c) for c in chunks)
        legend_handles.append(mpatches.Patch(color=UICC_COLORS[main], label=label))

    ax.legend(
        handles=legend_handles,
        title="Sub-stage  (n)", title_fontsize=8.5, fontsize=7,
        loc="upper left", bbox_to_anchor=(1.02, 1.0),
        framealpha=0.95, edgecolor="#cccccc",
        handlelength=1.0, labelspacing=0.8,
    )
    fig2.tight_layout()

    if save_path is not None:
        fig2.savefig(Path(save_path) / file_name_bubble, dpi=dpi, bbox_inches="tight")
        print(f"Plot 2 saved to: {Path(save_path) / file_name_bubble}")
    plt.show()
    plt.close(fig2)

    return df_best
