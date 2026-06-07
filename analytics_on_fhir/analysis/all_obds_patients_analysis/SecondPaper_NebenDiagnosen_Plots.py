"""
SecondPaper_NebenDiagnosen_Plots.py
=====================================
Combined module: plot library  +  main data loading / execution
for the Nebendiagnosen analysis (2nd Paper).

Plot types produced per level × mode × filter-set:
  1. Top-N horizontal bar chart
  2. Lorenz curve
  3. Log-histogram  (patients with 0 diagnoses included when
                     all_patient_ids is supplied → bar at 0 visible)

Additional plots (when icd_nebendiagnosen_einteilung is supplied):
  4. Ebene_1_Analytische_Rolle  distribution bar chart – all categories
  5. Ebene_2_Klinische_Domaene  distribution bar chart – all categories
     → both in modes "all" (every event) and "unique" (distinct per patient)

Filter sets applied automatically:
  • "standard"         – full df_conditions
  • "core_comorbidity" – rows where In_Core_Comorbidity_Analysis == "Ja"

Counting modes:
  • "all"    → every diagnosis row counts (incl. duplicates per patient)
  • "unique" → each (patient, code) pair counted at most once

Usage (standalone)
------------------
  Run this file directly.  All paths are configured in the
  ── CONFIGURATION ── block near the bottom.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── typing ────────────────────────────────────────────────────────────────────

Mode = Literal["all", "unique"]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 – SMALL UTILITIES
# ══════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class LevelConfig:
    """
    One ICD resolution level.

    name         – short internal key used in file names
    column       – column name in the DataFrame
    display_name – human-readable label for plot titles / axes
    """
    name: str
    column: str
    display_name: str


def _fmt(n: int | float) -> str:
    """Integer → German thousands separator (e.g. 1.234.567)."""
    return f"{int(n):,}".replace(",", ".")


def _truncate(text: object, max_len: int = 55) -> str:
    if pd.isna(text):
        return "Fehlend"
    s = str(text)
    return s if len(s) <= max_len else s[: max_len - 1] + "…"


def _savefig(
    fig: plt.Figure,
    output_dir: Optional[Path],
    fname: str,
    show: bool,
) -> None:
    fig.tight_layout()
    if output_dir and fname:
        output_dir.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_dir / fname, dpi=150, bbox_inches="tight")
    if show:
        plt.show()
    else:
        plt.close(fig)


def _mode_label(mode: Mode) -> str:
    return (
        "All events (incl. duplicates per patient)"
        if mode == "all"
        else "Unique ICD codes per patient"
    )


def _context_line(
    cohort: str,
    n_patients: int,
    n_rows: int,
    level: LevelConfig,
    n_categories: int,
    mode: Mode,
) -> str:
    return (
        f"Cohort: {cohort}  |  "
        f"Patients: {_fmt(n_patients)}  |  "
        f"Diagnosis rows: {_fmt(n_rows)}  |  "
        f"Level: {level.display_name}  |  "
        f"Categories: {_fmt(n_categories)}  |  "
        f"Counting: {_mode_label(mode)}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 – DATA PREPARATION
# ══════════════════════════════════════════════════════════════════════════════


def _prepare_counts(
    df: pd.DataFrame,
    patient_col: str,
    level: LevelConfig,
    mode: Mode,
) -> pd.DataFrame:
    """
    Return [level.column, 'count'] sorted descending.

    mode='all'    → count every row per category
    mode='unique' → count distinct patients per category
    """
    work = df.dropna(subset=[level.column]).copy()
    work[level.column] = work[level.column].astype(str)

    if mode == "all":
        counts = work.groupby(level.column).size().rename("count").reset_index()
    else:
        counts = (
            work.drop_duplicates(subset=[patient_col, level.column])
            .groupby(level.column)
            .size()
            .rename("count")
            .reset_index()
        )
    return counts.sort_values("count", ascending=False)


def _per_patient_series(
    df: pd.DataFrame,
    patient_col: str,
    level: LevelConfig,
    mode: Mode,
) -> pd.Series:
    """
    Series indexed by patient with event count (mode='all')
    or unique-code count (mode='unique').
    Patients with 0 diagnoses are NOT included here; they are added
    by plot_log_histogram when all_patient_ids is supplied.
    """
    work = df.dropna(subset=[level.column]).copy()
    work[level.column] = work[level.column].astype(str)
    if mode == "unique":
        work = work.drop_duplicates(subset=[patient_col, level.column])
    return work.groupby(patient_col)[level.column].count().rename("n_events")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 – INDIVIDUAL PLOT FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def plot_top_bar(
    df: pd.DataFrame,
    patient_col: str,
    level: LevelConfig,
    cohort_name: str,
    mode: Mode = "all",
    top_n: Optional[int] = 20,
    output_dir: Optional[Path] = None,
    show: bool = True,
    fname_suffix: str = "",
    show_title: bool = True,
) -> pd.DataFrame:
    """
    Horizontal bar chart.

    top_n=None  → show all categories (used for Ebene distribution plots).
    top_n=N     → show only the N most frequent categories.

    Returns the plotted DataFrame for downstream use.
    """
    counts = _prepare_counts(df, patient_col, level, mode)
    top = counts.head(top_n).copy() if top_n else counts.copy()
    top["label"] = top[level.column].apply(_truncate)

    n_patients  = df[patient_col].nunique()
    n_rows      = len(df.dropna(subset=[level.column]))
    n_cats      = counts[level.column].nunique()
    n_shown     = len(top)
    xlabel      = "No. of diagnosis rows" if mode == "all" else "No. of patients"
    title_pfx   = f"Top {n_shown}" if top_n else "All categories"

    fig, ax = plt.subplots(figsize=(12, max(6, n_shown * 0.45)))
    bars = ax.barh(
        list(range(n_shown)),
        top["count"].values[::-1],
        color="#2e6fba", edgecolor="white", linewidth=0.4,
    )

    for bar, val in zip(bars, top["count"].values[::-1]):
        ax.text(
            bar.get_width() + counts["count"].max() * 0.005,
            bar.get_y() + bar.get_height() / 2,
            _fmt(val),
            va="center", ha="left", fontsize=8, color="#333333",
        )

    ax.set_yticks(list(range(n_shown)))
    ax.set_yticklabels(top["label"].values[::-1], fontsize=9)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(level.display_name, fontsize=10)
    if show_title:
        ax.set_title(
            f"{title_pfx} most frequent categories – {level.display_name}",
            fontsize=13, fontweight="bold", pad=14,
        )
    ax.text(
        0.5, -0.10,
        _context_line(cohort_name, n_patients, n_rows, level, n_cats, mode),
        transform=ax.transAxes,
        ha="center", va="top", fontsize=7.5, color="#555555", style="italic",
    )
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt(x)))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    top_tag  = f"top{top_n}" if top_n else "all_cats"
    sfx      = f"_{fname_suffix}" if fname_suffix else ""
    _savefig(fig, output_dir, f"{level.name}_{mode}_{top_tag}_bar{sfx}.png", show)
    return top


def _gini(values: np.ndarray) -> float:
    v = np.sort(values[values >= 0].astype(float))
    if v.size == 0 or v.sum() == 0:
        return 0.0
    n   = v.size
    cum = np.cumsum(v)
    return (n + 1 - 2 * np.sum(cum) / cum[-1]) / n


def plot_lorenz_curve(
    df: pd.DataFrame,
    patient_col: str,
    level: LevelConfig,
    cohort_name: str,
    mode: Mode = "all",
    output_dir: Optional[Path] = None,
    show: bool = True,
    fname_suffix: str = "",
    all_patient_ids: Optional[pd.Index] = None,
    show_title: bool = True,
) -> None:
    """
    Lorenz curve: cumulative share of patients vs. cumulative share of
    diagnosis events.  The further the curve bows from the diagonal,
    the more concentrated the burden is among few patients.

    all_patient_ids
        If provided, patients with 0 diagnoses are included as zeros in the
        distribution.  This correctly shifts the curve right and raises the
        Gini coefficient to reflect true inequality across the full cohort.
    """
    per_patient = _per_patient_series(df, patient_col, level, mode)

    if all_patient_ids is not None:
        per_patient = per_patient.reindex(all_patient_ids, fill_value=0)

    values = np.sort(per_patient.dropna().to_numpy(dtype=float))

    if values.size == 0:
        print(f"[WARNING] No data for Lorenz curve: {level.name} / {mode}")
        return

    cum_vals = np.cumsum(values)
    cum_vals = np.insert(cum_vals, 0, 0)
    if cum_vals[-1] > 0:
        cum_vals = cum_vals / cum_vals[-1]
    cum_pop = np.linspace(0, 1, len(cum_vals))
    gini    = _gini(values)

    n_patients = len(values) if all_patient_ids is not None else df[patient_col].nunique()
    n_rows     = len(df.dropna(subset=[level.column]))
    n_cats     = df[level.column].nunique()

    fig, ax = plt.subplots(figsize=(7, 6.5))
    ax.plot(cum_pop, cum_vals, color="#2e6fba", linewidth=2.2, label="Lorenz curve")
    ax.fill_between(cum_pop, cum_vals, cum_pop, alpha=0.12, color="#2e6fba")
    ax.plot([0, 1], [0, 1], linestyle="--", color="#888888", linewidth=1.2, label="Perfect equality")
    ax.text(
        0.05, 0.93, f"Gini = {gini:.3f}",
        transform=ax.transAxes, fontsize=11, fontweight="bold", color="#2e6fba",
    )
    ax.set_xlabel("Cumulative share of patients (sorted ascending)", fontsize=10)
    ax.set_ylabel("Cumulative share of diagnosis events", fontsize=10)
    if show_title:
        ax.set_title(f"Lorenz Curve – {level.display_name}", fontsize=13, fontweight="bold", pad=14)
    ax.text(
        0.5, -0.12,
        _context_line(cohort_name, n_patients, n_rows, level, n_cats, mode),
        transform=ax.transAxes,
        ha="center", va="top", fontsize=7.5, color="#555555", style="italic",
    )
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)

    sfx = f"_{fname_suffix}" if fname_suffix else ""
    _savefig(fig, output_dir, f"{level.name}_{mode}_lorenz{sfx}.png", show)


def plot_log_histogram(
    df: pd.DataFrame,
    patient_col: str,
    level: LevelConfig,
    cohort_name: str,
    mode: Mode = "all",
    output_dir: Optional[Path] = None,
    show: bool = True,
    bins: int = 60,
    all_patient_ids: Optional[pd.Index | pd.Series] = None,
    fname_suffix: str = "",
    show_title: bool = True,
) -> None:
    """
    Log-y histogram of diagnosis events per patient.

    all_patient_ids
        If provided, every patient ID that is NOT present in df (i.e. has
        zero matching diagnoses) is added with count = 0.  This makes the
        leftmost bar at x=0 visible and answers "how many patients have
        no diagnosis at all in this cohort?".
    """
    per_patient = _per_patient_series(df, patient_col, level, mode)

    if all_patient_ids is not None:
        # Reindex to the full patient universe; missing → 0
        per_patient = per_patient.reindex(all_patient_ids, fill_value=0)

    series = per_patient.dropna()
    if series.empty:
        print(f"[WARNING] No data for histogram: {level.name} / {mode}")
        return

    n_patients = df[patient_col].nunique()
    n_rows     = len(df.dropna(subset=[level.column]))
    n_cats     = df[level.column].nunique()

    xlabel = (
        f"Diagnosis events per patient – {level.display_name}"
        if mode == "all"
        else f"Unique {level.display_name} codes per patient"
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(series, bins=bins, color="#2e6fba", edgecolor="white", linewidth=0.3)
    ax.set_yscale("log")
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel("No. of patients (log scale)", fontsize=10)
    if show_title:
        ax.set_title(
            f"Distribution of diagnosis events per patient – {level.display_name}",
            fontsize=13, fontweight="bold", pad=14,
        )

    # ── stat box ─────────────────────────────────────────────────────────────
    if all_patient_ids is not None:
        n_with    = int((series > 0).sum())
        n_without = int((series == 0).sum())
        pos       = series[series > 0]
        stat_txt  = (
            f"Total: {_fmt(len(series))}  |  "
            f"With ≥1 diagnosis: {_fmt(n_with)}  |  "
            f"Without diagnosis: {_fmt(n_without)}\n"
            f"(Patients with ≥1)  "
            f"Median={pos.median():.0f}  "
            f"Mean={pos.mean():.1f}  "
            f"P90={pos.quantile(0.90):.0f}  "
            f"Max={pos.max():.0f}"
        )
    else:
        stat_txt = (
            f"n={_fmt(len(series))}  "
            f"Median={series.median():.0f}  "
            f"Mean={series.mean():.1f}  "
            f"P90={series.quantile(0.90):.0f}  "
            f"Max={series.max():.0f}"
        )

    ax.text(
        0.98, 0.97, stat_txt,
        transform=ax.transAxes,
        ha="right", va="top", fontsize=8.5,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#cccccc"),
    )
    ax.text(
        0.5, -0.12,
        _context_line(cohort_name, n_patients, n_rows, level, n_cats, mode),
        transform=ax.transAxes,
        ha="center", va="top", fontsize=7.5, color="#555555", style="italic",
    )
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt(x)))
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt(x)))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)

    sfx = f"_{fname_suffix}" if fname_suffix else ""
    _savefig(fig, output_dir, f"{level.name}_{mode}_log_hist{sfx}.png", show)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 – EBENE DISTRIBUTION PLOTS
# ══════════════════════════════════════════════════════════════════════════════

_EBENE_COLS: dict[str, str] = {
    "Ebene_1_Analytische_Rolle": "Analytical Role (Level 1)",
    "Ebene_2_Klinische_Domaene": "Clinical Domain (Level 2)",
}


def _merge_ebene_columns(
    df: pd.DataFrame,
    icd_mapping_df: pd.DataFrame,
    icd_code_col_in_df: str,
    icd_code_col_in_mapping: str,
) -> pd.DataFrame:
    """
    Left-join Ebene_1 / Ebene_2 from icd_mapping_df into df.
    Deduplicates the mapping on ICD code before joining.
    """
    mapping_subset = (
        icd_mapping_df[[icd_code_col_in_mapping] + list(_EBENE_COLS.keys())]
        .drop_duplicates(subset=[icd_code_col_in_mapping])
    )
    return df.merge(
        mapping_subset,
        left_on=icd_code_col_in_df,
        right_on=icd_code_col_in_mapping,
        how="left",
    )


def run_ebene_distribution_plots(
    df: pd.DataFrame,
    patient_col: str,
    icd_mapping_df: pd.DataFrame,
    cohort_name: str,
    icd_code_col_in_df: str = "icd_code",
    icd_code_col_in_mapping: str = "ICD_Code",
    modes: list[Mode] = ("all", "unique"),
    output_dir: Optional[Path] = None,
    show: bool = True,
) -> tuple[dict, pd.DataFrame]:
    """
    Produce Ebene_1 and Ebene_2 distribution bar charts (all categories shown)
    for each requested mode.

    Returns (results_dict, df_with_ebene_columns).
    """
    df_merged = _merge_ebene_columns(
        df, icd_mapping_df, icd_code_col_in_df, icd_code_col_in_mapping
    )
    results: dict = {}

    for ebene_col, ebene_display in _EBENE_COLS.items():
        results[ebene_col] = {}
        level = LevelConfig(name=ebene_col, column=ebene_col, display_name=ebene_display)

        for mode in modes:
            print(f"  [Ebene-Plot] {ebene_col} / {_mode_label(mode)}")
            results[ebene_col][mode] = plot_top_bar(
                df=df_merged,
                patient_col=patient_col,
                level=level,
                cohort_name=cohort_name,
                mode=mode,
                top_n=None,          # show ALL categories
                output_dir=output_dir,
                show=show,
                fname_suffix="ebene",
            )

    return results, df_merged


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 – INTERNAL PLOT-BLOCK RUNNER
# ══════════════════════════════════════════════════════════════════════════════


def _run_plot_block(
    df: pd.DataFrame,
    patient_col: str,
    level_configs: list[LevelConfig],
    cohort_name: str,
    modes: list[Mode],
    top_n: int,
    output_dir: Optional[Path],
    show: bool,
    all_patient_ids: Optional[pd.Index | pd.Series],
    fname_suffix: str,
    results_dict: dict,
    block_label: str,
    show_title: bool = True,
) -> None:
    """
    Internal helper: run bar + lorenz + histogram for every level × mode.
    Populates results_dict in place.
    """
    for level in level_configs:
        results_dict[level.name] = {}
        n_cats = df[level.column].nunique(dropna=True)
        print(
            f"\n─── [{block_label}] {level.display_name}  "
            f"({level.column})  –  {_fmt(n_cats)} Kategorien ───"
        )

        for mode in modes:
            print(f"  Zählweise: {_mode_label(mode)}")

            top = plot_top_bar(
                df=df,
                patient_col=patient_col,
                level=level,
                cohort_name=cohort_name,
                mode=mode,
                top_n=top_n,
                output_dir=output_dir,
                show=show,
                fname_suffix=fname_suffix,
                show_title=show_title,
            )
            plot_lorenz_curve(
                df=df,
                patient_col=patient_col,
                level=level,
                cohort_name=cohort_name,
                mode=mode,
                output_dir=output_dir,
                show=show,
                fname_suffix=fname_suffix,
                all_patient_ids=all_patient_ids,
                show_title=show_title,
            )
            plot_log_histogram(
                df=df,
                patient_col=patient_col,
                level=level,
                cohort_name=cohort_name,
                mode=mode,
                output_dir=output_dir,
                show=show,
                all_patient_ids=all_patient_ids,
                fname_suffix=fname_suffix,
                show_title=show_title,
            )

            results_dict[level.name][mode] = {"top": top}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 – README HELPER + PUBLIC RUNNER
# ══════════════════════════════════════════════════════════════════════════════

_README_TEMPLATE = """\
# Nebendiagnosen – Ergebnisordner

Generated by `SecondPaper_NebenDiagnosen_Plots.py`
Kohorte  : {cohort_name}
Ebenen   : {levels}
Modi     : {modes}
Top-N    : {top_n}

---

## File naming scheme

```
{{Ebene}}_{{Modus}}_{{Plottyp}}[_{{Filter}}].png
```

---

## 1 · Level  —  ICD resolution

| Key                          | Meaning                                                 |
|------------------------------|---------------------------------------------------------|
| ICD_NAME                     | Full ICD code incl. sub-code (e.g. I10.00)              |
| ICD_BASE_NAME                | Basiskode 3-stellig (z. B. I10)                         |
| group_full                   | ICD group (e.g. I10-I15: Hypertension)                  |
| chapter_full                 | ICD chapter (e.g. IX: Diseases of the circulatory system)|
| Ebene_1_Analytische_Rolle    | Analytische Rolle gem. Klassifikationstabelle           |
| Ebene_2_Klinische_Domaene    | Klinische Domäne gem. Klassifikationstabelle            |

---

## 2 · Mode  —  Counting method

| Key     | Meaning                                                                        |
|---------|--------------------------------------------------------------------------------|
| all     | Every diagnosis row is counted individually (incl. duplicates per patient)       |
| unique  | Each (patient, code) combination is counted at most once (unique ICD codes per patient) |

---

## 3 · Plot type

| Key           | Plot type                  | Content                                                                                                  |
|---------------|----------------------------|----------------------------------------------------------------------------------------------------------|
| top{top_n}_bar   | Horizontal bar chart        | The {top_n} most frequent categories with absolute counts                                                |
| all_cats_bar  | Horizontal bar chart        | All categories shown (Ebene_1 / Ebene_2 only)                                                            |
| lorenz        | Lorenz curve               | Cumulative share of patients vs. cumulative share of diagnosis events; Gini coefficient shown in plot    |
| log_hist      | Log histogram              | Distribution of diagnosis events per patient (y-axis log scale); patients with 0 diagnoses shown at x=0 |

---

## 4 · Filter suffix  —  Dataset restriction

| Suffix        | Meaning                                                                                                |
|---------------|--------------------------------------------------------------------------------------------------------|
| (no suffix)   | Standard – all Nebendiagnose codes after C-code exclusion                                               |
| _core         | Core Comorbidity – only codes with In_Core_Comorbidity_Analysis == "Ja" in the classification table    |
| _ebene        | Level distribution plot – classification by analytical role / clinical domain (all categories shown)    |

---

## Vollständige Dateiliste

### Standard plots  (all codes)

{standard_files}

### Core comorbidity plots  (suffix _core)

Same plots as above, but restricted to codes with In_Core_Comorbidity_Analysis == "Ja":

{core_files}

### Level distribution plots  (suffix _ebene)

{ebene_files}

### Exported raw data

```
ICD_NAME_NebenDiagnosen_value_counts_INKL_ICD_D_Diagnosen.xlsx
```
Absolute frequencies of all full ICD codes before tumour cohort filter,
including D-diagnoses.

---

## Classification source

`Nebendiagnosen_Zuordnung_Ebenen_Domaenen_v1.xlsx`, Sheet `Code_Zuordnung`

| Column                      | Usage                                                                  |
|-----------------------------|------------------------------------------------------------------------|
| ICD_Code                    | Join key onto icd_code in the Conditions table                         |
| Ebene_1_Analytische_Rolle   | Grouping for Level-1 distribution plots                                |
| Ebene_2_Klinische_Domaene   | Grouping for Level-2 distribution plots                                |
| In_Core_Comorbidity_Analysis| Ja / Nein / Eingeschränkt  →  Core filter uses only Ja                |

---

## Notes on the log histogram

The log histogram also shows patients with 0 Nebendiagnosen (bar at x = 0).
The stat box in the plot contains:

- Total           – all patients from df_conditions_gesamt before tumour cohort filter
- With >= 1 diagnosis – patients with at least one match after all filters
- Without diagnosis  – difference (patients without a single matching code)
"""


def _write_readme(
    output_dir: Path,
    cohort_name: str,
    level_configs: list[LevelConfig],
    modes: list[Mode],
    top_n: int,
) -> None:
    """Write README_Nebendiagnosen_Plots.md into output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)

    level_names  = [l.name for l in level_configs]
    ebene_levels = list(_EBENE_COLS.keys())

    def _file_block(levels: list[str], suffix: str) -> str:
        lines = []
        for lv in levels:
            for mode in modes:
                for ptype in [f"top{top_n}_bar", "lorenz", "log_hist"]:
                    sfx = f"_{suffix}" if suffix else ""
                    lines.append(f"  {lv}_{mode}_{ptype}{sfx}.png")
        return "```\n" + "\n".join(lines) + "\n```"

    def _ebene_block() -> str:
        lines = []
        for lv in ebene_levels:
            for mode in modes:
                lines.append(f"  {lv}_{mode}_all_cats_bar_ebene.png")
        return "```\n" + "\n".join(lines) + "\n```"

    content = _README_TEMPLATE.format(
        cohort_name    = cohort_name,
        levels         = ", ".join(f"{l.name} ({l.display_name})" for l in level_configs),
        modes          = ", ".join(modes),
        top_n          = top_n,
        standard_files = _file_block(level_names, ""),
        core_files     = _file_block(level_names, "core"),
        ebene_files    = _ebene_block(),
    )

    readme_path = output_dir / "README_Nebendiagnosen_Plots.md"
    readme_path.write_text(content, encoding="utf-8")
    print(f"  README geschrieben → {readme_path}")





def run_nebendiagnosen_report(
    df: pd.DataFrame,
    patient_col: str,
    level_configs: list[LevelConfig],
    cohort_name: str = "Datensatz",
    modes: list[Mode] = ("all", "unique"),
    top_n: int = 20,
    output_dir: Optional[Path] = None,
    show: bool = True,
    # ── new parameters ────────────────────────────────────────────────────────
    icd_nebendiagnosen_einteilung: Optional[pd.DataFrame] = None,
    icd_code_col_in_df: str = "icd_code",
    icd_code_col_in_mapping: str = "ICD_Code",
    all_patient_ids: Optional[pd.Index | pd.Series] = None,
    show_title: bool = True,
) -> dict:
    """
    Master report runner.

    Standard plots (levels × modes)
    ─────────────────────────────────
    For every LevelConfig × mode:
      1. Top-N horizontal bar chart
      2. Lorenz curve
      3. Log-histogram
         → if all_patient_ids is given, patients with 0 diagnoses appear
           as the leftmost bar (x = 0)

    Core Comorbidity filter  [requires icd_nebendiagnosen_einteilung]
    ─────────────────────────
    All three standard plots are repeated on the subset of df whose
    icd_code is flagged In_Core_Comorbidity_Analysis == "Ja".
    File names get a "_core" suffix so they do not overwrite the standard set.

    Ebene distribution plots  [requires icd_nebendiagnosen_einteilung]
    ──────────────────────────
    Horizontal bar charts (all categories) for:
      • Ebene_1_Analytische_Rolle
      • Ebene_2_Klinische_Domaene
    × modes "all" and "unique".

    Parameters
    ----------
    df                           : one diagnosis row per row
    patient_col                  : patient identifier column
    level_configs                : ICD resolution levels to iterate over
    cohort_name                  : label printed in every plot
    modes                        : counting modes to run
    top_n                        : categories shown in bar charts
    output_dir                   : directory for PNG output (None → no save)
    show                         : call plt.show() after each plot
    icd_nebendiagnosen_einteilung: lookup table with Ebene / Core columns
    icd_code_col_in_df           : ICD code column name in df
    icd_code_col_in_mapping      : ICD code column name in the mapping table
    all_patient_ids              : full patient universe (incl. 0-diagnosis pats)

    Returns
    -------
    Nested dict  results[block][level.name][mode] = {"top": DataFrame, ...}
    """
    results: dict = {}

    n_total_patients = df[patient_col].nunique()
    n_total_rows     = len(df)
    print(
        f"\n{'=' * 65}\n"
        f"  Kohorte  : {cohort_name}\n"
        f"  Patienten (in Conditions): {_fmt(n_total_patients)}\n"
        f"  Patienten (Universum)    : "
        f"{_fmt(len(all_patient_ids)) if all_patient_ids is not None else 'nicht angegeben'}\n"
        f"  Diagnose-Zeilen : {_fmt(n_total_rows)}\n"
        f"  Ebenen          : {[l.display_name for l in level_configs]}\n"
        f"  Modi            : {list(modes)}\n"
        f"{'=' * 65}"
    )

    # ── README in Ausgabeordner schreiben ────────────────────────────────────
    if output_dir:
        _write_readme(output_dir, cohort_name, level_configs, modes, top_n)

    # ── A) Standard plots ────────────────────────────────────────────────────
    print("\n▶  Standard-Plots (alle Codes)")
    results["standard"] = {}
    _run_plot_block(
        df=df,
        patient_col=patient_col,
        level_configs=level_configs,
        cohort_name=cohort_name,
        modes=modes,
        top_n=top_n,
        output_dir=output_dir,
        show=show,
        all_patient_ids=all_patient_ids,
        fname_suffix="",
        results_dict=results["standard"],
        block_label="standard",
        show_title=show_title,
    )

    # ── B) Core Comorbidity filtered plots ────────────────────────────────────
    if icd_nebendiagnosen_einteilung is not None:
        core_codes = set(
            icd_nebendiagnosen_einteilung.loc[
                icd_nebendiagnosen_einteilung["In_Core_Comorbidity_Analysis"] == "Ja",
                icd_code_col_in_mapping,
            ]
        )
        df_core  = df[df[icd_code_col_in_df].isin(core_codes)].copy()
        n_core   = len(df_core)
        n_kept_pct = 100 * n_core / n_total_rows if n_total_rows else 0
        print(
            f"\n▶  Core-Comorbidity-Filter:\n"
            f"   {_fmt(n_core)} / {_fmt(n_total_rows)} Zeilen behalten "
            f"({n_kept_pct:.1f} %)"
        )

        results["core_comorbidity"] = {}
        _run_plot_block(
            df=df_core,
            patient_col=patient_col,
            level_configs=level_configs,
            cohort_name=f"{cohort_name} [Core Comorbidity]",
            modes=modes,
            top_n=top_n,
            output_dir=output_dir,
            show=show,
            all_patient_ids=all_patient_ids,
            fname_suffix="core",
            results_dict=results["core_comorbidity"],
            block_label="core_comorbidity",
        )

    # ── C) Ebene distribution plots ───────────────────────────────────────────
    if icd_nebendiagnosen_einteilung is not None:
        print(f"\n▶  Ebene-Verteilungsplots")
        ebene_results, _ = run_ebene_distribution_plots(
            df=df,
            patient_col=patient_col,
            icd_mapping_df=icd_nebendiagnosen_einteilung,
            cohort_name=cohort_name,
            icd_code_col_in_df=icd_code_col_in_df,
            icd_code_col_in_mapping=icd_code_col_in_mapping,
            modes=modes,
            output_dir=output_dir,
            show=show,
        )
        results["ebene_distribution"] = ebene_results

    print(f"\n{'=' * 65}\n  Report abgeschlossen.\n{'=' * 65}")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 – MAIN SCRIPT
# ══════════════════════════════════════════════════════════════════════════════
# Pfade und Einstellungen sind identisch zu main_one_path.py strukturiert.
# Imports die nur für den Standalone-Betrieb nötig sind, liegen im if-Block.

if __name__ == "__main__":

    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))

    import matplotlib
    matplotlib.use("Agg")

    import icd10gm2026_hierarchy_fast_helper

    # ── PFADE  (identisch zu main_one_path.py) ────────────────────────────────

    DATA  = r"C:\Users\boehnesn1\Desktop\Projects\BZKF\Data\all_obds_patients_11.05.26\parquet"
    PLOTS = Path(r"C:\Users\boehnesn1\Desktop\Projects\BZKF\2ndPaper_refactored\Plots2ndPaper_simplified")

    # Nebendiagnosen-Plots landen im selben Basisordner wie die übrigen Plots
    OUTPUT_DIR = PLOTS / "Nebendiagnosen"

    ICD_MAPPING_PATH  = (
        r"C:\Users\boehnesn1\Desktop\Projects\BZKF"
        r"\ZweitKarzinom\Resources\parquet_files_steven\DWH_ICD_CODE_MAPPING.parquet"
    )
    ICD_HIERARCHY_CSV = (
        r"C:\Users\boehnesn1\Desktop\Projects\BZKF"
        r"\ZweitKarzinom\Resources\icd10gm2026_basecode_lookup.csv"
    )

    COHORT_NAME    = "Alle-Diagnosen-UKer"
    PATIENT_COL    = "condition_patient_reference_hash"
    TOP_N          = 20
    TOP_N_ENTITIES = 20

    # ── ICD-MAPPING LADEN ─────────────────────────────────────────────────────

    _icd_lookup = (
        pd.read_parquet(ICD_MAPPING_PATH)[["ICD3_CODE", "ICD3_NAME"]]
        .drop_duplicates()
    )
    icd_lookup_dwh = pd.read_parquet(ICD_MAPPING_PATH)
    icd_dict       = icd_lookup_dwh.set_index("ICD_CODE")["ICD_NAME"].to_dict()
    icd_dict_base  = icd_lookup_dwh.set_index("ICD3_CODE")["ICD3_NAME"].to_dict()

    lookup_df_icd = icd10gm2026_hierarchy_fast_helper.load_icd_hierarchy_lookup(
        ICD_HIERARCHY_CSV
    )

    # ── NEBENDIAGNOSEN-EINTEILUNG LADEN ───────────────────────────────────────

    icd_nebendiagnosen_einteilung = pd.read_excel(
        os.path.join(DATA, "Nebendiagnosen_Zuordnung_Ebenen_Domaenen_v1.xlsx"),
        sheet_name="Code_Zuordnung",
    )

    # ── TUMORKOHORTE LADEN & FILTERN  (identisch zu main_one_path.py) ─────────

    df_tumore = pd.read_parquet(
        os.path.join(DATA, "histology_entity_recoding_df_all_obds_with_entity_final.parquet")
    )

    df_tumore = df_tumore[~df_tumore["icd10_parent_code"].str.contains("D", na=False)]
    df_tumore = df_tumore[
        ~df_tumore[["icd10_parent_code"]]
        .astype(str).apply(lambda s: s.str.contains("C44", na=False)).any(axis=1)
    ]
    df_tumore = df_tumore.merge(
        _icd_lookup, left_on="icd10_parent_code", right_on="ICD3_CODE", how="left"
    )
    df_tumore = df_tumore[
        df_tumore["entity_or_parent"].isin(
            df_tumore["entity_or_parent"].value_counts().iloc[:TOP_N_ENTITIES].index
        )
    ]

    cond_ids_gk = df_tumore["condition_id_hash"]
    print(f"\n[GK] Grundkohorte: {cond_ids_gk.nunique():,} cond_ids")

    # ── CONDITIONS (NEBENDIAGNOSEN) LADEN ─────────────────────────────────────

    df_conditions_gesamt = pd.read_parquet(
        os.path.join(DATA, "df_mii_conditions_all_obds_pats_asserted_deidentified.parquet")
    )

    # C-Codes entfernen (Tumordiagnosen); D-Codes bleiben
    df_conditions_gesamt = df_conditions_gesamt[
        ~df_conditions_gesamt["icd_code"].str.contains("C", na=False)
    ]

    # Full patient universe from the tumour cohort (patient-level, not cond_id-level).
    # Nebendiagnosen have no condition_id → we work with patient_resource_id_hash.
    # Captured BEFORE the entity filter so patients with 0 Nebendiagnosen
    # appear as the leftmost bar (x=0) in the log-histogram and are included
    # in the Lorenz curve.
    all_patient_ids_universe = pd.Index(
        df_tumore["patient_resource_id_hash"].unique()
    )

    # ICD-Codes auf Basiskode, Gruppe, Kapitel mappen
    df_conditions_mapped = icd10gm2026_hierarchy_fast_helper.map_icd_dataframe_fast(
        df=df_conditions_gesamt,
        lookup_df=lookup_df_icd,
        code_col="icd_code",
    )
    df_conditions_mapped["icd_basecode"]  = df_conditions_mapped["icd_code"].apply(
        lambda x: x.split(".")[0] if pd.notna(x) else x
    )
    df_conditions_mapped["ICD_NAME"]      = df_conditions_mapped["icd_code"].map(icd_dict)
    df_conditions_mapped["ICD_BASE_NAME"] = df_conditions_mapped["icd_basecode"].map(icd_dict_base)
    df_conditions_mapped["group_full"]    = (
        df_conditions_mapped["group_range"] + ": " + df_conditions_mapped["group_title"]
    )
    df_conditions_mapped["chapter_full"]  = (
        df_conditions_mapped["chapter_range"] + ": " + df_conditions_mapped["chapter_title"]
    )

    # Auf Patienten der Tumorkohorte einschränken
    df_conditions_mapped = df_conditions_mapped[
        df_conditions_mapped[PATIENT_COL].isin(df_tumore["patient_resource_id_hash"])
    ]

    print(
        f"[Nebendiagnosen] {len(df_conditions_mapped):,} Zeilen  |  "
        f"{df_conditions_mapped[PATIENT_COL].nunique():,} Patienten mit ≥1 Nebendiagnose  |  "
        f"Patients without any Nebendiagnose: "
        f"{len(all_patient_ids_universe) - df_conditions_mapped[PATIENT_COL].nunique():,}"
    )

    # ── OPTIONAL: ROHE VALUE COUNTS EXPORTIEREN ───────────────────────────────

    df_conditions_mapped["ICD_NAME"].value_counts().to_excel(
        PLOTS / "ICD_NAME_NebenDiagnosen_value_counts_INKL_ICD_D_Diagnosen.xlsx"
    )

    # ── LEVEL-KONFIGURATION ───────────────────────────────────────────────────

    level_configs = [
        LevelConfig("ICD_NAME",      "ICD_NAME",      "Full ICD code"),
        LevelConfig("ICD_BASE_NAME", "ICD_BASE_NAME", "Base code (3-digit)"),
        LevelConfig("group_full",    "group_full",     "ICD group"),
        LevelConfig("chapter_full",  "chapter_full",   "ICD chapter"),
    ]

    # ── REPORT STARTEN ────────────────────────────────────────────────────────

    results = run_nebendiagnosen_report(
        df                            = df_conditions_mapped,
        patient_col                   = PATIENT_COL,
        level_configs                 = level_configs,
        cohort_name                   = COHORT_NAME,
        modes                         = ["all", "unique"],
        top_n                         = TOP_N,
        output_dir                    = OUTPUT_DIR,
        show                          = False,
        show_title                    = False,
        icd_nebendiagnosen_einteilung = icd_nebendiagnosen_einteilung,
        icd_code_col_in_df            = "icd_code",
        icd_code_col_in_mapping       = "ICD_Code",
        # Vollständiges Universum aller Patienten mit potenziellen Nebendiagnosen
        # (vor Tumorkohorte-Filter erfasst) → Patienten mit 0 Nebendiagnosen
        # erscheinen als Balken bei x=0 im Log-Histogramm.
        all_patient_ids               = all_patient_ids_universe,
    )