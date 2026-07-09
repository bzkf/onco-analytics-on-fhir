"""
main_sprechend.py
=================
Steuerzentrale ("Orchestrator") der BZKF-oBDS-Kohortenauswertung.

Diese Datei lädt alle Quelldaten, baut die Kohorten schrittweise auf und ruft
sämtliche Plot-Funktionen (plots.py, PlotsICDDiagzuAlter.py,
SecondPaper_NebenDiagnosen_Plots.py, data_processing.py) auf.

Die Konsolenausgabe ist bewusst "sprechend": an JEDEM Filterschritt wird
ausgegeben, wie sich Zeilen / cond_ids / Patienten verändern, damit der
komplette Datenfluss ohne Blick in den Code nachvollziehbar ist.

────────────────────────────────────────────────────────────────────────────
PIPELINE-ÜBERBLICK (Reihenfolge der Filter)
────────────────────────────────────────────────────────────────────────────
    Eingang   :  Alle oBDS-Karzinom-cond_ids (df_obds; Patienten können
                 mehrere Tumore/cond_ids haben)
    Filter 1  :  D-Diagnosen entfernen (gutartige/unsichere Neubildungen)
    Filter 2  :  C44 entfernen (Hauttumoren ohne systemische Relevanz)
    Filter 2b :  Minderjährige entfernen (age_at_diagnosis < 18)
    Filter 3  :  Top-20 Tumor-Entitäten behalten  →  GRUNDKOHORTE (GK)
    Filter 4  :  asserted_year >= YEAR_CUTOFF  →  GK_17
                 (AKTUELL DEAKTIVIERT, siehe YEAR_CUTOFF unten)
    Filter 5  :  3-Monats-Cutoff für Therapie×Staging-PAARE (months_diff ≤ 3)

KOHORTEN (Master = df_tumore; alle Slave-Tabellen werden über cond_ids gefiltert)
    GK     – Grundkohorte (Top-20, kein D/C44, ≥18, Therapien nur K&P)
    GK_17  – GK + Jahresschnitt (derzeit ohne Effekt, da YEAR_CUTOFF deaktiviert)
    3M     – GK_17 + nur Therapie×Staging-Paare mit ≤ 3 Monaten Abstand

STAGING-QUELLEN (laufen PARALLEL, mischen sich NICHT):
    oBDS   – df_uicc_tnm + weitere_klassifikation(UICC)
    MUST   – Brigittes MUST-Tool (Result1_UICC_full.csv je Standort)
────────────────────────────────────────────────────────────────────────────
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import warnings

warnings.filterwarnings("ignore")

import matplotlib

matplotlib.use("Agg")

import os

# ICD-Hierarchie-Helper (für Nebendiagnosen-Mapping)
import icd10gm2026_hierarchy_fast_helper
import numpy as np
import pandas as pd

# Nebendiagnosen-Analyse
# Namenskonflikt mit plots.py: plot_lorenz_curve / plot_log_histogram → Alias
import SecondPaper_NebenDiagnosen_Plots as neben
from data_processing import (
    analyze_therapy_patterns,
    merge_and_plot_ecog_uicc_proximity,
    merge_with_nearest_date_matching,
    patient_dropout_by_cutoff,
    sweep_tolerance_fast,
)
from plots import (
    plot_age_distribution_grouped_bar,
    plot_dropout_curve,
    plot_ecog_distribution_grouped_bar,
    plot_log_histogram,
    plot_lorenz_curve,
    plot_merge_panel,
    plot_sweep_panel,
    plot_therapy_bias_analysis,
    plot_therapy_times,
    plot_uicc_distribution_grouped_bar,
    plot_uicc_ecog_inventory,
    scatterplot,
    plot_uicc_ecog_inventory_coverage
)

# Butterfly-Plots (demographischer Alters-/Geschlechts-Baum)
from PlotsICDDiagzuAlter import plot_population_pyramid_from_raw, plot_population_pyramid_topn, extract_contingency_tables_topn
from SecondPaper_NebenDiagnosen_Plots import LevelConfig, run_nebendiagnosen_report
from combined_paper_figures import create_ecog_uicc_panel
from domain_translation import add_final_domain_en

# TODO: Must hinzufügen und VGL Plot  <- warten auf anpassung
# TODO: CAST DTYPES AFTER READING PARQUET FILES!
# TODO: Ändere Plots auf high resolution PNG


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING-HELPER
# ──────────────────────────────────────────────────────────────────────────────


def _counts(
    df: pd.DataFrame,
    cond_col: str = "condition_id_hash",
    pat_col: str = "patient_resource_id_hash",
) -> tuple[int, int | None, int | None]:
    """
    Zentrale Kennzahlen eines DataFrames: (Zeilen, eindeutige cond_ids, eindeutige Patienten).

    Fehlt eine Spalte (z.B. Patienten-ID in einer Staging-Tabelle), wird für
    diese Kennzahl None zurückgegeben.
    """
    n_rows = len(df)
    n_cond = df[cond_col].nunique() if cond_col in df.columns else None
    n_pat = df[pat_col].nunique() if pat_col in df.columns else None
    return n_rows, n_cond, n_pat


def _log_load(
    label: str,
    df: pd.DataFrame,
    cond_col: str = "condition_id_hash",
    pat_col: str = "patient_resource_id_hash",
) -> None:
    """Gibt nach dem Laden eines DataFrames Zeilen / cond_ids / Patienten aus."""
    n_rows, n_cond, n_pat = _counts(df, cond_col, pat_col)
    cond_str = f"{n_cond:>8,}" if n_cond is not None else "       –"
    pat_str = f"{n_pat:>8,}" if n_pat is not None else "       –"
    print(f"    {label}: {n_rows:>9,} Zeilen  |  cond_ids: {cond_str}  |  Patienten: {pat_str}")


def _log_filter(label: str, n_before: int, n_after: int, unit: str = "cond_ids") -> None:
    """Druckt Filtereffekt EINER Kennzahl als Vorher → Nachher (Kurzform)."""
    pct = 100 * n_after / n_before if n_before > 0 else 0
    print(f"    Filter '{label}': {n_before:,} → {n_after:,} {unit}  ({pct:.1f}% verbleiben)")


def _log_step(
    label: str,
    before: tuple[int, int | None, int | None],
    after: tuple[int, int | None, int | None],
) -> None:
    """
    Druckt einen Filterschritt vollständig auf ALLEN drei Ebenen
    (Zeilen / cond_ids / Patienten) als Vorher → Nachher inkl. Delta und Anteil.

    `before`/`after` sind (Zeilen, cond_ids, Patienten)-Tupel, wie von
    `_counts()` geliefert. So lässt sich ohne Code-Blick exakt nachvollziehen,
    wieviele Zeilen, cond_ids und Patienten ein Filter entfernt.
    """
    names = ("Zeilen", "cond_ids", "Patienten")
    print(f"    Filter '{label}':")
    for name, b, a in zip(names, before, after):
        if b is None or a is None:
            print(f"      {name:<10}: –  (Spalte nicht vorhanden)")
            continue
        pct = 100 * a / b if b else 0
        print(f"      {name:<10}: {b:>11,} → {a:>11,}   ({a - b:>+10,}  |  {pct:5.1f}% bleibt)")


# ══════════════════════════════════════════════════════════════════════════════
# PFADE
# ══════════════════════════════════════════════════════════════════════════════

# read in all data
# BASE_DIR = Path(
#     "/home/coder/git/onco-analytics-on-fhir/"
#     "analytics_on_fhir/analysis/all_obds_patients_analysis/"
#     "all_obds_data_allsites"
# )


BASE_DIR = Path(
    #r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\onco-analytics-on-fhir\analytics_on_fhir\analysis\all_obds_patients_analysis\all_obds_data_allsites"
    r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\all_obds_data_allsites"
)

DATA = Path(
    r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\resources"
)
PLOTS = Path(
    r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\plots"
)

test_tnm = pd.read_parquet(r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\all_obds_data_allsites\tum\parquet\df_n_tnm_deidentified.parquet")

SITES = "UKER, TUM, UKA, LMU, UKR, UKW"
#SITES = "UKER"

# ──────────────────────────────────────────────────────────────────────────────
# JAHRES-CUTOFF (Filter 4)  –  EINZIGE Quelle der Wahrheit
# ──────────────────────────────────────────────────────────────────────────────
# Ursprünglich 2017: Vor 2017 war die Meldepflicht inkonsistent → Completeness-
# Bias. AKTUELL BEWUSST DEAKTIVIERT, indem der Schnitt auf 1900 gesetzt wird:
# Kein Diagnosejahr liegt vor 1900, daher entfernt Filter 4 praktisch nichts.
# Vorteil: Die Filterlogik bleibt im Code erhalten und kann durch Setzen auf
# 2017 jederzeit reaktiviert werden, ohne umzuprogrammieren.
#
#   → Zum Reaktivieren des Jahresschnitts:  YEAR_CUTOFF = 2017
YEAR_CUTOFF = 1900

FILES = {
    "obds": "df_all_obds_clean_deidentified.parquet",
    "weitere_klassifikation": "df_weitere_klassifikation_deidentified.parquet",
    "uicc_tnm": "df_uicc_tnm_deidentified.parquet",
    "ecog": "df_leistungszustand_ecog_karnofsky_deidentified.parquet",
    "mii_conditions": "df_mii_conditions_all_obds_pats_asserted_deidentified.parquet",
    "ops_grouped": "df_ops_grouped_deidentified.parquet",
    "radiotherapies_joined": "df_radiotherapies_joined_deidentified.parquet",
    "systemtherapies": "df_system_therapies_deidentified.parquet",
}

#site_list = [s.strip() for s in SITES.split(",")]
site_list = [s.strip().lower() for s in SITES.split(",")]


paths = {
    name: [BASE_DIR / site / "parquet" / filename for site in site_list]
    for name, filename in FILES.items()
}

# ── Sprechende Einlese-Übersicht (ersetzt rohen print(paths)-Dump) ───────────
print("\n" + "━" * 70)
print("DATEN-EINLESEN  –  Multi-Site-Parquets")
print("━" * 70)
print(f"  BASE_DIR : {BASE_DIR}")
print(f"  Standorte ({len(site_list)}): {', '.join(s.upper() for s in site_list)}")
print(f"  Datensätze je Standort ({len(FILES)}):")
for _name, _fn in FILES.items():
    print(f"    {_name:<24} ← {_fn}")

dfs = {}

for dataset_name, dataset_paths in paths.items():
    tmp_dfs = []
    _per_site_rows = []

    for path, site in zip(dataset_paths, site_list):
        #df = pd.read_parquet(path, engine="pyarrow")
        df = pd.read_parquet(path)

        # Standort mitführen (für Nachforschung Nebendiagnosen / UKW-Diagnose)
        df["site"] = site
        _per_site_rows.append(f"{site.upper()}={len(df):,}")
        tmp_dfs.append(df)

    dfs[dataset_name] = pd.concat(tmp_dfs, ignore_index=True)
    # Konkatenations-Ergebnis je Datensatz bestätigen (Zeilen gesamt + je Standort)
    print(
        f"    [LOAD] {dataset_name:<24}: {len(dfs[dataset_name]):>10,} Zeilen "
        f"({len(tmp_dfs)} Standorte: {', '.join(_per_site_rows)})"
    )

# ── MUST-UICC: pro Standort als CSV, NICHT im parquet/-Unterordner ────────────
# Pfadschema: BASE_DIR / <site> / "Result1_UICC_full.csv"
# Wird hier analog zu den Parquets über alle Standorte konkateniert.
print("\n  [LOAD] MUST-UICC pro Standort (Result1_UICC_full.csv):")
_must_tmp = []
for site in site_list:
    _must_path = BASE_DIR / site / "Result1_UICC_full.csv"
    try:
        _df_must_site = pd.read_csv(_must_path)
        _df_must_site["site"] = site
        print(f"    {site.upper():<6}: {len(_df_must_site):>9,} Zeilen  ←  {_must_path}")
        _must_tmp.append(_df_must_site)
    except FileNotFoundError:
        print(f"    {site.upper():<6}: DATEI FEHLT  ←  {_must_path}")
dfs["must_uicc"] = pd.concat(_must_tmp, ignore_index=True) if _must_tmp else pd.DataFrame()
print(f"    MUST-UICC gesamt: {len(dfs['must_uicc']):,} Zeilen über {len(_must_tmp)} Standorte")

df_obds = dfs["obds"]
df_weitere_klassifikation = dfs["weitere_klassifikation"]
df_uicc_tnm = dfs["uicc_tnm"]
df_ecog = dfs["ecog"]
df_mii_conditions = dfs["mii_conditions"]
df_ops_grouped = dfs["ops_grouped"]
df_radiotherapies_joined = dfs["radiotherapies_joined"]
df_systemtherapies = dfs["systemtherapies"]
df_must_uicc_all = dfs["must_uicc"]



# Namensschema:
#   01_GK     = Grundkohorte  (Top-20 Entitäten, kein D/C44, nur K&P)  – alle Jahre
#   02_GK_17  = + Filter asserted_year >= YEAR_CUTOFF (Filter 4; aktuell deaktiviert)
#   02_GK_17/3M = + max. 3 Monate Abstand ECOG/UICC zu Therapie
DIR_GK = PLOTS / "01_GK"
DIR_GK_17 = PLOTS / "02_GK_17"

# Unterordner für die neuen Analysen
DIR_BUTTERFLY = PLOTS / "Butterfly"
DIR_NEBENDIAG = DIR_GK / "Nebendiagnosen"

# Spalten die aus den Therapie-DataFrames mitgeführt werden
_TCOLS = [
    "therapy_typ",
    "condition_id_hash",
    "patient_resource_id_hash",
    "months_between_asserted_therapy_start_date",
    "asserted_year",
    "age_at_diagnosis",
]


# ══════════════════════════════════════════════════════════════════════════════
# HILFSFUNKTION: UICC/ECOG-INVENTAR-VERGLEICH  (GK vs. MUST)
# ══════════════════════════════════════════════════════════════════════════════


def plot_uicc_inventory_comparison(
    df_uicc_gk: pd.DataFrame,
    df_uicc_must: pd.DataFrame | None,
    df_ecog: pd.DataFrame,
    n_total: int,
    out_dir: Path,
    uicc_only_must: bool = False,
) -> None:
    _inv_kwargs = dict(
        uicc_col="uicc_tnm",
        ecog_col="ecog_performance_status",
        cond_col="condition_id_hash",
        n_total_cond_ids=n_total,
    )

    plot_uicc_ecog_inventory(
        df_uicc=df_uicc_gk,
        df_ecog=df_ecog,
        output_path=out_dir / "uicc_ecog_inventory.png",
        show_known_label=False,
        **_inv_kwargs,
    )

    if df_uicc_must is not None:
        plot_uicc_ecog_inventory(
            df_uicc=df_uicc_must,
            df_ecog=None if uicc_only_must else df_ecog,
            show_ecog=not uicc_only_must,
            output_path=out_dir / "uicc_ecog_inventory_must.png",
            show_known_label = False,
            **_inv_kwargs,
        )


# ══════════════════════════════════════════════════════════════════════════════
# DATEN LADEN  –  GRUNDKOHORTE
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "━" * 70)
print("DATEN LADEN – MASTER-TABELLE: TUMORKOHORTE")
print("━" * 70)

# ── Tumore (alle C-Diagnosen aus OBDS; noch ungefiltert) ─────────────────────
# Quelle: histology_entity_recoding_df_all_obds_with_entity_final.parquet
#         (erzeugt via Brigittes load_multisite_specific_data_with_sarcoma_entity.py)
# TODO: histology_entity_recoding_df_all_obds_with_entity_final.parquet muss via
# TODO: Brigittes "load_multisite_specific_data_with_sarcoma_entity.py" erzeugt werden!
print("\n  [LOAD] Tumorkohorte (Roh):")
print(f"    Quelle: df_all_obds_clean_deidentified.parquet")
df_tumore = df_obds
_step0 = _counts(df_tumore)
_n0_rows, _n0_cond, _n0_pat = _step0
print(f"    Zeilen: {_n0_rows:,}  |  cond_ids: {_n0_cond:,}  |  Patienten: {_n0_pat:,}")

# ── Filter 1: D-Diagnosen entfernen (gutartige/unsichere Neubildungen) ────────
print("\n  [FILTER 1] Entferne D-Diagnosen (icd10_parent_code beginnt mit 'D'):")
df_tumore = df_tumore[~df_tumore["icd10_parent_code"].str.startswith("D", na=False)]
_step1 = _counts(df_tumore)
_n1_cond = _step1[1]
_log_step("D-Diagnosen entfernt", _step0, _step1)

# ── Filter 2: C44 entfernen (Hauttumoren – keine systemische Relevanz) ────────
print("\n  [FILTER 2] Entferne C44 (Bösartige Neubildungen der Haut):")
df_tumore = df_tumore[~df_tumore["icd10_parent_code"].str.startswith("C44", na=False)]
_step2 = _counts(df_tumore)
_n2_cond = _step2[1]
_log_step("C44 entfernt", _step1, _step2)

# ── Filter 2b: Minderjährige entfernen (age_at_diagnosis < 18) ────────────────
# Wirkt auf die GESAMTKOHORTE, also vor Butterfly-Plot und Top-20-Bestimmung.
print("\n  [FILTER 2b] Entferne minderjährige Patienten (age_at_diagnosis < 18):")
_age_num = pd.to_numeric(df_tumore["age_at_diagnosis"], errors="coerce")
_n_underage_rows = int((_age_num < 18).sum())
_n_underage_pat = df_tumore.loc[_age_num < 18, "patient_resource_id_hash"].nunique()
_n_age_nan = int(_age_num.isna().sum())
df_tumore = df_tumore[_age_num >= 18]
_step2b = _counts(df_tumore)
_n2b_cond = _step2b[1]
print(
    f"    Direkt entfernt: {_n_underage_rows:,} Zeilen  |  {_n_underage_pat:,} minderjährige Patienten"
)
if _n_age_nan:
    print(
        f"    HINWEIS: {_n_age_nan:,} Zeilen ohne numerisches Alter fallen ebenfalls heraus "
        f"(age_at_diagnosis nicht parsebar → NaN, erfüllt '>= 18' nicht)."
    )
_log_step("Minderjährige (<18) entfernt", _step2, _step2b)

# ── ICD-Klartextnamen ergänzen (Lookup aus DWH) ───────────────────────────────
print("\n  [MERGE] ICD-Klartextnamen (DWH_ICD_CODE_MAPPING) → left join auf icd10_parent_code:")
_icd_lookup = pd.read_parquet(os.path.join(DATA, "DWH_ICD_CODE_MAPPING.parquet"))[
    ["ICD3_CODE", "ICD3_NAME"]
].drop_duplicates()
# df_tumore = df_tumore.merge(
#     _icd_lookup, left_on="icd10_parent_code", right_on="ICD3_CODE", how="left"
# )

# ══════════════════════════════════════════════════════════════════════════════
# ABSCHNITT A  –  BUTTERFLY-PLOT  (Demographischer Alters-/Geschlechts-Baum)
# Kohorte: df_tumore NACH D/C44/<18-Filter, aber VOR dem Top-20-Filter (Filter 3).
# Kein Jahres- oder Therapiefilter – vollständige demographische Übersicht.
# WICHTIG: Die Top-20-Auswahl für butterfly_topn.png wird NICHT hier, sondern
# INNERHALB von plot_population_pyramid_topn() per value_counts gebildet. Da die
# Auswahllogik identisch zu Filter 3 ist (value_counts auf entity_or_parent),
# stimmen die 20 Entitäten mit der späteren Grundkohorte überein.
# ══════════════════════════════════════════════════════════════════════════════

DIR_BUTTERFLY.mkdir(parents=True, exist_ok=True)

print("\n" + "━" * 70)
print("ABSCHNITT A  –  BUTTERFLY-PLOT  (Demographischer Alters-/Geschlechts-Baum)")
print("━" * 70)
print(f"  Kohorte: df_tumore NACH D/C44/<18-Filter, VOR Top-20-Filter")
print(
    f"  cond_ids: {df_tumore['condition_id_hash'].nunique():,}  |  "
    f"Patienten: {df_tumore['patient_resource_id_hash'].nunique():,}  |  "
    f"Entitäten (entity_or_parent): {df_tumore['entity_or_parent'].nunique():,}"
)
print(
    f"  HINWEIS: butterfly_topn.png wählt die Top-20-Entitäten intern "
    f"(in plot_population_pyramid_topn); <18 und ungültige Geschlechter werden "
    f"in der Plot-Funktion zusätzlich verworfen (siehe deren Printouts)."
)

tables = extract_contingency_tables_topn(
    df=df_tumore,
    age_col="age_at_diagnosis",
    sex_col="gender",
    diagnosis_col="entity_or_parent",
    top_n=20,
    male_label="male",
    female_label="female",
    output_path=DIR_BUTTERFLY,  # speichert als contingency_male.csv, contingency_female.csv
)

print(f"\n  [PLOT] butterfly_overall.png  – Alters-/Geschlechtsverteilung, alle Entitäten")
plot_population_pyramid_from_raw(
    df_tumore,
    age_col="age_at_diagnosis",
    sex_col="gender",
    title="Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\nGesamt Population UKer",
    age_step=5,
    show_title=False,
    output_path=DIR_BUTTERFLY / "butterfly_overall.png",
)

print(
    f"  [PLOT] butterfly_topn.png  – Alters-/Geschlechtsverteilung, Top-20 Entitäten farbkodiert"
)
plot_population_pyramid_topn(
    df_tumore,
    age_col="age_at_diagnosis",
    sex_col="gender",
    diagnosis_col="entity_or_parent",
    top_n=20,
    age_step=5,
    title=(
        "Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\n"
        "Gesamt Population UKer\n"
        "Aufgeteilt nach TOP 20 meist vorkommende C-ICD-Diagnosen"
    ),
    show_title=False,
    output_path=DIR_BUTTERFLY / "butterfly_topn.png",
)

print(f"  ✓ Abschnitt A abgeschlossen  →  {DIR_BUTTERFLY}")




print("\n" + "━" * 70)
print("FILTER 3  –  Top-20 Tumor-Entitäten (entity_or_parent)")
print("━" * 70)
print(f"  Rationale: Fokus auf häufige Tumorentitäten mit ausreichender Fallzahl")
_step_pre_top20 = _counts(df_tumore)
_n_entities_all = df_tumore["entity_or_parent"].nunique()
df_tumore = df_tumore[
    df_tumore["entity_or_parent"].isin(df_tumore["entity_or_parent"].value_counts().iloc[:20].index)
]
cond_ids_gk = df_tumore["condition_id_hash"]
_step3 = _counts(df_tumore)
_n3_cond = _step3[1]
_n3_pat = _step3[2]
print(
    f"  Entitäten: {_n_entities_all:,} vorhanden  →  20 behalten "
    f"({_n_entities_all - 20:,} seltenere Entitäten verworfen)"
)
_log_step("Top-20 Entitäten behalten", _step_pre_top20, _step3)
print(f"  ─ GRUNDKOHORTE (GK) festgelegt ─────────────────────────────────────")
print(f"  Master-DF: df_tumore  →  {_n3_cond:,} cond_ids  |  {_n3_pat:,} Patienten")
print(f"  Alle nachgelagerten DataFrames werden auf diese cond_ids gefiltert.")
_top20_entities = df_tumore["entity_or_parent"].value_counts().iloc[:20]
print(f"  Top-20 Entitäten:")
for ent, cnt in _top20_entities.items():
    print(f"    {ent:<45} n={cnt:,}")

# ── GENDER-VALIDIERUNG: Zeige Werte die NICHT im Plot sind ──────────────────
print("\n" + "=" * 80)
print("GENDER-VALUE CHECK: Welche Werte kommen vor und sind NICHT in den Plots?")
print("=" * 80)

gender_counts = df_tumore["gender"].value_counts(dropna=False)
print(f"\nAlle Gender-Werte in df_tumore:")
print(gender_counts)

# Filtere nach Werten, die NICHT female oder male sind
valid_genders = {"female", "male"}
excluded_genders = gender_counts[~gender_counts.index.isin(valid_genders)]

if len(excluded_genders) > 0:
    print(f"\n⚠️  ACHTUNG: Folgende Gender-Werte sind NICHT in den Plots enthalten:")
    for gender_val, count in excluded_genders.items():
        print(f"   • {repr(gender_val)}: {count:,} Einträge")
    print(f"\n   Gesamt ausgeschlossene Einträge: {excluded_genders.sum():,}")
else:
    print(f"\n✓ Alle Einträge haben gültige Gender-Werte (female/male)")


#TODO: BRIGITTE HIER!
print("\n" + "━" * 70)
print("DATEN LADEN – SLAVE-TABELLEN: UICC, ECOG, THERAPIEN")
print("  (Alle auf GK gefiltert via cond_ids_gk / patient_resource_id_hash)")
print("━" * 70)

# ── UICC  GRUNDKOHORTE oBDS = df_uicc_tnm + weitere_klassifikation ────────────
print("\n  [LOAD] UICC-Staging oBDS – Quelle 1a: df_uicc_tnm_deidentified.parquet")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")

# print("FIND: CHECK UICC weitere_klassifikation Raw Numbers:")
# print("Value Counts:")
# print(df_weitere_klassifikation[df_weitere_klassifikation["weitere_klassifikation_name"] == "UICC"]["weitere_klassifikation_value_code"].value_counts(dropna=False))
# print("Cond_Ids:")
# print(df_weitere_klassifikation[df_weitere_klassifikation["weitere_klassifikation_name"] == "UICC"]["condition_id_hash"].nunique())

_wk = df_weitere_klassifikation
_wk = _wk[_wk["condition_id_hash"].isin(cond_ids_gk)]
# WICHTIG: weitere_klassifikation hat eine EIGENE Zeitspalte
# (months_between_asserted_weitere_klassifikation_date). Diese muss auf die
# gemeinsame UICC-Zeitspalte umbenannt werden, sonst fehlt sie nach dem concat
# und alle wk-UICC-Zeilen fallen bei jedem zeitbasierten dropna heraus.
_wk_uicc = (
    _wk[_wk["weitere_klassifikation_name"] == "UICC"]
    .rename(
        {
            "weitere_klassifikation_value_code": "uicc_tnm",
            "months_between_asserted_weitere_klassifikation_date": "months_between_asserted_uicc_tnm_date",
        },
        axis=1,
    )
    .loc[:, lambda d: ~d.columns.duplicated()]
)
_wk_uicc = (
    _wk_uicc.dropna(
        subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]
    )
)

# print("FIND: CHECK UICC weitere_klassifikation Filtered Numbers:")
# print("Value Counts:")
# print(_wk_uicc["uicc_tnm"].value_counts(dropna=False))
# print("Cond_Ids:")
# print(_wk_uicc['condition_id_hash'].nunique())


print('_wk_uicc = (_wk_uicc.dropna(subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]')
print('Wegen Right Join und sonst nicht nutzbaren Werten')
_wk_with_time = _wk_uicc["months_between_asserted_uicc_tnm_date"].notna().sum() if "months_between_asserted_uicc_tnm_date" in _wk_uicc.columns else 0
print(
    f"    weitere_klassifikation → UICC-Einträge: {len(_wk_uicc):,} Zeilen  |  cond_ids: {_wk_uicc['condition_id_hash'].nunique():,}"
)
print(f"      davon mit Zeitstempel (nach Umbenennung): {_wk_with_time:,}")

df_uicc_raw = df_uicc_tnm

# print("FIND: CHECK UICC OBDS RAW Numbers:")
# print("Value Counts:")
# print(df_uicc_raw["uicc_tnm"].value_counts(dropna=False))
# print("Cond_Ids:")
# print(df_uicc_raw['condition_id_hash'].nunique())

df_uicc_raw = df_uicc_raw[df_uicc_raw["condition_id_hash"].isin(cond_ids_gk)]
df_uicc_raw["months_between_asserted_uicc_tnm_date"].value_counts(dropna=False)# <-- 83678 times none
df_uicc_raw["uicc_tnm"].value_counts(dropna=False)# <-- 329797 times none

_n_uicc_before_drop = len(df_uicc_raw)
df_uicc_raw["uicc_tnm"] = df_uicc_raw["uicc_tnm"].fillna("missing")
df_uicc_raw = (
    df_uicc_raw.dropna(
        subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]
    )
    .sort_values(["condition_id_hash", "months_between_asserted_uicc_tnm_date"])
    .loc[:, lambda d: ~d.columns.duplicated()]
)

# print("FIND: CHECK UICC OBDS Filtered Numbers:")
# print("Value Counts:")
# print(df_uicc_raw["uicc_tnm"].value_counts(dropna=False))
# print("Cond_Ids:")
# print(df_uicc_raw['condition_id_hash'].nunique())

print('df_uicc_raw.dropna(subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]')
print('Wegen Right Join und sonst nicht nutzbaren Werten')
print(
    f"    df_uicc_tnm: {_n_uicc_before_drop:,} Zeilen geladen  →  {len(df_uicc_raw):,} nach dropna  |  cond_ids: {df_uicc_raw['condition_id_hash'].nunique():,}"
)
print(f"    NaN-UICC-Werte wurden als 'missing' kodiert.")



print(f"\n  [MERGE] oBDS-Grundkohorte: df_uicc_tnm + weitere_klassifikation (UICC) → df_uicc_gk")
df_uicc_raw["source"] = "OBDS"
_wk_uicc["source"] = "WK"

df_uicc_gk = pd.concat([df_uicc_raw, _wk_uicc], ignore_index=True, sort=False)
df_uicc_gk["uicc_tnm"] = df_uicc_gk["uicc_tnm"].fillna("missing")
df_uicc_gk = df_uicc_gk.drop_duplicates(subset=['uicc_tnm', 'condition_id_hash', 'months_between_asserted_uicc_tnm_date'])
#TODO: Drop Duplicates.
_uicc_known = df_uicc_gk[df_uicc_gk["uicc_tnm"].astype(str).ne("missing")][
    "condition_id_hash"
].nunique()
_uicc_miss = df_uicc_gk[df_uicc_gk["uicc_tnm"].astype(str).eq("missing")][
    "condition_id_hash"
].nunique()
_uicc_none = _n3_cond - df_uicc_gk["condition_id_hash"].nunique()
print(
    f"    df_uicc_gk (oBDS): {len(df_uicc_gk):,} Zeilen  |  cond_ids: {df_uicc_gk['condition_id_hash'].nunique():,}"
)
print(
    f"    Davon bekannte UICC:  {_uicc_known:,} cond_ids  |  'missing': {_uicc_miss:,}  |  kein Eintrag: {_uicc_none:,}"
)

# print("FIND: CHECK UICC joined OBDS+WK Filtered Numbers:")
# print("Value Counts:")
# print(df_uicc_raw["uicc_tnm"].value_counts(dropna=False))
# print("Cond_Ids:")
# print(df_uicc_raw['condition_id_hash'].nunique())

# ── UICC  MUST-Tool (Brigitte) – multi-site, MIT Zeitstempel ─────────────────
print(f"\n  [LOAD] UICC-Staging – Quelle 2 (MUST-Tool, Brigitte): Result1_UICC_full.csv (alle Standorte)")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")
# df_must_uicc_all wurde oben im Multi-Site-Loop konkateniert (inkl. site-Spalte).
# MUST ist eine EIGENSTÄNDIGE, vollständige UICC-Quelle (Kollegin hat ihre
# Merge-/1-Wochen-Logik bereits in der CSV umgesetzt). MUST mischt sich NICHT
# mit oBDS – beide laufen parallel. Zeitspalte wird auf die gemeinsame
# UICC-Zeitspalte umbenannt.
df_uicc_must = (
    df_must_uicc_all[["ID", "RESULTING_UICC", "site", "MONTHS_BETWEEN_ASSERTED_PARENT_TNM_DATE"]]
    .rename(columns={
        "ID": "condition_id_hash",
        "RESULTING_UICC": "uicc_tnm",
        "MONTHS_BETWEEN_ASSERTED_PARENT_TNM_DATE": "months_between_asserted_uicc_tnm_date",
    })
    .pipe(lambda d: d[d["condition_id_hash"].isin(cond_ids_gk)])
    .reset_index(drop=True)
)
df_uicc_must["uicc_tnm"] = df_uicc_must["uicc_tnm"].fillna("missing")
df_uicc_must = df_uicc_must.dropna(
    subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]
)
print('df_uicc_must = df_uicc_must.dropna(subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]')
print('Wegen Right Join und sonst nicht nutzbaren Werten')

_must_known = df_uicc_must["uicc_tnm"].notna().sum()
_must_miss = df_uicc_must["uicc_tnm"].isna().sum()
print(
    f"    Zeilen: {len(df_uicc_must):,}  |  cond_ids: {df_uicc_must['condition_id_hash'].nunique():,}"
)
print(
    f"    Bekannte UICC: {_must_known:,}  |  NaN (kein Staging aus TNM ableitbar): {_must_miss:,}"
)
print(f"    Pro Standort:")
for _site, _grp in df_uicc_must.groupby("site"):
    _k = _grp["uicc_tnm"].notna().sum()
    print(f"      {_site.upper():<6}: {len(_grp):>8,} Zeilen  |  bekannt: {_k:,}")
print(f"    MUST ist eigenständige UICC-Quelle (läuft parallel zu oBDS).")

# ── Bestandsvergleich GK-OBDS vs. MUST ────────────────────────────────────────
print("\n" + "━" * 70)
print("UICC QUELLENVERGLEICH: OBDS-Tabelle vs. MUST-Tool (Brigitte)")
print("  Ziel: Quantifizieren wieviele cond_ids durch das MUST-Tool")
print("  neu erschlossen werden bzw. welche Coverage verloren geht.")
print("━" * 70)

_gk_all_ids = set(df_uicc_gk["condition_id_hash"].dropna().unique())
_must_all_ids = set(df_uicc_must["condition_id_hash"].dropna().unique())
_gk_known_ids = set(
    df_uicc_gk.loc[
        df_uicc_gk["uicc_tnm"].astype(str).str.strip().ne("missing"),
        "condition_id_hash",
    ]
    .dropna()
    .unique()
)
_must_known_ids = set(
    df_uicc_must.loc[df_uicc_must["uicc_tnm"].notna(), "condition_id_hash"].unique()
)
_gk_missing_ids = _gk_all_ids - _gk_known_ids
_must_missing_ids = _must_all_ids - _must_known_ids
_only_in_gk = _gk_all_ids - _must_all_ids
_only_in_must = _must_all_ids - _gk_all_ids
_in_both = _gk_all_ids & _must_all_ids
_newly_covered = _gk_missing_ids & _must_known_ids
_coverage_lost = _gk_known_ids & _must_missing_ids
_n_gk_total = cond_ids_gk.nunique()

#Check auf gründe der differenz zw. OBDS und MUST UICC
df_uicc_gk[~(df_uicc_gk["condition_id_hash"].isin(df_uicc_must["condition_id_hash"]))]
fehlende_must_ids = list(df_uicc_gk[~(df_uicc_gk["condition_id_hash"].isin(df_uicc_must["condition_id_hash"]))]["condition_id_hash"])
fehlende_must_icd_codes_df_tumore = df_tumore[df_tumore["condition_id_hash"].isin(fehlende_must_ids)]["entity_or_parent"].value_counts()

fehlende_entitäten_UICC = df_tumore[df_tumore["condition_id_hash"].isin(df_uicc_gk["condition_id_hash"])]["entity_or_parent"].value_counts()
fehlende_entitäten_MUST = df_tumore[df_tumore["condition_id_hash"].isin(df_uicc_must["condition_id_hash"])]["entity_or_parent"].value_counts()


print(f"\n  Gesamtkohorte GK aus Tumortabelle              : {_n_gk_total:>8,}  cond_ids")
print(
    f"\n  OBDS-Table  – cond_ids mit UICC-Einträgen      : {len(_gk_all_ids):>8,}  (≥1 Zeile in df_uicc_gk)"
)
print(f"  OBDS-Table  – davon mit bekannter UICC          : {len(_gk_known_ids):>8,}")
print(f"  OBDS-Table  – davon nur 'missing' UICC          : {len(_gk_missing_ids):>8,}")
print(f"  OBDS-Table  – ohne jeden UICC-Eintrag           : {_n_gk_total - len(_gk_all_ids):>8,}")
print(f"\n  MUST – cond_ids gesamt (GK-gefiltert)          : {len(_must_all_ids):>8,}")
print(f"  MUST – davon mit bekannter UICC                 : {len(_must_known_ids):>8,}")
print(f"  MUST – davon ohne UICC (NaN)                    : {len(_must_missing_ids):>8,}")
print(
    f"  MUST – nicht in MUST enthalten                  : {_n_gk_total - len(_must_all_ids):>8,}  (GK-cond_ids, die im CSV fehlen)"
)
print(f"\n  Überschneidung GK ∩ MUST inkl. Missings        : {len(_in_both):>8,}  cond_ids")
print(f"  Nur in OBDS-Table (nicht in MUST)               : {len(_only_in_gk):>8,}  cond_ids")
print(f"  Nur in MUST (nicht in GK)                       : {len(_only_in_must):>8,}  cond_ids")
print(f"\n  Neu bedeckt durch MUST                         : {len(_newly_covered):>8,}  cond_ids")
print(f"    (in OBDS-Table 'missing', im MUST-Tool bekannt)")
print(f"  Coverage verloren durch MUST                    : {len(_coverage_lost):>8,}  cond_ids")
print(f"    (in OBDS-Table bekannt, im MUST-Tool missing)")
print("━" * 70)

df_uicc_must["uicc_tnm"] = df_uicc_must["uicc_tnm"].fillna("missing")

# Parallel-Quellenliste: über diese wird jede UICC-Analyse doppelt gefahren.



print("═" * 70)

#TODO: BRIGITTE HIER! ENDE


# ── ECOG  (gefiltert auf GK) ──────────────────────────────────────────────────
print(f"\n  [LOAD] ECOG-Leistungszustand: df_leistungszustand_ecog_karnofsky_deidentified.parquet")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")
df_ecog_gk = df_ecog.copy()
_n_ecog_raw = len(df_ecog_gk[df_ecog_gk["condition_id_hash"].isin(cond_ids_gk)])
df_ecog_gk = (
    df_ecog_gk[df_ecog_gk["condition_id_hash"].isin(cond_ids_gk)]
    .dropna(
        subset=[
            "ecog_performance_status",
            "condition_id_hash",
            "months_between_asserted_effective_dateTime",
        ]
    )
    .sort_values(["condition_id_hash", "months_between_asserted_effective_dateTime"])
)
_ecog_known = df_ecog_gk[df_ecog_gk["ecog_performance_status"].astype(str).ne("U")][
    "condition_id_hash"
].nunique()
print(f"    GK-gefiltert: {_n_ecog_raw:,} Zeilen  →  {len(df_ecog_gk):,} nach dropna")
print(
    f"    cond_ids: {df_ecog_gk['condition_id_hash'].nunique():,}  |  davon mit bekanntem ECOG (≠ U): {_ecog_known:,}"
)

# ── Therapien  (nur K & P Intention; gefiltert auf GK) ───────────────────────
print(f"\n  [LOAD] Therapien – alle drei Typen auf GK gefiltert (intention K & P):")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")
print(
    f"    Ausgeschlossen: intention ≠ K (kurativ) und ≠ P (palliativ) (z.B. diagnostisch, unbekannt)"
)


def _load_therapy(df, dropna_cols: list, label: str) -> pd.DataFrame:
    """
    Bereitet eine Therapietabelle für die Grundkohorte auf und gibt JEDEN
    Filterschritt sprechend aus (Zeilen + cond_ids), damit kein Filtereffekt
    – insbesondere der K&P-Intentions-Filter – unsichtbar bleibt.

    Schritte (in dieser Reihenfolge):
        0) Roh
        1) dropna auf `dropna_cols` (Pflichtfelder)
        2) auf GK einschränken (condition_id_hash ∈ cond_ids_gk)
        3) "" → "unknown" ersetzen, dann nur Intention K (kurativ) / P (palliativ)

    Args:
        df:          Roh-DataFrame des Therapietyps (bereits multi-site konkateniert)
        dropna_cols: Pflichtspalten, ohne die eine Zeile unbrauchbar ist
        label:       Anzeigename des Therapietyps (für die Printout-Zuordnung)
    """
    def _rc(d):
        return len(d), d["condition_id_hash"].nunique()

    n0, c0 = _rc(df)
    df = df.dropna(subset=dropna_cols)
    n1, c1 = _rc(df)
    df = df.sort_values(["condition_id_hash", "months_between_asserted_therapy_start_date"])
    df = df[df["condition_id_hash"].isin(cond_ids_gk)]
    n2, c2 = _rc(df)
    df = df.replace("", "unknown")
    df = df[df["therapy_intention"].isin(["K", "P"])]
    n3, c3 = _rc(df)

    print(f"    [{label}]")
    print(f"      0) Roh:                  {n0:>9,} Zeilen | {c0:>8,} cond_ids")
    print(f"      1) nach dropna({', '.join(dropna_cols)}):")
    print(f"                               {n1:>9,} Zeilen | {c1:>8,} cond_ids   ({n1 - n0:>+9,} Zeilen)")
    print(f"      2) nach GK-cond_id-Filter:")
    print(f"                               {n2:>9,} Zeilen | {c2:>8,} cond_ids   ({n2 - n1:>+9,} Zeilen)")
    print(f"      3) nach Intention K&P:   {n3:>9,} Zeilen | {c3:>8,} cond_ids   ({n3 - n2:>+9,} Zeilen)")
    print(f"         (entfernt Intention ≠ K/P, z.B. diagnostisch/unbekannt)")
    return df


print(f"    [LOAD] OP: df_ops_grouped_deidentified.parquet")
df_ops_gk = _load_therapy(df_ops_grouped, ["condition_id_hash", "ops_code"], label="OP")

print(f"    [LOAD] Radiotherapie: df_radiotherapies_joined_deidentified.parquet")
df_radio_gk = _load_therapy(
    df_radiotherapies_joined, ["condition_id_hash", "zielgebiet"], label="Radiotherapie"
)

print(f"    [LOAD] Systemtherapie: df_system_therapies_deidentified.parquet")
df_system_gk = _load_therapy(
    df_systemtherapies,
    ["condition_id_hash", "therapy_protocol_text", "months_between_asserted_therapy_start_date"],
    label="Systemtherapie",
)

df_ops_gk["therapy_typ"] = "op"
df_radio_gk["therapy_typ"] = "radiotherapie"
df_system_gk["therapy_typ"] = "systemtherapie"

_cond_any_therapy = (
    set(df_ops_gk["condition_id_hash"])
    | set(df_radio_gk["condition_id_hash"])
    | set(df_system_gk["condition_id_hash"])
)
print(f"\n  GK-Therapieübersicht (K+P):")
print(
    f"    OP:            {len(df_ops_gk):>8,} Zeilen  |  {df_ops_gk['condition_id_hash'].nunique():,} cond_ids"
)
print(
    f"    Radiotherapie: {len(df_radio_gk):>8,} Zeilen  |  {df_radio_gk['condition_id_hash'].nunique():,} cond_ids"
)
print(
    f"    Systemtherapie:{len(df_system_gk):>8,} Zeilen  |  {df_system_gk['condition_id_hash'].nunique():,} cond_ids"
)
print(
    f"    cond_ids mit ≥1 Therapie: {len(_cond_any_therapy):,}  |  "
    f"ohne Therapieeintrag: {_n3_cond - len(_cond_any_therapy):,}"
)

#TODO: Bugfixing unbedingt wieder einfügen!

# ══════════════════════════════════════════════════════════════════════════════
# ABSCHNITT B  –  NEBENDIAGNOSEN
# Grundkohorte: patient_resource_id_hash aus df_tumore (Top-20 Entitäten).
# Patienten-Level (nicht cond_id-Level), da Nebendiagnosen keine condition_id haben.
# ══════════════════════════════════════════════════════════════════════════════

DIR_NEBENDIAG.mkdir(parents=True, exist_ok=True)

print("\n" + "━" * 70)
print("ABSCHNITT B  –  NEBENDIAGNOSEN")
print("  Analyse auf Patienten-Level (patient_resource_id_hash), da Neben-")
print("  diagnosen keine condition_id tragen. Master: df_tumore (GK, Top-20).")
print("━" * 70)

# ── Ressourcenpfade ───────────────────────────────────────────────────────────
ICD_MAPPING_PATH = os.path.join(DATA, "DWH_ICD_CODE_MAPPING.parquet")

ICD_HIERARCHY_CSV = os.path.join(DATA, "icd10gm2026_basecode_lookup.csv")
NEBENDIAG_EXCEL = os.path.join(DATA, "Nebendiagnosen_Zuordnung_Ebenen_Domaenen_v1.xlsx")
NEBENDIAG_EXCEL_CSV_Update = os.path.join(DATA, "nebendiagnosen_review_export_v10_conservative_learned.csv")

# ── Join-Konfiguration (EINSTELLBAR, STANDORTABHÄNGIG) ───────────────────────
# Der Patienten-Join Nebendiagnosen ↔ Tumore läuft je Standort über
# UNTERSCHIEDLICHE Spalten:
#   • UKER:        Nebendiag. 'condition_patient_reference_hash' ↔ Tumore 'patient_resource_id_hash'
#   • alle anderen:Nebendiag. 'patient_mrn_hash'                 ↔ Tumore 'patid_pseudonym_hash'
# Die Befüllung beider Spalten ist je Tabelle identisch; entscheidend ist nur,
# WELCHE Spalte je Standort den Match liefert.
# Pro Standort als (Nebendiag.-Spalte, Tumor-Spalte) konfigurierbar:
JOIN_COLS_BY_SITE = {
    "uker": ("condition_patient_reference_hash", "patient_resource_id_hash"),
    "tum":  ("patient_mrn_hash",                 "patid_pseudonym_hash"),
    "uka":  ("patient_mrn_hash",                 "patid_pseudonym_hash"),
    "lmu":  ("patient_mrn_hash",                 "patid_pseudonym_hash"),
    "ukr":  ("patient_mrn_hash",                 "patid_pseudonym_hash"),
    "ukw":  ("patient_mrn_hash",                 "patid_pseudonym_hash"),
}
# Fallback für Standorte, die oben nicht gelistet sind:
JOIN_COLS_DEFAULT = ("patient_mrn_hash", "patid_pseudonym_hash")
TOP_N_NEBEN = 20

# ── Einheitlichen Join-Key je Seite bauen (standortabhängig befüllt) ─────────
# Idee: Auf beiden Tabellen wird EINE neue Spalte '_join_key' erzeugt, die je
# Zeile aus der für den Standort korrekten Quellspalte stammt. Danach kann
# klassisch über '_join_key' verglichen/gefiltert werden.
def _build_join_key(df, side: str) -> pd.Series:
    """side='neben' → erste Tupelspalte, side='tumore' → zweite Tupelspalte.
    Baut den Join-Key vektorisiert: pro Standort wird die korrekte Quellspalte
    übernommen. Effizient auch bei mehreren Mio. Zeilen."""
    idx = 0 if side == "neben" else 1
    if "site" not in df.columns:
        raise KeyError("Spalte 'site' fehlt – wird für den standortabhängigen Join benötigt.")
    site_lower = df["site"].astype(str).str.lower()
    key = pd.Series(pd.NA, index=df.index, dtype="object")
    for _site in site_lower.unique():
        col = JOIN_COLS_BY_SITE.get(_site, JOIN_COLS_DEFAULT)[idx]
        mask = site_lower == _site
        if col in df.columns:
            key.loc[mask] = df.loc[mask, col].values
        else:
            print(f"    ⚠ Standort {_site.upper()}: Spalte '{col}' fehlt ({side}) – keine Keys gesetzt.")
    return key

# ── ICD-Mapping & Hierarchie laden ───────────────────────────────────────────
_icd_lookup_neben = pd.read_parquet(ICD_MAPPING_PATH)
_icd_dict = _icd_lookup_neben.set_index("ICD_CODE")["ICD_NAME"].to_dict()
_icd_dict_base = _icd_lookup_neben.set_index("ICD3_CODE")["ICD3_NAME"].to_dict()

lookup_df_icd = icd10gm2026_hierarchy_fast_helper.load_icd_hierarchy_lookup(ICD_HIERARCHY_CSV)

# ── Nebendiagnosen-Einteilung laden ──────────────────────────────────────────
icd_nebendiagnosen_einteilung = pd.read_excel(NEBENDIAG_EXCEL, sheet_name="Code_Zuordnung")
icd_nebendiagnosen_einteilung_CSV_UPDATE = pd.read_csv(NEBENDIAG_EXCEL_CSV_Update, sep=";")

icd_nebendiagnosen_einteilung_CSV_UPDATE = icd_nebendiagnosen_einteilung_CSV_UPDATE.rename(columns={
        "final_analytical_role": "Ebene_1_Analytische_Rolle",
        "final_core_comorbidity": "In_Core_Comorbidity_Analysis",
        "code": "ICD_Code",
        "final_domain" : "Ebene_2_Klinische_Domaene"
    })


# Variante B – final_domain direkt überschreiben (das, was du wörtlich gefragt hast):
icd_nebendiagnosen_einteilung_CSV_UPDATE = add_final_domain_en(
    icd_nebendiagnosen_einteilung_CSV_UPDATE, target_col="Ebene_2_Klinische_Domaene"
)

# ── Vollständiges Patienten-Universum der GK (vor Nebendiagnosen-Filter) ──────
# Wird für Log-Histogramm und Lorenz-Kurve benötigt, damit Patienten mit
# 0 Nebendiagnosen korrekt als Balken bei x=0 erscheinen.
# Tumor-seitigen Join-Key standortabhängig bauen.
df_tumore = df_tumore.copy()
df_tumore["_join_key"] = _build_join_key(df_tumore, side="tumore")
all_patient_ids_gk = pd.Index(df_tumore["_join_key"].dropna().unique())
print(
    f"\n  Patienten-Universum GK: {len(all_patient_ids_gk):,} eindeutige Join-Keys (standortabhängig)"
)
print(f"  (Patienten mit 0 Nebendiagnosen werden in Lorenz/Histogramm als x=0 eingeschlossen)")

# ── Nebendiagnosen laden & C-Diagnosen entfernen ─────────────────────────────
print(f"\n  [LOAD] Nebendiagnosen: df_mii_conditions_all_obds_pats_asserted_deidentified.parquet")
df_conditions_raw = df_mii_conditions
_n_cond_raw = len(df_conditions_raw)
# C-Codes = Tumordiagnosen → raus; D-Codes bleiben als Nebendiagnosen
df_conditions_raw = df_conditions_raw[~df_conditions_raw["icd_code"].str.contains("C", na=False)]
df_conditions_raw = df_conditions_raw[~df_conditions_raw["icd_code"].str.contains("NaN", na=False)]
print(
    f"    Roh: {_n_cond_raw:,} Zeilen  →  {len(df_conditions_raw):,} nach Entfernen von C-Diagnosen"
)
print(f"    (C-Codes = Tumordiagnosen werden ausgeschlossen; D-Diagnosen bleiben)")

# ── ICD-Hierarchie mappen ─────────────────────────────────────────────────────
print(f"\n  [MAP] ICD-Hierarchie via icd10gm2026_hierarchy_fast_helper")
print(f"    Adds: icd_basecode, group_range/title, chapter_range/title")
df_conditions_mapped = icd10gm2026_hierarchy_fast_helper.map_icd_dataframe_fast(
    df=df_conditions_raw,
    lookup_df=lookup_df_icd,
    code_col="icd_code",
)
df_conditions_mapped["icd_basecode"] = df_conditions_mapped["icd_code"].apply(
    lambda x: x.split(".")[0] if pd.notna(x) else x
)
df_conditions_mapped["ICD_NAME"] = df_conditions_mapped["icd_code"].map(_icd_dict)
df_conditions_mapped["ICD_BASE_NAME"] = df_conditions_mapped["icd_basecode"].map(_icd_dict_base)
df_conditions_mapped["group_full"] = (
    df_conditions_mapped["group_range"] + ": " + df_conditions_mapped["group_title"]
)
df_conditions_mapped["chapter_full"] = (
    df_conditions_mapped["chapter_range"] + ": " + df_conditions_mapped["chapter_title"]
)

# ── Standortabhängigen Join-Key auf Nebendiagnosen-Seite bauen ───────────────
df_conditions_mapped["_join_key"] = _build_join_key(df_conditions_mapped, side="neben")

# ── Join-Diagnostik: matchen die IDs pro Standort? ───────────────────────────
print(f"\n  [DIAGNOSE] Standortabhängiger Join-Test (Nebendiag. ↔ Tumore):")
_gk_ids_all = set(all_patient_ids_gk)
_neben_ids_all = set(df_conditions_mapped["_join_key"].dropna().unique())
_matched_ids = _neben_ids_all & _gk_ids_all
print(f"    Eindeutige Join-Keys Nebendiagnosen: {len(_neben_ids_all):,}")
print(f"    Eindeutige Join-Keys GK-Tumore:      {len(_gk_ids_all):,}")
print(f"    Schnittmenge (matchen):              {len(_matched_ids):,}")
if len(_neben_ids_all) > 0:
    print(f"    Match-Rate (Nebendiag.-Seite): {100 * len(_matched_ids) / len(_neben_ids_all):.1f}%")
if "site" in df_conditions_mapped.columns:
    print(f"    Match pro Standort (mit jeweils standortspezifischen Spalten):")
    for _site, _grp in df_conditions_mapped.groupby("site"):
        _ncol, _tcol = JOIN_COLS_BY_SITE.get(str(_site).lower(), JOIN_COLS_DEFAULT)
        _site_ids = set(_grp["_join_key"].dropna().unique())
        _site_match = _site_ids & _gk_ids_all
        _pct = 100 * len(_site_match) / len(_site_ids) if _site_ids else 0
        _flag = "  ⚠ AUFFÄLLIG WENIG" if _pct < 50 else ""
        print(f"      {str(_site).upper():<6} [{_ncol} ↔ {_tcol}]")
        print(f"             {len(_grp):>9,} Zeilen  |  IDs: {len(_site_ids):>8,}  |  match: {len(_site_match):>8,} ({_pct:.1f}%){_flag}")

df_conditions_mapped["icd_code"].value_counts().to_excel(r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\Nebendiagnosen_value_counts.xlsx")
# Auf Patienten der GK einschränken (über den standortabhängigen Join-Key)
df_conditions_mapped = df_conditions_mapped[
    df_conditions_mapped["_join_key"].isin(all_patient_ids_gk)
]
# gefiltert_für_komische_werte_analyse = df_conditions_mapped[
#     df_conditions_mapped['icd_code'].isna() |
#     df_conditions_mapped['icd_code'].astype(str).str.contains('!', na=False) |
#     (df_conditions_mapped["icd_code"] == "NaN")
# ]
#
# gefiltert_für_komische_werte_analyse_na = df_conditions_mapped[
#     df_conditions_mapped['icd_code'].isna()
# ]
#
# gefiltert_für_komische_werte_analyse_ausrufezeichen = df_conditions_mapped[
#     df_conditions_mapped['icd_code'].astype(str).str.contains('!', na=False)
# ]
#
# gefiltert_für_komische_werte_analyse_NaN = df_conditions_mapped[
#     (df_conditions_mapped["icd_code"] == "NaN")  # <-- Hier Klammern setzen!
# ]
# gefiltert_für_komische_werte_analyse_NaN["site"].value_counts()


_n_with_neben = df_conditions_mapped["_join_key"].nunique()
_n_without = len(all_patient_ids_gk) - _n_with_neben

print(
    f"\n  [FILTER] Auf GK-Patienten einschränken (_join_key ∈ all_patient_ids_gk):"
)
print(
    f"    Nebendiagnosen-Zeilen: {len(df_conditions_mapped):,}  |  "
    f"Patienten mit ≥1 Nebendiagnose: {_n_with_neben:,}  |  "
    f"Patienten ohne Nebendiagnose: {_n_without:,}"
)
print(
    f"    HINWEIS: Dies sind die Zahlen VOR der klinischen Filterung (Kerscher-Liste /"
)
print(
    f"    icd_nebendiagnosen_einteilung). Die klinisch gefilterten Zahlen werden in der"
)
print(
    f"    Plot-Methode run_nebendiagnosen_report ermittelt und dort zusätzlich ausgegeben."
)

print(f"\n  [EXPORT] ICD_NAME_NebenDiagnosen_value_counts.xlsx  (rohe Häufigkeiten)")
# ── Rohe Value Counts exportieren ─────────────────────────────────────────────
df_conditions_mapped["ICD_NAME"].value_counts().to_excel(
    DIR_NEBENDIAG / "ICD_NAME_NebenDiagnosen_value_counts.xlsx"
)

# ── Level-Konfiguration ───────────────────────────────────────────────────────

#TODO Geändert wegen deutscher Namen. Mappingtable muss geändert/übersetzt werden oder ein Mappingtable von außen hinzugefügt werden
# level_configs_neben = [
#     LevelConfig("ICD_NAME", "ICD_NAME", "Full ICD code"),
#     LevelConfig("ICD_BASE_NAME", "ICD_BASE_NAME", "Base code (3-digit)"),
#     LevelConfig("group_full", "group_full", "ICD group"),
#     LevelConfig("chapter_full", "chapter_full", "ICD chapter"),
# ]

level_configs_neben = [
    LevelConfig("icd_code", "icd_code", "Full ICD code"),
    LevelConfig("base_code", "base_code", "ICD Base code (3-digit)"),
    LevelConfig("group_range", "group_range", "ICD group"),
    LevelConfig("chapter_range", "chapter_range", "ICD chapter"),
]

#── Report starten ────────────────────────────────────────────────────────────
#panelplots für paper
from combined_paper_figures import create_icd_code_filter_panel

fig_neben, tops = create_icd_code_filter_panel(
    df_conditions=df_conditions_mapped,
    patient_col="_join_key",
    icd_nebendiagnosen_einteilung=icd_nebendiagnosen_einteilung_CSV_UPDATE,
    # level=None → Default: LevelConfig("icd_code", "icd_code", "Full ICD code")
    cohort_name="Top20 Cohort",
    mode="unique",
    top_n=TOP_N_NEBEN,
    icd_code_col_in_mapping = "ICD_Code",
    panel_titles=("A) All Available non-C ICD-Codes", "B) After Core Comorbidity Filtering"),
    nrows=2, ncols=1, figsize=(9, 12), wspace=0.15, hspace = 0.25, context_line_y=-0.1,   # falls noch mehr Abstand gewünscht
    save_path=str(DIR_NEBENDIAG / "icd_code_before_after_core_filter_panel.png"),
)


#────────────────────────────────────────────────────────────
print(f"\n  [REPORT] run_nebendiagnosen_report startet …")
print(f"    Level: ICD_NAME, ICD_BASE_NAME, group_full, chapter_full")
print(f"    Modi: 'all' + 'unique'  |  Top-N: {TOP_N_NEBEN}")
print(f"    Filter: standard (alle) + core_comorbidity (In_Core_Comorbidity_Analysis==Ja)")
results_neben = run_nebendiagnosen_report(
    df=df_conditions_mapped,
    patient_col="_join_key",
    level_configs=level_configs_neben,
    cohort_name="Top20 Cohort",
    modes=["all", "unique"],
    top_n=TOP_N_NEBEN,
    output_dir=DIR_NEBENDIAG,
    show=False,
    show_title=False,
    icd_nebendiagnosen_einteilung=icd_nebendiagnosen_einteilung_CSV_UPDATE,
    icd_code_col_in_df="icd_code",
    icd_code_col_in_mapping="ICD_Code",
    all_patient_ids=all_patient_ids_gk,
)
print(f"  [SAVE] Alle Nebendiagnosen-Plots & Excel-Exporte → {DIR_NEBENDIAG}")
print(f"  ✓ Abschnitt B abgeschlossen  →  {DIR_NEBENDIAG}")

#══════════════════════════════════════════════════════════════════════════════
#ABSCHNITT C  –  GRUNDKOHORTE  (alle Jahre)
#Plots: Überblick über die Kohorte vor jeglichem zeitlichem Filter.
#══════════════════════════════════════════════════════════════════════════════

DIR_GK.mkdir(parents=True, exist_ok=True)

print("\n" + "━" * 70)
print("ABSCHNITT C  –  GRUNDKOHORTE  (alle Jahre, alle Therapien K+P)")
print(f"  Master: df_tumore  |  {_n3_cond:,} cond_ids  |  {_n3_pat:,} Patienten")
print(
    f"  Therapien: OP {len(df_ops_gk):,}  Zeilen | Radio {len(df_radio_gk):,} Zeilen | System {len(df_system_gk):,} Zeilen"
)
print("━" * 70)

df_alle_gk = pd.concat(
    [df_radio_gk[_TCOLS], df_system_gk[_TCOLS], df_ops_gk[_TCOLS]],
    ignore_index=True,
)
print(f"\n  df_alle_gk: {len(df_alle_gk):,} Therapiezeilen gesamt (alle Typen)")

# ── Plot 1: Altersverteilung  (5-Jahres- und 10-Jahres-Gruppen) ──────────────
print(f"\n  [PLOT] Altersverteilung je Therapietyp → age_dist_5yr.png / age_dist_10yr.png")
for bracket, fname in [(5, "age_dist_5yr.png"), (10, "age_dist_10yr.png")]:
    plot_age_distribution_grouped_bar(
        dataframes={
            "Surgical Procedure": df_ops_gk,
            "Systemic Therapy": df_system_gk,
            "Radiation Therapy": df_radio_gk,
        },
        age_column="age_at_diagnosis",
        age_bracket=bracket,
        title=False,
        save_path=str(DIR_GK / fname),
    )

# ── Plot 2: Bias-Analyse  (Diagnosejahr vs. rel. Therapiestart) ──────────────
print(f"  [PLOT] Bias-Analyse Diagnosejahr vs. rel. Therapiestart → bias_op/system/radio.png")
print(f"    Zeigt: Zusammenhang zwischen Diagnosejahr und Zeitabstand Diagnose→Therapie")
print(f"    Detektiert Meldebias (ältere Jahre: lückenhafte Therapiedokumentation)")
for df_t, name in [(df_ops_gk, "op"), (df_system_gk, "system"), (df_radio_gk, "radio")]:
    plot_therapy_bias_analysis(
        df_t,
        relative_col="months_between_asserted_therapy_start_date",
        diagnosis_year_col="asserted_year",
        figsize=(10, 4),
        save_path=str(DIR_GK / f"bias_{name}.png"),
    )

# ── Plot 3: UICC/ECOG Inventar  (GK-OBDS + MUST) ─────────────────────────────
print(f"  [PLOT] UICC/ECOG Inventar → uicc_ecog_inventory.png + uicc_ecog_inventory_must.png")
print(f"    Zeigt: Bestand und Staging-Verteilung vor Zeitfilter")
#df_uicc_gk["uicc_tnm"].value_counts().sum()
plot_uicc_inventory_comparison(
    df_uicc_gk=df_uicc_gk,
    df_uicc_must=df_uicc_must,
    df_ecog=df_ecog_gk,
    n_total=cond_ids_gk.nunique(),
    out_dir=DIR_GK,
)

# ── Plot 3b: UICC/ECOG Coverage (cond_id-Ebene, zweigeteilt) ─────────────────
# Wie Plot 3, aber die Missing/Unknown-Zeile ist durch einen 100%-Balken auf
# cond_id-Ebene ersetzt: nutzbare ECOG/UICC (>=1 Eintrag != missing/U) vs.
# Kohorten-cond_ids OHNE nutzbares Staging (nur-missing/U UND ganz ohne Eintrag).
# MUSS vor der STAGING-BEREINIGUNG laufen (df_uicc_gk/df_ecog_gk enthalten hier
# noch 'missing'/'U') – sonst stimmt der Nenner gegen die volle Kohorte nicht.
print(f"  [PLOT] UICC/ECOG Coverage → uicc_ecog_inventory_coverage.png + ..._must.png")
print(f"    Zeigt: nutzbare Zeilen (oben) + cond_id-Coverage der Kohorte (unten)")
_cov_kwargs = dict(
    df_ecog=df_ecog_gk,
    uicc_col="uicc_tnm",
    ecog_col="ecog_performance_status",
    cond_col="condition_id_hash",
    n_total_cond_ids=cond_ids_gk.nunique(),
    show_known_label=False,
    show=False,
)
plot_uicc_ecog_inventory_coverage(
    df_uicc=df_uicc_gk,
    output_path=DIR_GK / "uicc_ecog_inventory_coverage.png",
    **_cov_kwargs,
)
if df_uicc_must is not None:
    plot_uicc_ecog_inventory_coverage(
        df_uicc=df_uicc_must,
        output_path=DIR_GK / "uicc_ecog_inventory_coverage_must.png",
        **_cov_kwargs,
    )

# ── Plot 4: Lorenz-Kurve + Log-Histogramm  (alle Therapien konkateniert) ──────
print(f"  [PLOT] Lorenz-Kurve Therapieverteilung → lorenz.png")
print(f"    Zeigt: Wie ungleich sind Therapien auf cond_ids verteilt?")
plot_lorenz_curve(
    df_alle_gk,
    n_total_patients=cond_ids_gk.nunique(),
    cond_col="condition_id_hash",
    output_path=DIR_GK / "lorenz.png",
)
print(f"  [PLOT] Log-Histogramm Therapieverteilung → log_histo.png")
print(f"    Zeigt: Häufigkeitsverteilung der Therapieanzahl pro cond_id (log-skaliert)")
plot_log_histogram(
    df_alle_gk,
    n_total_patients=cond_ids_gk.nunique(),
    cond_col="condition_id_hash",
    output_path=DIR_GK / "log_histo.png",
)



# ── Plot 5: ECOG × UICC Nähe – PARALLEL für beide Quellen (oBDS + MUST) ──────
UICC_SOURCES_unfiltered = [
    ("obds", df_uicc_gk),
    ("must", df_uicc_must),
]

print(f"\n  [PLOT] ECOG × UICC Zeitliche Nähe (keine Toleranz + 3M-Cutoff):")
print(f"    Methode: nearest-date-Merge (direction=nearest) pro cond_id")
print(f"    Läuft PARALLEL für beide UICC-Quellen (Dateien mit _obds / _must Suffix).")
for _src_label, _df_uicc_src in UICC_SOURCES_unfiltered:
    print(f"    [{_src_label.upper()}] df_ecog_gk ({len(df_ecog_gk):,}) × df_uicc_{_src_label}_time ({len(_df_uicc_src):,} Zeilen)")
    for max_m, dist_f, bub_f in [
        (None, f"ecog_uicc_dist_no_tolerance_{_src_label}.png", f"ecog_uicc_bubble_no_tolerance_{_src_label}.png"),
        (3.0,  f"ecog_uicc_dist_3months_{_src_label}.png",      f"ecog_uicc_bubble_3months_{_src_label}.png"),
    ]:
        merge_and_plot_ecog_uicc_proximity(
            df_leistungszustand=df_ecog_gk,
            df_uicc=_df_uicc_src,
            id_col="condition_id_hash",
            ecog_time_col="months_between_asserted_effective_dateTime",
            uicc_time_col="months_between_asserted_uicc_tnm_date",
            max_months=max_m,
            save_path=str(DIR_GK),
            file_name_dist=dist_f,
            file_name_bubble=bub_f,
        )

print("\n" + "━" * 70)
print("STAGING-BEREINIGUNG  –  Entferne 'missing' UICC und 'U' ECOG")
print("  Gilt AB HIER für alle Therapie×Staging-Merges (Abschnitt C/D/E).")
print("  WICHTIG: Die unfilterten Inventar-/Bubble-Plots OBERHALB enthalten")
print("  'missing'/'U' noch (bewusst, um die Rohabdeckung zu zeigen).")
print("━" * 70)

_b_must = _counts(df_uicc_must)
df_uicc_must = df_uicc_must[~(df_uicc_must["uicc_tnm"] == "missing")]
_log_step("UICC MUST: 'missing' entfernt", _b_must, _counts(df_uicc_must))

_b_uicc = _counts(df_uicc_gk)
df_uicc_gk = df_uicc_gk[~(df_uicc_gk["uicc_tnm"] == "missing")]
_log_step("UICC oBDS: 'missing' entfernt", _b_uicc, _counts(df_uicc_gk))

_b_ecog = _counts(df_ecog_gk)
df_ecog_gk = df_ecog_gk[~(df_ecog_gk["ecog_performance_status"] == "U")]
_log_step("ECOG: 'U' (unbekannt) entfernt", _b_ecog, _counts(df_ecog_gk))

UICC_SOURCES = [
    ("obds", df_uicc_gk),
    ("must", df_uicc_must),
]

# ── Plot 6: Merge Therapie × ECOG/UICC  (nächstes Datum, ohne Toleranz) ───────
print(f"\n  [MERGE] Nearest-date-Merge: 6 Therapie-Staging-Paare (GK, alle Jahre)")
print(f"    Methode: merge_asof (direction=backward, tolerance=None) pro cond_id")
print(f"    Für jede Therapie wird das zeitlich nächste zurückliegende ECOG/UICC gesucht.")
print(f"    ECOG: einmal. UICC: PARALLEL für beide Quellen (_obds / _must).")
print(f"    HINWEIS: Resultierende DFs (df_se, df_su_obds/_must, …) werden nach dem")
print(f"    Jahresfilter (asserted_year >= {YEAR_CUTOFF}) auf cond_ids_17 eingeschränkt")
print(f"    → Basis für 3M-Cutoff (Abschnitt E).")
_merge_args = dict(
    id_col="condition_id_hash",
    therapy_time_col="months_between_asserted_therapy_start_date",
    direction="backward",
    tolerance=None,
    plot=True,
    log_y=True,
    save_path=str(DIR_GK),
)

for _src_label, _df_uicc_src in UICC_SOURCES:
    print(f"    [{_src_label.upper()}] df_ecog_gk ({len(df_ecog_gk):,}) × df_uicc_{_src_label}_time ({len(_df_uicc_src):,} Zeilen)")

def _run_nearest_merge(df_t, df_s, lz_col, title, delta_f):
    df_m, stats = merge_with_nearest_date_matching(
        df_t, df_s, lz_time_col=lz_col, plot_title=title, save_name=delta_f, **_merge_args,
    )
    df_m = (
        df_m.rename(columns={
            "condition_id_hash_x": "condition_id_hash",
            "patient_resource_id_hash_x": "patient_resource_id_hash",
        })
        .drop(columns=["condition_id_hash_y", "patient_resource_id_hash_y"], errors="ignore")
        .dropna(subset=["months_diff"])
    )
    return df_m, stats

scatter_info = []

# ── ECOG-Merges (einmal, unabhängig von UICC-Quelle) ─────────────────────────
df_se, s_se = _run_nearest_merge(df_system_gk, df_ecog_gk, "months_between_asserted_effective_dateTime",
                                 "Systemic Therapy × ECOG", "delta_system_ecog.png")
scatter_info.append((df_se, "months_between_asserted_effective_dateTime", "Systemic Therapy × ECOG", "scatter_system_ecog"))
df_re, s_re = _run_nearest_merge(df_radio_gk, df_ecog_gk, "months_between_asserted_effective_dateTime",
                                 "Radiotherapy × ECOG", "delta_radio_ecog.png")
scatter_info.append((df_re, "months_between_asserted_effective_dateTime", "Radiotherapy × ECOG", "scatter_radio_ecog"))
df_oe, s_oe = _run_nearest_merge(df_ops_gk, df_ecog_gk, "months_between_asserted_effective_dateTime",
                                 "OP × ECOG", "delta_op_ecog.png")
scatter_info.append((df_oe, "months_between_asserted_effective_dateTime", "OP × ECOG", "scatter_op_ecog"))

# ── UICC-Merges (PARALLEL für beide Quellen) ─────────────────────────────────
# Ergebnis-Dicts: df_su_by_src["obds"], df_su_by_src["must"], etc.
df_su_by_src, df_ru_by_src, df_ou_by_src = {}, {}, {}
s_su_by_src, s_ru_by_src, s_ou_by_src = {}, {}, {}
for _src_label, _df_uicc_src in UICC_SOURCES:
    print(f"    [{_src_label.upper()}] UICC-Merges (3 Therapien) mit df_uicc_{_src_label}_time ({len(_df_uicc_src):,} Zeilen)")
    _su, _ssu = _run_nearest_merge(df_system_gk, _df_uicc_src, "months_between_asserted_uicc_tnm_date",
                                   f"Systemic Therapy × UICC ({_src_label})", f"delta_system_uicc_{_src_label}.png")
    _ru, _sru = _run_nearest_merge(df_radio_gk, _df_uicc_src, "months_between_asserted_uicc_tnm_date",
                                   f"Radiotherapy × UICC ({_src_label})", f"delta_radio_uicc_{_src_label}.png")
    _ou, _sou = _run_nearest_merge(df_ops_gk, _df_uicc_src, "months_between_asserted_uicc_tnm_date",
                                   f"OP × UICC ({_src_label})", f"delta_op_uicc_{_src_label}.png")
    df_su_by_src[_src_label], s_su_by_src[_src_label] = _su, _ssu
    df_ru_by_src[_src_label], s_ru_by_src[_src_label] = _ru, _sru
    df_ou_by_src[_src_label], s_ou_by_src[_src_label] = _ou, _sou
    scatter_info.append((_su, "months_between_asserted_uicc_tnm_date", f"Systemic Therapy × UICC ({_src_label})", f"scatter_system_uicc_{_src_label}"))
    scatter_info.append((_ru, "months_between_asserted_uicc_tnm_date", f"Radiotherapy × UICC ({_src_label})", f"scatter_radio_uicc_{_src_label}"))
    scatter_info.append((_ou, "months_between_asserted_uicc_tnm_date", f"OP × UICC ({_src_label})", f"scatter_op_uicc_{_src_label}"))

print(f"\n  [PLOT] Scatter-Plots Therapiepaar → scatter_[typ]_[staging].png")
print(f"    Zeigt: Streuen ECOG/UICC-Datum und Therapiedatum gemeinsam?")

# Scatter-Plots: ECOG/UICC-Datum vs. Therapiedatum
for df_m, lz_col, titel, fname in scatter_info:
    x_col = "months_between_asserted_therapy_start_date"
    scatterplot(
        df_m[x_col],
        df_m[lz_col],
        titel=titel,
        x_label="Therapy start (months)",
        y_label=lz_col,
        farbe="red",
        punktgroesse=5,
        speichern=True,
        dateiname=fname,
        ordner=str(DIR_GK),
    )

print(f"  [PLOT] Panel-Plot Merge-Histogramme → panel_months_diff_[obds/must].png")
# Panel-Plot je UICC-Quelle (ECOG-Einträge identisch, UICC quellenspezifisch)
for _src_label in [s for s, _ in UICC_SOURCES]:
    plot_merge_panel(
        panel_entries=[
            {"df": df_se, "title": "Systemic Therapy × ECOG", "stats": s_se, "staging_label": "ECOG in"},
            {"df": df_su_by_src[_src_label], "title": f"Systemic Therapy × UICC ({_src_label})", "stats": s_su_by_src[_src_label], "staging_label": "UICC in"},
            {"df": df_re, "title": "Radiotherapy × ECOG", "stats": s_re, "staging_label": "ECOG in"},
            {"df": df_ru_by_src[_src_label], "title": f"Radiotherapy × UICC ({_src_label})", "stats": s_ru_by_src[_src_label], "staging_label": "UICC in"},
            {"df": df_oe, "title": "OP × ECOG", "stats": s_oe, "staging_label": "ECOG in"},
            {"df": df_ou_by_src[_src_label], "title": f"OP × UICC ({_src_label})", "stats": s_ou_by_src[_src_label], "staging_label": "UICC in"},
        ],
        log_y=True,
        save_path=str(DIR_GK),
        file_name=f"panel_months_diff_{_src_label}.png",
        dpi=300,
    )

print(f"  ✓ Abschnitt C abgeschlossen  →  {DIR_GK}")

print(f"\n  Merge-Ergebnisse GK (alle Jahre) – Überblick:")
_overview = [
    ("System × ECOG", df_se, s_se),
    ("Radio  × ECOG", df_re, s_re),
    ("OP     × ECOG", df_oe, s_oe),
]
for _src_label in [s for s, _ in UICC_SOURCES]:
    _overview += [
        (f"System × UICC [{_src_label}]", df_su_by_src[_src_label], s_su_by_src[_src_label]),
        (f"Radio  × UICC [{_src_label}]", df_ru_by_src[_src_label], s_ru_by_src[_src_label]),
        (f"OP     × UICC [{_src_label}]", df_ou_by_src[_src_label], s_ou_by_src[_src_label]),
    ]
for label, df_m, s in _overview:
    pct = 100 * s["n_matched"] / s["n_therapy"] if s["n_therapy"] > 0 else 0
    print(
        f"    {label:<22}: {s['n_therapy']:>7,} Therapien → {s['n_matched']:>7,} gematcht ({pct:.1f}%)  |  {s['n_unmatched']:>6,} unmatched"
    )


# ══════════════════════════════════════════════════════════════════════════════
# FILTER 4: asserted_year >= YEAR_CUTOFF
# ══════════════════════════════════════════════════════════════════════════════
# Single Source of Truth ist die oben definierte Konstante YEAR_CUTOFF.
# Der Block erkennt selbst, ob der Schnitt tatsächlich greift, und gibt es
# ehrlich aus (kein hartkodiertes "2017" mehr in den Printouts).

# Ist der Cutoff so niedrig gewählt, dass er nichts entfernen kann?
_year_series = pd.to_numeric(df_tumore["asserted_year"], errors="coerce")
_year_min = int(_year_series.min()) if _year_series.notna().any() else YEAR_CUTOFF
_cutoff_active = YEAR_CUTOFF > _year_min

print("\n" + "━" * 70)
print(f"FILTER 4  –  Jahresschnitt asserted_year >= {YEAR_CUTOFF}")
if not _cutoff_active:
    print(f"  *** AKTUELL DEAKTIVIERT ***")
    print(f"  YEAR_CUTOFF={YEAR_CUTOFF} liegt am/unter dem frühesten Diagnosejahr")
    print(f"  in den Daten ({_year_min}) → es wird praktisch nichts entfernt.")
    print(f"  (Zum Reaktivieren oben YEAR_CUTOFF = 2017 setzen.)")
else:
    print(f"  Rationale: Vor {YEAR_CUTOFF} inkonsistente Meldepflicht → Completeness-Bias.")
print("  Alle nachgelagerten DataFrames werden auf diesen Schnitt reduziert.")
print("━" * 70)

_step_pre_year = _counts(df_tumore)
df_tumore_17 = df_tumore[_year_series >= YEAR_CUTOFF]
cond_ids_17 = df_tumore_17["condition_id_hash"]
_log_step(f"asserted_year >= {YEAR_CUTOFF}", _step_pre_year, _counts(df_tumore_17))

print(f"\n  Slave-DataFrames auf cond_ids_17 einschränken:")
df_ops_17 = df_ops_gk[df_ops_gk["condition_id_hash"].isin(cond_ids_17)]
df_radio_17 = df_radio_gk[df_radio_gk["condition_id_hash"].isin(cond_ids_17)]
df_system_17 = df_system_gk[df_system_gk["condition_id_hash"].isin(cond_ids_17)]
df_ecog_17 = df_ecog_gk[df_ecog_gk["condition_id_hash"].isin(cond_ids_17)]
# UICC ≥ Cutoff je Quelle (für die Sweeps, parallel)
df_uicc_17_by_src = {
    _src: _df[_df["condition_id_hash"].isin(cond_ids_17)]
    for _src, _df in UICC_SOURCES
}
print(
    f"    df_ops_17:    {len(df_ops_17):>8,} Zeilen  |  cond_ids: {df_ops_17['condition_id_hash'].nunique():,}"
)
print(
    f"    df_radio_17:  {len(df_radio_17):>8,} Zeilen  |  cond_ids: {df_radio_17['condition_id_hash'].nunique():,}"
)
print(
    f"    df_system_17: {len(df_system_17):>8,} Zeilen  |  cond_ids: {df_system_17['condition_id_hash'].nunique():,}"
)
print(
    f"    df_ecog_17:   {len(df_ecog_17):>8,} Zeilen  |  cond_ids: {df_ecog_17['condition_id_hash'].nunique():,}"
)
for _src_label in [s for s, _ in UICC_SOURCES]:
    _u17 = df_uicc_17_by_src[_src_label]
    print(
        f"    df_uicc_17 [{_src_label}]: {len(_u17):>8,} Zeilen  |  cond_ids: {_u17['condition_id_hash'].nunique():,}"
    )

print(f"\n  Merge-Ergebnisse aus GK auf cond_ids_17 einschränken")
print(f"  (df_se→df_se_17 etc. – diese sind die Basis für den 3M-Cutoff in Abschnitt E):")
df_se_17 = df_se[df_se["condition_id_hash"].isin(cond_ids_17)]
df_re_17 = df_re[df_re["condition_id_hash"].isin(cond_ids_17)]
df_oe_17 = df_oe[df_oe["condition_id_hash"].isin(cond_ids_17)]
# UICC-Merge-Ergebnisse je Quelle auf cond_ids_17 (Jahresfilter) einschränken
df_su_17_by_src, df_ru_17_by_src, df_ou_17_by_src = {}, {}, {}
for _src_label in [s for s, _ in UICC_SOURCES]:
    df_su_17_by_src[_src_label] = df_su_by_src[_src_label][df_su_by_src[_src_label]["condition_id_hash"].isin(cond_ids_17)]
    df_ru_17_by_src[_src_label] = df_ru_by_src[_src_label][df_ru_by_src[_src_label]["condition_id_hash"].isin(cond_ids_17)]
    df_ou_17_by_src[_src_label] = df_ou_by_src[_src_label][df_ou_by_src[_src_label]["condition_id_hash"].isin(cond_ids_17)]
_overview17 = [
    ("System × ECOG", df_se_17),
    ("Radio  × ECOG", df_re_17),
    ("OP     × ECOG", df_oe_17),
]
for _src_label in [s for s, _ in UICC_SOURCES]:
    _overview17 += [
        (f"System × UICC [{_src_label}]", df_su_17_by_src[_src_label]),
        (f"Radio  × UICC [{_src_label}]", df_ru_17_by_src[_src_label]),
        (f"OP     × UICC [{_src_label}]", df_ou_17_by_src[_src_label]),
    ]
for label, df_m in _overview17:
    print(f"    {label:<22}: {len(df_m):>7,} Zeilen verbleiben")


# ══════════════════════════════════════════════════════════════════════════════
# ABSCHNITT D  –  GK_17  (alle Therapien, asserted_year >= YEAR_CUTOFF)
# ══════════════════════════════════════════════════════════════════════════════

DIR_GK_17.mkdir(parents=True, exist_ok=True)

print("\n" + "━" * 70)
print(f"ABSCHNITT D  –  GK_17  (alle Therapien, asserted_year >= {YEAR_CUTOFF})")
print(
    f"  Kohorte: df_tumore_17  |  {cond_ids_17.nunique():,} cond_ids  |  "
    f"{df_tumore_17['patient_resource_id_hash'].nunique():,} Patienten"
)
print(f"  Fokus: Zeitlicher Abstand Diagnose → Therapie + ECOG/UICC-Matching-Problemraum")
print("━" * 70)

df_alle_17 = pd.concat(
    [df_radio_17[_TCOLS], df_system_17[_TCOLS], df_ops_17[_TCOLS]],
    ignore_index=True,
)
print(f"\n  df_alle_17: {len(df_alle_17):,} Therapiezeilen gesamt")

# ── Plot 1: Violin-Plots  (gesamt + je Therapietyp) ───────────────────────────
print(f"\n  [PLOT] Violin-Plots Therapiezeitpunkte relativ zur Diagnose → violin_*.png")
print(f"    Zeigt: Verteilung des Zeitabstands Erstdiagnose → Therapiestart (Monate)")
print(f"    Negative Werte = Therapie vor formaler Diagnose (Datenkurationsproblem)")
plot_therapy_times(
    df_alle_17,
    rel_time_col="months_between_asserted_therapy_start_date",
    title="Alle Therapien – Zeitpunkt relativ zur Diagnose",
    save_path=str(DIR_GK_17 / "violin_alle_therapien.png"),
    allow_negative=True,
)
for df_t, fname in [
    (df_ops_17, "violin_op.png"),
    (df_radio_17, "violin_radio.png"),
    (df_system_17, "violin_system.png"),
]:
    plot_therapy_times(
        df_t,
        rel_time_col="months_between_asserted_therapy_start_date",
        save_path=str(DIR_GK_17 / fname),
        allow_negative=True,
    )

# ── Plot 2: Dropout-Kurven  (je Therapietyp) ─────────────────────────────────
print(f"\n  [PLOT] Dropout-Kurven (Cutoff-Sweep 0–12 Monate) → dropout_*.png")
print(f"    Zeigt: Wieviele cond_ids haben eine Therapie innerhalb von X Monaten nach Diagnose?")
print(f"    Highlight: 3 Monate (gewählter Cutoff)")
for df_t, fname in [
    (df_ops_17, "dropout_op.png"),
    (df_radio_17, "dropout_radio.png"),
    (df_system_17, "dropout_system.png"),
]:
    plot_dropout_curve(
        patient_dropout_by_cutoff(
            df_t,
            "condition_id_hash",
            "months_between_asserted_therapy_start_date",
            max_month=12,
        ),
        speichern=True,
        highlight_month=3,
        ordner=str(DIR_GK_17),
        dateiname=fname,
    )

# ── Plot 3: Toleranz-Sweeps – ECOG einmal, UICC parallel (oBDS + MUST) ───────
print(f"\n  [PLOT] Toleranz-Sweeps 0–12 Monate → sweep_*.png + sweep_panel_[obds/must].png")
print(f"    Inputs: df_*_17 Therapien × df_ecog_17 / df_uicc_17_by_src")
print(f"    Zeigt: Match-Rate (%) bei zunehmendem Toleranzfenster zwischen Therapie und Staging")
print(f"    ECOG-Sweeps einmal; UICC-Sweeps PARALLEL je Quelle (_obds / _must).")
_tols = range(0, 13)
df_sweeps = {}

# ECOG-Sweeps (einmal)
_ecog_sweep_specs = [
    (df_system_17, df_ecog_17, "System → ECOG", "sweep_system_ecog.png", len(df_system_17)),
    (df_radio_17,  df_ecog_17, "Radio → ECOG",  "sweep_radio_ecog.png",  len(df_radio_17)),
    (df_ops_17,    df_ecog_17, "OP → ECOG",     "sweep_op_ecog.png",     len(df_ops_17)),
]
for df_t, df_s, title, fname, n_t in _ecog_sweep_specs:
    df_sweeps[fname] = sweep_tolerance_fast(
        df_t, df_s, lz_time_col="months_between_asserted_effective_dateTime",
        tolerances=_tols, save_plot=True, save_dir=str(DIR_GK_17),
        file_name=fname, title=title, highlight_tolerance=3,
    )

# UICC-Sweeps (je Quelle)
for _src_label in [s for s, _ in UICC_SOURCES]:
    _u17 = df_uicc_17_by_src[_src_label]
    print(f"    [{_src_label.upper()}] UICC-Sweeps mit df_uicc_17 ({len(_u17):,} Zeilen)")
    for df_t, title, fname in [
        (df_system_17, f"System → UICC ({_src_label})", f"sweep_system_uicc_{_src_label}.png"),
        (df_radio_17,  f"Radio → UICC ({_src_label})",  f"sweep_radio_uicc_{_src_label}.png"),
        (df_ops_17,    f"OP → UICC ({_src_label})",     f"sweep_op_uicc_{_src_label}.png"),
    ]:
        df_sweeps[fname] = sweep_tolerance_fast(
            df_t, _u17, lz_time_col="months_between_asserted_uicc_tnm_date",
            tolerances=_tols, save_plot=True, save_dir=str(DIR_GK_17),
            file_name=fname, title=title, highlight_tolerance=3,
        )

# Panel je UICC-Quelle (ECOG-Sweeps identisch, UICC quellenspezifisch)
for _src_label in [s for s, _ in UICC_SOURCES]:
    plot_sweep_panel(
        sweeps=[
            {"df": df_sweeps["sweep_system_ecog.png"], "therapy": "System Therapy", "pair": "ECOG", "highlight": 3, "n_total": len(df_system_17)},
            {"df": df_sweeps[f"sweep_system_uicc_{_src_label}.png"], "therapy": "System Therapy", "pair": f"UICC ({_src_label})", "highlight": 3, "n_total": len(df_system_17)},
            {"df": df_sweeps["sweep_radio_ecog.png"], "therapy": "Radiotherapy", "pair": "ECOG", "highlight": 3, "n_total": len(df_radio_17)},
            {"df": df_sweeps[f"sweep_radio_uicc_{_src_label}.png"], "therapy": "Radiotherapy", "pair": f"UICC ({_src_label})", "highlight": 3, "n_total": len(df_radio_17)},
            {"df": df_sweeps["sweep_op_ecog.png"], "therapy": "OP", "pair": "ECOG", "highlight": 3, "n_total": len(df_ops_17)},
            {"df": df_sweeps[f"sweep_op_uicc_{_src_label}.png"], "therapy": "OP", "pair": f"UICC ({_src_label})", "highlight": 3, "n_total": len(df_ops_17)},
        ],
        save_plot=True,
        save_dir=str(DIR_GK_17),
        file_name=f"sweep_panel_{_src_label}.png",
        dpi=300,
    )


print(f"  ✓ Abschnitt D abgeschlossen  →  {DIR_GK_17}")


# ══════════════════════════════════════════════════════════════════════════════
# FILTER 5  +  ABSCHNITT E  –  3M-CUTOFF  (Unterordner 3M/)
# ══════════════════════════════════════════════════════════════════════════════

DIR_3M = DIR_GK_17 / "3M"
DIR_3M.mkdir(exist_ok=True)

print("\n" + "━" * 70)
print("FILTER 5  +  ABSCHNITT E  –  3M-Cutoff  (months_diff ≤ 3.0)")
print("  Rationale: Nur Therapie-Staging-Paare mit ≤ 3 Monaten Zeitabstand gelten")
print("  als klinisch valides Staging für die jeweilige Therapie.")
print("  WICHTIG: Die THERAPIEN selbst werden NICHT gefiltert.")
print("  Nur die Therapie-Staging-PAARE werden eingeschränkt.")
print("  Basis: df_*_17 (Merge-Ergebnisse GK, eingeschränkt auf cond_ids_17)")
print("━" * 70)


def _cut3m(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["months_diff"] <= 3.0].copy()


# ECOG-3M (einmal)
df_se_3m = _cut3m(df_se_17)
df_re_3m = _cut3m(df_re_17)
df_oe_3m = _cut3m(df_oe_17)
# UICC-3M je Quelle
df_su_3m_by_src, df_ru_3m_by_src, df_ou_3m_by_src = {}, {}, {}
for _src_label in [s for s, _ in UICC_SOURCES]:
    df_su_3m_by_src[_src_label] = _cut3m(df_su_17_by_src[_src_label])
    df_ru_3m_by_src[_src_label] = _cut3m(df_ru_17_by_src[_src_label])
    df_ou_3m_by_src[_src_label] = _cut3m(df_ou_17_by_src[_src_label])

print(f"\n  3M-Cutoff Ergebnis – verbleibende Therapie-Staging-Paare:")
_cut_overview = [
    ("System × ECOG", df_se_17, df_se_3m),
    ("Radio  × ECOG", df_re_17, df_re_3m),
    ("OP     × ECOG", df_oe_17, df_oe_3m),
]
for _src_label in [s for s, _ in UICC_SOURCES]:
    _cut_overview += [
        (f"System × UICC [{_src_label}]", df_su_17_by_src[_src_label], df_su_3m_by_src[_src_label]),
        (f"Radio  × UICC [{_src_label}]", df_ru_17_by_src[_src_label], df_ru_3m_by_src[_src_label]),
        (f"OP     × UICC [{_src_label}]", df_ou_17_by_src[_src_label], df_ou_3m_by_src[_src_label]),
    ]
for label, df_before, df_after in _cut_overview:
    pct = 100 * len(df_after) / len(df_before) if len(df_before) > 0 else 0
    print(f"    {label:<22}: {len(df_before):>7,} → {len(df_after):>7,} Paare  ({pct:.1f}% verbleiben)")

# ── UICC-Verteilung  PARALLEL je Quelle ──────────────────────────────────────
print(f"\n  [PLOT] UICC-Staging-Verteilung je Therapietyp → uicc_distribution_[obds/must].png")

# Nenner = ALLE Therapien des jeweiligen Typs (vor Matching/3M-Cutoff/Ausschluss).
# Dadurch zeigen die Balken den Anteil der Therapien mit NUTZBARER UICC/ECOG;
# 100% = Gesamtzahl der Therapien (nicht die gematchten). Für oBDS und MUST
# identisch, da nur die Staging-QUELLE wechselt, die Therapien aber dieselben sind
# → der Coverage-Vorteil von MUST wird so sichtbar.
# Hinweis: len(df_*_17) zählt ALLE Therapiezeilen des Typs. Wer stattdessen nur
# die matching-fähigen (mit gültigem Therapiedatum) als Basis möchte, kann hier
# den jeweiligen stats["n_therapy"]-Wert aus dem Merge einsetzen.
_therapy_totals = {
    "Surgical Procedure": len(df_ops_17),
    "Systemic Therapy": len(df_system_17),
    "Radiation Therapy": len(df_radio_17),
}
print(f"    Gesamt-Therapien (Nenner, 100%): "
      f"OP={_therapy_totals['Surgical Procedure']:,}, "
      f"System={_therapy_totals['Systemic Therapy']:,}, "
      f"Radio={_therapy_totals['Radiation Therapy']:,}")

for _src_label in [s for s, _ in UICC_SOURCES]:
    print(f"    [{_src_label.upper()}] df_ou/su/ru_3m ({_src_label})")
    plot_uicc_distribution_grouped_bar(
        dataframes={
            "Surgical Procedure": df_ou_3m_by_src[_src_label],
            "Systemic Therapy": df_su_3m_by_src[_src_label],
            "Radiation Therapy": df_ru_3m_by_src[_src_label],
        },
        totals=_therapy_totals,
        save_path=str(DIR_3M / f"uicc_distribution_{_src_label}.png"),
    )


# ── ECOG-Verteilung  ─────────────────────────────────────────────────────────
print(f"  [PLOT] ECOG-Performance-Verteilung je Therapietyp → ecog_distribution.png")
print(f"    Inputs: df_oe/se/re_3m (3M-gefilterte Paare)")
plot_ecog_distribution_grouped_bar(
    dataframes={
        "Surgical Procedure": df_oe_3m,
        "Systemic Therapy": df_se_3m,
        "Radiation Therapy": df_re_3m,
    },
    totals=_therapy_totals,
    save_path=str(DIR_3M / "ecog_distribution.png"),
)
#combined_panelplots
fig_panel = create_ecog_uicc_panel(
    df_ecog={
        "Surgical Procedure": df_oe_3m,
        "Systemic Therapy": df_se_3m,
        "Radiation Therapy": df_re_3m,
    },
    df_uicc_obds={
        "Surgical Procedure": df_ou_3m_by_src["obds"],
        "Systemic Therapy": df_su_3m_by_src["obds"],
        "Radiation Therapy": df_ru_3m_by_src["obds"],
    },
    df_uicc_must={
        "Surgical Procedure": df_ou_3m_by_src["must"],
        "Systemic Therapy": df_su_3m_by_src["must"],
        "Radiation Therapy": df_ru_3m_by_src["must"],
    },
    totals=_therapy_totals,
    save_path=str(DIR_3M / "ecog_uicc_combined_panel.png"),
    nrows=3, ncols=1, figsize=(8, 11), wspace=0.15, hspace = 0.45,
 # Subplot-Orientierung
    fontsize_axis_label=11, fontsize_tick_label=10,  # Schriftgrößen
    fontsize_subplot_title=13, fontsize_legend=9,
    titles=("A) ECOG", "B) UICC (oBDS)", "C) UICC (MUST)"),
    show_titles=True,
    fontsize_bar_label=8,
)

# ── Altersverteilung nach 3M-Cutoff ──────────────────────────────────────────
print(f"  [PLOT] Altersverteilung nach 3M-Match → age_dist_5yr.png / age_dist_10yr.png")
fig = plot_age_distribution_grouped_bar(
    dataframes={
        "Surgical Procedure": df_oe_3m,
        "Systemic Therapy": df_se_3m,
        "Radiation Therapy": df_re_3m,
    },
    age_column="age_at_diagnosis",
    age_bracket=5,
    title=False,
    save_path=str(DIR_3M / "age_dist_5yr.png"),
    show_bar_numbers=True,       # erzwingt Anzeige, egal was PLOT_CONFIG sagt
    color_shade=1,                # 0=gedeckt, 1=kräftiger, 2/3=heller
    show_total_in_legend=True,
)





# ── Violin-Plots nach 3M-Cutoff ──────────────────────────────────────────────
print(f"  [PLOT] Violin-Plots Therapiezeitpunkte (3M-Match) → violin_*.png")
df_alle_3m = pd.concat(
    [
        df_re_3m[[c for c in _TCOLS if c in df_re_3m.columns]],
        df_se_3m[[c for c in _TCOLS if c in df_se_3m.columns]],
        df_oe_3m[[c for c in _TCOLS if c in df_oe_3m.columns]],
    ],
    ignore_index=True,
)
plot_therapy_times(
    df_alle_3m,
    rel_time_col="months_between_asserted_therapy_start_date",
    title="Alle Therapien (3M-Match) – Zeitpunkt relativ zur Diagnose",
    save_path=str(DIR_3M / "violin_alle_therapien.png"),
    allow_negative=True,
)
for df_m, fname in [
    (df_oe_3m, "violin_op.png"),
    (df_re_3m, "violin_radio.png"),
    (df_se_3m, "violin_system.png"),
]:
    plot_therapy_times(
        df_m,
        rel_time_col="months_between_asserted_therapy_start_date",
        save_path=str(DIR_3M / fname),
        allow_negative=True,
    )

print(f"  ✓ Abschnitt E abgeschlossen  →  {DIR_3M}")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE-ZUSAMMENFASSUNG
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "━" * 70)
print("PIPELINE-ZUSAMMENFASSUNG")
print("━" * 70)
print(f"  {'Schritt':<46}{'cond_ids':>12}{'Patienten':>12}")
print("  " + "─" * 68)
print(f"  {'Eingabe: alle oBDS-Karzinom-cond_ids':<46}{_step0[1]:>12,}{_step0[2]:>12,}")
print(f"  {'Filter 1: D-Diagnosen entfernt':<46}{_step1[1]:>12,}{_step1[2]:>12,}")
print(f"  {'Filter 2: C44 entfernt':<46}{_step2[1]:>12,}{_step2[2]:>12,}")
print(f"  {'Filter 2b: Minderjährige (<18) entfernt':<46}{_step2b[1]:>12,}{_step2b[2]:>12,}")
print(f"  {'Filter 3: Top-20 Entitäten → GRUNDKOHORTE (GK)':<46}{_step3[1]:>12,}{_step3[2]:>12,}")
_f4_label = (
    f"Filter 4: asserted_year >= {YEAR_CUTOFF} → GK_17"
    + ("" if _cutoff_active else "  [DEAKTIVIERT]")
)
print(f"  {_f4_label:<46}{cond_ids_17.nunique():>12,}{df_tumore_17['patient_resource_id_hash'].nunique():>12,}")
print("  " + "─" * 68)
print(f"  Filter 5: 3M-Cutoff ECOG/UICC zu Therapie (valide Therapie×Staging-Paare):")
print(f"    {'System × ECOG':<30}{len(df_se_3m):>9,} Paare")
print(f"    {'Radio  × ECOG':<30}{len(df_re_3m):>9,} Paare")
print(f"    {'OP     × ECOG':<30}{len(df_oe_3m):>9,} Paare")
for _src_label in [s for s, _ in UICC_SOURCES]:
    print(f"    {f'System × UICC [{_src_label}]':<30}{len(df_su_3m_by_src[_src_label]):>9,} Paare")
    print(f"    {f'Radio  × UICC [{_src_label}]':<30}{len(df_ru_3m_by_src[_src_label]):>9,} Paare")
    print(f"    {f'OP     × UICC [{_src_label}]':<30}{len(df_ou_3m_by_src[_src_label]):>9,} Paare")
print("━" * 70)
print("\n✓ Pipeline vollständig abgeschlossen.")
