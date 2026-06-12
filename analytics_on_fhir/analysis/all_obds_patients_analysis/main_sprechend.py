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
)

# Butterfly-Plots (demographischer Alters-/Geschlechts-Baum)
from PlotsICDDiagzuAlter import plot_population_pyramid_from_raw, plot_population_pyramid_topn
from SecondPaper_NebenDiagnosen_Plots import LevelConfig, run_nebendiagnosen_report

# TODO: Must hinzufügen und VGL Plot  <- warten auf anpassung
# TODO: CAST DTYPES AFTER READING PARQUET FILES!
# TODO: Ändere Plots auf high resolution PNG


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING-HELPER
# ──────────────────────────────────────────────────────────────────────────────


def _log_load(
    label: str,
    df: pd.DataFrame,
    cond_col: str = "condition_id_hash",
    pat_col: str = "patient_resource_id_hash",
) -> None:
    """Gibt nach dem Laden eines DataFrames Zeilen / cond_ids / Patienten aus."""
    n_cond = df[cond_col].nunique() if cond_col in df.columns else None
    n_pat = df[pat_col].nunique() if pat_col in df.columns else None
    cond_str = f"{n_cond:>8,}" if n_cond is not None else "       –"
    pat_str = f"{n_pat:>8,}" if n_pat is not None else "       –"
    print(f"    {label}: {len(df):>9,} Zeilen  |  cond_ids: {cond_str}  |  Patienten: {pat_str}")


def _log_filter(label: str, n_before: int, n_after: int, unit: str = "cond_ids") -> None:
    """Druckt Filtereffekt als Vorher → Nachher."""
    pct = 100 * n_after / n_before if n_before > 0 else 0
    print(f"    Filter '{label}': {n_before:,} → {n_after:,} {unit}  ({pct:.1f}% verbleiben)")


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


SITES = "UKER, TUM, UKA, LMU, UKR, UKW"
#SITES = "UKER"

asserted_year = 1970

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
print(paths)
dfs = {}

for dataset_name, dataset_paths in paths.items():
    tmp_dfs = []

    for path, site in zip(dataset_paths, site_list):
        #df = pd.read_parquet(path, engine="pyarrow")
        df = pd.read_parquet(path)

        # Standort mitführen (für Nachforschung Nebendiagnosen / UKW-Diagnose)
        df["site"] = site

        #print("\nFILE:", path)
        tmp_dfs.append(df)

    dfs[dataset_name] = pd.concat(
        tmp_dfs,
        ignore_index=True,
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

df_obds = df_obds[~df_obds["icd10_code"].str.startswith(("C44", "D"), na=False)]
print(df_obds.shape[0])
df_obds = df_obds[df_obds["asserted_year"] >= asserted_year]
print(df_obds.shape[0])
df_obds.shape[0]


DATA = Path(
    r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\resources"
)
PLOTS = Path(
    r"C:\Users\boehnesn1\Desktop\Projects\BZKF_GIT\plots"
)  # TODO:Ändern

# Namensschema:
#   01_GK     = Grundkohorte  (Top-20 Entitäten, kein D/C44, nur K&P)  – alle Jahre
#   02_GK_17  = + Filter asserted_year >= 2017
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
) -> None:
    """
    Erzeugt das UICC/ECOG-Inventar-Plot für die GK-Quelle und – sofern
    df_uicc_must übergeben wird – ein zweites Plot für Brigitte's MUST-Tool.

    Erwartete Spalten in df_uicc_must: condition_id_hash, uicc_tnm
    (d.h. bereits umbenannt und auf die Kohorte gefiltert).

    Ausgabe:
        out_dir/uicc_ecog_inventory.tiff       – GK-Quelle (immer)
        out_dir/uicc_ecog_inventory_must.tiff  – MUST-Quelle (wenn übergeben)
    """
    _inv_kwargs = dict(
        df_ecog=df_ecog,
        uicc_col="uicc_tnm",
        ecog_col="ecog_performance_status",
        cond_col="condition_id_hash",
        n_total_cond_ids=n_total,
    )

    plot_uicc_ecog_inventory(
        df_uicc=df_uicc_gk,
        output_path=out_dir / "uicc_ecog_inventory.tiff",
        **_inv_kwargs,
    )

    if df_uicc_must is not None:
        plot_uicc_ecog_inventory(
            df_uicc=df_uicc_must,
            output_path=out_dir / "uicc_ecog_inventory_must.tiff",
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
_n0_rows = len(df_tumore)
_n0_cond = df_tumore["condition_id_hash"].nunique()
_n0_pat = df_tumore["patient_resource_id_hash"].nunique()
print(f"    Zeilen: {_n0_rows:,}  |  cond_ids: {_n0_cond:,}  |  Patienten: {_n0_pat:,}")

# ── Filter 1: D-Diagnosen entfernen (gutartige Neubildungen) ──────────────────
print("\n  [FILTER 1] Entferne D-Diagnosen (icd10_parent_code enthält 'D'):")
df_tumore = df_tumore[~df_tumore["icd10_parent_code"].str.contains("D", na=False)]
_n1_cond = df_tumore["condition_id_hash"].nunique()
_log_filter("D-Diagnosen entfernt", _n0_cond, _n1_cond)

# ── Filter 2: C44 entfernen (Hauttumoren – keine systemische Relevanz) ────────
print("\n  [FILTER 2] Entferne C44 (Bösartige Neubildungen der Haut):")
df_tumore = df_tumore[
    ~df_tumore[["icd10_parent_code"]]
    .astype(str)
    .apply(lambda s: s.str.contains("C44", na=False))
    .any(axis=1)
]
_n2_cond = df_tumore["condition_id_hash"].nunique()
_log_filter("C44 entfernt", _n1_cond, _n2_cond)

# ── Filter 2b: Minderjährige entfernen (age_at_diagnosis < 18) ────────────────
# Wirkt auf die GESAMTKOHORTE, also vor Butterfly-Plot und Top-20-Bestimmung.
print("\n  [FILTER 2b] Entferne minderjährige Patienten (age_at_diagnosis < 18):")
_age_num = pd.to_numeric(df_tumore["age_at_diagnosis"], errors="coerce")
_n_underage_rows = int((_age_num < 18).sum())
_n_underage_pat  = df_tumore.loc[_age_num < 18, "patient_resource_id_hash"].nunique()
df_tumore = df_tumore[_age_num >= 18]
_n2b_cond = df_tumore["condition_id_hash"].nunique()
print(f"    Entfernt: {_n_underage_rows:,} Zeilen  |  {_n_underage_pat:,} minderjährige Patienten")
_log_filter("Minderjährige (<18) entfernt", _n2_cond, _n2b_cond)

# ── ICD-Klartextnamen ergänzen (Lookup aus DWH) ───────────────────────────────
print("\n  [MERGE] ICD-Klartextnamen (DWH_ICD_CODE_MAPPING) → left join auf icd10_parent_code:")
_icd_lookup = pd.read_parquet(os.path.join(DATA, "DWH_ICD_CODE_MAPPING.parquet"))[
    ["ICD3_CODE", "ICD3_NAME"]
].drop_duplicates()
df_tumore = df_tumore.merge(
    _icd_lookup, left_on="icd10_parent_code", right_on="ICD3_CODE", how="left"
)

# ══════════════════════════════════════════════════════════════════════════════
# ABSCHNITT A  –  BUTTERFLY-PLOT  (Demographischer Alters-/Geschlechts-Baum)
# Grundkohorte: df_tumore (Top-20 Entitäten, kein D/C44).
# Kein Jahres- oder Therapiefilter – vollständige demographische Übersicht.
# ══════════════════════════════════════════════════════════════════════════════

DIR_BUTTERFLY.mkdir(parents=True, exist_ok=True)

print("\n" + "━" * 70)
print("ABSCHNITT A  –  BUTTERFLY-PLOT  (Demographischer Alters-/Geschlechts-Baum)")
print("━" * 70)
print(f"  Kohorte: df_tumore NACH D/C44-Filter, VOR Top-20-Filter")
print(
    f"  cond_ids: {df_tumore['condition_id_hash'].nunique():,}  |  "
    f"Patienten: {df_tumore['patient_resource_id_hash'].nunique():,}  |  "
    f"Entitäten (entity_or_parent): {df_tumore['entity_or_parent'].nunique():,}"
)

DIR_BUTTERFLY.mkdir(parents=True, exist_ok=True)

print(f"\n  [PLOT] butterfly_overall.tiff  – Alters-/Geschlechtsverteilung, alle Entitäten")
plot_population_pyramid_from_raw(
    df_tumore,
    age_col="age_at_diagnosis",
    sex_col="gender",
    title="Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\nGesamt Population UKer",
    show_title=False,
    output_path=DIR_BUTTERFLY / "butterfly_overall.tiff",
)

print(
    f"  [PLOT] butterfly_topn.tiff  – Alters-/Geschlechtsverteilung, Top-20 Entitäten farbkodiert"
)
plot_population_pyramid_topn(
    df_tumore,
    age_col="age_at_diagnosis",
    sex_col="gender",
    diagnosis_col="entity_or_parent",
    top_n=20,
    title=(
        "Butterfly Plot\nAlter bei Diagnose, aller C Diagnosen\n"
        "Gesamt Population UKer\n"
        "Aufgeteilt nach TOP 20 meist vorkommende C-ICD-Diagnosen"
    ),
    show_title=False,
    output_path=DIR_BUTTERFLY / "butterfly_topn.tiff",
)

print(f"  ✓ Abschnitt A abgeschlossen  →  {DIR_BUTTERFLY}")


print("\n" + "━" * 70)
print("FILTER 3  –  Top-20 Tumor-Entitäten (entity_or_parent)")
print("━" * 70)
print(f"  Rationale: Fokus auf häufige Tumorentitäten mit ausreichender Fallzahl")
_n_pre_top20 = df_tumore["condition_id_hash"].nunique()
df_tumore = df_tumore[
    df_tumore["entity_or_parent"].isin(df_tumore["entity_or_parent"].value_counts().iloc[:20].index)
]
cond_ids_gk = df_tumore["condition_id_hash"]
_n3_cond = cond_ids_gk.nunique()
_n3_pat = df_tumore["patient_resource_id_hash"].nunique()
_log_filter("Top-20 Entitäten behalten", _n_pre_top20, _n3_cond)
print(f"  ─ GRUNDKOHORTE (GK) festgelegt ─────────────────────────────────────")
print(f"  Master-DF: df_tumore  →  {_n3_cond:,} cond_ids  |  {_n3_pat:,} Patienten")
print(f"  Alle nachgelagerten DataFrames werden auf diese cond_ids gefiltert.")
_top20_entities = df_tumore["entity_or_parent"].value_counts().iloc[:20]
print(f"  Top-20 Entitäten:")
for ent, cnt in _top20_entities.items():
    print(f"    {ent:<45} n={cnt:,}")


print("\n" + "━" * 70)
print("DATEN LADEN – SLAVE-TABELLEN: UICC, ECOG, THERAPIEN")
print("  (Alle auf GK gefiltert via cond_ids_gk / patient_resource_id_hash)")
print("━" * 70)

# ── UICC  GRUNDKOHORTE oBDS = df_uicc_tnm + weitere_klassifikation ────────────
print("\n  [LOAD] UICC-Staging oBDS – Quelle 1a: df_uicc_tnm_deidentified.parquet")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")
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
_wk_with_time = _wk_uicc["months_between_asserted_uicc_tnm_date"].notna().sum() if "months_between_asserted_uicc_tnm_date" in _wk_uicc.columns else 0
print(
    f"    weitere_klassifikation → UICC-Einträge: {len(_wk_uicc):,} Zeilen  |  cond_ids: {_wk_uicc['condition_id_hash'].nunique():,}"
)
print(f"      davon mit Zeitstempel (nach Umbenennung): {_wk_with_time:,}")

df_uicc_raw = df_uicc_tnm
df_uicc_raw = df_uicc_raw[df_uicc_raw["condition_id_hash"].isin(cond_ids_gk)]
_n_uicc_before_drop = len(df_uicc_raw)
df_uicc_raw["uicc_tnm"] = df_uicc_raw["uicc_tnm"].fillna("missing")
df_uicc_raw = (
    df_uicc_raw.dropna(
        subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"]
    )
    .sort_values(["condition_id_hash", "months_between_asserted_uicc_tnm_date"])
    .loc[:, lambda d: ~d.columns.duplicated()]
)
print(
    f"    df_uicc_tnm: {_n_uicc_before_drop:,} Zeilen geladen  →  {len(df_uicc_raw):,} nach dropna  |  cond_ids: {df_uicc_raw['condition_id_hash'].nunique():,}"
)
print(f"    NaN-UICC-Werte wurden als 'missing' kodiert.")

print(f"\n  [MERGE] oBDS-Grundkohorte: df_uicc_tnm + weitere_klassifikation (UICC) → df_uicc_gk")
df_uicc_gk = pd.concat([df_uicc_raw, _wk_uicc], ignore_index=True, sort=False)
df_uicc_gk["uicc_tnm"] = df_uicc_gk["uicc_tnm"].fillna("missing")
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

# ── Zeit-aufbereitete Varianten BEIDER Quellen (parallel) ────────────────────
# Beide Quellen werden identisch aufbereitet: dropna auf (uicc, cond_id, Zeit) + sort.
# Sie laufen anschließend PARALLEL durch alle UICC-Analysen, gekennzeichnet
# mit _obds / _must. Der Vergleichsplot zeigt die Differenz.
def _prep_uicc_time(df, label):
    out = (
        df.replace({"uicc_tnm": {"missing": np.nan}})
        .dropna(subset=["uicc_tnm", "condition_id_hash", "months_between_asserted_uicc_tnm_date"])
        .sort_values(["condition_id_hash", "months_between_asserted_uicc_tnm_date"])
        .loc[:, lambda d: ~d.columns.duplicated()]
    )
    print(f"    df_uicc_{label}_time: {len(out):,} Zeilen  |  cond_ids: {out['condition_id_hash'].nunique():,}")
    return out

print(f"\n  [PREP] Beide UICC-Quellen für zeitbasierte Merges aufbereitet (dropna + sort):")
df_uicc_obds_time = _prep_uicc_time(df_uicc_gk, "obds")
df_uicc_must_time = _prep_uicc_time(df_uicc_must, "must")

# Parallel-Quellenliste: über diese wird jede UICC-Analyse doppelt gefahren.
UICC_SOURCES = [
    ("obds", df_uicc_obds_time),
    ("must", df_uicc_must_time),
]
print(f"    → Parallelbetrieb: beide Quellen laufen durch alle UICC-Plots/Merges (_obds / _must).")

# HINWEIS zur Rollenverteilung der UICC-Quellen:
#   • oBDS (df_uicc_gk = df_uicc_tnm + weitere_klassifikation) und MUST (df_uicc_must)
#     sind ZWEI EIGENSTÄNDIGE, vollständige Quellen.
#   • Beide laufen parallel durch alle Analysen, gekennzeichnet _obds / _must.
#   • Der Vergleichsplot plot_uicc_inventory_comparison zeigt die Differenz.
# Es wird NICHT konkateniert (MUST mischt sich nicht mit oBDS).


# ══════════════════════════════════════════════════════════════════════════════
# [DEBUG] UICC-QUELLENVERGLEICH: oBDS vs. MUST  (Kopf-an-Kopf, zeitbasiert)
# Antwortet auf: Warum matcht MUST beim zeitbasierten Merge schlechter als oBDS?
# Bei Bedarf auskommentieren – rein diagnostisch, verändert keine Daten.
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "═" * 70)
print("[DEBUG] UICC-QUELLENVERGLEICH  oBDS vs. MUST  (zeitbasiertes Matching)")
print("═" * 70)

def _uicc_diag(df, name, time_col="months_between_asserted_uicc_tnm_date",
               uicc_col="uicc_tnm", cond_col="condition_id_hash"):
    n_rows = len(df)
    n_cond = df[cond_col].nunique()
    known_mask = df[uicc_col].notna() & df[uicc_col].astype(str).str.strip().str.lower().ne("missing")
    n_known_rows = int(known_mask.sum())
    n_known_cond = df.loc[known_mask, cond_col].nunique()
    has_time = df[time_col].notna() if time_col in df.columns else pd.Series(False, index=df.index)
    n_time_rows = int(has_time.sum())
    n_time_cond = df.loc[has_time, cond_col].nunique()
    rows_per_cond = n_rows / n_cond if n_cond else 0
    print(f"\n  {name}:")
    print(f"    Zeilen gesamt:            {n_rows:>9,}  |  cond_ids: {n_cond:>9,}")
    print(f"    Ø Zeilen pro cond_id:     {rows_per_cond:>9.2f}   (Verlaufstiefe → mehr = besseres asof-Matching)")
    print(f"    bekannte UICC:            {n_known_rows:>9,}  |  cond_ids: {n_known_cond:>9,}")
    if time_col in df.columns:
        print(f"    MIT Zeitstempel:          {n_time_rows:>9,}  |  cond_ids: {n_time_cond:>9,}")
        if n_cond:
            print(f"    OHNE Zeitstempel (cond):  {n_cond - n_time_cond:>9,}  ({100*(n_cond-n_time_cond)/n_cond:.1f}% der cond_ids)")
    else:
        print(f"    Spalte '{time_col}' NICHT vorhanden!")
    return set(df.loc[has_time, cond_col].unique()) if time_col in df.columns else set()

_obds_full = pd.concat([
    df_uicc_tnm[df_uicc_tnm["condition_id_hash"].isin(cond_ids_gk)],
    _wk_uicc,
], ignore_index=True, sort=False)
_obds_time_ids = _uicc_diag(_obds_full, "oBDS (df_uicc_tnm + weitere_klassifikation, roh)")
_ = _uicc_diag(df_uicc_gk, "oBDS df_uicc_gk (im Vergleichsplot genutzt)")
_ = _uicc_diag(df_uicc_must, "MUST df_uicc_must (roh, GK-gefiltert)")
_must_time_ids = _uicc_diag(df_uicc_must_time, "MUST df_uicc_must_time (im Merge genutzt)")

print(f"\n  ── Zeitstempel-fähige cond_ids (Schnittmengen) ──")
print(f"    oBDS mit Zeitstempel:           {len(_obds_time_ids):>9,}")
print(f"    MUST mit Zeitstempel:           {len(_must_time_ids):>9,}")
print(f"    In BEIDEN:                      {len(_obds_time_ids & _must_time_ids):>9,}")
print(f"    NUR oBDS (MUST fehlt Zeit):     {len(_obds_time_ids - _must_time_ids):>9,}")
print(f"    NUR MUST (oBDS fehlt Zeit):     {len(_must_time_ids - _obds_time_ids):>9,}")

print(f"\n  ── Zeitstempel-Wertebereich (months_between_asserted_uicc_tnm_date) ──")
for _name, _df in [("oBDS", df_uicc_gk), ("MUST", df_uicc_must_time)]:
    _col = "months_between_asserted_uicc_tnm_date"
    if _col in _df.columns:
        _s = pd.to_numeric(_df[_col], errors="coerce").dropna()
        if len(_s):
            print(f"    {_name:<6}: min={_s.min():.1f}  median={_s.median():.1f}  max={_s.max():.1f}  |  n={len(_s):,}")

print(f"\n  ── dtype Zeitstempel ──")
for _name, _df in [("oBDS df_uicc_gk", df_uicc_gk), ("MUST df_uicc_must_time", df_uicc_must_time)]:
    _col = "months_between_asserted_uicc_tnm_date"
    if _col in _df.columns:
        print(f"    {_name:<24}: {_df[_col].dtype}")
print("═" * 70)


# ── ECOG  (gefiltert auf GK) ──────────────────────────────────────────────────
print(f"\n  [LOAD] ECOG-Leistungszustand: df_leistungszustand_ecog_karnofsky_deidentified.parquet")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")
df_ecog_gk = df_ecog
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


# ══════════════════════════════════════════════════════════════════════════════
# GESCHLECHTERVERGLEICH: UICC- und ECOG-Verteilung nach Geschlecht
# Forschungsfrage: Werden Männer mit höhergradigem UICC diagnostiziert?
# Betrachtung NUR zur Erstdiagnose (erste condition_id pro Patient).
# UICC/ECOG werden auf die Hauptstufe gemappt (IB1 → I, etc.).
# Ergebnis wird als DataFrame (Excel) rausgeschrieben.
# UICC-Quelle: MUST (Standard). ECOG: df_ecog_gk.
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━" * 70)
print("GESCHLECHTERVERGLEICH  –  UICC & ECOG nach Geschlecht (Erstdiagnose)")
print("  Forschungsfrage: Werden Männer höhergradig (UICC/ECOG) diagnostiziert?")
print("  Ebene: erste condition_id pro Patient | UICC auf Hauptstufe gemappt")
print("━" * 70)

# ── Hauptstufen-Mapping UICC: alles platt auf 0 / I / II / III / IV ───────────
def _uicc_to_main_stage(val):
    """Mappt UICC-Substufen auf die römische Hauptstufe (IB1→I, IIIA→III …)."""
    if pd.isna(val):
        return "missing"
    s = str(val).strip().upper()
    if s in ("", "MISSING", "U", "UNKNOWN", "X"):
        return "missing"
    # 0-Stufen (0, 0a, 0is) → "0"
    if s.startswith("0"):
        return "0"
    # römische Hauptzahl am Anfang extrahieren (IV vor III vor II vor I)
    for prefix in ("IV", "III", "II", "I"):
        if s.startswith(prefix):
            return prefix
    return "missing"

# ── Erstdiagnose je Patient bestimmen (erste condition_id pro patient) ────────
# df_tumore ist nach cond_id sortiert; "erste" = früheste verfügbare Diagnose.
# Wir mappen jede cond_id → (patient, gender) und wählen pro Patient die erste cond_id.
_tumore_first = (
    df_tumore.sort_values(["patient_resource_id_hash", "condition_id_hash"])
    .drop_duplicates("patient_resource_id_hash", keep="first")
)
_first_cond_ids = set(_tumore_first["condition_id_hash"])
_gender_map = _tumore_first.set_index("condition_id_hash")["gender"]
print(f"  Erstdiagnosen (1 cond_id je Patient): {len(_first_cond_ids):,}")

def _gender_stage_crosstab(df_stage, stage_col, label, mapper=None, drop_values=None):
    """
    Kreuztabelle Geschlecht × Stage, NUR Erstdiagnosen, optional gemappt.
    Gibt (ct_abs, ct_pct, tidy_df) zurück.
    """
    tmp = df_stage.dropna(subset=[stage_col, "condition_id_hash"]).copy()
    # nur Erstdiagnose-cond_ids
    tmp = tmp[tmp["condition_id_hash"].isin(_first_cond_ids)]
    # ein Eintrag pro cond_id (erster nach Sortierung = frühester Messwert)
    tmp = tmp.drop_duplicates("condition_id_hash")
    tmp["gender"] = tmp["condition_id_hash"].map(_gender_map)
    # Stage ggf. auf Hauptstufe mappen
    if mapper is not None:
        tmp["_stage"] = tmp[stage_col].map(mapper)
    else:
        tmp["_stage"] = tmp[stage_col].astype(str)
    # unerwünschte Werte (missing/U) raus
    if drop_values is not None:
        tmp = tmp[~tmp["_stage"].astype(str).isin(drop_values)]
    ct = pd.crosstab(tmp["gender"], tmp["_stage"])
    ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
    print(f"\n  {label} × Geschlecht (Erstdiagnose, cond_id-Ebene):")
    print(f"    {'Geschlecht':<10}" + "".join(f"{str(c):>9}" for c in ct.columns) + f"{'Summe':>9}")
    for g, row in ct.iterrows():
        print(f"    {str(g):<10}" + "".join(f"{int(v):>9,}" for v in row.values) + f"{int(row.sum()):>9,}")
    print(f"    --- Zeilen-% (Verteilung je Geschlecht über Stages) ---")
    for g, row in ct_pct.iterrows():
        print(f"    {str(g):<10}" + "".join(f"{v:>8.1f}%" for v in row.values))
    # tidy long-form für DataFrame-Export
    tidy = ct.reset_index().melt(id_vars="gender", var_name="stage", value_name="n")
    tidy["pct_within_gender"] = (
        ct_pct.reset_index().melt(id_vars="gender", var_name="stage", value_name="pct")["pct"].values
    )
    tidy.insert(0, "analysis", label)
    return ct, ct_pct, tidy

# UICC × Geschlecht  – PARALLEL für beide Quellen, auf Hauptstufe gemappt
_tidy_uicc_list = []
ct_uicc_gender_by_src, ct_uicc_gender_pct_by_src = {}, {}
for _src_label, _src_df in [("obds", df_uicc_gk), ("must", df_uicc_must)]:
    _ct, _ct_pct, _tidy = _gender_stage_crosstab(
        _src_df, "uicc_tnm", f"UICC ({_src_label}, Hauptstufe)",
        mapper=_uicc_to_main_stage, drop_values={"missing"},
    )
    ct_uicc_gender_by_src[_src_label] = _ct
    ct_uicc_gender_pct_by_src[_src_label] = _ct_pct
    _tidy_uicc_list.append(_tidy)
# ECOG × Geschlecht  – bekannte Werte (0–4), 'U' raus (einmal)
ct_ecog_gender, ct_ecog_gender_pct, _tidy_ecog = _gender_stage_crosstab(
    df_ecog_gk, "ecog_performance_status", "ECOG",
    mapper=lambda v: "missing" if str(v).strip().upper() in ("U", "", "UNKNOWN", "X") else str(v).strip(),
    drop_values={"missing"},
)

# ── Als DataFrame rausschreiben (beide UICC-Quellen + ECOG) ──────────────────
DIR_GK.mkdir(parents=True, exist_ok=True)
_gender_out = pd.concat(_tidy_uicc_list + [_tidy_ecog], ignore_index=True)
_gender_xlsx = DIR_GK / "geschlechtervergleich_uicc_ecog_erstdiagnose.xlsx"
_gender_out.to_excel(_gender_xlsx, index=False)
print(f"\n  [EXPORT] Geschlechtervergleich (oBDS + MUST + ECOG) → {_gender_xlsx}")
print("━" * 70)


# ── Therapien  (nur K & P Intention; gefiltert auf GK) ───────────────────────
print(f"\n  [LOAD] Therapien – alle drei Typen auf GK gefiltert (intention K & P):")
print(f"    Master: df_tumore  |  Slave-Filter: condition_id_hash ∈ cond_ids_gk")
print(
    f"    Ausgeschlossen: intention ≠ K (kurativ) und ≠ P (palliativ) (z.B. diagnostisch, unbekannt)"
)


def _load_therapy(df, dropna_cols: list) -> pd.DataFrame:
    # df = pd.read_parquet(os.path.join(DATA, filename))
    _n_raw = len(df)
    df = df.dropna(subset=dropna_cols)
    df = df.sort_values(["condition_id_hash", "months_between_asserted_therapy_start_date"])
    df = df[df["condition_id_hash"].isin(cond_ids_gk)]
    df = df.replace("", "unknown")
    _n_before_kp = len(df)
    df = df[df["therapy_intention"].isin(["K", "P"])]
    return df


print(f"    [LOAD] OP: df_ops_grouped_deidentified.parquet")
df_ops_gk = _load_therapy(df_ops_grouped, ["condition_id_hash", "ops_code"])
print(
    f"      → {len(df_ops_gk):,} Zeilen  |  cond_ids: {df_ops_gk['condition_id_hash'].nunique():,}"
)

print(f"    [LOAD] Radiotherapie: df_radiotherapies_joined_deidentified.parquet")
df_radio_gk = _load_therapy(df_radiotherapies_joined, ["condition_id_hash", "zielgebiet"])
print(
    f"      → {len(df_radio_gk):,} Zeilen  |  cond_ids: {df_radio_gk['condition_id_hash'].nunique():,}"
)

print(f"    [LOAD] Systemtherapie: df_system_therapies_deidentified.parquet")
df_system_gk = _load_therapy(
    df_systemtherapies,
    ["condition_id_hash", "therapy_protocol_text", "months_between_asserted_therapy_start_date"],
)
print(
    f"      → {len(df_system_gk):,} Zeilen  |  cond_ids: {df_system_gk['condition_id_hash'].nunique():,}"
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

# Auf Patienten der GK einschränken (über den standortabhängigen Join-Key)
df_conditions_mapped = df_conditions_mapped[
    df_conditions_mapped["_join_key"].isin(all_patient_ids_gk)
]
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
level_configs_neben = [
    LevelConfig("ICD_NAME", "ICD_NAME", "Full ICD code"),
    LevelConfig("ICD_BASE_NAME", "ICD_BASE_NAME", "Base code (3-digit)"),
    LevelConfig("group_full", "group_full", "ICD group"),
    LevelConfig("chapter_full", "chapter_full", "ICD chapter"),
]

# ── Report starten ────────────────────────────────────────────────────────────
print(f"\n  [REPORT] run_nebendiagnosen_report startet …")
print(f"    Level: ICD_NAME, ICD_BASE_NAME, group_full, chapter_full")
print(f"    Modi: 'all' + 'unique'  |  Top-N: {TOP_N_NEBEN}")
print(f"    Filter: standard (alle) + core_comorbidity (In_Core_Comorbidity_Analysis==Ja)")
results_neben = run_nebendiagnosen_report(
    df=df_conditions_mapped,
    patient_col="_join_key",
    level_configs=level_configs_neben,
    cohort_name="GK-Top20",
    modes=["all", "unique"],
    top_n=TOP_N_NEBEN,
    output_dir=DIR_NEBENDIAG,
    show=False,
    show_title=False,
    icd_nebendiagnosen_einteilung=icd_nebendiagnosen_einteilung,
    icd_code_col_in_df="icd_code",
    icd_code_col_in_mapping="ICD_Code",
    all_patient_ids=all_patient_ids_gk,
)
print("Speichern im Nebendiagnose Unterordner im Plotordner")
print(f"  ✓ Abschnitt B abgeschlossen  →  {DIR_NEBENDIAG}")


# ══════════════════════════════════════════════════════════════════════════════
# ABSCHNITT C  –  GRUNDKOHORTE  (alle Jahre)
# Plots: Überblick über die Kohorte vor jeglichem zeitlichem Filter.
# ══════════════════════════════════════════════════════════════════════════════

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
print(f"\n  [PLOT] Altersverteilung je Therapietyp → age_dist_5yr.tiff / age_dist_10yr.tiff")
for bracket, fname in [(5, "age_dist_5yr.tiff"), (10, "age_dist_10yr.tiff")]:
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
print(f"  [PLOT] Bias-Analyse Diagnosejahr vs. rel. Therapiestart → bias_op/system/radio.tiff")
print(f"    Zeigt: Zusammenhang zwischen Diagnosejahr und Zeitabstand Diagnose→Therapie")
print(f"    Detektiert Meldebias (ältere Jahre: lückenhafte Therapiedokumentation)")
for df_t, name in [(df_ops_gk, "op"), (df_system_gk, "system"), (df_radio_gk, "radio")]:
    plot_therapy_bias_analysis(
        df_t,
        relative_col="months_between_asserted_therapy_start_date",
        diagnosis_year_col="asserted_year",
        figsize=(10, 4),
        save_path=str(DIR_GK / f"bias_{name}.tiff"),
    )

# ── Plot 3: UICC/ECOG Inventar  (GK-OBDS + MUST) ─────────────────────────────
print(f"  [PLOT] UICC/ECOG Inventar → uicc_ecog_inventory.tiff + uicc_ecog_inventory_must.tiff")
print(f"    Zeigt: Bestand und Staging-Verteilung vor Zeitfilter")
#df_uicc_gk["uicc_tnm"].value_counts().sum()
plot_uicc_inventory_comparison(
    df_uicc_gk=df_uicc_gk,
    df_uicc_must=df_uicc_must,
    df_ecog=df_ecog_gk,
    n_total=cond_ids_gk.nunique(),
    out_dir=DIR_GK,
)

# ── Plot 4: Lorenz-Kurve + Log-Histogramm  (alle Therapien konkateniert) ──────
print(f"  [PLOT] Lorenz-Kurve Therapieverteilung → lorenz.tiff")
print(f"    Zeigt: Wie ungleich sind Therapien auf cond_ids verteilt?")
plot_lorenz_curve(
    df_alle_gk,
    n_total_patients=cond_ids_gk.nunique(),
    cond_col="condition_id_hash",
    output_path=DIR_GK / "lorenz.tiff",
)
print(f"  [PLOT] Log-Histogramm Therapieverteilung → log_histo.tiff")
print(f"    Zeigt: Häufigkeitsverteilung der Therapieanzahl pro cond_id (log-skaliert)")
plot_log_histogram(
    df_alle_gk,
    n_total_patients=cond_ids_gk.nunique(),
    cond_col="condition_id_hash",
    output_path=DIR_GK / "log_histo.tiff",
)

# ── Plot 5: ECOG × UICC Nähe – PARALLEL für beide Quellen (oBDS + MUST) ──────
print(f"\n  [PLOT] ECOG × UICC Zeitliche Nähe (keine Toleranz + 3M-Cutoff):")
print(f"    Methode: nearest-date-Merge (direction=nearest) pro cond_id")
print(f"    Läuft PARALLEL für beide UICC-Quellen (Dateien mit _obds / _must Suffix).")
for _src_label, _df_uicc_src in UICC_SOURCES:
    print(f"    [{_src_label.upper()}] df_ecog_gk ({len(df_ecog_gk):,}) × df_uicc_{_src_label}_time ({len(_df_uicc_src):,} Zeilen)")
    for max_m, dist_f, bub_f in [
        (None, f"ecog_uicc_dist_no_tolerance_{_src_label}.tiff", f"ecog_uicc_bubble_no_tolerance_{_src_label}.tiff"),
        (3.0,  f"ecog_uicc_dist_3months_{_src_label}.tiff",      f"ecog_uicc_bubble_3months_{_src_label}.tiff"),
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

# ── Plot 6: Merge Therapie × ECOG/UICC  (nächstes Datum, ohne Toleranz) ───────
print(f"\n  [MERGE] Nearest-date-Merge: 6 Therapie-Staging-Paare (GK, alle Jahre)")
print(f"    Methode: merge_asof (direction=backward, tolerance=None) pro cond_id")
print(f"    Für jede Therapie wird das zeitlich nächste zurückliegende ECOG/UICC gesucht.")
print(f"    ECOG: einmal. UICC: PARALLEL für beide Quellen (_obds / _must).")
print(f"    HINWEIS: Resultierende DFs (df_se, df_su_obds/_must, …) werden nach 2017-Filter")
print(f"    auf cond_ids_17 eingeschränkt → Basis für 3M-Cutoff (Abschnitt E).")
_merge_args = dict(
    id_col="condition_id_hash",
    therapy_time_col="months_between_asserted_therapy_start_date",
    direction="backward",
    tolerance=None,
    plot=True,
    log_y=True,
    save_path=str(DIR_GK),
)

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
                                 "Systemic Therapy × ECOG", "delta_system_ecog.tiff")
scatter_info.append((df_se, "months_between_asserted_effective_dateTime", "Systemic Therapy × ECOG", "scatter_system_ecog"))
df_re, s_re = _run_nearest_merge(df_radio_gk, df_ecog_gk, "months_between_asserted_effective_dateTime",
                                 "Radiotherapy × ECOG", "delta_radio_ecog.tiff")
scatter_info.append((df_re, "months_between_asserted_effective_dateTime", "Radiotherapy × ECOG", "scatter_radio_ecog"))
df_oe, s_oe = _run_nearest_merge(df_ops_gk, df_ecog_gk, "months_between_asserted_effective_dateTime",
                                 "OP × ECOG", "delta_op_ecog.tiff")
scatter_info.append((df_oe, "months_between_asserted_effective_dateTime", "OP × ECOG", "scatter_op_ecog"))

# ── UICC-Merges (PARALLEL für beide Quellen) ─────────────────────────────────
# Ergebnis-Dicts: df_su_by_src["obds"], df_su_by_src["must"], etc.
df_su_by_src, df_ru_by_src, df_ou_by_src = {}, {}, {}
s_su_by_src, s_ru_by_src, s_ou_by_src = {}, {}, {}
for _src_label, _df_uicc_src in UICC_SOURCES:
    print(f"    [{_src_label.upper()}] UICC-Merges (3 Therapien) mit df_uicc_{_src_label}_time ({len(_df_uicc_src):,} Zeilen)")
    _su, _ssu = _run_nearest_merge(df_system_gk, _df_uicc_src, "months_between_asserted_uicc_tnm_date",
                                   f"Systemic Therapy × UICC ({_src_label})", f"delta_system_uicc_{_src_label}.tiff")
    _ru, _sru = _run_nearest_merge(df_radio_gk, _df_uicc_src, "months_between_asserted_uicc_tnm_date",
                                   f"Radiotherapy × UICC ({_src_label})", f"delta_radio_uicc_{_src_label}.tiff")
    _ou, _sou = _run_nearest_merge(df_ops_gk, _df_uicc_src, "months_between_asserted_uicc_tnm_date",
                                   f"OP × UICC ({_src_label})", f"delta_op_uicc_{_src_label}.tiff")
    df_su_by_src[_src_label], s_su_by_src[_src_label] = _su, _ssu
    df_ru_by_src[_src_label], s_ru_by_src[_src_label] = _ru, _sru
    df_ou_by_src[_src_label], s_ou_by_src[_src_label] = _ou, _sou
    scatter_info.append((_su, "months_between_asserted_uicc_tnm_date", f"Systemic Therapy × UICC ({_src_label})", f"scatter_system_uicc_{_src_label}"))
    scatter_info.append((_ru, "months_between_asserted_uicc_tnm_date", f"Radiotherapy × UICC ({_src_label})", f"scatter_radio_uicc_{_src_label}"))
    scatter_info.append((_ou, "months_between_asserted_uicc_tnm_date", f"OP × UICC ({_src_label})", f"scatter_op_uicc_{_src_label}"))

print(f"\n  [PLOT] Scatter-Plots Therapiepaar → scatter_[typ]_[staging].tiff")
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

print(f"  [PLOT] Panel-Plot Merge-Histogramme → panel_months_diff_[obds/must].tiff")
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
        file_name=f"panel_months_diff_{_src_label}.tiff",
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
# FILTER: asserted_year >= 2017
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "━" * 70)
print("FILTER 4  –  Jahresschnitt asserted_year >= 2017")
print("  Rationale: Vor 2017 inkonsistente Meldepflicht → Completeness-Bias.")
print("  Alle nachgelagerten DataFrames werden auf diesen Schnitt reduziert.")
print("━" * 70)

df_tumore_17 = df_tumore[df_tumore["asserted_year"] >= 2017]
cond_ids_17 = df_tumore_17["condition_id_hash"]
_log_filter("asserted_year >= 2017", _n3_cond, cond_ids_17.nunique())
print(f"  Patienten nach 2017-Filter: {df_tumore_17['patient_resource_id_hash'].nunique():,}")

print(f"\n  Slave-DataFrames auf cond_ids_17 einschränken:")
df_ops_17 = df_ops_gk[df_ops_gk["condition_id_hash"].isin(cond_ids_17)]
df_radio_17 = df_radio_gk[df_radio_gk["condition_id_hash"].isin(cond_ids_17)]
df_system_17 = df_system_gk[df_system_gk["condition_id_hash"].isin(cond_ids_17)]
df_ecog_17 = df_ecog_gk[df_ecog_gk["condition_id_hash"].isin(cond_ids_17)]
# UICC ≥2017 je Quelle (für die Sweeps, parallel)
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
# UICC-Merge-Ergebnisse je Quelle auf 2017 einschränken
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
# ABSCHNITT D  –  GK_17  (alle Therapien, >= 2017)
# ══════════════════════════════════════════════════════════════════════════════

DIR_GK_17.mkdir(parents=True, exist_ok=True)

print("\n" + "━" * 70)
print("ABSCHNITT D  –  GK_17  (alle Therapien, asserted_year >= 2017)")
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
print(f"\n  [PLOT] Violin-Plots Therapiezeitpunkte relativ zur Diagnose → violin_*.tiff")
print(f"    Zeigt: Verteilung des Zeitabstands Erstdiagnose → Therapiestart (Monate)")
print(f"    Negative Werte = Therapie vor formaler Diagnose (Datenkurationsproblem)")
plot_therapy_times(
    df_alle_17,
    rel_time_col="months_between_asserted_therapy_start_date",
    title="Alle Therapien – Zeitpunkt relativ zur Diagnose",
    save_path=str(DIR_GK_17 / "violin_alle_therapien.tiff"),
    allow_negative=True,
)
for df_t, fname in [
    (df_ops_17, "violin_op.tiff"),
    (df_radio_17, "violin_radio.tiff"),
    (df_system_17, "violin_system.tiff"),
]:
    plot_therapy_times(
        df_t,
        rel_time_col="months_between_asserted_therapy_start_date",
        save_path=str(DIR_GK_17 / fname),
        allow_negative=True,
    )

# ── Plot 2: Dropout-Kurven  (je Therapietyp) ─────────────────────────────────
print(f"\n  [PLOT] Dropout-Kurven (Cutoff-Sweep 0–12 Monate) → dropout_*.tiff")
print(f"    Zeigt: Wieviele cond_ids haben eine Therapie innerhalb von X Monaten nach Diagnose?")
print(f"    Highlight: 3 Monate (gewählter Cutoff)")
for df_t, fname in [
    (df_ops_17, "dropout_op.tiff"),
    (df_radio_17, "dropout_radio.tiff"),
    (df_system_17, "dropout_system.tiff"),
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
print(f"\n  [PLOT] Toleranz-Sweeps 0–12 Monate → sweep_*.tiff + sweep_panel_[obds/must].tiff")
print(f"    Inputs: df_*_17 Therapien × df_ecog_17 / df_uicc_17_by_src")
print(f"    Zeigt: Match-Rate (%) bei zunehmendem Toleranzfenster zwischen Therapie und Staging")
print(f"    ECOG-Sweeps einmal; UICC-Sweeps PARALLEL je Quelle (_obds / _must).")
_tols = range(0, 13)
df_sweeps = {}

# ECOG-Sweeps (einmal)
_ecog_sweep_specs = [
    (df_system_17, df_ecog_17, "System → ECOG", "sweep_system_ecog.tiff", len(df_system_17)),
    (df_radio_17,  df_ecog_17, "Radio → ECOG",  "sweep_radio_ecog.tiff",  len(df_radio_17)),
    (df_ops_17,    df_ecog_17, "OP → ECOG",     "sweep_op_ecog.tiff",     len(df_ops_17)),
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
        (df_system_17, f"System → UICC ({_src_label})", f"sweep_system_uicc_{_src_label}.tiff"),
        (df_radio_17,  f"Radio → UICC ({_src_label})",  f"sweep_radio_uicc_{_src_label}.tiff"),
        (df_ops_17,    f"OP → UICC ({_src_label})",     f"sweep_op_uicc_{_src_label}.tiff"),
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
            {"df": df_sweeps["sweep_system_ecog.tiff"], "therapy": "System Therapy", "pair": "ECOG", "highlight": 3, "n_total": len(df_system_17)},
            {"df": df_sweeps[f"sweep_system_uicc_{_src_label}.tiff"], "therapy": "System Therapy", "pair": f"UICC ({_src_label})", "highlight": 3, "n_total": len(df_system_17)},
            {"df": df_sweeps["sweep_radio_ecog.tiff"], "therapy": "Radiotherapy", "pair": "ECOG", "highlight": 3, "n_total": len(df_radio_17)},
            {"df": df_sweeps[f"sweep_radio_uicc_{_src_label}.tiff"], "therapy": "Radiotherapy", "pair": f"UICC ({_src_label})", "highlight": 3, "n_total": len(df_radio_17)},
            {"df": df_sweeps["sweep_op_ecog.tiff"], "therapy": "OP", "pair": "ECOG", "highlight": 3, "n_total": len(df_ops_17)},
            {"df": df_sweeps[f"sweep_op_uicc_{_src_label}.tiff"], "therapy": "OP", "pair": f"UICC ({_src_label})", "highlight": 3, "n_total": len(df_ops_17)},
        ],
        save_plot=True,
        save_dir=str(DIR_GK_17),
        file_name=f"sweep_panel_{_src_label}.tiff",
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
print(f"\n  [PLOT] UICC-Staging-Verteilung je Therapietyp → uicc_distribution_[obds/must].tiff")
for _src_label in [s for s, _ in UICC_SOURCES]:
    print(f"    [{_src_label.upper()}] df_ou/su/ru_3m ({_src_label})")
    plot_uicc_distribution_grouped_bar(
        dataframes={
            "Surgical Procedure": df_ou_3m_by_src[_src_label],
            "Systemic Therapy": df_su_3m_by_src[_src_label],
            "Radiation Therapy": df_ru_3m_by_src[_src_label],
        },
        save_path=str(DIR_3M / f"uicc_distribution_{_src_label}.tiff"),
    )


# ── ECOG-Verteilung  ─────────────────────────────────────────────────────────
print(f"  [PLOT] ECOG-Performance-Verteilung je Therapietyp → ecog_distribution.tiff")
print(f"    Inputs: df_oe/se/re_3m (3M-gefilterte Paare)")
plot_ecog_distribution_grouped_bar(
    dataframes={
        "Surgical Procedure": df_oe_3m,
        "Systemic Therapy": df_se_3m,
        "Radiation Therapy": df_re_3m,
    },
    save_path=str(DIR_3M / "ecog_distribution.tiff"),
)

# ── Altersverteilung nach 3M-Cutoff ──────────────────────────────────────────
print(f"  [PLOT] Altersverteilung nach 3M-Match → age_dist_5yr.tiff / age_dist_10yr.tiff")
for bracket, fname in [(5, "age_dist_5yr.tiff"), (10, "age_dist_10yr.tiff")]:
    plot_age_distribution_grouped_bar(
        dataframes={
            "Surgical Procedure": df_oe_3m,
            "Systemic Therapy": df_se_3m,
            "Radiation Therapy": df_re_3m,
        },
        age_column="age_at_diagnosis",
        age_bracket=bracket,
        title=False,
        save_path=str(DIR_3M / fname),
    )

# ── Violin-Plots nach 3M-Cutoff ──────────────────────────────────────────────
print(f"  [PLOT] Violin-Plots Therapiezeitpunkte (3M-Match) → violin_*.tiff")
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
    save_path=str(DIR_3M / "violin_alle_therapien.tiff"),
    allow_negative=True,
)
for df_m, fname in [
    (df_oe_3m, "violin_op.tiff"),
    (df_re_3m, "violin_radio.tiff"),
    (df_se_3m, "violin_system.tiff"),
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
print(f"  Eingabe:    Alle OBDS-Karzinom-cond_ids              {_n0_cond:>9,}")
print(f"  Filter 1:   D-Diagnosen entfernt                     {_n1_cond:>9,}")
print(f"  Filter 2:   C44 entfernt                             {_n2_cond:>9,}")
print(f"  Filter 2b:  Minderjährige (<18) entfernt             {_n2b_cond:>9,}")
print(f"  Filter 3:   Top-20 Entitäten → GRUNDKOHORTE (GK)     {_n3_cond:>9,}")
print(f"  Filter 4:   asserted_year >= 2017 → GK_17            {cond_ids_17.nunique():>9,}")
print(f"  Filter 5:   3M-Cutoff ECOG/UICC zu Therapie")
print(f"    System × ECOG valide Paare:                        {len(df_se_3m):>9,}")
print(f"    Radio  × ECOG valide Paare:                        {len(df_re_3m):>9,}")
print(f"    OP     × ECOG valide Paare:                        {len(df_oe_3m):>9,}")
for _src_label in [s for s, _ in UICC_SOURCES]:
    print(f"    System × UICC [{_src_label}] valide Paare:               {len(df_su_3m_by_src[_src_label]):>9,}")
    print(f"    Radio  × UICC [{_src_label}] valide Paare:               {len(df_ru_3m_by_src[_src_label]):>9,}")
    print(f"    OP     × UICC [{_src_label}] valide Paare:               {len(df_ou_3m_by_src[_src_label]):>9,}")
print("━" * 70)
print("\n✓ Pipeline vollständig abgeschlossen.")
