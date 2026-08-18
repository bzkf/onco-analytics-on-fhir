"""
domain_translation.py
=====================

Übersetzungsmapping (DE -> EN) für die Nebendiagnosen-Domänen-Einteilung
des BZKF oBDS Cohort-Description-Papers.

Enthält:
- DOMAIN_DE_TO_EN : dict          -> die reine Übersetzungstabelle
- add_final_domain_en(df, ...)    -> mappt die deutschen final_domain-Werte
                                     robust auf Englisch (Whitespace-tolerant,
                                     warnt bei nicht gemappten Werten)

Beispiel:
    from domain_translation import add_final_domain_en
    icd_nebendiagnosen_einteilung_CSV_UPDATE = add_final_domain_en(
        icd_nebendiagnosen_einteilung_CSV_UPDATE
    )
    # -> neue Spalte 'final_domain_en'

    # oder direkt in-place die Spalte final_domain überschreiben:
    icd_nebendiagnosen_einteilung_CSV_UPDATE = add_final_domain_en(
        icd_nebendiagnosen_einteilung_CSV_UPDATE, target_col="final_domain"
    )
"""

from __future__ import annotations

import warnings
import pandas as pd


# ---------------------------------------------------------------------------
# Übersetzungstabelle DE -> EN
# ---------------------------------------------------------------------------
DOMAIN_DE_TO_EN: dict[str, str] = {
    "Kardiometabolisch": "Cardiometabolic",
    "COVID-/Sonder-/Screeningcodes": "COVID/Special/Screening codes",
    "Nicht zuordenbar": "Unclassifiable",
    "Screening/Abklärung/Beratung/Kontakt": "Screening/Workup/Counseling/Encounter",
    "Versorgung/Medizinprodukt": "Care/Medical device",
    "Ernährung/Frailty/Funktion": "Nutrition/Frailty/Function",
    "Therapie-/Medikationshistorie": "Therapy/Medication history",
    "Onkologische Nachsorge": "Oncological follow-up",
    "Funktion/Frailty/Rehabilitation": "Function/Frailty/Rehabilitation",
    "Symptome/Befunde": "Symptoms/Findings",
    "Endokrin/metabolisch": "Endocrine/Metabolic",
    "Onkologische Therapiehistorie": "Oncological treatment history",
    "Neoplasien/benigne": "Neoplasms/Benign",
    "Infektiologie/Erregernachweis": "Infectious disease/Pathogen detection",
    "Renal/Urogenital": "Renal/Urogenital",
    "Sonstige Z-Codes": "Other Z-codes",
    "Hämatologisch/Immunologisch": "Hematological/Immunological",
    "Prozedur-/Device-/Nachsorgekontakt": "Procedure/Device/Follow-up encounter",
    "Kardiovaskulär": "Cardiovascular",
    "Therapie-/Risikostatus": "Therapy/Risk status",
    "Komplikationen/iatrogener Kontext": "Complications/Iatrogenic context",
    "Tumoranamnese / potenzielle Endpunktnähe": "Tumor history / potential endpoint proximity",
    "Pulmonal": "Pulmonary",
    "Therapiekomplikationen": "Treatment complications",
    "Neoplasien/unklar": "Neoplasms/Unclear",
    "Psychiatrisch/Substanzgebrauch": "Psychiatric/Substance use",
    "GI/hepatisch": "GI/Hepatic",
    "Operations-/Organverlusthistorie": "Surgical/Organ-loss history",
    "Infektiologie": "Infectious disease",
    "Labor-/Bildgebungsbefunde": "Laboratory/Imaging findings",
    "Therapie-/Nachbehandlungskontext": "Therapy/Aftercare context",
    "Status nach Eingriff / Implantate": "Status post procedure / Implants",
    "Neoplasien/in situ": "Neoplasms/In situ",
    "Neurologisch": "Neurological",
    "Äußere Ursachen": "External causes",
    "Dermatologisch": "Dermatological",
    "Ophthalmologisch/HNO": "Ophthalmological/ENT",
    "Pulmonal/Infektiologie": "Pulmonary/Infectious disease",
    "Infektiologie/Resistenz": "Infectious disease/Resistance",
    "Muskuloskelettal/Rheuma": "Musculoskeletal/Rheumatological",
    "Onkologische Therapiehistorie/-durchführung": "Oncological treatment history/administration",
    "Tumor-Anamnese": "Tumor history",
    "Angeboren/genetisch": "Congenital/Genetic",
    "Verletzung/Komplikationen": "Injury/Complications",
    "Verletzung/Trauma": "Injury/Trauma",
    "Schwangerschaft/Geburt/Wochenbett": "Pregnancy/Childbirth/Puerperium",
    "U-/Sondercodes": "U/Special codes",
    "Toxikologie/Komplikationen": "Toxicology/Complications",
    "Perinatal": "Perinatal",
}


# ---------------------------------------------------------------------------
# Mapping-Funktion
# ---------------------------------------------------------------------------
def add_final_domain_en(
    df: pd.DataFrame,
    source_col: str = "Ebene_2_Klinische_Domaene",
    target_col: str = "Ebene_2_Klinische_Domaene_en",
    mapping: dict[str, str] | None = None,
    warn_unmapped: bool = True,
    fallback: str = "keep",
) -> pd.DataFrame:
    """
    Fügt die englischen Domänen-Bezeichnungen in eine (neue) Spalte ein.

    Parameters
    ----------
    df : pd.DataFrame
        z. B. icd_nebendiagnosen_einteilung_CSV_UPDATE
    source_col : str
        Spalte mit den deutschen final_domain-Werten (default: 'final_domain').
    target_col : str
        Zielspalte für die englischen Werte (default: 'final_domain_en').
        -> target_col='final_domain' setzen, um die Originalspalte zu überschreiben.
    mapping : dict, optional
        Übersetzungstabelle; default = DOMAIN_DE_TO_EN.
    warn_unmapped : bool
        Wenn True, wird gewarnt, falls final_domain-Werte auftauchen,
        die nicht in der Übersetzungstabelle stehen.
    fallback : str
        Verhalten für nicht gemappte Werte:
        - "keep" : deutschen Originalwert behalten (default)
        - "nan"  : auf <NA> setzen
        - <str>  : festen Ersatzstring einsetzen (z. B. "Other")

    Returns
    -------
    pd.DataFrame
        Kopie des DataFrames mit gefüllter target_col.
    """
    if mapping is None:
        mapping = DOMAIN_DE_TO_EN

    if source_col not in df.columns:
        raise KeyError(f"Spalte '{source_col}' ist nicht im DataFrame vorhanden.")

    # Whitespace-tolerante Lookup-Tabelle (falls Werte führende/abschließende
    # Leerzeichen haben, was bei CSV-Roundtrips gern passiert)
    norm_map = {str(k).strip(): v for k, v in mapping.items()}

    src = df[source_col].astype("string").str.strip()
    mapped = src.map(norm_map)

    # nicht gemappte Werte identifizieren (Wert vorhanden, aber kein Treffer)
    unmapped_mask = mapped.isna() & src.notna()
    if warn_unmapped and unmapped_mask.any():
        missing = sorted(src[unmapped_mask].unique().tolist())
        warnings.warn(
            f"[add_final_domain_en] {len(missing)} nicht gemappte "
            f"'{source_col}'-Wert(e): {missing}",
            stacklevel=2,
        )

    # Fallback anwenden
    if fallback == "keep":
        mapped = mapped.fillna(src)          # deutschen Originalwert behalten
    elif fallback == "nan":
        pass                                  # bleibt <NA>
    else:
        mapped = mapped.fillna(fallback)      # fester Ersatzstring

    df = df.copy()
    df[target_col] = mapped
    return df
