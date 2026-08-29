import re
import pandas as pd


def load_icd_hierarchy_lookup(csv_path="icd10gm2026_basecode_lookup.csv"):
    """
    Lädt die vorberechnete ICD-10-GM-2026-Lookup-Tabelle auf Basiskategorie-Ebene.

    Wichtige Spalten:
    - base_code
    - chapter
    - chapter_range
    - chapter_title
    - group_range
    - group_title
    - subgroup_range
    - subgroup_title
    - subgroup_path_ranges
    - subgroup_path_titles
    - most_specific_level
    - most_specific_range
    - most_specific_title
    """
    return pd.read_csv(csv_path)


def extract_icd_base(code):
    """
    Extrahiert aus einem ICD-Kode die Basiskategorie:
    - C50.9   -> C50
    - c509    -> C50
    - Z12.31  -> Z12
    - M46     -> M46
    """
    if pd.isna(code):
        return None

    code = str(code).strip().upper()
    code = code.replace(" ", "").replace("-", "")
    m = re.match(r"^([A-Z])(\d{2})", code)

    if not m:
        return None

    return f"{m.group(1)}{m.group(2)}"


def build_unique_icd_code_table(df, code_col):
    """
    Erstellt eine kleine Tabelle mit allen einzigartigen ICD-Kodes des Eingabedatenrahmens.
    """
    unique_df = pd.DataFrame({code_col: pd.Series(df[code_col].dropna().astype(str).unique())})
    unique_df["base_code"] = unique_df[code_col].apply(extract_icd_base)
    return unique_df


def map_icd_dataframe_fast(df, lookup_df, code_col="icd_code", keep_original_code=True):
    """
    Sehr schnelles Mapping für große Datenmengen:
    1. einzigartige ICD-Kodes bestimmen
    2. Basiskategorie extrahieren
    3. nur diese kleine Tabelle mit der Lookup-Tabelle zusammenführen
    4. Ergebnis zurück auf den großen Datenrahmen mergen

    Ergebnis-Spalten:
    - base_code
    - chapter
    - chapter_range
    - chapter_title
    - group_range
    - group_title
    - subgroup_range
    - subgroup_title
    - subgroup_path_ranges
    - subgroup_path_titles
    - most_specific_level
    - most_specific_range
    - most_specific_title
    """
    unique_codes = build_unique_icd_code_table(df, code_col=code_col)

    mapped_unique = unique_codes.merge(
        lookup_df,
        on="base_code",
        how="left"
    )

    if keep_original_code:
        return df.merge(mapped_unique, on=code_col, how="left")

    out = df.copy()
    out["base_code"] = out[code_col].apply(extract_icd_base)
    out = out.merge(lookup_df, on="base_code", how="left")
    return out


def map_icd_series_fast(series, lookup_df):
    """
    Komfortfunktion für eine einzelne Pandas-Serie.
    """
    tmp = pd.DataFrame({"icd_code": series})
    out = map_icd_dataframe_fast(tmp, lookup_df, code_col="icd_code", keep_original_code=True)
    out.index = series.index
    return out