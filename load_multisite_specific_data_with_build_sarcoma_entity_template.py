
# %%
# ============================================================
# General multisite parquet loader with atypical histology recoding
# ============================================================
#
# Purpose
# -------
# This script loads one selected parquet dataset per site, adds a site label
# named "standort", concatenates all selected sites into one dataframe, and
# creates a new entity variable in which selected atypical histology groups
# are removed from the original organ-based entities.
#
# The purpose is analytic harmonization:
# Tumors with histologically distinct or atypical morphology are separated
# from otherwise organ-based entities, so that entities such as C50, C34,
# C18-C21, etc. remain more homogeneous for downstream analyses.
#
# Required columns
# ----------------
# This step requires both columns:
#
#     entity_or_parent
#     icdo3_morphology
#
# If one of these columns is missing, the script will stop with an error.
# Therefore, use this script only for datasets that contain tumor entity
# and ICD-O-3 morphology information.
#
# Recoding principle
# ------------------
# - entity_original keeps the original organ-based entity.
# - morphology_code_4digit contains the extracted ICD-O-3 morphology code.
# - histology_group classifies selected morphology ranges.
# - entity_final equals entity_original for typical histologies.
# - entity_final is replaced by histology_group for atypical histologies.
#
# Output columns created
# ----------------------
#     entity_original
#     icdo3_morphology_clean
#     morphology_code_4digit
#     histology_group
#     is_atypical_histology
#     entity_final
#
# Expected folder structure
# -------------------------
# BASE_DATA_DIR
# ├── erl
# │   └── <SPECIFIC_FILENAME>
# ├── tum
# │   └── <SPECIFIC_FILENAME>
# └── reg
#     └── <SPECIFIC_FILENAME>
#
# How to use
# ----------
# 1. Adjust BASE_PROJECT_DIR if needed.
# 2. Adjust STANDORTE.
# 3. Adjust SPECIFIC_FILENAME.
# 4. Make sure ENTITY_COLUMN and MORPHOLOGY_COLUMN match your dataset.
# 5. Run the script.
# 6. Use df_specific_raw or the exported CSV files for analysis.
#
# ============================================================

import re
from pathlib import Path
from datetime import datetime

import pandas as pd


# %%
# ============================================================
# 1. USER SETTINGS
# ============================================================

BASE_PROJECT_DIR = Path(
    r"P:\BZKF_Zweittumor"
)

BASE_DATA_DIR = (
    BASE_PROJECT_DIR /
    "data" /
    "data_bzkf"
)

STANDORTE = ["erl", "tum", "reg"]

SPECIFIC_FILENAME = "df_specific_deidentified.parquet"

ENTITY_COLUMN = "entity_or_parent"
MORPHOLOGY_COLUMN = "icdo3_morphology"

ANALYSIS_TAG = "multisite_specific_with_histology_entity"

OUTPUT_DIR = BASE_PROJECT_DIR / f"results_{ANALYSIS_TAG}"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# %%
# ============================================================
# 2. HISTOLOGY GROUP SETTINGS
# ============================================================
#
# Labels are intentionally written as one-word style labels without spaces,
# slashes, or special characters. This makes them easier to use in filenames,
# plots, groupby operations, and exported tables.
#
# "Typical" means that no atypical histology rule was matched.
#
# You can extend or modify this list later if additional morphology groups
# should be separated from organ-based entities.
#
# ============================================================

HISTOLOGY_GROUPS = [
    {
        "label": "SarcomaMesenchymal",
        "ranges": [(8800, 8999)],
    },
    {
        "label": "Melanocytic",
        "ranges": [(8720, 8799)],
    },
    {
        "label": "Hematologic",
        "ranges": [(9590, 9993)],
    },
    {
        "label": "MesotheliomaKaposi",
        "ranges": [(9050, 9055), (9140, 9140)],
    },
]


# %%
# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def remove_duplicate_columns(df, name):
    duplicated = df.columns[df.columns.duplicated()].tolist()

    if duplicated:
        print(f"WARNING: duplicated columns in {name}: {duplicated}")
        df = df.loc[:, ~df.columns.duplicated()].copy()

    return df


def clean_string_series(series):
    if isinstance(series, pd.DataFrame):
        print("WARNING: clean_string_series received a DataFrame. Using first column.")
        series = series.iloc[:, 0]

    return (
        series.astype("string")
        .str.strip()
        .replace(
            {
                "": pd.NA,
                "None": pd.NA,
                "NONE": pd.NA,
                "none": pd.NA,
                "nan": pd.NA,
                "NaN": pd.NA,
                "NAN": pd.NA,
                "null": pd.NA,
                "NULL": pd.NA,
                "na": pd.NA,
                "NA": pd.NA,
                "N/A": pd.NA,
            }
        )
    )


def extract_icdo3_morphology_code(value):
    if pd.isna(value):
        return pd.NA

    value_as_string = str(value).strip()
    match = re.search(r"(\d{4})", value_as_string)

    if match:
        return int(match.group(1))

    return pd.NA


def assign_histology_group(code):
    if pd.isna(code):
        return pd.NA

    code = int(code)

    for group in HISTOLOGY_GROUPS:
        for lower, upper in group["ranges"]:
            if lower <= code <= upper:
                return group["label"]

    return "Typical"


def load_site_data(standort):
    data_dir = BASE_DATA_DIR / standort
    file_specific = data_dir / SPECIFIC_FILENAME

    print(f"\nLoading site: {standort}")
    print(f"File:\n{file_specific}")

    if not file_specific.exists():
        raise FileNotFoundError(
            f"Missing specific file for site '{standort}':\n{file_specific}"
        )

    df_specific = pd.read_parquet(file_specific)

    df_specific = remove_duplicate_columns(
        df_specific,
        f"specific_{standort}"
    )

    df_specific["standort"] = standort

    print(f"{standort} shape: {df_specific.shape}")

    return df_specific


# %%
# ============================================================
# 4. LOAD AND COMBINE ALL SELECTED SITES
# ============================================================

run_timestamp = datetime.now().isoformat(timespec="seconds")

print_section("Run information")
print(f"Run timestamp: {run_timestamp}")
print(f"Base project dir: {BASE_PROJECT_DIR}")
print(f"Base data dir: {BASE_DATA_DIR}")
print(f"Selected sites: {STANDORTE}")
print(f"Specific filename: {SPECIFIC_FILENAME}")
print(f"Output dir: {OUTPUT_DIR}")

print_section("Loading selected site files")

specific_list = []

for standort in STANDORTE:
    df_specific_site = load_site_data(standort)

    specific_list.append(
        df_specific_site
    )

df_specific_raw = pd.concat(
    specific_list,
    ignore_index=True,
)

df_specific_raw = remove_duplicate_columns(
    df_specific_raw,
    "df_specific_raw",
)

print("\nCombined specific shape:")
print(df_specific_raw.shape)

print("\nSite distribution:")
print(
    df_specific_raw["standort"]
    .value_counts(dropna=False)
)


# %%
# ============================================================
# 5. CREATE HISTOLOGY-RECODED ENTITY VARIABLE
# ============================================================

print_section("Creating histology-recoded entity variable")

if ENTITY_COLUMN not in df_specific_raw.columns:
    raise ValueError(
        f"ENTITY_COLUMN not found: {ENTITY_COLUMN}\n"
        f"Available columns: {df_specific_raw.columns.tolist()}"
    )

if MORPHOLOGY_COLUMN not in df_specific_raw.columns:
    raise ValueError(
        f"MORPHOLOGY_COLUMN not found: {MORPHOLOGY_COLUMN}\n"
        f"Available columns: {df_specific_raw.columns.tolist()}"
    )

df_specific_raw["entity_original"] = clean_string_series(
    df_specific_raw[ENTITY_COLUMN]
)

df_specific_raw["icdo3_morphology_clean"] = clean_string_series(
    df_specific_raw[MORPHOLOGY_COLUMN]
)

df_specific_raw["morphology_code_4digit"] = (
    df_specific_raw[MORPHOLOGY_COLUMN]
    .apply(extract_icdo3_morphology_code)
)

df_specific_raw["histology_group"] = (
    df_specific_raw["morphology_code_4digit"]
    .apply(assign_histology_group)
)

df_specific_raw["is_atypical_histology"] = (
    df_specific_raw["histology_group"] != "Typical"
)

df_specific_raw["entity_final"] = (
    df_specific_raw["entity_original"]
)

df_specific_raw.loc[
    df_specific_raw["is_atypical_histology"],
    "entity_final",
] = df_specific_raw.loc[
    df_specific_raw["is_atypical_histology"],
    "histology_group",
]

print("\nTop original entities:")
print(
    df_specific_raw["entity_original"]
    .value_counts(dropna=False)
    .head(30)
)

print("\nTop final entities after histology recoding:")
print(
    df_specific_raw["entity_final"]
    .value_counts(dropna=False)
    .head(30)
)

print("\nHistology group counts:")
print(
    df_specific_raw["histology_group"]
    .value_counts(dropna=False)
)

print("\nAtypical histology counts:")
print(
    df_specific_raw["is_atypical_histology"]
    .value_counts(dropna=False)
)

print("\nTop morphology codes in atypical histology groups:")
print(
    df_specific_raw.loc[
        df_specific_raw["is_atypical_histology"],
        [
            "histology_group",
            "morphology_code_4digit",
        ],
    ]
    .value_counts(dropna=False)
    .head(50)
)


# %%
# ============================================================
# 6. CREATE SUMMARY TABLES
# ============================================================

print_section("Creating summary tables")

entity_recode_summary = (
    df_specific_raw
    .groupby(
        [
            "entity_original",
            "entity_final",
            "histology_group",
            "is_atypical_histology",
        ],
        dropna=False,
        as_index=False,
    )
    .agg(
        n_rows=("entity_final", "size"),
    )
    .sort_values(
        [
            "n_rows",
            "entity_original",
            "entity_final",
            "histology_group",
        ],
        ascending=[False, True, True, True],
    )
    .reset_index(drop=True)
)

entity_final_by_site_summary = (
    df_specific_raw
    .groupby(
        [
            "standort",
            "entity_final",
        ],
        dropna=False,
        as_index=False,
    )
    .agg(
        n_rows=("entity_final", "size"),
    )
    .sort_values(
        [
            "standort",
            "n_rows",
            "entity_final",
        ],
        ascending=[True, False, True],
    )
    .reset_index(drop=True)
)

histology_group_by_site_summary = (
    df_specific_raw
    .groupby(
        [
            "standort",
            "histology_group",
        ],
        dropna=False,
        as_index=False,
    )
    .agg(
        n_rows=("histology_group", "size"),
    )
    .sort_values(
        [
            "standort",
            "n_rows",
            "histology_group",
        ],
        ascending=[True, False, True],
    )
    .reset_index(drop=True)
)

atypical_morphology_summary = (
    df_specific_raw.loc[
        df_specific_raw["is_atypical_histology"]
    ]
    .groupby(
        [
            "histology_group",
            "morphology_code_4digit",
            "icdo3_morphology_clean",
            "entity_original",
        ],
        dropna=False,
        as_index=False,
    )
    .agg(
        n_rows=("entity_final", "size"),
    )
    .sort_values(
        [
            "histology_group",
            "n_rows",
            "morphology_code_4digit",
            "entity_original",
        ],
        ascending=[True, False, True, True],
    )
    .reset_index(drop=True)
)

print("\nEntity recode summary:")
print(entity_recode_summary.head(50))

print("\nFinal entity by site summary:")
print(entity_final_by_site_summary.head(50))

print("\nHistology group by site summary:")
print(histology_group_by_site_summary.head(50))

print("\nAtypical morphology summary:")
print(atypical_morphology_summary.head(50))


# %%
# ============================================================
# 7. EXPORT RESULTS
# ============================================================

print_section("Exporting results")

specific_export_file = (
    OUTPUT_DIR /
    f"{ANALYSIS_TAG}_combined_specific_with_entity_final.csv"
)

entity_recode_summary_file = (
    OUTPUT_DIR /
    f"{ANALYSIS_TAG}_entity_recode_summary.csv"
)

entity_final_by_site_summary_file = (
    OUTPUT_DIR /
    f"{ANALYSIS_TAG}_entity_final_by_site_summary.csv"
)

histology_group_by_site_summary_file = (
    OUTPUT_DIR /
    f"{ANALYSIS_TAG}_histology_group_by_site_summary.csv"
)

atypical_morphology_summary_file = (
    OUTPUT_DIR /
    f"{ANALYSIS_TAG}_atypical_morphology_summary.csv"
)

df_specific_raw.to_csv(
    specific_export_file,
    index=False,
    encoding="utf-8-sig",
)

entity_recode_summary.to_csv(
    entity_recode_summary_file,
    index=False,
    encoding="utf-8-sig",
)

entity_final_by_site_summary.to_csv(
    entity_final_by_site_summary_file,
    index=False,
    encoding="utf-8-sig",
)

histology_group_by_site_summary.to_csv(
    histology_group_by_site_summary_file,
    index=False,
    encoding="utf-8-sig",
)

atypical_morphology_summary.to_csv(
    atypical_morphology_summary_file,
    index=False,
    encoding="utf-8-sig",
)

print("Saved:", specific_export_file)
print("Saved:", entity_recode_summary_file)
print("Saved:", entity_final_by_site_summary_file)
print("Saved:", histology_group_by_site_summary_file)
print("Saved:", atypical_morphology_summary_file)


# %%
# ============================================================
# 8. FINAL SUMMARY
# ============================================================

print_section("Final summary")

summary = pd.DataFrame(
    {
        "metric": [
            "run_timestamp",
            "selected_sites",
            "specific_filename",
            "combined_rows",
            "combined_columns",
            "typical_rows",
            "atypical_histology_rows",
            "unique_entity_original",
            "unique_entity_final",
            "unique_histology_groups",
        ],
        "value": [
            run_timestamp,
            ", ".join(STANDORTE),
            SPECIFIC_FILENAME,
            df_specific_raw.shape[0],
            df_specific_raw.shape[1],
            int((df_specific_raw["histology_group"] == "Typical").sum()),
            int(df_specific_raw["is_atypical_histology"].sum()),
            df_specific_raw["entity_original"].nunique(dropna=True),
            df_specific_raw["entity_final"].nunique(dropna=True),
            df_specific_raw["histology_group"].nunique(dropna=True),
        ],
    }
)

summary_file = OUTPUT_DIR / f"{ANALYSIS_TAG}_final_summary.csv"

summary.to_csv(
    summary_file,
    index=False,
    encoding="utf-8-sig",
)

print(summary)
print("Saved:", summary_file)

print("\nDone.")
print(f"Output directory: {OUTPUT_DIR}")

