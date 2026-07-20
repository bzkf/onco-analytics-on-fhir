# %%
# ============================================================
# General multisite parquet loader for BZKF / oBDS data
# ============================================================
#
# Purpose
# -------
# This file provides a reusable template to load parquet files from
# several site folders, add a site label, and combine all selected sites
# into one shared dataframe.
#
# Typical use cases
# -----------------
# - Load one general/base parquet file per site, for example all oBDS cases.
# - Load one additional specific parquet file per site, for example:
#     - second tumor data
#     - TNM data
#     - UICC data
#     - therapy data
#     - recurrence data
#     - any other site-specific extract
# - Add a variable named "standort" to both dataframes.
# - Combine all selected sites into:
#     - df_base_raw
#     - df_specific_raw
#
# Folder structure expected by this script
# ----------------------------------------
# BASE_DATA_DIR
# ├── erl
# │   ├── df_all_obds_clean_deidentified.parquet
# │   └── <SPECIFIC_FILENAME>
# ├── tum
# │   ├── df_all_obds_clean_deidentified.parquet
# │   └── <SPECIFIC_FILENAME>
# └── reg
#     ├── df_all_obds_clean_deidentified.parquet
#     └── <SPECIFIC_FILENAME>
#
# Example resulting paths
# -----------------------
# ----\BZKF_Zweittumor\data\data_bzkf\erl\df_all_obds_clean_deidentified.parquet
# ----\BZKF_Zweittumor\data\data_bzkf\erl\df_specific_deidentified.parquet
#
# How to use
# ----------
# 1. Adjust BASE_PROJECT_DIR if needed.
# 2. Adjust STANDORTE to select the sites you want to load.
# 3. Adjust BASE_FILENAME if your general/base file has another name.
# 4. Adjust SPECIFIC_FILENAME to the file you want to load in addition.
# 5. Run the script.
# 6. Continue your analysis using:
#       df_base_raw
#       df_specific_raw
#
# Important
# ---------
# - Files are expected directly inside each site folder.
# - The variable "standort" is added automatically.
# - The selected sites are combined using pd.concat().
#
# ============================================================

from pathlib import Path
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

# Select the site folders that should be loaded.
# Examples:
# STANDORTE = ["erl"]
# STANDORTE = ["erl", "tum"]
STANDORTE = ["erl", "tum", "reg"]

# General/base file loaded for each site.
BASE_FILENAME = "df_all_obds_clean_deidentified.parquet"

# Specific file loaded for each site.
# Replace this with the file needed for your current analysis.
# Examples:
# SPECIFIC_FILENAME = "df_tnm_deidentified.parquet"
# SPECIFIC_FILENAME = "df_uicc_deidentified.parquet"
# SPECIFIC_FILENAME = "df_second_tumor_deidentified.parquet"
# SPECIFIC_FILENAME = "df_therapy_deidentified.parquet"
SPECIFIC_FILENAME = "df_specific_deidentified.parquet"


# %%
# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def remove_duplicate_columns(df, name):
    """
    Remove duplicated column names from a dataframe.
    The first occurrence of each duplicated column is kept.
    """

    duplicated = df.columns[df.columns.duplicated()].tolist()

    if duplicated:
        print(f"WARNING: duplicated columns in {name}: {duplicated}")
        df = df.loc[:, ~df.columns.duplicated()].copy()

    return df


def load_site_data(standort):
    """
    Load the base parquet file and the specific parquet file for one site.

    Parameters
    ----------
    standort : str
        Name of the site folder, for example "erl", "tum", or "reg".

    Returns
    -------
    df_base : pandas.DataFrame
        Base dataframe for the selected site.

    df_specific : pandas.DataFrame
        Specific dataframe for the selected site.
    """

    data_dir = (
        BASE_DATA_DIR /
        standort
    )

    file_base = (
        data_dir /
        BASE_FILENAME
    )

    file_specific = (
        data_dir /
        SPECIFIC_FILENAME
    )

    print(f"\nLoading site: {standort}")
    print(f"Data directory:\n{data_dir}")
    print(f"Base file:\n{file_base}")
    print(f"Specific file:\n{file_specific}")

    if not file_base.exists():
        raise FileNotFoundError(
            f"Missing base file for site '{standort}':\n{file_base}"
        )

    if not file_specific.exists():
        raise FileNotFoundError(
            f"Missing specific file for site '{standort}':\n{file_specific}"
        )

    df_base = pd.read_parquet(
        file_base
    )

    df_specific = pd.read_parquet(
        file_specific
    )

    df_base = remove_duplicate_columns(
        df_base,
        f"base_{standort}"
    )

    df_specific = remove_duplicate_columns(
        df_specific,
        f"specific_{standort}"
    )

    # Add site label so that site information is retained after concatenation.
    df_base["standort"] = standort
    df_specific["standort"] = standort

    print(f"{standort} base shape: {df_base.shape}")
    print(f"{standort} specific shape: {df_specific.shape}")

    return df_base, df_specific


# %%
# ============================================================
# 3. LOAD AND COMBINE ALL SELECTED SITES
# ============================================================

print_section("Loading selected site files")

base_list = []
specific_list = []

for standort in STANDORTE:
    df_base_site, df_specific_site = load_site_data(
        standort
    )

    base_list.append(
        df_base_site
    )

    specific_list.append(
        df_specific_site
    )


# Combine all selected sites.
df_base_raw = pd.concat(
    base_list,
    ignore_index=True
)

df_specific_raw = pd.concat(
    specific_list,
    ignore_index=True
)

# Safety check after concatenation.
df_base_raw = remove_duplicate_columns(
    df_base_raw,
    "df_base_raw"
)

df_specific_raw = remove_duplicate_columns(
    df_specific_raw,
    "df_specific_raw"
)


# %%
# ============================================================
# 4. CHECK RESULT
# ============================================================

print_section("Combined data check")

print("\nSelected sites:")
print(STANDORTE)

print("\nCombined base shape:")
print(df_base_raw.shape)

print("\nCombined specific shape:")
print(df_specific_raw.shape)

print("\nBase site distribution:")
print(
    df_base_raw["standort"]
    .value_counts(dropna=False)
)

print("\nSpecific site distribution:")
print(
    df_specific_raw["standort"]
    .value_counts(dropna=False)
)

print("\nBase columns:")
print(
    df_base_raw.columns.tolist()
)

print("\nSpecific columns:")
print(
    df_specific_raw.columns.tolist()
)

print("\nDone.")
