# %%
# ============================================================
# General multisite parquet loader for one specific BZKF / oBDS dataset
# ============================================================
#
# Purpose
# -------
# This file loads exactly one parquet dataset per selected site folder,
# adds a site label named "standort", and combines all selected sites
# into one shared dataframe.
#
# ----------------------------------------
# BASE_DATA_DIR
# ├── erl
# │   └── <SPECIFIC_FILENAME>
# ├── tum
# │   └── <SPECIFIC_FILENAME>
# └── reg
#     └── <SPECIFIC_FILENAME>
#
# Example resulting paths
# -----------------------
# P:\BZKF_Zweittumor\data\data_bzkf\erl\df_specific_deidentified.parquet
# P:\BZKF_Zweittumor\data\data_bzkf\tum\df_specific_deidentified.parquet
# P:\BZKF_Zweittumor\data\data_bzkf\reg\df_specific_deidentified.parquet
#
# How to use
# ----------
# 1. Adjust BASE_PROJECT_DIR if needed.
# 2. Adjust STANDORTE to select the sites you want to load.
# 3. Adjust SPECIFIC_FILENAME to the parquet file you want to load.
# 4. Run the script.
# 5. Continue your analysis using:
#       df_specific_raw
#
# Important
# ---------
# - Files are expected directly inside each site folder.
# - Only one dataset is loaded per site.
# - The variable "standort" is added automatically before concatenation.
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
# STANDORTE = ["erl", "tum", "reg"]
STANDORTE = ["erl", "tum", "reg"]

# One specific file loaded for each site.
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
    Load one specific parquet file for one site and add the site label.

    Parameters
    ----------
    standort : str
        Name of the site folder, for example "erl", "tum", or "reg".

    Returns
    -------
    df_specific : pandas.DataFrame
        Specific dataframe for the selected site, including the column "standort".
    """

    data_dir = (
        BASE_DATA_DIR /
        standort
    )

    file_specific = (
        data_dir /
        SPECIFIC_FILENAME
    )

    print(f"\nLoading site: {standort}")
    print(f"Data directory:\n{data_dir}")
    print(f"Specific file:\n{file_specific}")

    if not file_specific.exists():
        raise FileNotFoundError(
            f"Missing specific file for site '{standort}':\n{file_specific}"
        )

    df_specific = pd.read_parquet(
        file_specific
    )

    df_specific = remove_duplicate_columns(
        df_specific,
        f"specific_{standort}"
    )

    # Add site label so that site information is retained after concatenation.
    df_specific["standort"] = standort

    print(f"{standort} specific shape: {df_specific.shape}")

    return df_specific


# %%
# ============================================================
# 3. LOAD AND COMBINE ALL SELECTED SITES
# ============================================================

print_section("Loading selected site files")

specific_list = []

for standort in STANDORTE:
    df_specific_site = load_site_data(
        standort
    )

    specific_list.append(
        df_specific_site
    )


# Combine all selected sites.
df_specific_raw = pd.concat(
    specific_list,
    ignore_index=True
)

# Safety check after concatenation.
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

print("\nCombined specific shape:")
print(df_specific_raw.shape)

print("\nSpecific site distribution:")
print(
    df_specific_raw["standort"]
    .value_counts(dropna=False)
)

print("\nSpecific columns:")
print(
    df_specific_raw.columns.tolist()
)

print("\nDone.")
