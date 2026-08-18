from pathlib import Path

import pandas as pd

DTYPES = {
    "asserted_year": "Int64",
    "age_at_diagnosis": "Float64",
    "is_deceased": "boolean",
    # "months_between_*": "Float64", - s. unten
    "icd10_code": "string",
    "patient_resource_id_hash": "string",
    "condition_id_hash": "string",
}


def load_multisite_parquets(base_dir, files, sites):
    site_list = [s.strip().lower() for s in sites.split(",")]
    dfs = {}

    for name, filename in files.items():
        tmp = []

        for site in site_list:
            path = base_dir / site / "parquet" / filename
            df = pd.read_parquet(path)

            df["site"] = site

            # 1) fixe dtypes
            for col, dtype in DTYPES.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)

            # 2) alles was mit months_between anfängt -> Float64
            for col in df.columns:
                if col.startswith("months_between"):
                    df[col] = df[col].astype("Float64")

            tmp.append(df)

        dfs[name] = pd.concat(tmp, ignore_index=True)

    return dfs


def load_multisite_parquets_from_paths(paths_list):
    """
    paths_list = Liste von vollständigen parquet paths (pro site)
    """
    dfs = []

    for path in paths_list:
        df = pd.read_parquet(path)

        # site aus path extrahieren
        site = Path(path).parent.parent.name
        df["site"] = site

        # dtypes anwenden
        for col, dtype in DTYPES.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        for col in df.columns:
            if col.startswith("months_between"):
                df[col] = df[col].astype("Float64")

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def build_all_obds_paths(base_dir, config, sites):
    sites = [s.strip().lower() for s in sites.split(",")]

    paths = {key: [] for key in config.keys()}

    for site in sites:
        for key, cfg in config.items():
            path = Path(base_dir) / site / cfg["subdir"] / cfg["file"]
            paths[key].append(str(path))

    return paths
