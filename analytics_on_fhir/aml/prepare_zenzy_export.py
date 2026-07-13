# /// script
# dependencies = [
#   "pandas==2.2.3",
#   "openpyxl==3.1.5",
# ]
# ///

import csv
import datetime
from pathlib import Path
import pandas as pd

files = Path("data").rglob("*.xlsx")

df = pd.concat((pd.read_excel(file, dtype=str) for file in files), ignore_index=True)

df["Dosis"] = df["Dosis"].astype(str).str.replace("\n", "|")
df["Dosierung"] = df["Dosierung"].astype(str).str.replace("\n", "|")
df["Wirkstoff"] = df["Wirkstoff"].astype(str).str.replace("\n", "|")

today = datetime.datetime.now().strftime("%Y-%m-%d")

df[
    [
        "Datum",
        "Zeit",
        "Herstellungs-ID",
        "Herstellungsdatum",
        "Herstellungszeit",
        "KIS-Patienten-ID",
        "Station",
        "Wirkstoff",
        "Dosierung",
        "Dosis",
        "Trägerlösung",
        "AdVolumen",
        "Volumen (ml)",
        "Applikationsart",
        "Therapieprotokoll",
        "Retoure",
    ]
].to_csv(
    f"data/{today}-zenzy-export.csv",
    sep=";",
    index=False,
    quoting=csv.QUOTE_ALL,
    escapechar="\\",
)
