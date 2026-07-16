# /// script
# dependencies = [
#   "pandas==2.2.3",
# ]
# ///

import csv
import datetime
from pathlib import Path

import pandas as pd

# Cato CSV exports don't always contain the same set of columns, so every
# source column is looked up defensively instead of assumed to be present.
DATETIME_FORMAT = "%d %m %Y %H:%M"


def column_or_blank(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name].fillna("")
    return pd.Series([""] * len(df), index=df.index)


def split_datetime(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    parsed = pd.to_datetime(series, format=DATETIME_FORMAT, errors="coerce")
    date = parsed.dt.strftime("%d.%m.%Y").fillna("")
    time = parsed.dt.strftime("%H:%M").fillna("")
    return date, time


files = [file for file in Path("data").rglob("*.csv") if "cato-export" not in file.name]

df = pd.concat(
    (
        pd.read_csv(file, sep=";", dtype=str, encoding="utf-8", encoding_errors="replace")
        for file in files
    ),
    ignore_index=True,
)

datum, zeit = split_datetime(column_or_blank(df, "Geplanter Verabreichungszeitpunkt"))
herstellungsdatum, herstellungszeit = split_datetime(
    column_or_blank(df, "Herstellungsstart Medikation")
)

out = pd.DataFrame(
    {
        "Datum": datum,
        "Zeit": zeit,
        "Herstellungs-ID": column_or_blank(df, "Herst.Nr."),
        "Herstellungsdatum": herstellungsdatum,
        "Herstellungszeit": herstellungszeit,
        "Privatpatient": "",
        "Patienten-ID": "",
        "KIS-Patienten-ID": column_or_blank(df, "Pat.Nr."),
        "Kostenstellennummer": "",
        "Station": column_or_blank(df, "Station"),
        "Wirkstoff": column_or_blank(df, "Arzneiform"),
        "Dosierung": column_or_blank(df, "Verordnete Dosis"),
        "Dosis": column_or_blank(df, "Ist-Dosis "),
        "Reduktion": column_or_blank(df, "Differenz"),
        "Trägerlösung": "",
        "AdVolumen": "",
        "Volumen (ml)": "",
        "Applikationsart": "",
        "Therapieprotokoll": "",
        "Aufnahmenummer": "",
        "Retoure": "",
    }
)

today = datetime.datetime.now().strftime("%Y-%m-%d")

out.to_csv(
    f"data/{today}-cato-export-zenzy-formatted.csv",
    sep=",",
    index=False,
    quoting=csv.QUOTE_ALL,
    escapechar="\\",
)
