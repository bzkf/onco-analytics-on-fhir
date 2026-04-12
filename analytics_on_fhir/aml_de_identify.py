import hashlib
import hmac
import os
import secrets
from pathlib import Path

import pandas as pd
from settings import settings

HERE = Path(os.path.abspath(os.path.dirname(__file__)))
CRYPTO_HASH_KEY = secrets.token_bytes(64)
DAY_SHIFT = secrets.randbelow(201) - 100


def crypto_hash(s: str):
    return hmac.new(CRYPTO_HASH_KEY, s.encode("utf-8"), hashlib.sha256).hexdigest()


df = pd.read_csv(
    HERE / "results" / "aml" / "aml_matched_zenzy_labs.csv",
    sep=",",
    dtype={"lab_quantity_value": "float"},
    parse_dates=["lab_dateTime"],
)

patients_with_diagnoses = pd.read_csv(HERE / "results" / "aml" / "aml_all_patients.csv")

diagnosis_unique = patients_with_diagnoses.drop_duplicates(
    subset="condition_patient_reference",
    keep="last",  # or "first"
)


lab_with_diagnosis = df.merge(
    diagnosis_unique[["condition_patient_reference", "patient_mrn"]],
    left_on="observation_patient_reference",
    right_on="condition_patient_reference",
    how="left",
)

lab_with_diagnosis["observation_id"] = lab_with_diagnosis["observation_id"].apply(
    lambda x: crypto_hash(x)
)
lab_with_diagnosis["patient_mrn"] = lab_with_diagnosis["patient_mrn"].apply(
    lambda x: crypto_hash(x)
)
lab_with_diagnosis["observation_patient_reference"] = lab_with_diagnosis["observation_id"].apply(
    lambda x: crypto_hash(x)
)
lab_with_diagnosis["condition_patient_reference"] = lab_with_diagnosis[
    "condition_patient_reference"
].apply(lambda x: crypto_hash(x))
lab_with_diagnosis["lab_dateTime"] = lab_with_diagnosis["lab_dateTime"] + pd.to_timedelta(
    DAY_SHIFT, unit="D"
)

zenzy_df = pd.read_csv(settings.aml.csv_input_file, sep=";")
zenzy_df["Applikationszeitpunkt"] = pd.to_datetime(
    zenzy_df["Datum"] + " " + zenzy_df["Zeit"], format="%d.%m.%Y %H:%M", errors="raise"
)

zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"].apply(
    lambda x: x + pd.Timedelta(hours=1) if x.day == 31 and x.month == 3 and x.hour == 2 else x
)

zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"].dt.tz_localize(
    "Europe/Berlin", ambiguous="NaT"
)

zenzy_df["label"] = (
    "Wirkstoff: "
    + zenzy_df["Wirkstoff"].astype(str)
    + " ("
    + "Dosis: "
    + zenzy_df["Dosis"].astype(str)
    + ") "
    + "Protokoll: "
    + zenzy_df["Therapieprotokoll"].astype(str)
    + " "
    + "Applikationsart: "
    + zenzy_df["Applikationsart"].astype(str)
    + " "
    + "Retoure?: "
    + zenzy_df["Retoure"].astype(str)
)

zenzy_df["Herstellungs-ID"] = zenzy_df["Herstellungs-ID"].apply(lambda x: crypto_hash(str(x)))
zenzy_df["patient_mrn"] = zenzy_df["KIS-Patienten-ID"].apply(lambda x: crypto_hash(x))
zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"] + pd.to_timedelta(
    DAY_SHIFT, unit="D"
)

zenzy_df = zenzy_df.drop(
    columns=[
        "Station",
        "KIS-Patienten-ID",
        "Aufnahmenummer",
        "Datum",
        "Zeit",
        "Herstellungsdatum",
        "Herstellungszeit",
    ],
    inplace=False,
)

de_identified_dir = HERE / "results" / "aml" / "de-identified"
Path.mkdir(de_identified_dir, parents=True, exist_ok=True)

lab_with_diagnosis.to_csv(de_identified_dir / "aml_matched_zenzy_labs.csv", index=False, sep=";")
zenzy_df.to_csv(de_identified_dir / "aml_zenzy.csv", index=False, sep=";")
