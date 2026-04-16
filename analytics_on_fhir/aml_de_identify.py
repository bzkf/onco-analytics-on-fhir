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

patients_with_diagnoses = pd.read_csv(
    HERE / "results" / "aml" / "aml_all_patients.csv",
    parse_dates=[
        "diagnosis_onsetDateTime",
        "diagnosis_recordedDate",
        "deceased_dateTime",
        "birth_date",
    ],
    dtype={"patient_mrn": str},
)

patients_with_diagnoses["condition_id"] = patients_with_diagnoses["condition_id"].apply(
    lambda x: crypto_hash(x)
)

patients_with_diagnoses["birth_year"] = patients_with_diagnoses["birth_date"].dt.year

patients_with_diagnoses = patients_with_diagnoses.drop(columns=["birth_date"])


patients_with_diagnoses["condition_patient_reference"] = patients_with_diagnoses[
    "condition_patient_reference"
].apply(lambda x: crypto_hash(x))

patients_with_diagnoses["condition_patient_reference"] = patients_with_diagnoses[
    "condition_patient_reference"
].apply(lambda x: crypto_hash(x))

patients_with_diagnoses["patient_mrn"] = patients_with_diagnoses["patient_mrn"].apply(
    lambda x: crypto_hash(str(x))
)

columns_to_shift = ["diagnosis_onsetDateTime", "diagnosis_recordedDate", "deceased_dateTime"]
for column in columns_to_shift:
    patients_with_diagnoses[column] = patients_with_diagnoses[column] + pd.to_timedelta(
        DAY_SHIFT, unit="D"
    )


diagnosis_unique = patients_with_diagnoses.drop_duplicates(
    subset="condition_patient_reference",
    keep="last",  # or "first"
)


# skip lab
# lab_with_diagnosis = df.merge(
#     diagnosis_unique[["condition_patient_reference", "patient_mrn"]],
#     left_on="observation_patient_reference",
#     right_on="condition_patient_reference",
#     how="left",
# )

# lab_with_diagnosis["observation_id"] = lab_with_diagnosis["observation_id"].apply(
#     lambda x: crypto_hash(x)
# )
# lab_with_diagnosis["patient_mrn"] = lab_with_diagnosis["patient_mrn"].apply(
#     lambda x: crypto_hash(str(x))
# )
# lab_with_diagnosis["observation_patient_reference"] = lab_with_diagnosis["observation_id"].apply(
#     lambda x: crypto_hash(x)
# )
# lab_with_diagnosis["condition_patient_reference"] = lab_with_diagnosis[
#     "condition_patient_reference"
# ].apply(lambda x: crypto_hash(str(x)))
# lab_with_diagnosis["lab_dateTime"] = lab_with_diagnosis["lab_dateTime"] + pd.to_timedelta(
#     DAY_SHIFT, unit="D"
# )

zenzy_df = pd.read_csv(
    settings.aml.csv_input_file,
    sep=";",
    dtype={"patient_mrn": str},
).drop(columns=["Volumen (ml)"])

zenzy_df = zenzy_df[zenzy_df["KIS-Patienten-ID"] != "*** VALUE NOT FOUND ***"]

zenzy_df["Applikationszeitpunkt"] = pd.to_datetime(
    zenzy_df["Datum"] + " " + zenzy_df["Zeit"], format="%d.%m.%Y %H:%M", errors="raise"
)

zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"].apply(
    lambda x: (
        x + pd.Timedelta(hours=1)
        if (x.day == 31 and x.month == 3 and x.hour == 2)
        or (x.day == 25 and x.month == 3 and x.hour == 2)
        else x
    )
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

zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"] + pd.to_timedelta(
    DAY_SHIFT, unit="D"
)

zenzy_df["Herstellungs-ID"] = zenzy_df["Herstellungs-ID"].apply(lambda x: crypto_hash(str(x)))
zenzy_df["patient_mrn"] = zenzy_df["KIS-Patienten-ID"].apply(lambda x: crypto_hash(str(x)))

zenzy_df = zenzy_df.drop(
    columns=[
        "KIS-Patienten-ID",
        "Datum",
        "Zeit",
        "Herstellungsdatum",
        "Herstellungszeit",
    ],
    inplace=False,
)

# obds_systemtherapien = pd.read_csv(
#     HERE / "results" / "aml" / "df_obds_systemtherapien.csv",
#     parse_dates=[
#         "medication_start_date",
#     ],
#     dtype={"patient_mrn": str},
# )

# columns_to_hash = ["patient_mrn", "medication_statement_id", "patient_reference", "condition_id"]
# for column in columns_to_hash:
#     obds_systemtherapien[column] = obds_systemtherapien[column].apply(crypto_hash)

# columns_to_shift = ["medication_start_date"]
# for column in columns_to_shift:
#     obds_systemtherapien[column] = obds_systemtherapien[column] + pd.to_timedelta(
#         DAY_SHIFT, unit="D"
#     )

# obds_weitere_klassifikationen = pd.read_csv(
#     HERE / "results" / "aml" / "df_obds_weitere_klassifikationen.csv",
#     parse_dates=[
#         "effective_date_time",
#     ],
#     dtype={"patient_mrn": str},
# )

# columns_to_hash = [
#     "patient_mrn",
#     "observation_id",
#     "observation_patient_reference",
#     "observation_condition_reference",
#     "patient_id",
# ]
# for column in columns_to_hash:
#     obds_weitere_klassifikationen[column] = obds_weitere_klassifikationen[column].apply(crypto_hash)

# columns_to_shift = ["effective_date_time"]
# for column in columns_to_shift:
#     obds_weitere_klassifikationen[column] = obds_weitere_klassifikationen[column] + pd.to_timedelta(
#         DAY_SHIFT, unit="D"
#     )

# with open(
#     HERE / "results" / "aml" / "2026-04-15 F_MED_REZEPTE_202604151635 Pseudonymisiert_qs.csv",
#     encoding="utf-16-le",
# ) as f:
#     import re

#     csv = f.read()

#     target_column = "Pseudonyme von PATIENT_ID"

#     # Find column index from header (first line)
#     header = re.match(r"^[^\n]*", csv).group(0)
#     columns = re.findall(r"[^;]+", header)
#     col_idx = columns.index(target_column)

#     # Extract that column from every row (including header), preserving row order/newlines
#     pattern = rf"^(?:[^;\n]*;){{{col_idx}}}([^;\n]*)(?:;|$)"
#     extracted_values = re.findall(pattern, csv, flags=re.MULTILINE)

#     with open("fixed.csv", "w") as file:
#         lines = [l + "\n" for l in extracted_values]
#         file.writelines(lines)

# original_sap_medikation = pd.read_csv(
#     HERE / "results" / "aml" / "F_MED_REZEPTE_202604151635.csv", sep=";"
# )

# fixed = pd.read_csv(HERE / "fixed.csv")

# original_sap_medikation["Pseudonyme von PATIENT_ID"] = fixed.iloc[:, 0].values

# original_sap_medikation.to_csv(HERE / "sap_medikation_working_pseuded.csv", index=False, sep=";")

sap_medikation = (
    pd.read_csv(
        HERE / "sap_medikation_working_pseuded.csv",
        sep=";",
        parse_dates=["AUFNAHME_DATUM", "ENTLASS_DATUM"],
    )
    .drop(columns=["FALL_ID", "TEILFALL_ID", "PATIENT_ID"])
    .rename(columns={"Pseudonyme von PATIENT_ID": "patient_mrn"})
)

sap_medikation["REZEPT_DATUM"] = pd.to_datetime(
    sap_medikation["REZEPT_DATUM"], format="%Y-%m-%d", errors="coerce"
)

sap_medikation["ENTLASS_DATUM"] = pd.to_datetime(
    sap_medikation["ENTLASS_DATUM"], format="%Y-%m-%d", errors="coerce"
)
sap_medikation["AUFNAHME_DATUM"] = pd.to_datetime(
    sap_medikation["AUFNAHME_DATUM"], format="%Y-%m-%d", errors="coerce"
)

columns_to_hash = [
    "REZEPT_ID",
    "patient_mrn",
]
for column in columns_to_hash:
    sap_medikation[column] = sap_medikation[column].apply(crypto_hash)

columns_to_shift = ["REZEPT_DATUM", "ENTLASS_DATUM", "AUFNAHME_DATUM"]
for column in columns_to_shift:
    sap_medikation[column] = sap_medikation[column] + pd.to_timedelta(DAY_SHIFT, unit="D")


meona_medikation = pd.read_csv(
    HERE / "results" / "aml" / "aml_all_med_reqs_stats_admins.csv",
    sep=",",
    parse_dates=["datetime", "period_end"],
)

columns_to_hash = [
    "id",
    "patient_reference",
    "patient_mrn",
]
for column in columns_to_hash:
    meona_medikation[column] = meona_medikation[column].apply(crypto_hash)

columns_to_shift = ["datetime", "period_end"]
for column in columns_to_shift:
    meona_medikation[column] = meona_medikation[column] + pd.to_timedelta(DAY_SHIFT, unit="D")


de_identified_dir = HERE / "results" / "aml" / "de-identified"
Path.mkdir(de_identified_dir, parents=True, exist_ok=True)

# lab_with_diagnosis.to_csv(de_identified_dir / "aml_matched_zenzy_labs.csv", index=False)
zenzy_df.to_csv(de_identified_dir / "aml_zenzy.csv", index=False)
patients_with_diagnoses.to_csv(de_identified_dir / "aml_diagnoses.csv", index=False)
sap_medikation.to_csv(de_identified_dir / "sap_medikation.csv", index=False)
meona_medikation.to_csv(de_identified_dir / "meona_medikation.csv", index=False)

# obds_weitere_klassifikationen.to_csv(
#     de_identified_dir / "aml_obds_weitere_klassifikationen.csv", index=False
# )
# obds_systemtherapien.to_csv(de_identified_dir / "aml_obds_systemtherapien.csv", index=False)
