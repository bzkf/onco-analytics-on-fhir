import os
from hashlib import sha256
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from loguru import logger
from settings import settings

HERE = Path.cwd()

patients_with_diagnoses = pd.read_csv(
    HERE / "results" / "aml" / "aml_all_patients.csv",
    sep=",",
    dtype={"lab_quantity_value": "float"},
    parse_dates=["diagnosis_recordedDate", "diagnosis_onsetDateTime"],
)

patients_with_diagnoses.groupby("icd_code")
