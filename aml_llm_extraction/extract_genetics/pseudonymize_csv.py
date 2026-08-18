import csv
import hashlib
import hmac
import secrets
from pathlib import Path

from loguru import logger

GENES_CSV_IN = Path("output/genes_combined.csv")
GENES_CSV_OUT = Path("output/genes_combined.pseudonymized.csv")
KARYOTYPES_CSV_IN = Path("output/karyotypes_combined.csv")
KARYOTYPES_CSV_OUT = Path("output/karyotypes_combined.pseudonymized.csv")
ELN_RISK_CSV_IN = Path("output/eln_risk_combined.csv")
ELN_RISK_CSV_OUT = Path("output/eln_risk_combined.pseudonymized.csv")

HASHED_COLUMNS = ["patient_mrn", "account_id", "source_file"]


def hash_value(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def pseudonymize_csv(input_path: Path, output_path: Path, *, key: bytes) -> None:
    with input_path.open("r", newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    for row in rows:
        for column in HASHED_COLUMNS:
            if row.get(column):
                row[column] = hash_value(row[column], key)

    with output_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Hashed {} row(s) in {} -> {}", len(rows), input_path, output_path)


def main() -> None:
    # A fresh, cryptographically random key each run. It's used for all three files
    # below so patients stay joinable between them, but logged since it's the only way
    # to reproduce or verify these exact hashes afterward — otherwise it's gone for good.
    key = secrets.token_bytes(32)
    logger.info("Generated random HMAC key: {}", key.hex())

    pseudonymize_csv(GENES_CSV_IN, GENES_CSV_OUT, key=key)
    pseudonymize_csv(KARYOTYPES_CSV_IN, KARYOTYPES_CSV_OUT, key=key)
    pseudonymize_csv(ELN_RISK_CSV_IN, ELN_RISK_CSV_OUT, key=key)


if __name__ == "__main__":
    main()
