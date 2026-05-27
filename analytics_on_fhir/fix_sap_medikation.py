import pandas as pd


def extract_column_raw(
    csv_path: str,
    column_name: str,
    sep: str = ";",
) -> list:
    """
    Extract a column by splitting physical lines directly.
    Ignores CSV quoting rules completely.
    """

    values = []

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # header
    header = lines[0].rstrip("\n\r").split(sep)

    if column_name not in header:
        raise ValueError(f"Column '{column_name}' not found.")

    col_idx = header.index(column_name)

    # data rows
    for line_number, line in enumerate(lines[1:], start=2):
        parts = line.rstrip("\n\r").split(sep)

        if len(parts) > col_idx:
            values.append(parts[col_idx])
        else:
            print(f"Warning: Line {line_number} has only {len(parts)} columns")
            values.append(None)

    return values


def add_column_from_pseudonymized(
    original_csv_path: str,
    pseudonymized_csv_path: str,
    column_to_add: str,
    output_csv_path: str,
) -> None:

    original_df = pd.read_csv(original_csv_path, sep=";")

    pseudo_values = extract_column_raw(
        pseudonymized_csv_path,
        column_to_add,
    )

    print("Original rows:", len(original_df))
    print("Pseudo rows:", len(pseudo_values))

    if len(original_df) != len(pseudo_values):
        raise ValueError(f"Row count mismatch:\n{len(original_df)} vs {len(pseudo_values)}")

    original_df[column_to_add] = pseudo_values

    original_df.to_csv(output_csv_path, index=False, sep=";")


if __name__ == "__main__":
    add_column_from_pseudonymized(
        original_csv_path="./data/F_MED_REZEPTE_plausible_202604151635.csv",
        pseudonymized_csv_path="./data/2026-05-26 F_MED_REZEPTE_plausible_202604151635 Pseudonymisiert gPAS.csv",
        column_to_add="Pseudonyme von PATIENT_ID",
        output_csv_path="./data/original_with_column.csv",
    )
