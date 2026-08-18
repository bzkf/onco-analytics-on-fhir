import re
from pathlib import Path

import pandas as pd
import yaml
from loguru import logger

from . import excel_letters
from .models import LetterDocument
from .xml_letters import THERAPIE_SECTION_ID, build_letter


def _str_presenter(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


yaml.add_representer(str, _str_presenter)


def sanitize_for_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-")


def write_letter(letter: LetterDocument, path: Path) -> None:
    path.write_text(
        yaml.dump(letter.model_dump(), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _letter_stem(letter: LetterDocument, idx: object) -> str:
    date_part = sanitize_for_filename(letter.document_date) if letter.document_date else None
    parts = [sanitize_for_filename(letter.patient_id)]
    if letter.account_id:
        parts.append(sanitize_for_filename(letter.account_id))
    parts.append(date_part or str(idx))
    return "_".join(parts)


def _write_letter_file(letter: LetterDocument, output_dir: Path, idx: object) -> None:
    stem = _letter_stem(letter, idx)
    write_letter(letter, output_dir / f"{stem}.yaml")


def process_parquet(
    parquet_path: Path,
    output_dir: Path,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(parquet_path)

    written = 0
    for idx, row in df.iterrows():
        letter = build_letter(row.to_dict())
        if letter is None:
            logger.warning(
                "[{}] skipping patid={}: section '{}' not found or empty",
                idx,
                row.get("patid"),
                THERAPIE_SECTION_ID,
            )
            continue

        _write_letter_file(letter, output_dir, idx)
        written += 1

    return written


def process_excel(
    excel_path: Path,
    output_dir: Path,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_excel(excel_path)

    written = 0
    for idx, row in df.iterrows():
        letter = excel_letters.build_letter(row.to_dict())
        if letter is None:
            logger.warning(
                "[{}] skipping patid={}: no usable free text found in any of the "
                "histology/molecular-patho/immunohisto/risk-stratification columns",
                idx,
                row.get(excel_letters.PATIENT_ID_COLUMN),
            )
            continue

        _write_letter_file(letter, output_dir, idx)
        written += 1

    return written
