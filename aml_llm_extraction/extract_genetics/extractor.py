import csv
from pathlib import Path

import yaml
from loguru import logger

from .llm import DEFAULT_NUM_CTX, DEFAULT_NUM_PREDICT, extract_genes
from .models import LetterDocument, LetterExtraction

GENES_CSV_HEADER = [
    "patient_mrn",
    "account_id",
    "document_date",
    "letter_status",
    "template_name",
    "source_type",
    "source_file",
    "gene",
    "date",
    "mutated",
    "variant_detail",
    "evidence_text",
]

KARYOTYPES_CSV_HEADER = [
    "patient_mrn",
    "account_id",
    "document_date",
    "letter_status",
    "template_name",
    "source_type",
    "source_file",
    "raw_text",
    "date",
    "classification",
    "evidence_text",
]

ELN_RISK_CSV_HEADER = [
    "patient_mrn",
    "account_id",
    "document_date",
    "letter_status",
    "template_name",
    "source_type",
    "source_file",
    "raw_text",
    "date",
    "risk_category",
    "classification_version",
    "evidence_text",
]


def iter_letters(input_dir: Path) -> list[Path]:
    return sorted(p for ext in ("*.yaml", "*.yml") for p in input_dir.glob(ext))


def load_letter(path: Path) -> LetterDocument:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return LetterDocument.model_validate(data)


def genes_csv_rows(extraction: LetterExtraction) -> list[list]:
    return [
        [
            extraction.patient_id,
            extraction.account_id or "",
            extraction.document_date or "",
            extraction.letter_status or "",
            extraction.template_name or "",
            extraction.source_type,
            extraction.source_file,
            finding.gene,
            finding.date or "",
            finding.mutated,
            finding.variant_detail or "",
            finding.evidence_text or "",
        ]
        for finding in extraction.genes
    ]


def karyotype_csv_rows(extraction: LetterExtraction) -> list[list]:
    return [
        [
            extraction.patient_id,
            extraction.account_id or "",
            extraction.document_date or "",
            extraction.letter_status or "",
            extraction.template_name or "",
            extraction.source_type,
            extraction.source_file,
            karyotype.raw_text,
            karyotype.date or "",
            karyotype.classification,
            karyotype.evidence_text or "",
        ]
        for karyotype in extraction.karyotypes
    ]


def eln_risk_csv_rows(extraction: LetterExtraction) -> list[list]:
    return [
        [
            extraction.patient_id,
            extraction.account_id or "",
            extraction.document_date or "",
            extraction.letter_status or "",
            extraction.template_name or "",
            extraction.source_type,
            extraction.source_file,
            finding.raw_text,
            finding.date or "",
            finding.risk_category,
            finding.classification_version or "",
            finding.evidence_text or "",
        ]
        for finding in extraction.eln_risk
    ]


def process_folder(
    input_dir: Path,
    output_dir: Path,
    *,
    model: str,
    host: str,
    overwrite: bool = False,
    num_ctx: int = DEFAULT_NUM_CTX,
    num_predict: int = DEFAULT_NUM_PREDICT,
) -> list[LetterExtraction]:
    output_dir.mkdir(parents=True, exist_ok=True)
    letters = iter_letters(input_dir)
    if not letters:
        logger.warning("No letter files (.yaml/.yml) found in {}", input_dir)

    processed: list[LetterExtraction] = []
    genes_csv_path = output_dir / "genes_combined.csv"
    karyotypes_csv_path = output_dir / "karyotypes_combined.csv"
    eln_risk_csv_path = output_dir / "eln_risk_combined.csv"

    with (
        genes_csv_path.open("w", newline="", encoding="utf-8") as genes_file,
        karyotypes_csv_path.open("w", newline="", encoding="utf-8") as karyotypes_file,
        eln_risk_csv_path.open("w", newline="", encoding="utf-8") as eln_risk_file,
    ):
        genes_writer = csv.writer(genes_file)
        genes_writer.writerow(GENES_CSV_HEADER)
        karyotypes_writer = csv.writer(karyotypes_file)
        karyotypes_writer.writerow(KARYOTYPES_CSV_HEADER)
        eln_risk_writer = csv.writer(eln_risk_file)
        eln_risk_writer.writerow(ELN_RISK_CSV_HEADER)

        for i, letter_path in enumerate(letters, start=1):
            json_path = output_dir / f"{letter_path.stem}.json"
            if json_path.exists() and not overwrite:
                logger.info(
                    "[{}/{}] skipping {} (already processed)",
                    i,
                    len(letters),
                    letter_path.name,
                )
                extraction = LetterExtraction.model_validate_json(
                    json_path.read_text(encoding="utf-8")
                )
            else:
                logger.info("[{}/{}] processing {}", i, len(letters), letter_path.name)
                try:
                    letter = load_letter(letter_path)
                except Exception as exc:
                    logger.error("  failed to load: {}", exc)
                    continue

                try:
                    result = extract_genes(
                        letter,
                        model=model,
                        host=host,
                        num_ctx=num_ctx,
                        num_predict=num_predict,
                    )
                except Exception as exc:
                    logger.error("  failed: {}", exc)
                    continue

                extraction = LetterExtraction.from_letter(letter, letter_path.name, result)
                json_path.write_text(extraction.model_dump_json(indent=2), encoding="utf-8")

            genes_writer.writerows(genes_csv_rows(extraction))
            genes_file.flush()
            karyotypes_writer.writerows(karyotype_csv_rows(extraction))
            karyotypes_file.flush()
            eln_risk_writer.writerows(eln_risk_csv_rows(extraction))
            eln_risk_file.flush()
            processed.append(extraction)

    return processed


def write_genes_csv(processed: list[LetterExtraction], csv_path: Path) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(GENES_CSV_HEADER)
        for extraction in processed:
            writer.writerows(genes_csv_rows(extraction))


def write_karyotypes_csv(processed: list[LetterExtraction], csv_path: Path) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(KARYOTYPES_CSV_HEADER)
        for extraction in processed:
            writer.writerows(karyotype_csv_rows(extraction))


def write_eln_risk_csv(processed: list[LetterExtraction], csv_path: Path) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(ELN_RISK_CSV_HEADER)
        for extraction in processed:
            writer.writerows(eln_risk_csv_rows(extraction))
