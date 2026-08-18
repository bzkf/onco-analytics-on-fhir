import argparse
from pathlib import Path

from loguru import logger

from .extractor import write_eln_risk_csv, write_genes_csv, write_karyotypes_csv
from .models import LetterExtraction


def rebuild_csvs(json_dir: Path, output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    extractions: list[LetterExtraction] = []
    for json_path in sorted(json_dir.glob("*.json")):
        try:
            extractions.append(
                LetterExtraction.model_validate_json(json_path.read_text(encoding="utf-8"))
            )
        except Exception as exc:
            logger.error("skipping {}: {}", json_path.name, exc)

    write_genes_csv(extractions, output_dir / "genes_combined.csv")
    write_karyotypes_csv(extractions, output_dir / "karyotypes_combined.csv")
    write_eln_risk_csv(extractions, output_dir / "eln_risk_combined.csv")
    return len(extractions)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rebuild-csv",
        description=(
            "Rebuild genes_combined.csv/karyotypes_combined.csv/eln_risk_combined.csv "
            "from a folder of per-letter <letter>.json extraction results, e.g. after a "
            "crashed or interrupted extract-genetics run."
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Folder containing <letter>.json extraction results",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Folder to write the combined CSVs into",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    count = rebuild_csvs(args.input_dir, args.output_dir)
    logger.info("Rebuilt CSVs from {} letter(s) into {}", count, args.output_dir)


if __name__ == "__main__":
    main()
