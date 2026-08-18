import argparse
from pathlib import Path

from loguru import logger

from .prepare_letters import process_excel, process_parquet


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prepare-letters",
        description=(
            "Write one letter file per row into --output-dir, ready for extract-genetics. "
            "Source is either a parquet export of XML clinical documents (--parquet-file, "
            "M5_Therapie/M5_Diagnosen sections) or a tumor documentation Excel export "
            "(--excel-file, PatID/Histologie/Molekularpatho/Immunhisto/Risikostrat. columns)."
        ),
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--parquet-file",
        type=Path,
        help="Parquet file with columns xmldocument, patid, patientaccountid, "
        "lastsaveddatetime, letterstatus, templatename",
    )
    source.add_argument(
        "--excel-file",
        type=Path,
        help="Tumor documentation Excel export (.xlsx) with columns PatID, Dokumentdatum, "
        "'Histologie/Zytologie:', 'Molekularpatho/zytologie:', 'Immunhisto/zytologie:', "
        "'Initiale TNM-Klassif./Risikostrat.:'",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Folder to write one letter file per row into",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.excel_file:
        written = process_excel(args.excel_file, args.output_dir)
    else:
        written = process_parquet(args.parquet_file, args.output_dir)
    logger.info("Wrote {} letter(s) to {}", written, args.output_dir)


if __name__ == "__main__":
    main()
