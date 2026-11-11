import argparse
from pathlib import Path

from loguru import logger

from .prepare_letters import process_csv, process_excel, process_parquet


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prepare-letters",
        description=(
            "Write one letter file per row into --output-dir, ready for extract-genetics. "
            "Source is a parquet export of XML clinical documents (--parquet-file, "
            "M5_Therapie/M5_Diagnosen sections), a tumor documentation Excel export "
            "(--excel-file, PatID/Histologie/Molekularpatho/Immunhisto/Risikostrat. columns), "
            "or a pathology report CSV export (--csv-file, PID/Befundtext columns)."
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
    source.add_argument(
        "--csv-file",
        type=Path,
        help="Comma-separated pathology report export (UTF-8) with columns PID and "
        "Befundtext; other columns (e.g. DIAGKURZ, SendingApplication) are ignored",
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
    elif args.csv_file:
        written = process_csv(args.csv_file, args.output_dir)
    else:
        written = process_parquet(args.parquet_file, args.output_dir)
    logger.info("Wrote {} letter(s) to {}", written, args.output_dir)


if __name__ == "__main__":
    main()
