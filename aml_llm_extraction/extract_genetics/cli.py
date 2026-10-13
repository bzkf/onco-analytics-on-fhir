import argparse
from pathlib import Path

import mlflow
from loguru import logger

from .extractor import process_folder
from .llm import DEFAULT_NUM_CTX, DEFAULT_NUM_PREDICT
from .prompts import ensure_system_prompt_registered, read_system_prompt_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extract-genetics",
        description=(
            "Extract gene mutation findings from AML discharge letters using a "
            "locally hosted LLM (via Ollama)."
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Folder containing letter files (.yaml/.yml) produced by prepare-letters",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Folder to write per-letter JSON results and a combined CSV",
    )
    parser.add_argument(
        "--model",
        default="llama3.1",
        help="Ollama model tag to use (default: %(default)s)",
    )
    parser.add_argument(
        "--host",
        default="http://localhost:11434",
        help="Ollama server host (default: %(default)s)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Reprocess letters even if a result JSON already exists",
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=DEFAULT_NUM_CTX,
        help="Ollama context window size in tokens (default: %(default)s)",
    )
    parser.add_argument(
        "--num-predict",
        type=int,
        default=DEFAULT_NUM_PREDICT,
        help="Max tokens the model may generate for one response (default: %(default)s)",
    )
    parser.add_argument(
        "--system-prompt-file",
        type=Path,
        default=None,
        help=(
            "Plain-text (.txt, UTF-8) file whose content replaces the built-in system "
            "prompt. It is registered in the MLflow prompt registry like the built-in one"
        ),
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=None,
        help="MLflow tracking URI (default: local sqlite:///mlflow.db)",
    )
    parser.add_argument(
        "--mlflow-experiment",
        default="extract-genetics",
        help="MLflow experiment name (default: %(default)s)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Read before touching MLflow, so a bad path fails without side effects.
    custom_prompt = None
    if args.system_prompt_file:
        try:
            custom_prompt = read_system_prompt_file(args.system_prompt_file)
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            parser.error(f"--system-prompt-file: {exc}")

    if args.mlflow_tracking_uri:
        mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    mlflow.set_experiment(args.mlflow_experiment)
    mlflow.openai.autolog()

    if custom_prompt is None:
        prompt = ensure_system_prompt_registered()
    else:
        prompt = ensure_system_prompt_registered(custom_prompt, source=str(args.system_prompt_file))
        logger.info("Using system prompt from {}", args.system_prompt_file)
    logger.info("Using prompt '{}' version {}", prompt.name, prompt.version)

    process_folder(
        args.input_dir,
        args.output_dir,
        model=args.model,
        host=args.host,
        overwrite=args.overwrite,
        num_ctx=args.num_ctx,
        num_predict=args.num_predict,
        # Pinned to the exact version rather than @latest, so a concurrent run registering
        # a different prompt can't change which prompt this run uses mid-way.
        prompt_uri=f"prompts:/{prompt.name}/{prompt.version}",
    )


if __name__ == "__main__":
    main()
