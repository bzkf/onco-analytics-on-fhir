import mlflow
from loguru import logger
from openai import OpenAI

from .models import ExtractionResult, LetterDocument
from .prompts import PROMPT_NAME

# Sometimes the model returns an empty result (no genes, no karyotypes) even though the
# same wording is correctly extracted elsewhere - retry a few times at a small nonzero
# temperature to nudge it off whatever produced the empty response, capped at 0.1 so it
# never strays far enough from temperature=0 to start hallucinating.
MAX_RETRIES = 3
RETRY_TEMPERATURES = [0.0, 0.1, 0.1, 0.1]

# Defaults for the Ollama context window / max output tokens, overridable via
# extract-genetics --num-ctx/--num-predict for models with different context limits.
DEFAULT_NUM_CTX = 32000
DEFAULT_NUM_PREDICT = 16000


def _build_user_message(letter: LetterDocument) -> str:
    parts = []
    if letter.document_date:
        parts.append(f"## Document date\n{letter.document_date}")
    if letter.diagnosis_text:
        parts.append(f"## Diagnosis summary\n{letter.diagnosis_text}")
    if letter.therapie_text:
        parts.append(f"## Therapy/follow-up timeline\n{letter.therapie_text}")
    if letter.histologie_text:
        parts.append(f"## Histology/cytology\n{letter.histologie_text}")
    if letter.molekularpatho_text:
        parts.append(f"## Molecular pathology/cytogenetics\n{letter.molekularpatho_text}")
    if letter.immunhisto_text:
        parts.append(f"## Immunohistochemistry/immunophenotyping\n{letter.immunhisto_text}")
    if letter.initial_risk_stratification_text:
        parts.append(f"## Initial risk stratification\n{letter.initial_risk_stratification_text}")
    return "\n\n".join(parts)


def _trace_request_preview(letter: LetterDocument) -> str:
    parts = [
        letter.patient_id,
        letter.account_id,
        letter.document_date,
        letter.template_name,
    ]
    return " / ".join(part for part in parts if part)


def _is_empty_result(result: ExtractionResult) -> bool:
    return not result.genes and not result.karyotypes and not result.eln_risk


def _call_llm(
    letter: LetterDocument,
    *,
    model: str,
    host: str,
    prompt_template: str,
    temperature: float,
    num_ctx: int,
    num_predict: int,
) -> ExtractionResult:
    # Ollama exposes an OpenAI-compatible API; going through the openai client (rather
    # than the native ollama client) lets mlflow.openai.autolog() trace these calls.
    client = OpenAI(base_url=f"{host}/v1", api_key="ollama")
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": prompt_template},
            {"role": "user", "content": _build_user_message(letter)},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "extraction_result",
                "schema": ExtractionResult.model_json_schema(),
                "strict": True,
            },
        },
        temperature=temperature,
        extra_body={"options": {"num_ctx": num_ctx, "num_predict": num_predict}},
    )
    content = response.choices[0].message.content
    if content is None:
        raise ValueError("Ollama returned an empty response")
    return ExtractionResult.model_validate_json(content)


@mlflow.trace
def extract_genes(
    letter: LetterDocument,
    *,
    model: str,
    host: str,
    num_ctx: int = DEFAULT_NUM_CTX,
    num_predict: int = DEFAULT_NUM_PREDICT,
) -> ExtractionResult:
    # Replace the default request preview (a JSON dump of the raw letter, including the
    # full therapy text) with a short, human-readable identifier for the MLflow UI.
    mlflow.update_current_trace(request_preview=_trace_request_preview(letter))

    # Loading the prompt inside the traced call (rather than passing an already-resolved
    # PromptVersion in) is what makes MLflow auto-link it to this trace (mlflow.linkedPrompts).
    prompt = mlflow.genai.load_prompt(f"prompts:/{PROMPT_NAME}@latest")

    attempt = 0
    result = _call_llm(
        letter,
        model=model,
        host=host,
        prompt_template=prompt.template,
        temperature=RETRY_TEMPERATURES[attempt],
        num_ctx=num_ctx,
        num_predict=num_predict,
    )
    while _is_empty_result(result) and attempt < MAX_RETRIES:
        attempt += 1
        temperature = RETRY_TEMPERATURES[attempt]
        logger.warning(
            "Empty extraction result for {}/{}, retrying with temperature={} (attempt {}/{})",
            letter.patient_id,
            letter.account_id,
            temperature,
            attempt,
            MAX_RETRIES,
        )
        result = _call_llm(
            letter,
            model=model,
            host=host,
            prompt_template=prompt.template,
            temperature=temperature,
            num_ctx=num_ctx,
            num_predict=num_predict,
        )

    mlflow.update_current_trace(tags={"extraction_retries": str(attempt)})
    return result
