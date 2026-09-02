# extract-genetics

Extract gene mutation findings (e.g. FLT3, NPM1, TP53, ...), the karyotype/cytogenetics
result, and any already-stated
[ELN 2022](https://ashpublications.org/blood/article/140/12/1345/485817/Diagnosis-and-management-of-AML-in-adults-2022)
risk classification from AML discharge letters or tumor documentation exports using a
locally hosted LLM, via [Ollama](https://ollama.com).

The pipeline has two steps:

1. `prepare-letters` — turn a parquet export of XML clinical documents, or a tumor
   documentation Excel export, into one letter file per document/row, ready for
   extraction.
2. `extract-genetics` — send each letter to a local LLM and extract structured findings.

## Setup

Install [Ollama](https://ollama.com/download) and pull a model:

```sh
ollama pull llama3.1
```

Install project dependencies:

```sh
uv sync
```

## Step 1: prepare letters from a source export

`prepare-letters` takes one of two mutually exclusive source formats and writes one
YAML letter file per row into `--output-dir`, ready for `extract-genetics`.

### Option A: parquet export of XML clinical documents

Source documents arrive as a parquet file where each row is a clinical document
export, with these columns:

- `xmldocument` — the document as an XML string. Body sections carry their text as
  HTML inside a `CDATA` block, e.g. a section with `ID="M5_Therapie"` containing
  `<![CDATA[<HTML>...<p>Zytogenetik (Labor XYZ): komplex aberranter Karyotyp.</p>...</HTML>]]>`.
- `patid` — patient id
- `patientaccountid` — encounter/account number
- `lastsaveddatetime`, `letterstatus`, `templatename` — additional metadata

`prepare-letters --parquet-file` always extracts the `M5_Therapie` ("Therapie und
Verlauf") and `M5_Diagnosen` ("Diagnosen") sections, converts the embedded HTML to
Markdown, and writes one YAML file per row containing both the metadata and the text
from both sections.

`M5_Therapie` is often a multi-year chronological table where the diagnostic
mutation/karyotype status only appears once (in the earliest entry) surrounded by
years of repetitive follow-up monitoring — `M5_Diagnosen` tends to be a much more
concise, reliable statement of the primary finding, so both are passed to the LLM
labeled separately (see `Step 2`).

```sh
uv run prepare-letters --parquet-file data/export.parquet --output-dir data/letters
```

Rows whose XML has no `M5_Therapie` section are skipped with a warning (a missing
`M5_Diagnosen` is not fatal — that field is just left empty).

A test fixture is available: `test/demo-doc.xml` is a redacted sample document, and
`uv run python test/generate_sample_parquet.py` builds `test/sample.parquet` from it
(columns as above) for trying out `prepare-letters` without real patient data.

Output example (`data/letters/12345_1111111_2026-04-30-08-31-00.yaml`):

```yaml
source_type: xml
patient_id: '12345'
account_id: '1111111'
document_date: '2026-04-30 08:31:00'
letter_status: final
template_name: M5 Arztbrief
therapie_section: M5_Therapie
therapie_text: |-
  ... M5_Therapie content converted to Markdown ...
therapie_source_html: |-
  ... original HTML extracted from the M5_Therapie section's CDATA ...
diagnosis_section: M5_Diagnosen
diagnosis_text: |-
  ... M5_Diagnosen content converted to Markdown ...
diagnosis_source_html: |-
  ... original HTML extracted from the M5_Diagnosen section's CDATA ...
```

### Option B: tumor documentation Excel export

Alternatively, source documents can come from a tumor documentation system's Excel
export (`.xlsx`), where each row is a documentation entry with (among others) these
columns:

- `PatID` — patient id
- `Dokumentdatum` — the date this documentation entry was created
- `Histologie/Zytologie:`, `Molekularpatho/zytologie:`, `Immunhisto/zytologie:` — free-text
  histology, molecular pathology/cytogenetics, and immunohistochemistry/immunophenotyping
  report text, all searched for gene/karyotype findings
- `Initiale TNM-Klassif./Risikostrat.:` — free-text initial risk stratification, the most
  likely place for an explicitly stated ELN risk category

```sh
uv run prepare-letters --excel-file data/export.xlsx --output-dir data/letters
```

Rows with no usable text in any of the four columns above are skipped with a warning.
This source has no encounter/account-id concept, so `account_id` is left `null`, and the
XML-specific fields (`therapie_section`, `diagnosis_text`, etc.) are also `null`. Light
inline markup used by the source system (e.g. `<F>...</>`) is stripped from the free text.

A test fixture is available: `test/Test_AML_Projekt.XLSX` is a sample export row for
trying out `prepare-letters --excel-file` without real patient data.

## Step 2: extract findings

```sh
uv run extract-genetics --input-dir data/letters --output-dir output
```

Options:

- `--model` — Ollama model tag to use (default: `llama3.1`)
- `--host` — Ollama server URL (default: `http://localhost:11434`)
- `--overwrite` — reprocess letters that already have a result file
- `--num-ctx` — Ollama context window size in tokens (default: `32000`)
- `--num-predict` — max tokens the model may generate for one response (default: `16000`)
- `--mlflow-tracking-uri` — MLflow tracking URI (default: local `sqlite:///mlflow.db`)
- `--mlflow-experiment` — MLflow experiment name (default: `extract-genetics`)

A model with a smaller native context window than the `--num-ctx` default may fail or
silently truncate its output — check the model's context limit and lower `--num-ctx` (and
`--num-predict` if needed) to fit.

The model occasionally returns an empty result (no genes, no karyotypes) for a letter
where the same wording is correctly extracted elsewhere. When that happens,
`extract_genes()` (in `llm.py`) retries up to 3 times at `temperature=0.1` (vs. `0.0` on
the first attempt) to get a different sample without straying far enough to start
hallucinating, and stops as soon as a non-empty result comes back. Each trace is tagged
with `extraction_retries` (how many retries it took) for visibility into how often this
happens.

## Tracing with MLflow

Every LLM call is traced with [MLflow](https://mlflow.org) (via
[`mlflow.openai.autolog()`](https://mlflow.org/docs/latest/genai/tracing/integrations/listing/ollama/),
since Ollama exposes an OpenAI-compatible API). Each trace captures the exact prompt, the
model's raw response, latency, and token usage — useful for debugging extraction quality
and comparing models.

By default traces are written to a local `mlflow.db` SQLite file in the working directory.
To inspect them:

```sh
uv run mlflow ui
```

then open <http://localhost:5000> and select the `extract-genetics` experiment. Point
`--mlflow-tracking-uri` at a remote MLflow server to centralize tracking instead.

### Prompt registry

The system prompt (`extract_genetics/prompts.py`) and its response schema
(`ExtractionResult.model_json_schema()`) are registered with
[MLflow's prompt registry](https://mlflow.org/docs/latest/genai/prompt-registry/) under the
name `extract-genetics-system-prompt`. On every run, `extract-genetics` registers a new
version only if the prompt text or schema in `prompts.py` has changed since the last run —
otherwise it reuses the existing version.

`extract_genes()` (in `llm.py`) is decorated with `@mlflow.trace` and calls
`mlflow.genai.load_prompt("prompts:/extract-genetics-system-prompt@latest")` *inside* that
traced call. Loading a prompt from within an active trace is what makes MLflow record the
proper `mlflow.linkedPrompts` trace tag automatically — the officially-linked way to see
which exact prompt version produced a given extraction from the MLflow UI, rather than an
ad-hoc custom tag. Note that MLflow's `response_format` is stored for documentation/versioning
only — the actual schema enforcement still happens via the `response_format` we pass to the
chat completion, sourced directly from `ExtractionResult.model_json_schema()`.

By default, a trace's title in the MLflow UI is a JSON dump of its raw inputs — for us
that would be the entire letter, including the full therapy text. `extract_genes()` overrides
this via `mlflow.update_current_trace(request_preview=...)` with a short
`patient_id / account_id / document_date / template_name` identifier instead.

## Output

For each letter file, a `<letter>.json` is written to `--output-dir` with the patient
metadata and the structured extraction result:

```json
{
  "source_type": "xml",
  "patient_id": "12345",
  "account_id": "1111111",
  "document_date": "2026-04-30 08:31:00",
  "letter_status": "final",
  "template_name": "M5 Arztbrief",
  "therapie_section": "M5_Therapie",
  "source_file": "12345_1111111_2026-04-30-08-31-00.yaml",
  "genes": [
    {
      "gene": "FLT3",
      "mutated": true,
      "variant_detail": "ITD",
      "date": "2019-12",
      "evidence_text": "molekulargenetisch Nachweis einer FLT3-ITD-Mutation"
    },
    {
      "gene": "NPM1",
      "mutated": false,
      "variant_detail": null,
      "date": "2019-12",
      "evidence_text": "NPM1 Wildtyp"
    }
  ],
  "karyotypes": [
    {
      "raw_text": "komplex aberranter Karyotyp",
      "date": "2019-12",
      "classification": "complex_aberrant",
      "evidence_text": "Zytogenetik (Labor XYZ): komplex aberranter Karyotyp."
    }
  ],
  "eln_risk": [
    {
      "raw_text": "ELN-Risiko: adverse",
      "date": "2019-12",
      "risk_category": "adverse",
      "classification_version": "2022",
      "evidence_text": "Risikostratifizierung nach ELN 2022: adverse"
    }
  ],
  "notes": null
}
```

`karyotypes` and `eln_risk` are empty lists if the letter does not mention cytogenetics or an
ELN classification at all. Unlike `genes` — which is deduplicated to one entry per gene using
its initial/diagnostic status — `karyotypes` and `eln_risk` can each hold multiple entries,
since both can be reassessed at different points in time (e.g. at diagnosis, after treatment,
at relapse) and the result can genuinely change; each entry has its own `date`.
`classification` is one of `normal`, `aberrant`, `complex_aberrant`, `not_performed`,
`unknown`. `eln_risk` is only populated when the letter states the category explicitly (e.g.
"ELN-Risiko: intermediär") — the model is instructed not to derive it itself from the
karyotype/gene findings; `risk_category` is one of `favorable`, `intermediate`,
`intermediate_1`, `intermediate_2` (the ELN 2010 Intermediate-I/-II subcategories), or
`adverse`. `classification_version` is the ELN version the classification is based on
(`2010`, `2017` or `2022`), and is only set when the letter names that version explicitly
(e.g. "Risikostratifizierung nach ELN 2022") — a risk category stated without a version
leaves it `null`, since the model is instructed never to guess the version or infer it from
the letter's date.

A gene's, karyotype's, or ELN risk finding's `date` is normalized to `YYYY`, `YYYY-MM`, or
`YYYY-MM-DD` from the letter's table date/month column for that finding (e.g. a table entry of
`"12/19"` becomes `"2019-12"`), falling back to the letter's own `document_date` when a
section has no internal date/month context of its own (e.g. an Excel-sourced free-text
pathology report), or `null` if no date context was available at all or it couldn't be
confidently normalized. This is enforced at the schema level
(`pattern=r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$"`), so the model cannot return a
non-conforming date string — note that Ollama's structured-output grammar compiler rejects
`\d`-style regex shorthand, so the pattern must spell out `[0-9]` explicitly.

`source_type` is `"xml"` or `"excel"` depending on which `prepare-letters` input produced
the letter (see `Step 1`). Excel-sourced letters have no encounter/account-id concept, so
`account_id` and `therapie_section` are `null` for them.

All findings across letters are also combined for downstream analysis into:

- `output/genes_combined.csv` — one row per gene finding per letter
- `output/karyotypes_combined.csv` — one row per karyotype assessment per letter
- `output/eln_risk_combined.csv` — one row per ELN risk classification per letter

All three CSVs include `patient_mrn`, `account_id`, `document_date`, `letter_status`,
`template_name`, `source_type`, and `source_file` columns (`patient_mrn` is the same value
as the per-letter JSON's `patient_id`). **Each letter's row is appended and flushed to these
CSVs as soon as that letter finishes processing** — if a run crashes partway through, the
CSVs already contain every letter processed before the crash.

## Recovering from a crash

If a run is interrupted in a way that leaves `genes_combined.csv`/
`karyotypes_combined.csv`/`eln_risk_combined.csv` missing or stale (e.g. an older version of
this tool without incremental writes, or the CSVs were deleted) but the per-letter
`<letter>.json` files in `--output-dir` are still there, rebuild the combined CSVs from them
directly, without re-running the LLM:

```sh
uv run rebuild-csv --input-dir output --output-dir output
```

## Pseudonymizing the combined CSVs

To share `genes_combined.csv`/`karyotypes_combined.csv`/`eln_risk_combined.csv` without
exposing raw patient/encounter identifiers, `pseudonymize-csv` replaces the `patient_mrn`,
`account_id`, and `source_file` columns (the latter because our letter filenames embed
the patient id, account id, and date) with HMAC-SHA256 hashes, leaving every other column
untouched. It reads `output/genes_combined.csv`, `output/karyotypes_combined.csv`, and
`output/eln_risk_combined.csv`, and writes `output/genes_combined.pseudonymized.csv` /
`output/karyotypes_combined.pseudonymized.csv` / `output/eln_risk_combined.pseudonymized.csv`
(edit the paths at the top of `pseudonymize_csv.py` if yours differ):

```sh
uv run pseudonymize-csv
```

Each run generates a fresh, cryptographically random HMAC key (used for all three files, so
patients stay joinable between them within that run) and logs it — save that log line if
you need to reproduce or verify the same hashes later, since there is no other way to
recover it afterward. Different runs use different keys, so hashes are **not** joinable
across separate runs of this script.
