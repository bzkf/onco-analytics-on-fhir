import math
import re

from .models import LetterDocument

PATIENT_ID_COLUMN = "PatID"
DOCUMENT_DATE_COLUMN = "Dokumentdatum"
HISTOLOGY_COLUMN = "Histologie/Zytologie:"
MOLECULAR_PATHO_COLUMN = "Molekularpatho/zytologie:"
IMMUNOHISTO_COLUMN = "Immunhisto/zytologie:"
INITIAL_RISK_STRATIFICATION_COLUMN = "Initiale TNM-Klassif./Risikostrat.:"

# These free-text columns carry light inline markup from the source system (e.g.
# "<F>Zytologie: </>" for bold), not real HTML/XML worth parsing - just strip any
# "<...>" tag-like fragment.
_TAG_RE = re.compile(r"<[^>]*>")


def _cell_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = _TAG_RE.sub("", str(value)).strip()
    return text if text else None


def _id_str(value: object) -> str | None:
    text = _cell_str(value)
    if text is None:
        return None
    # A patient id column with any blank cells gets cast to float by pandas/Excel,
    # turning e.g. 12345 into "12345.0" - undo that.
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def build_letter(row: dict) -> LetterDocument | None:
    patient_id = _id_str(row.get(PATIENT_ID_COLUMN))
    if patient_id is None:
        return None

    histologie_text = _cell_str(row.get(HISTOLOGY_COLUMN))
    molekularpatho_text = _cell_str(row.get(MOLECULAR_PATHO_COLUMN))
    immunhisto_text = _cell_str(row.get(IMMUNOHISTO_COLUMN))
    initial_risk_stratification_text = _cell_str(row.get(INITIAL_RISK_STRATIFICATION_COLUMN))
    if not any(
        (
            histologie_text,
            molekularpatho_text,
            immunhisto_text,
            initial_risk_stratification_text,
        )
    ):
        return None

    return LetterDocument(
        source_type="excel",
        patient_id=patient_id,
        document_date=_cell_str(row.get(DOCUMENT_DATE_COLUMN)),
        histologie_text=histologie_text,
        molekularpatho_text=molekularpatho_text,
        immunhisto_text=immunhisto_text,
        initial_risk_stratification_text=initial_risk_stratification_text,
    )
