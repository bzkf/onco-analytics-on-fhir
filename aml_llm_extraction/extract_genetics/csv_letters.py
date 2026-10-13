import math

from .models import LetterDocument

PATIENT_ID_COLUMN = "PID"
REPORT_TEXT_COLUMN = "Befundtext"


def _cell_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    # Unlike the Excel export, the report text is passed through verbatim: there is no
    # known inline markup to strip, and a "<...>" pattern would also eat lab values such
    # as "VAF <5% und >3%".
    text = str(value).strip()
    return text if text else None


def build_letter(row: dict) -> LetterDocument | None:
    patient_id = _cell_str(row.get(PATIENT_ID_COLUMN))
    befund_text = _cell_str(row.get(REPORT_TEXT_COLUMN))
    if patient_id is None or befund_text is None:
        return None

    return LetterDocument(
        source_type="csv",
        patient_id=patient_id,
        befund_text=befund_text,
    )
