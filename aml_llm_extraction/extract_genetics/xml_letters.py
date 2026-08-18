import warnings

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from markdownify import markdownify

from .models import LetterDocument

# These clinical-document exports are not always well-formed XML (e.g. stray closing
# tags with no matching opener). An HTML parser tolerates that kind of tag soup instead
# of failing outright, so we deliberately parse the "XML" with one.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

THERAPIE_SECTION_ID = "M5_Therapie"
DIAGNOSIS_SECTION_ID = "M5_Diagnosen"


def extract_section_html(xml_text: str, section_id: str) -> str | None:
    soup = BeautifulSoup(xml_text, "html.parser")
    return _extract_section_html_from_soup(soup, section_id)


def _extract_section_html_from_soup(soup: BeautifulSoup, section_id: str) -> str | None:
    section = soup.find("section", id=section_id)
    if section is None:
        return None

    parts = [text for c in section.find_all("content") if (text := c.get_text().strip())]
    if not parts:
        return None
    return "\n\n".join(parts)


def html_to_text(html: str) -> str:
    return markdownify(html, heading_style="ATX").strip()


def build_letter(row: dict) -> LetterDocument | None:
    soup = BeautifulSoup(row["xmldocument"], "html.parser")

    therapie_html = _extract_section_html_from_soup(soup, THERAPIE_SECTION_ID)
    if therapie_html is None:
        return None

    diagnosis_html = _extract_section_html_from_soup(soup, DIAGNOSIS_SECTION_ID)

    return LetterDocument(
        source_type="xml",
        patient_id=str(row["patid"]),
        account_id=str(row["patientaccountid"]),
        document_date=_stringify(row.get("lastsaveddatetime")),
        letter_status=_stringify(row.get("letterstatus")),
        template_name=_stringify(row.get("templatename")),
        therapie_section=THERAPIE_SECTION_ID,
        therapie_text=html_to_text(therapie_html),
        therapie_source_html=therapie_html,
        diagnosis_section=DIAGNOSIS_SECTION_ID if diagnosis_html else None,
        diagnosis_text=html_to_text(diagnosis_html) if diagnosis_html else None,
        diagnosis_source_html=diagnosis_html,
    )


def _stringify(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
