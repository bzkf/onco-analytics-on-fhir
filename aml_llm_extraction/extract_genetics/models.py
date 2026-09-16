from typing import Literal

from pydantic import BaseModel, Field


class GeneFinding(BaseModel):
    gene: str = Field(
        max_length=50,
        description="Gene symbol as mentioned in the letter, e.g. FLT3, NPM1, TP53",
    )
    mutated: bool = Field(
        description=(
            "True if the letter states this gene is mutated/positive, "
            "False if explicitly stated as wildtype/negative"
        )
    )
    variant_detail: str | None = Field(
        default=None,
        max_length=100,
        description="Specific variant/mutation detail if given, e.g. 'ITD', 'D835Y', 'R140Q'",
    )
    date: str | None = Field(
        default=None,
        max_length=10,
        pattern=r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$",
        description=(
            "The rough date this finding was reported, normalized to 'YYYY', 'YYYY-MM', "
            "or 'YYYY-MM-DD' depending on how precise the letter's date/month table "
            "column is (e.g. '12/19' -> '2019-12', '13.12.19' -> '2019-12-13'). Null if "
            "no date context is available or it cannot be confidently normalized to "
            "one of these formats."
        ),
    )
    evidence_text: str | None = Field(
        default=None,
        max_length=300,
        description=(
            "A short verbatim sentence or phrase (not a whole paragraph) from the letter "
            "supporting this finding"
        ),
    )


class KaryotypeFinding(BaseModel):
    raw_text: str = Field(
        max_length=300,
        description=(
            "The karyotype as reported, e.g. an ISCN string like "
            "'46,XX,t(8;21)(q22;q22)[20]' or a descriptive result like "
            "'komplex aberranter Karyotyp'. Exclude lab/reference names."
        ),
    )
    classification: Literal[
        "normal", "aberrant", "complex_aberrant", "not_performed", "unknown"
    ] = Field(
        description=(
            "'normal' for a normal/unremarkable karyotype (e.g. '46,XX' or "
            "'unauffälliger Karyotyp'), 'complex_aberrant' for a complex aberrant karyotype "
            "(e.g. 'komplex aberranter Karyotyp', typically >=3 abnormalities), 'aberrant' "
            "for any other abnormal karyotype, 'not_performed' if cytogenetic testing was "
            "not done or the result is pending, 'unknown' if the letter mentions "
            "cytogenetics but the result cannot be determined"
        )
    )
    date: str | None = Field(
        default=None,
        max_length=10,
        pattern=r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$",
        description=(
            "The rough date this karyotype assessment was reported, normalized to "
            "'YYYY', 'YYYY-MM', or 'YYYY-MM-DD' depending on how precise the letter's "
            "date/month table column is (e.g. '12/19' -> '2019-12', '13.12.19' -> "
            "'2019-12-13'). Null if no date context is available or it cannot be "
            "confidently normalized to one of these formats."
        ),
    )
    evidence_text: str | None = Field(
        default=None,
        max_length=300,
        description=(
            "A short verbatim sentence or phrase (not a whole paragraph) from the letter "
            "supporting this finding"
        ),
    )


ELNCategory = Literal["favorable", "intermediate", "intermediate_1", "intermediate_2", "adverse"]
ELNVersion = Literal["2010", "2017", "2022"]


class ElnRiskFinding(BaseModel):
    risk_category: ELNCategory = Field(
        description=(
            "The ELN (European LeukemiaNet) risk category as explicitly stated in the "
            "letter. German terms map as: 'günstig' -> favorable, "
            "'intermediär'/'intermediate' -> intermediate, 'ungünstig'/'hoch'/'high-risk' "
            "-> adverse. Use 'intermediate_1'/'intermediate_2' only for the ELN 2010 "
            "subcategories Intermediate-I/Intermediate-II (German: 'Intermediär I'/"
            "'Intermediär II'); for a plain 'intermediär' without a subcategory use "
            "'intermediate'."
        )
    )
    classification_version: ELNVersion | None = Field(
        default=None,
        description=(
            "The ELN classification version the category is based on ('2010', '2017' or "
            "'2022'), but ONLY if the letter explicitly names it (e.g. 'ELN 2022: adverse', "
            "'nach ELN2017'). Null if the letter states a risk category without naming a "
            "version — never guess the version or infer it from the letter's date."
        ),
    )
    raw_text: str = Field(
        max_length=200,
        description=(
            "The risk classification as stated in the letter, e.g. 'ELN-Risiko: adverse' or "
            "'ELN 2022 intermediäres Risiko'"
        ),
    )
    date: str | None = Field(
        default=None,
        max_length=10,
        pattern=r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$",
        description=(
            "The rough date this risk classification was reported, normalized to 'YYYY', "
            "'YYYY-MM', or 'YYYY-MM-DD' depending on how precise the letter's date/month "
            "table column is (e.g. '12/19' -> '2019-12', '13.12.19' -> '2019-12-13'). Null "
            "if no date context is available or it cannot be confidently normalized to one "
            "of these formats."
        ),
    )
    evidence_text: str | None = Field(
        default=None,
        max_length=300,
        description=(
            "A short verbatim sentence or phrase (not a whole paragraph) from the letter "
            "supporting this finding"
        ),
    )


class ExtractionResult(BaseModel):
    genes: list[GeneFinding] = Field(
        default_factory=list,
        max_length=20,
        description=(
            "Distinct genes with mutation status mentioned in the letter. Report each gene "
            "at most once, even if it is mentioned many times (e.g. in repeated MRD/monitoring "
            "results) — see rules below."
        ),
    )
    karyotypes: list[KaryotypeFinding] = Field(
        default_factory=list,
        max_length=10,
        description=(
            "Distinct cytogenetics/karyotype assessments mentioned in the letter, each with "
            "its own date. Unlike genes, karyotype can be reassessed and change over time "
            "(e.g. at diagnosis vs. at relapse) — see rules below."
        ),
    )
    eln_risk: list[ElnRiskFinding] = Field(
        default_factory=list,
        max_length=5,
        description=(
            "ELN risk classifications already stated in the letter, each with its own "
            "date and — only when the letter names one — its own classification version. "
            "Only include this if the letter states the classification explicitly — do not "
            "derive it yourself from the karyotype or gene findings. Leave empty if the "
            "letter never states an ELN risk category."
        ),
    )
    notes: str | None = Field(
        default=None,
        max_length=500,
        description="Any relevant ambiguity or caveats about the extraction",
    )


class LetterDocument(BaseModel):
    """A single letter's text, extracted from either a parquet/XML export (source_type
    'xml') or a tumor documentation Excel export (source_type 'excel')."""

    source_type: Literal["xml", "excel"] = Field(
        description="Which source format this letter's text was extracted from"
    )
    patient_id: str = Field(
        description="Patient id (parquet column 'patid', or Excel column 'PatID')"
    )
    account_id: str | None = Field(
        default=None,
        description=(
            "Encounter/account number (parquet column 'patientaccountid'). Not "
            "applicable for Excel-sourced letters, which have no encounter concept."
        ),
    )
    document_date: str | None = Field(
        default=None,
        description=(
            "Document save date (parquet column 'lastsaveddatetime', or Excel column "
            "'Dokumentdatum')"
        ),
    )
    letter_status: str | None = Field(
        default=None, description="Letter status (parquet column 'letterstatus')"
    )
    template_name: str | None = Field(
        default=None, description="Letter template name (parquet column 'templatename')"
    )
    therapie_section: str | None = Field(
        default=None,
        description="XML section ID the therapy timeline text was extracted from (M5_Therapie)",
    )
    therapie_text: str | None = Field(
        default=None,
        description="Therapy timeline section content converted from HTML to Markdown",
    )
    therapie_source_html: str | None = Field(
        default=None,
        description="Original HTML extracted from the therapy timeline section's CDATA",
    )

    diagnosis_section: str | None = Field(
        default=None,
        description=(
            "XML section ID the diagnosis summary text was extracted from (M5_Diagnosen), if found"
        ),
    )
    diagnosis_text: str | None = Field(
        default=None,
        description="Diagnosis summary section content converted from HTML to Markdown",
    )
    diagnosis_source_html: str | None = Field(
        default=None,
        description="Original HTML extracted from the diagnosis summary section's CDATA",
    )

    histologie_text: str | None = Field(
        default=None,
        description="Excel column 'Histologie/Zytologie:' — histology/cytology report text",
    )
    molekularpatho_text: str | None = Field(
        default=None,
        description=(
            "Excel column 'Molekularpatho/zytologie:' — molecular pathology/cytogenetics "
            "report text"
        ),
    )
    immunhisto_text: str | None = Field(
        default=None,
        description=(
            "Excel column 'Immunhisto/zytologie:' — immunohistochemistry/immunophenotyping "
            "report text"
        ),
    )
    initial_risk_stratification_text: str | None = Field(
        default=None,
        description=(
            "Excel column 'Initiale TNM-Klassif./Risikostrat.:' — initial risk "
            "stratification text, the most likely place for an explicitly stated ELN "
            "risk category"
        ),
    )


class LetterExtraction(BaseModel):
    """LLM extraction result together with the letter metadata it was derived from."""

    source_type: Literal["xml", "excel"]
    patient_id: str
    account_id: str | None = None
    document_date: str | None = None
    letter_status: str | None = None
    template_name: str | None = None
    therapie_section: str | None = None
    source_file: str
    genes: list[GeneFinding] = Field(default_factory=list)
    karyotypes: list[KaryotypeFinding] = Field(default_factory=list)
    eln_risk: list[ElnRiskFinding] = Field(default_factory=list)
    notes: str | None = None

    @classmethod
    def from_letter(
        cls, letter: LetterDocument, source_file: str, result: ExtractionResult
    ) -> "LetterExtraction":
        return cls(
            source_type=letter.source_type,
            patient_id=letter.patient_id,
            account_id=letter.account_id,
            document_date=letter.document_date,
            letter_status=letter.letter_status,
            template_name=letter.template_name,
            therapie_section=letter.therapie_section,
            source_file=source_file,
            genes=result.genes,
            karyotypes=result.karyotypes,
            eln_risk=result.eln_risk,
            notes=result.notes,
        )
