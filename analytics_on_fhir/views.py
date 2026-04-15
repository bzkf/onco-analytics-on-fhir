from fhir_constants import (
    FHIR_CODE_SYSTEM_ECOG,
    FHIR_CODE_SYSTEM_KARNOFSKY,
    FHIR_CODE_SYSTEM_VERLAUF_GESAMTBEURTEILUNG,
    FHIR_SYSTEM_GRADING,
    FHIR_SYSTEM_LOINC,
    FHIR_SYSTEM_SCT,
    FHIR_SYSTEMS_WEITERE_KLASSIFIKATION,
)
from pathling import datasource
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def progression_view(
    data: datasource.DataSource,
) -> DataFrame:
    progressions = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "observation_id",
                    },
                    {
                        "path": "subject.getReferenceKey()",
                        "name": "observation_patient_reference",
                    },
                    {
                        "path": "effective.ofType(dateTime)",
                        "name": "effective_dateTime",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "path": "value.ofType(CodeableConcept).coding"
                        + f".where(system = '{FHIR_CODE_SYSTEM_VERLAUF_GESAMTBEURTEILUNG}')"
                        + ".code",
                        "name": "overall_assessment_of_tumor_regression",
                    },
                    {
                        "path": (
                            "component.where("
                            "code.coding.where(system = 'http://snomed.info/sct' and code = "
                            "'445200009').exists()).valueCodeableConcept.coding.code.first()"
                        ),
                        "name": "primary_tumor_status",
                    },
                    {
                        "path": (
                            "component.where("
                            "code.coding.where(system = 'http://snomed.info/sct' and code = "
                            "'399656008').exists()).valueCodeableConcept.coding.code.first()"
                        ),
                        "name": "lymph_node_status",
                    },
                    {
                        "path": (
                            "component.where("
                            "code.coding.where(system = 'http://snomed.info/sct' and code = "
                            "'399608002').exists()).valueCodeableConcept.coding.code.first()"
                        ),
                        "name": "distant_metastasis_status",
                    },
                ],
            }
        ],
        where=[
            {
                "description": "Only 'Status of regression of tumor' observations",
                "path": f"code.coding.where(system='{FHIR_SYSTEM_SCT}' "
                + "and code='396432002').exists()",
            }
        ],
    )
    return progressions


def grading_view(
    data: datasource.DataSource,
) -> DataFrame:

    gradings = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "description": "Observation ID",
                        "path": "getResourceKey()",
                        "name": "observation_id",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Grading",
                        "path": (
                            "value.ofType(CodeableConcept)"
                            f".coding.where(system = '{FHIR_SYSTEM_GRADING}')"
                            ".code.first()"
                        ),
                        "name": "grading",
                    },
                    {
                        "description": "Grading Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "grading_date",
                    },
                ]
            }
        ],
        where=[
            {
                "description": (
                    "Nur Observations, deren Observation.code.coding.code dieser LOINC code ist."
                ),
                "path": (
                    "code.coding.where("
                    f"system = '{FHIR_SYSTEM_LOINC}' and "
                    "code = '33732-9'"
                    ").exists()"
                ),
            }
        ],
    )
    gradings.orderBy(F.col("condition_id")).show()
    return gradings


# to do: fix
def weitere_klassifikation_view(
    data: datasource.DataSource,
) -> DataFrame:
    weitere_klassifikationen = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "description": "Observation.code text",
                        "path": "code.text",
                        "name": "weitere_klassifikation_name",
                    },
                    {
                        "description": "meta profile",
                        "path": "meta.profile",
                        "name": "meta_profile",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Value text",
                        "path": "value.ofType(CodeableConcept).first().text",
                        "name": "weitere_klassifikation_name_value",
                    },
                    {
                        "description": "Observation value",
                        "path": "value.ofType(CodeableConcept).coding.code.first()",  # können das mehrere sein pro Observation?
                        "name": "weitere_klassifikation_stadium",
                    },
                    {
                        "description": "Weitere Klassifikation Observation Date",
                        "path": "effective.ofType(dateTime)",
                        "name": "weitere_klassifikation_date",
                    },
                ],
            }
        ],
    )
    weitere_klassifikationen.show()
    weitere_klassifikationen = weitere_klassifikationen.filter(
        F.col("meta_profile").startswith(f"{FHIR_SYSTEMS_WEITERE_KLASSIFIKATION}")
    )

    weitere_klassifikationen.show()

    return weitere_klassifikationen


def leistungszustand_ecog_karnofsky_view(
    data: datasource.DataSource,
) -> DataFrame:
    leistungszustand_ecog_karnofsky = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "observation_id",
                    },
                    {
                        "path": "subject.getReferenceKey()",
                        "name": "observation_patient_reference",
                    },
                    {
                        "path": "effective.ofType(dateTime)",
                        "name": "effective_dateTime",
                    },
                    {
                        "description": "Focus Condition (Primary Diagnosis)",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "path": "value.ofType(CodeableConcept).coding"
                        + f".where(system = '{FHIR_CODE_SYSTEM_ECOG}').code",
                        "name": "ecog_performance_status",
                    },
                    {
                        "path": "value.ofType(CodeableConcept).coding"
                        + f".where(system = '{FHIR_CODE_SYSTEM_KARNOFSKY}').code",
                        "name": "karnofsky_performance_status",
                    },
                ],
            }
        ],
        where=[
            {
                "description": "Only ecog and karnofsky observations",
                "path": f"code.coding.where(system='{FHIR_SYSTEM_SCT}' "
                + "and code='423740007' or code='761869008').exists()",
            }
        ],
    )

    leistungszustand_ecog_karnofsky.show()
    return leistungszustand_ecog_karnofsky
