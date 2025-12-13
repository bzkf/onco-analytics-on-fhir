from pyspark.sql.dataframe import DataFrame
from pathling import DataSource, PathlingContext
from pyspark.sql.functions import to_date, to_timestamp, round, min, nth_value
from pyspark.sql.window import Window


def extract(data: DataSource) -> DataFrame:
    conditions = data.view(
        "Condition",
        select=[
            {
                "column": [
                    {
                        "description": "Condition ID",
                        "path": "getResourceKey()",
                        "name": "id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "patient_id",
                    },
                    {
                        "description": "Asserted Date",
                        "path": "extension('http://hl7.org/fhir/StructureDefinition/condition-assertedDate').value.ofType(dateTime)",
                        "name": "asserted_date",
                    },
                ],
            }
        ],
        where=[
            {
                "description": "Only PCa",
                "path": "code.coding.exists(system = 'http://fhir.de/CodeSystem/bfarm/icd-10-gm' and code='C61')",
            }
        ],
    )

    tnm = data.view(
        "Observation",
        select=[
            {
                "column": [
                    {
                        "description": "Observation ID",
                        "path": "getResourceKey()",
                        "name": "id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "patient_id",
                    },
                    {
                        "description": "Condition ID",
                        "path": "focus.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Observation date time",
                        "path": "effective.ofType(dateTime)",
                        "name": "date_time",
                    },
                    {
                        "description": "Code",
                        "path": "code.coding.where(system = 'http://snomed.info/sct').code",
                        "name": "code_coding_sct",
                    },
                    {
                        "description": "Observation value",
                        "path": "value.ofType(CodeableConcept).coding.where(system = 'https://www.uicc.org/resources/tnm').code",
                        "name": "tnm_value",
                    },
                ],
                "select": [
                    {
                        "forEach": "code.coding",
                        "column": [
                            {
                                "description": "Observation code",
                                "path": "code",
                                "name": "code",
                            },
                            {
                                "description": "Observation display",
                                "path": "display",
                                "name": "display",
                            },
                        ],
                    },
                ],
            }
        ],
        where=[
            {
                "description": "p/cM and p/cN TNM categories",
                "path": "code.coding.exists(system = 'http://snomed.info/sct' and (code='399387003' or code='371497001' or code='371494008' or code='399534004'))",
            }
        ],
    )

    medication_statements = data.view(
        "MedicationStatement",
        select=[
            {
                "column": [
                    {
                        "description": "MedicationStatement ID",
                        "path": "getResourceKey()",
                        "name": "id",
                    },
                    {
                        "description": "Patient ID",
                        "path": "subject.getReferenceKey()",
                        "name": "patient_id",
                    },
                    {
                        "description": "Condition ID",
                        "path": "reasonReference.getReferenceKey()",
                        "name": "condition_id",
                    },
                    {
                        "description": "Effective Start Date",
                        "path": "effective.ofType(Period).start",
                        "name": "start_date",
                    },
                    {
                        "description": "Medication Text",
                        "path": "medication.ofType(CodeableConcept).text",
                        "name": "medication_text",
                    },
                    {
                        "description": "Medication Code",
                        "path": "medication.ofType(CodeableConcept).coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').code",
                        "name": "medication_atc_code",
                    },
                ],
            }
        ],
    )

    # TODO: flowchart: num c61 patients, num M0 oder N0 (jeweils), num enzalutamid, 
    # num vor april 2024, num nach april 2024 

    return medication_statements


def run(data: DataSource):
    extract(data)
