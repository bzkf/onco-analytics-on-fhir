from loguru import logger
from pathling import DataSource
from pyspark.sql.functions import lit
from settings import Settings
from utils import (
    save_final_df,
)


class DQStudy:
    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, data: DataSource) -> None:
        medication_statements = data.view(
            "MedicationStatement",
            select=[
                {
                    "column": [
                        {
                            "description": "Medication Text",
                            "path": "medication.ofType(CodeableConcept).text",
                            "name": "Substanzbezeichnung",
                        },
                    ],
                }
            ],
            where=[
                {
                    "description": "Only MedicationStatements without an ATC code set",
                    "path": "medication.ofType(CodeableConcept).coding.where(system='http://fhir.de/CodeSystem/bfarm/atc').code.empty()",
                }
            ],
        )

        logger.info(f"Found {medication_statements.count()} MedicationStatements without ATC code")

        medication_mapping_table = (
            medication_statements.groupBy("Substanzbezeichnung")
            .count()
            .orderBy("count", ascending=False)
            .withColumn("ATC-Code", lit(""))
            .select("Substanzbezeichnung", "ATC-Code", "count")
        )

        save_final_df(
            medication_mapping_table,
            self.settings,
            suffix="substanzen-to-atc-mappings",
        )

        medication_mapping_table.show()

        weitere_klassifikation_without_coding = data.view(
            "Observation",
            select=[
                {
                    "column": [
                        {
                            "description": "Observation.code text",
                            "path": "code.text",
                            "name": "name",
                        },
                        {
                            "description": "Observation value",
                            "path": "value.ofType(CodeableConcept).coding.code",
                            "name": "stadium",
                        },
                    ],
                }
            ],
            where=[
                {
                    "description": "Only Observations without a coding",
                    "path": "code.coding.code.empty()",
                },
                # This doesn't work yet because Pathling doesn't support startsWith
                # {
                #     "description": "Only weitere Klassifikationen",
                #     "path": "meta.profile.where($this.startsWith('https://www.medizininformatik-initiative.de/fhir/ext/modul-onko/StructureDefinition/mii-pr-onko-weitere-klassifikationen')).exists()",
                # }
            ],
        )

        logger.info(
            f"Found {weitere_klassifikation_without_coding.count()} Observations without coding"
        )

        weitere_klassifikation_mapping_table = (
            weitere_klassifikation_without_coding.groupBy("name", "stadium")
            .count()
            .orderBy(["name", "stadium"], ascending=False)
            .withColumn("snomed_code", lit(""))
            .withColumn("snomed_display", lit(""))
            .withColumn("value_snomed_code", lit(""))
            .withColumn("value_snomed_display", lit(""))
            .select(
                "name",
                "stadium",
                "snomed_code",
                "snomed_display",
                "value_snomed_code",
                "value_snomed_display",
                "count",
            )
        )

        save_final_df(
            weitere_klassifikation_mapping_table,
            self.settings,
            suffix="weitere-klassifikationen-mappings",
        )

        weitere_klassifikation_mapping_table.show()
