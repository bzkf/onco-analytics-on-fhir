from loguru import logger
from pathling import PathlingContext, datasource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics_on_fhir.settings import settings


def extract_pca_ops(
    pc: PathlingContext,
    data: datasource.DataSource,
    settings: settings,
    spark: SparkSession,
) -> DataFrame:

    logger.info("extract pca ops.")

    df_ops = data.view(
        "Procedure",
        select=[
            {
                "column": [
                    {
                        "path": "getResourceKey()",
                        "name": "therapy_resource_id",
                        "description": "Procedure ID",
                    },
                ]
            }
        ],
    )

    # OLD
    # Spark-Teil wie vorher
    """ df_ops = df_ops.filter(
        (F.col("therapy_type") == "OP") & F.col("icd10_code").like("C61%")
    ).orderBy(F.col("condition_id"))
 """
    logger.info("df_ops_count = {}", df_ops.count())

    # downstream unverändert
    # df_ops = preprocess_therapy_df(df_ops)

    return df_ops
