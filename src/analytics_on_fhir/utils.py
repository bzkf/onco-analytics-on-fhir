from pyspark.sql.functions import (
    abs,
    col,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


def find_closest_to_diagnosis(
    df: DataFrame, date_diagnosis_col: str, other_date_col: str
) -> DataFrame:
    df_filtered = df.filter(col(other_date_col).isNotNull())

    window = Window.partitionBy("condition_id").orderBy(
        abs(
            col(other_date_col).cast("timestamp")
            - col(date_diagnosis_col).cast("timestamp")
        )
    )

    return (
        df_filtered.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumnRenamed(other_date_col, other_date_col + "_first")
    )
