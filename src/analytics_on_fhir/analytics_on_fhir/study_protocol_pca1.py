from typing import Optional

from loguru import logger
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics_on_fhir.study_protocol_pca_utils import extract_pca_ops
from analytics_on_fhir.utils import (
    cast_study_dates,
    compute_age,
    extract_df_study_protocol_a_d_mii,
    save_final_df,
)


class StudyProtocolPCa1:

    def __init__(
        self,
        pc: PathlingContext,
        data: DataSource,
        spark: SparkSession,
        settings,
    ) -> None:
        self.pc: PathlingContext = pc
        self.data: DataSource = data
        self.settings = settings
        self.spark: SparkSession = spark

        self.df_extract: Optional[DataFrame] = None
        self.df_all_pivot: Optional[DataFrame] = None
        self.year_min: Optional[int] = None
        self.year_max: Optional[int] = None

    def extract(self) -> DataFrame:
        df = extract_df_study_protocol_a_d_mii(
            self.pc, self.data, self.settings, self.spark
        )
        df_c61 = df.filter(F.col("icd10_code").startswith("C61"))

        logger.info("df_c61_count = {}", df_c61.count())

        # TO DO: extract therapies here
        df_ops = extract_pca_ops(self.pc, self.data, self.settings, self.spark)
        # self.df_extract = df_c61
        return df

    def run(self):
        logger.info("StudyProtocolPCa1 pipeline started")

        # 1) Extract
        # TO DO: Add therapies here
        df_extract = self.extract()

        return df_extract

        # 2) Prepare
        df_prepare = self.prepare(df_extract)

        # 3) Clean + Jahr-Range
        df_clean = self.clean(df_prepare)
        self.year_min = df_clean.select(F.min(F.year("asserted_date"))).first()[0]
        self.year_max = df_clean.select(F.max(F.year("asserted_date"))).first()[0]
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        save_final_df(df_clean, self.settings, suffix="study_protocol_d")

        logger.info("StudyProtocolPCa1 pipeline finished")

    def prepare(self, df: DataFrame) -> DataFrame:

        df = cast_study_dates(
            df,
            [
                "asserted_date",
                "deceased_datetime",
                "date_death",
                "gleason_date_first",
                # therapy dates
            ],
        )
        df = compute_age(df)
        df = df.checkpoint(eager=True)
        return df

    def clean(self, df: DataFrame) -> DataFrame:

        df = df.filter(F.col("asserted_date") > F.lit("1950-01-01"))
        df = df.filter(F.col("age_at_diagnosis") > 0)

        return df
