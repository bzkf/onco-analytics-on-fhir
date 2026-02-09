from typing import Optional

from loguru import logger
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics_on_fhir.study_protocol_pca_utils import (
    extract_radiotherapies,
    extract_surgeries,
    extract_systemtherapies,
    flag_young_highrisk_cohort,
)
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

        self.df_c61: Optional[DataFrame] = None
        self.df_all_pivot: Optional[DataFrame] = None
        self.year_min: Optional[int] = None
        self.year_max: Optional[int] = None

    def extract(self) -> DataFrame:
        # later, join therapy data to condition/gleason/metastasis data
        # filtered by C61 from here
        df = extract_df_study_protocol_a_d_mii(
            self.pc, self.data, self.settings, self.spark
        )
        df_c61 = df.filter(F.col("icd10_code").startswith("C61"))

        logger.info("df_c61_count = {}", df_c61.count())
        self.df_c61 = df_c61

        return df_c61

        # extend this later
        # TO DO: extract therapies here
        df_radiotherapies = extract_radiotherapies(
            self.pc, self.data, self.settings, self.spark
        )

        df_medication_statements = extract_systemtherapies(
            self.pc, self.data, self.settings, self.spark
        )
        # df_ops = extract_surgeries(self.pc, self.data, self.settings, self.spark)
        # self.df_c61 = df_c61
        # return df_c61

    def run(self):
        logger.info("StudyProtocolPCa1 pipeline started")

        # 1) Extract
        # TO DO: Add therapies here
        df_extract = self.extract()

        # adapt this later
        # 2) Prepare
        df_prepare = self.prepare(df_extract)

        # 3) Clean + Jahr-Range
        df_clean = self.clean(df_prepare)
        self.year_min = df_clean.select(F.min(F.year("asserted_date"))).first()[0]
        self.year_max = df_clean.select(F.max(F.year("asserted_date"))).first()[0]
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        save_final_df(df_clean, self.settings, suffix="study_protocol_pca1")

        logger.info("StudyProtocolPCa1 pipeline finished")

    def prepare(self, df: DataFrame) -> DataFrame:

        df = cast_study_dates(
            df,
            [
                "asserted_date",
                "deceased_datetime",
                "date_death",
                "gleason_date_first",
                # therapy dates later
            ],
        )
        df = compute_age(df)
        df = flag_young_highrisk_cohort(
            df, age_col="age_at_diagnosis", gleason_col="gleason_score"
        )
        df = df.checkpoint(eager=True)
        return df

    def clean(self, df: DataFrame) -> DataFrame:

        df = df.filter(F.col("asserted_date") > F.lit("1950-01-01"))
        df = df.filter(F.col("age_at_diagnosis") > 0)

        return df

    def plot(self, df: DataFrame) -> DataFrame:
        logger.info("StudyProtocolPCa1 pipeline - start plotting")
        # to do
        # z.B. Kaplan Meier Plots Johannes hier einbinden
