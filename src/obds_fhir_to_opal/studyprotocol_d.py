from typing import Optional

import pandas as pd
from loguru import logger
from pathling import PathlingContext
from pathling.datasource import DataSource
from pydantic import BaseSettings
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from studyprotocol_d_utils import (
    aggregate_malignancy_pairs,
    cast_study_dates,
    compute_age,
    create_1_mal_df,
    create_2_mals_df,
    group_entities_df,
    pivot_multi_single,
    plot_pair_boxplot_horizontal_custom,
    plot_pair_bubble,
    plot_pair_bubble_gender,
)
from utils_onco_analytics import extract_df_study_protocol_a0_1_3_7_d, save_final_df


class StudyProtocolD:
    def __init__(
        self,
        pc: PathlingContext,
        data: DataSource,
        settings: BaseSettings,
        spark: SparkSession,
    ) -> None:
        self.pc: PathlingContext = pc
        self.data: DataSource = data
        self.settings: BaseSettings = settings
        self.spark: SparkSession = spark

        self.df_extract: Optional[DataFrame] = None
        self.df_all_pivot: Optional[DataFrame] = None
        self.year_min: Optional[int] = None
        self.year_max: Optional[int] = None

    def extract(self) -> DataFrame:
        df = extract_df_study_protocol_a0_1_3_7_d(
            self.pc, self.data, self.settings, self.spark
        )
        df = df.drop("obs_id_gleason", "gleason_date_first", "gleason")
        df = df.checkpoint(eager=True)

        self.df_extract = df
        return df

    def run(self) -> DataFrame:
        logger.info("StudyProtocolD pipeline started")

        # 1) Extract
        df_extract = self.extract()

        # 2) Prepare
        df_prepare = self.prepare(df_extract)

        # 3) Clean + Jahr-Range
        df_clean = self.clean(df_prepare)
        self.year_min = df_clean.select(F.min(F.year("date_diagnosis"))).first()[0]
        self.year_max = df_clean.select(F.max(F.year("date_diagnosis"))).first()[0]
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")

        # 4) 2mals vs single (1mal)
        df_2_mals = create_2_mals_df(df_clean)
        df_1_mal = create_1_mal_df(df_clean)

        # 5) pivot malignancy 2, union with single
        # use this later for comparison between groups: 1 mal and 2 mal
        df_all_pivot = pivot_multi_single(df_clean, df_2_mals, df_1_mal)
        self.df_all_pivot = df_all_pivot
        save_final_df(df_all_pivot, self.settings, suffix="all_pivot")

        # 6) aggregate pairs from df_2_mals - two malignancies
        df_pairs_agg = aggregate_malignancy_pairs(
            df_2_mals, presuffix="entity_or_parent"
        )
        save_final_df(
            df_pairs_agg.drop(
                "outliers_age_1", "outliers_age_2", "outliers_months_between"
            ),
            self.settings,
            suffix="pairs_agg",
        )

        logger.info("StudyProtocolD pipeline finished")
        return df_pairs_agg

    def prepare(self, df: DataFrame) -> DataFrame:

        df = group_entities_df(df, code_col="icd10_code", target_col="entity_or_parent")
        df = cast_study_dates(df)
        df = compute_age(df)
        df = df.checkpoint(eager=True)
        return df

    def clean(self, df: DataFrame) -> DataFrame:

        df = df.filter(F.col("date_diagnosis") > F.lit("1950-01-01"))
        df = df.filter(F.col("age_at_diagnosis") > 0)

        return df
