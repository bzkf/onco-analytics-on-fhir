import re

from loguru import logger
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from study_protocol_pca_utils import (
    extract_radiotherapies,
    extract_surgeries,
    extract_systemtherapies,
    flag_young_highrisk_cohort,
    group_ops,
    join_radiotherapies,
    union_sort_pivot_join,
    with_mapped_atc_column,
)
from utils import (
    FHIR_SYSTEM_PRIMAERTUMOR,
    cast_study_dates,
    compute_age,
    extract_df_study_protocol_a_d_mii,
    months_diff,
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

        self.df_c61: DataFrame | None = None
        self.df_c61_clean: DataFrame | None = None
        # nachnutzbar für andere jobs bauen
        self.df_parents_radiotherapies: DataFrame | None = None
        self.df_children_bestrahlung: DataFrame | None = None
        self.df_radiotherapy_joined: DataFrame | None = None
        self.df_ops: DataFrame | None = None
        self.df_ops_grouped: DataFrame | None = None
        self.df_system_therapies: DataFrame | None = None
        self.year_min: int | None = None
        self.year_max: int | None = None

    def extract(self) -> DataFrame:
        df = extract_df_study_protocol_a_d_mii(self.pc, self.data, self.settings, self.spark)
        df_c61 = df.filter(F.col("icd10_code").startswith("C61"))
        logger.info("df_c61_count fruehere and primaertumor = {}", df_c61.count())

        # filter out fruehere tumorerkrankung - only primaerdiagnose
        df_c61 = df_c61.filter(F.col("meta_profile").startswith(FHIR_SYSTEM_PRIMAERTUMOR))
        logger.info("df_c61_count only primaertumor = {}", df_c61.count())

        self.df_c61 = df_c61

        # TO DO: remove all the df transformation logic here, separate function
        df_system_therapies = extract_systemtherapies(self.pc, self.data, self.settings, self.spark)
        df_system_therapies = cast_study_dates(
            df_system_therapies,
            [
                "therapy_start_date",
                "therapy_end_date",
            ],
        )
        self.df_system_therapies = df_system_therapies
        save_final_df(df_system_therapies, self.settings, suffix="df_system_therapies")

        df_parents_radiotherapies, df_children_bestrahlung = extract_radiotherapies(
            self.pc, self.data, self.settings, self.spark
        )

        self.df_parents_radiotherapies = df_parents_radiotherapies
        self.df_children_bestrahlung = df_children_bestrahlung

        df_radiotherapies_joined = join_radiotherapies(
            df_parents_radiotherapies, df_children_bestrahlung
        )
        df_radiotherapies_joined = cast_study_dates(
            df_radiotherapies_joined,
            [
                "therapy_start_date",
                "therapy_end_date",
            ],
        )
        self.df_radiotherapies_joined = df_radiotherapies_joined
        save_final_df(df_radiotherapies_joined, self.settings, suffix="radiotherapies_joined")

        df_ops = extract_surgeries(self.pc, self.data, self.settings, self.spark)
        df_radiotherapies_joined = cast_study_dates(
            df_radiotherapies_joined,
            [
                "therapy_start_date",
            ],
        )
        self.df_ops = df_ops

        save_final_df(
            df_ops,
            self.settings,
            suffix="ops",
        )

        df_ops_grouped = group_ops(df_ops)
        self.df_ops_grouped = df_ops_grouped

        save_final_df(df_ops_grouped, self.settings, suffix="ops_grouped")

    def run(self):
        logger.info("StudyProtocolPCa1 pipeline started")

        # 1) Extract
        self.extract()

        # 2) Prepare dates, age, cohort
        df_c61 = self.prepare(self.df_c61)
        df_c61.show()

        # 3) Clean + Jahr-Range
        df_c61_clean = self.clean(df_c61)
        logger.info("df_c61_clean count = {}", df_c61_clean.count())
        self.year_min = df_c61_clean.select(F.min(F.year("asserted_date"))).first()[0]
        self.year_max = df_c61_clean.select(F.max(F.year("asserted_date"))).first()[0]
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        self.df_c61_clean = df_c61_clean
        save_final_df(df_c61_clean, self.settings, suffix="df_c61_clean")

        # 4) therapy sequence all therapies - for REACTO
        """ df_therapy_sequence = union_sort_pivot_join(
            df_c61_clean,
            self.df_ops_grouped,
            self.df_system_therapies,
            self.df_radiotherapies_joined,
        )
        save_final_df(df_therapy_sequence, self.settings, suffix="df_therapy_sequence")

        self.generate_sequence_csv(df_therapy_sequence) """

        # 5) first line therapy - within 4 months of diagnosis
        df_therapy_sequence_first_line_4_months = union_sort_pivot_join(
            df_c61_clean,
            self.df_ops_grouped,
            self.df_system_therapies,
            self.df_radiotherapies_joined,
            first_line_months_threshold=4,
        )
        df_therapy_sequence_first_line_4_months = with_mapped_atc_column(
            df_therapy_sequence_first_line_4_months, self.spark
        )
        save_final_df(
            df_therapy_sequence_first_line_4_months,
            self.settings,
            suffix="therapy_sequence_first_line_4_months",
        )

        logger.info("StudyProtocolPCa1 pipeline finished")

    def prepare(self, df: DataFrame) -> DataFrame:
        df = cast_study_dates(
            df,
            [
                "asserted_date",
                "recorded_date",
                "deceased_datetime",
                "date_death",
                "gleason_date_first",
                "metastasis_date_first",
            ],
        )
        df = compute_age(df)
        df = flag_young_highrisk_cohort(df, age_col="age_at_diagnosis", gleason_col="gleason_score")
        # calculate gleason and metastasis months diff
        df = months_diff(df, "gleason_date_first", "asserted_date")
        df = months_diff(df, "metastasis_date_first", "asserted_date")
        logger.info("months_diff")
        df.show()

        df = df.checkpoint(eager=True)
        return df

    def clean(self, df: DataFrame) -> DataFrame:
        df = df.filter(F.col("asserted_date") > F.lit("1970-12-31"))  # 1970 likely placeholder date
        df = df.filter(F.col("age_at_diagnosis") > 0)

        return df

    def generate_sequence_csv(self, df: DataFrame) -> DataFrame:
        logger.info("StudyProtocolPCa1 pipeline - start plotting")

        # nur Erst-, Zweit- und Dritttherapie - ggf später/für andere Entitäten
        df = df.select(
            [c for c in df.columns if not re.search(r"_\d+$", c) or re.search(r"_[123]$", c)]
        )

        df.show()
        save_final_df(df, self.settings, suffix="therapy_sequence123")

        result_df = with_mapped_atc_column(df, self.spark)

        result_df.show()

        df.show()
        save_final_df(df, self.settings, suffix="therapy_sequence1")
