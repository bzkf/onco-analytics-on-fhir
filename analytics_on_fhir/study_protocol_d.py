from loguru import logger
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from study_protocol_d_utils import (
    aggregate_malignancy_pairs,
    create_1_mal_df,
    create_2_mals_df,
    group_entity_or_parent,
    pivot_multi_single,
    plot_pair_boxplot_horizontal_custom,
    plot_pair_bubble_gender,
)
from utils import (
    FHIR_SYSTEM_PRIMAERTUMOR,
    cast_study_dates,
    compute_age,
    extract_df_study_protocol_a_d_mii,
    save_final_df,
)


class StudyProtocolD:
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

        self.df_extract: DataFrame | None = None
        self.df_all_pivot: DataFrame | None = None
        self.year_min: int | None = None
        self.year_max: int | None = None

    def extract(self) -> DataFrame:
        df = extract_df_study_protocol_a_d_mii(self.pc, self.data, self.settings, self.spark)
        df = df.checkpoint(eager=True)
        logger.info("df count fruehere and primaertumor = {}", df.count())

        # filter out fruehere tumorerkrankung - only primaerdiagnose
        df = df.filter(F.col("meta_profile").startswith(FHIR_SYSTEM_PRIMAERTUMOR))
        logger.info("df_count only primaertumor = {}", df.count())

        self.df_extract = df
        return df

    def run(self):
        logger.info("StudyProtocolD pipeline started")

        # 1) Extract
        df_extract = self.extract()

        # 2) Prepare
        df_prepare = self.prepare(df_extract)

        # 3) Clean + Jahr-Range
        df_clean = self.clean(df_prepare)
        self.year_min = df_clean.select(F.min(F.year("asserted_date"))).first()[0]
        self.year_max = df_clean.select(F.max(F.year("asserted_date"))).first()[0]
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        save_final_df(df_clean, self.settings, suffix="study_protocol_d")

        # 4) 2mals vs single (1mal)
        df_2_mals = create_2_mals_df(df_clean)
        save_final_df(df_2_mals, self.settings, suffix="2_malignancies")
        df_1_mal = create_1_mal_df(df_clean)  # use later for comparisons maybe

        # 5) pivot malignancy 2, union with single
        # use this later for comparison between groups: 1 mal and 2 mal
        df_all_pivot = pivot_multi_single(df_clean, df_2_mals, df_1_mal)
        self.df_all_pivot = df_all_pivot
        save_final_df(df_all_pivot, self.settings, suffix="all_pivot")

        # 6) aggregate pairs from df_2_mals - two malignancies
        df_pairs_agg = aggregate_malignancy_pairs(df_2_mals, presuffix="entity_or_parent")
        save_final_df(
            df_pairs_agg.drop("outliers_age_1", "outliers_age_2", "outliers_months_between"),
            self.settings,
            suffix="pairs_agg",
        )

        # 7) plot
        # later: synchron/metachron and plot pair bubble gender (+age, +months between)
        # later: prepare kaplan meier data, safe csv, combine sites later and plot after
        df_pairs_agg_pd = df_pairs_agg.toPandas()

        # filter df: top30, top100, >5
        top30_pairs_pd = df_pairs_agg_pd.sort_values("count_pair", ascending=False).head(30)
        top100_pairs_pd = df_pairs_agg_pd.sort_values("count_pair", ascending=False).head(100)
        pairs_greater_than_5_pd = df_pairs_agg_pd[df_pairs_agg_pd["count_pair"] > 5]

        # diagnosis pairs + gender
        # age
        # months between
        for df_name, df_plot in [
            ("top30", top30_pairs_pd),
            ("top100", top100_pairs_pd),
            ("gt5", pairs_greater_than_5_pd),
            # ("all", df_pairs_agg_pd), # OOM in container
        ]:
            self.plot_pairs(df_plot, df_name)
            self.plot_age(df_plot, df_name)
            self.plot_months_between(df_plot, df_name)

        logger.info("StudyProtocolD pipeline finished")

    def prepare(self, df: DataFrame) -> DataFrame:
        df = group_entity_or_parent(df, code_col="icd10_code", target_col="entity_or_parent")
        df = df.withColumn("icd10_parent_code", F.split(df["icd10_code"], r"\.")[0])
        df = cast_study_dates(
            df,
            [
                "asserted_date",
                "birthdate",
                "deceased_datetime",
                "date_death",
                "gleason_date_first",
                "metastasis_date_first",
            ],
        )
        df = compute_age(df)
        df = df.checkpoint(eager=True)
        return df

    def clean(self, df: DataFrame) -> DataFrame:
        df = df.filter(F.col("asserted_date") > F.lit("1970-12-31"))  # 1970 likely placeholder date
        df = df.filter(F.col("age_at_diagnosis") > 0)

        return df

    def plot_pairs(self, df: DataFrame, df_name: str) -> DataFrame:
        plot_pair_bubble_gender(
            df=df,
            value_col="count_pair",
            subgroup_cbar_col="count_female_pair",
            settings=self.settings,
            plot_name=f"{df_name}_pairs_gender",
            feature="pair frequencies with gender proportion",
            period=f"diagnosis year {self.year_min} to {self.year_max}",
            cbar_label="female ratio",
        )

    def plot_age(self, df: DataFrame, df_name: str) -> DataFrame:
        plot_pair_boxplot_horizontal_custom(
            df=df,
            plot_name=f"{df_name}_age_boxplot_quartiles",
            period=f"diagnosis year {self.year_min} to {self.year_max}",
            settings=self.settings,
            value_col="count_pair",
            var_name="age at diagnosis",
            var_name_median=["age_median_1", "age_median_2"],
            var_name_q1=["age_q1_1", "age_q1_2"],
            var_name_q3=["age_q3_1", "age_q3_2"],
            var_name_min=["age_min_1", "age_min_2"],
            var_name_max=["age_max_1", "age_max_2"],
            var_name_outliers=["outliers_age_1", "outliers_age_2"],
            show_entity1=True,
        )

    def plot_months_between(self, df: DataFrame, df_name: str) -> DataFrame:
        plot_pair_boxplot_horizontal_custom(
            df=df,
            plot_name=f"{df_name}_months_between_boxplot_quartiles",
            period=f"diagnosis year {self.year_min} to {self.year_max}",
            settings=self.settings,
            value_col="count_pair",
            var_name="months between diagnoses (first and second malignancy)",
            var_name_median="months_between_median_2",
            var_name_q1="months_between_q1_2",
            var_name_q3="months_between_q3_2",
            var_name_min="months_between_min_2",
            var_name_max="months_between_max_2",
            var_name_outliers=["outliers_months_between"],
            show_entity1=False,
        )
