import hashlib
import hmac
import os

from loguru import logger
from mii_conditions_labs import PyRateQuery
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from study_protocol_d_utils import (
    IDENTIFYING_COLS,
    aggregate_malignancy_pairs,
    create_1_mal_df,
    create_2_mals_df,
    group_entity_or_parent,
    pivot_multi_single,
    plot_pair_boxplot_horizontal_custom,
    plot_pair_bubble_gender,
    run_r_script,
    show_r_plots,
)
from utils import (
    FHIR_SYSTEM_PRIMAERTUMOR,
    HERE,
    cast_study_dates,
    compute_age,
    deidentify,
    extract_conditions_patients_death,
    extract_metastasis,
    extract_radiotherapies,
    extract_surgeries,
    extract_systemtherapies,
    group_ops,
    join_radiotherapies,
    save_final_df,
    save_final_df_parquet,
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
        df = extract_conditions_patients_death(self.pc, self.data, self.settings, self.spark)
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

        # 4) 2mals vs single (1mal)
        df_2_mals = create_2_mals_df(df_clean)
        df_2_mals_cond_id_asserted = df_2_mals.select(F.col("condition_id"), F.col("asserted_date"))
        df_2_mals_cond_id_asserted.show()
        df_1_mal = create_1_mal_df(df_clean)
        df_1_mal_cond_id_asserted = df_1_mal.select(F.col("condition_id"), F.col("asserted_date"))
        df_1_mal_cond_id_asserted.show()

        # collect all condition ids and asserted dates
        df_all_conditions = df_1_mal_cond_id_asserted.union(df_2_mals_cond_id_asserted)
        # generate crypto hashed dict
        key = os.urandom(32)
        condition_ids = (
            df_all_conditions.select("condition_id").distinct().rdd.flatMap(lambda x: x).collect()
        )
        condition_hash_lookup = {
            cid: hmac.digest(key, str(cid).encode("utf-8"), hashlib.sha256).hex()
            for cid in condition_ids
        }
        df_lookup = self.spark.createDataFrame(
            [(k, v) for k, v in condition_hash_lookup.items()],
            ["condition_id", "condition_id_hash"],
        )

        # save all conditions
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        df_clean_deidentified = deidentify(df_clean, IDENTIFYING_COLS, df_lookup)
        save_final_df(
            df_clean_deidentified, self.settings, suffix="oBDS_primaerdiagnosen_deidentified"
        )
        save_final_df_parquet(
            df_clean_deidentified, self.settings, suffix="oBDS_primaerdiagnosen_deidentified"
        )

        # extract MII conditions
        pandas_df_2_mals = df_2_mals.toPandas()
        df_list_2_mals = pandas_df_2_mals["condition_patient_resource_id"].dropna()
        df_list_2_mals.drop_duplicates(inplace=True)
        self.extract_mii_conditions(df_list_2_mals, suffix="_2_mals")
        pandas_df_1_mal = df_1_mal.toPandas()
        df_list_1_mal = pandas_df_1_mal["condition_patient_resource_id"].dropna()
        df_list_1_mal.drop_duplicates(inplace=True)
        self.extract_mii_conditions(df_list_1_mal, suffix="_1_mal")

        # save 1 and 2 tumors
        df_2_mals_deidentified = deidentify(df_2_mals, IDENTIFYING_COLS, df_lookup)
        save_final_df(df_2_mals_deidentified, self.settings, suffix="2_malignancies_deidentified")
        save_final_df_parquet(
            df_2_mals_deidentified, self.settings, suffix="2_malignancies_deidentified"
        )
        df_1_mal_deidentified = deidentify(df_1_mal, IDENTIFYING_COLS, df_lookup)
        save_final_df(df_1_mal_deidentified, self.settings, suffix="1_malignancy_deidentified")
        save_final_df_parquet(
            df_1_mal_deidentified, self.settings, suffix="1_malignancy_deidentified"
        )

        # 5) pivot malignancy 2, union with single malignancy patients
        df_all_pivot = pivot_multi_single(df_clean, df_2_mals, df_1_mal)
        self.df_all_pivot = df_all_pivot
        save_final_df(df_all_pivot, self.settings, suffix="all_pivot")
        df_all_pivot_deidentified = deidentify(df_all_pivot, IDENTIFYING_COLS, df_lookup)
        save_final_df(df_all_pivot_deidentified, self.settings, suffix="all_pivot_deidentified")
        save_final_df_parquet(
            df_all_pivot_deidentified, self.settings, suffix="all_pivot_deidentified"
        )

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

        self.extract_save_metastasis(df_all_conditions, df_lookup)

        self.extract_save_therapies(df_all_conditions, df_lookup)

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
            ],
        )
        df = compute_age(df)
        return df

    def clean(self, df: DataFrame) -> DataFrame:
        df = df.filter(F.col("asserted_date") > F.lit("1970-12-31"))  # 1970 likely placeholder date
        df = df.filter(F.col("age_at_diagnosis") > 0)
        df = df.filter(~F.col("icd10_code").startswith("C44"))
        df = df.filter(~F.col("icd10_code").startswith("D04"))

        return df

    def extract_mii_conditions(self, df_list, suffix):
        logger.info("start PyRate query for conditions.")
        query = PyRateQuery(self.settings)
        query.extract_conditions(df_list, suffix)

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

    def run_r_analysis(self):
        logger.info("running r analysis")
        script = os.path.join(HERE, "metastasis_analysis.R")
        logger.info("script = {}", script)
        run_r_script(script)
        show_r_plots(os.path.join(HERE, "results/study_protocol_d/plots_r"))

    def extract_save_metastasis(self, df_all_conditions, df_lookup):
        df_metastasis = extract_metastasis(self.pc, self.data, self.settings, self.spark)
        df_metastasis.show()
        logger.info(f"df_1_2_cond_id_asserted.count() = : {df_all_conditions.count()}")
        df_metastasis = df_all_conditions.join(df_metastasis, "condition_id", "left")
        logger.info(f"df_1_2_mals_metastasis.count() = : {df_metastasis.count()}")

        df_metastasis_deidentified = deidentify(df_metastasis, IDENTIFYING_COLS, df_lookup)
        df_metastasis_deidentified.show()

        save_final_df(df_metastasis_deidentified, self.settings, suffix="metastasis_deidentified")
        save_final_df_parquet(
            df_metastasis_deidentified, self.settings, suffix="metastasis_deidentified"
        )

    def extract_save_therapies(self, df_all_conditions, df_lookup):
        # system therapy
        df_system_therapies = extract_systemtherapies(self.pc, self.data, self.settings, self.spark)
        logger.info(f"df_system_therapies.count() = : {df_system_therapies.count()}")
        df_system_therapies = cast_study_dates(
            df_system_therapies,
            [
                "therapy_start_date",
                "therapy_end_date",
            ],
        )
        df_system_therapies = df_system_therapies.join(
            df_all_conditions,
            df_all_conditions.condition_id == df_system_therapies.reason_reference,
            "left",
        )
        df_system_therapies_deidentified = deidentify(
            df_system_therapies, IDENTIFYING_COLS, df_lookup
        )
        df_system_therapies_deidentified.show()
        save_final_df(
            df_system_therapies_deidentified,
            self.settings,
            suffix="system_therapies_deidentified",
        )
        save_final_df_parquet(
            df_system_therapies_deidentified,
            self.settings,
            suffix="system_therapies_deidentified",
        )

        # radio therapy
        df_parents_radiotherapies, df_children_bestrahlung = extract_radiotherapies(
            self.pc, self.data, self.settings, self.spark
        )

        df_radiotherapies_joined = join_radiotherapies(
            df_parents_radiotherapies, df_children_bestrahlung
        )
        logger.info(f"df_radiotherapies_joined.count() = : {df_radiotherapies_joined.count()}")
        df_radiotherapies_joined = cast_study_dates(
            df_radiotherapies_joined,
            [
                "therapy_start_date",
                "therapy_end_date",
            ],
        )
        df_radiotherapies_joined = df_radiotherapies_joined.join(
            df_all_conditions,
            df_all_conditions.condition_id == df_radiotherapies_joined.reason_reference,
            "left",
        )
        df_radiotherapies_joined_deidentified = deidentify(
            df_radiotherapies_joined, IDENTIFYING_COLS, df_lookup
        )
        df_radiotherapies_joined_deidentified.show()
        save_final_df(
            df_radiotherapies_joined_deidentified,
            self.settings,
            suffix="radiotherapies_joined_deidentified",
        )
        save_final_df_parquet(
            df_radiotherapies_joined_deidentified,
            self.settings,
            suffix="radiotherapies_joined_deidentified",
        )

        # surgery
        df_ops = extract_surgeries(self.pc, self.data, self.settings, self.spark)
        df_ops = cast_study_dates(
            df_ops,
            [
                "therapy_start_date",
            ],
        )

        df_ops_grouped = group_ops(df_ops)
        logger.info(f"df_ops_grouped.count() = : {df_ops_grouped.count()}")
        df_ops_grouped = df_ops_grouped.join(
            df_all_conditions,
            df_all_conditions.condition_id == df_ops_grouped.reason_reference,
            "left",
        )
        df_ops_grouped_deidentified = deidentify(df_ops_grouped, IDENTIFYING_COLS, df_lookup)
        df_ops_grouped_deidentified.show()
        save_final_df(df_ops_grouped_deidentified, self.settings, suffix="ops_grouped_deidentified")
        save_final_df_parquet(
            df_ops_grouped_deidentified, self.settings, suffix="ops_grouped_deidentified"
        )
