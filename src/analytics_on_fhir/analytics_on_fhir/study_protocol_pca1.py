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
    group_ops,
    join_radiotherapies,
    union_sort_pivot_join,
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

        self.df_c61: DataFrame | None = None
        self.df_c61_clean: DataFrame | None = None
        # nachnutzbar für andere jobs bauen
        self.df_parents_radiotherapies: DataFrame | None = None
        self.df_children_bestrahlung: DataFrame | None = None
        self.df_radiotherapy_joined: DataFrame | None = None
        self.df_ops: DataFrame | None = None
        self.df_ops_grouped: DataFrame | None = None
        self.df_medication_statements: DataFrame | None = None
        self.year_min: int | None = None
        self.year_max: int | None = None

    def extract(self) -> DataFrame:
        """df = extract_df_study_protocol_a_d_mii(
            self.pc, self.data, self.settings, self.spark
        )
        df_c61 = df.filter(F.col("icd10_code").startswith("C61"))"""

        # read from csv for dev so its faster
        df_c61 = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(
                "/home/coder/git/onco-analytics-on-fhir/src/analytics_on_fhir/analytics_on_fhir/results/study_protocol_pca1/df_df_c61_clean.csv"
            )
        )

        logger.info("df_c61_count = {}", df_c61.count())
        self.df_c61 = df_c61

        # TO DO: remove all the df transformation logic here, separate function
        df_medication_statements = extract_systemtherapies(
            self.pc, self.data, self.settings, self.spark
        )

        self.df_medication_statements = df_medication_statements
        save_final_df(
            df_medication_statements, self.settings, suffix="medication_statements"
        )

        df_parents_radiotherapies, df_children_bestrahlung = extract_radiotherapies(
            self.pc, self.data, self.settings, self.spark
        )

        self.df_parents_radiotherapies = df_parents_radiotherapies
        self.df_children_bestrahlung = df_children_bestrahlung

        save_final_df(
            df_parents_radiotherapies, self.settings, suffix="parents_radiotherapies"
        )
        save_final_df(
            df_children_bestrahlung, self.settings, suffix="children_bestrahlung"
        )
        df_radiotherapies_joined = join_radiotherapies(
            df_parents_radiotherapies, df_children_bestrahlung
        )
        self.df_radiotherapies_joined = df_radiotherapies_joined
        save_final_df(
            df_radiotherapies_joined, self.settings, suffix="radiotherapies_joined"
        )

        df_ops = extract_surgeries(self.pc, self.data, self.settings, self.spark)
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

        """ # 1) Extract
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

        # 4) therapy sequence
        df_therapy_sequence = union_sort_pivot_join(
            df_c61_clean,
            self.df_ops_grouped,
            self.df_medication_statements,
            self.df_radiotherapies_joined,
        )
        save_final_df(df_therapy_sequence, self.settings, suffix="df_therapy_sequence") """
        # read csv
        # plot
        # read from csv for dev so its faster
        df_therapy_sequence = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(
                "/home/coder/git/onco-analytics-on-fhir/src/analytics_on_fhir/analytics_on_fhir/results/study_protocol_pca1/df_df_therapy_sequence.csv"
            )
        )
        self.plot(df_therapy_sequence)

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
                # therapy dates later
            ],
        )
        # df = compute_age(df) # schon drin
        df = flag_young_highrisk_cohort(
            df, age_col="age_at_diagnosis", gleason_col="gleason_score"
        )
        df = df.checkpoint(eager=True)
        return df

    def clean(self, df: DataFrame) -> DataFrame:
        df = df.filter(
            F.col("asserted_date") > F.lit("1970-01-01")
        )  # likely placeholder date
        df = df.filter(F.col("age_at_diagnosis") > 0)

        return df

    def plot(self, df: DataFrame) -> DataFrame:
        logger.info("StudyProtocolPCa1 pipeline - start plotting")
        # Wähle die Spalten für die ersten drei Therapien
        seq_cols = [
            "therapy_type_1",
            "therapy_type_2",
            "therapy_type_3",
            "zielgebiet_1",
            "zielgebiet_2",
            "zielgebiet_3",
            "ops_code_1",
            "ops_code_2",
            "ops_code_3",
            "medication_statement_atc_code_1",
            "medication_statement_atc_code_2",
            "medication_statement_atc_code_3",
        ]

        # Gruppieren nach Therapie-Sequenz
        df_sequence_counts = (
            df.groupBy("therapy_type_1", "therapy_type_2", "therapy_type_3")
            .agg(
                F.count("*").alias("count"),
                F.collect_set("zielgebiet_1").alias("zielgebiet_1_set"),
                F.collect_set("zielgebiet_2").alias("zielgebiet_2_set"),
                F.collect_set("zielgebiet_3").alias("zielgebiet_3_set"),
                F.collect_set("ops_code_1").alias("ops_code_1_set"),
                F.collect_set("ops_code_2").alias("ops_code_2_set"),
                F.collect_set("ops_code_3").alias("ops_code_3_set"),
                F.collect_set("medication_statement_atc_code_1").alias("atc_1_set"),
                F.collect_set("medication_statement_atc_code_2").alias("atc_2_set"),
                F.collect_set("medication_statement_atc_code_3").alias("atc_3_set"),
            )
            .orderBy(F.col("count").desc())
        )

        df_sequence_counts.show(200, truncate=False)
