import re

from loguru import logger
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from study_protocol_pca_utils import (
    HERE,
    aggregate_age_gleason,
    aggregate_diagnosis_year_gleason,
    aggregate_metastases_age,
    aggregate_therapy_combinations,
    aggregate_therapy_combinations_with_metastasis,
    extract_radiotherapies,
    extract_surgeries,
    extract_systemtherapies,
    flag_young_highrisk_cohort,
    group_ops,
    join_radiotherapies,
    plot_therapies_metastasis_split,
    plot_therapy_combinations,
    plot_therapy_sequence,
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
        """ self.extract()

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
        # df_therapy_sequence = union_sort_pivot_join(
        #     df_c61_clean,
        #     self.df_ops_grouped,
        #     self.df_system_therapies,
        #     self.df_radiotherapies_joined,
        # )
        # save_final_df(df_therapy_sequence, self.settings, suffix="df_therapy_sequence")

        # self.generate_sequence_csv(df_therapy_sequence)

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
        ) """

        # DEV hack faster read in
        df_therapy_sequence_first_line_4_months = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .option("sep", ";")
            .csv(HERE + "/results/study_protocol_pca1/df_therapy_sequence_first_line_4_months.csv")
        )

        # 6) aggregate csvs and save
        # diagnosis year gleason
        pandas_df_therapy_sequence_first_line_4_months = (
            df_therapy_sequence_first_line_4_months.toPandas()
        )
        df_diagnosis_year = aggregate_diagnosis_year_gleason(
            pandas_df_therapy_sequence_first_line_4_months, self.settings
        )
        print(df_diagnosis_year)
        # metastasis age
        df_metastases = aggregate_metastases_age(
            pandas_df_therapy_sequence_first_line_4_months, self.settings
        )
        print(df_metastases)
        # age classes cohort gleason
        df_age_gleason = aggregate_age_gleason(
            pandas_df_therapy_sequence_first_line_4_months, self.settings
        )
        print(df_age_gleason)

        # aggregate therapies
        df_therapy_sequence_first_line_4_months_cohort = (
            df_therapy_sequence_first_line_4_months.filter(F.col("cohort_flag") == 1)
        )
        df_therapy_sequence_first_line_4_months_rest = (
            df_therapy_sequence_first_line_4_months.filter(F.col("cohort_flag") == 0)
        )
        # to do - vereinheitlichen, keine Zeit gehabt, mitgewachsen
        df_therapy_cohort = aggregate_therapy_combinations(
            df_therapy_sequence_first_line_4_months_cohort, "cohort"
        )
        df_therapy_rest = aggregate_therapy_combinations(
            df_therapy_sequence_first_line_4_months_rest, "rest"
        )
        df_therapy_cohort_met = aggregate_therapy_combinations_with_metastasis(
            df_therapy_sequence_first_line_4_months_cohort, "cohort"
        )
        df_therapy_rest_met = aggregate_therapy_combinations_with_metastasis(
            df_therapy_sequence_first_line_4_months_rest, "rest"
        )

        # 7) plot therapies
        year_min_cohort = df_therapy_sequence_first_line_4_months_cohort.select(
            F.min(F.year("asserted_date"))
        ).first()[0]
        year_max_cohort = df_therapy_sequence_first_line_4_months_cohort.select(
            F.max(F.year("asserted_date"))
        ).first()[0]
        plot_therapy_combinations(
            df_therapy_cohort, "cohort", self.settings, year_min_cohort, year_max_cohort
        )
        year_min_rest = df_therapy_sequence_first_line_4_months_rest.select(
            F.min(F.year("asserted_date"))
        ).first()[0]
        year_max_rest = df_therapy_sequence_first_line_4_months_rest.select(
            F.max(F.year("asserted_date"))
        ).first()[0]
        plot_therapy_combinations(
            df_therapy_rest, "rest", self.settings, year_min_rest, year_max_rest
        )

        plot_therapies_metastasis_split(
            df_therapy_cohort_met, "cohort", self.settings, year_min_cohort, year_max_cohort
        )
        plot_therapies_metastasis_split(
            df_therapy_rest_met, "rest", self.settings, year_min_rest, year_max_rest
        )

        # plot diagnosis year before 1990, 1990-2000, 2000-2010, 2010-2020, 2020-2026 z.B.
        # filter date diagnosis, cohort, metastasis
        # Cohort-Flag
        df_cohort = df_therapy_sequence_first_line_4_months_cohort
        df_rest = df_therapy_sequence_first_line_4_months_rest

        year_bins = [
            # ("<1990", "0000-01-01", "1990-01-01"),
            ("1990-2000", "1990-01-01", "2000-01-01"),
            ("2000-2010", "2000-01-01", "2010-01-01"),
            ("2010-2020", "2010-01-01", "2020-01-01"),
            ("2020-2026", "2020-01-01", "2026-12-31"),
        ]

        for label_flag, df_group in [("cohort", df_cohort), ("rest", df_rest)]:
            for year_label, start, end in year_bins:
                df_interval = df_group.filter(
                    (F.col("asserted_date") >= F.lit(start)) & (F.col("asserted_date") < F.lit(end))
                )

                df_therapy_met = aggregate_therapy_combinations_with_metastasis(
                    df_interval, f"{label_flag} {year_label}"
                )

                # Berechne min/max Jahre für Plot
                year_min = df_interval.select(F.min(F.year("asserted_date"))).first()[0]
                year_max = df_interval.select(F.max(F.year("asserted_date"))).first()[0]

                plot_therapies_metastasis_split(
                    df_therapy_met, f"{label_flag}_{year_label}", self.settings, year_min, year_max
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
