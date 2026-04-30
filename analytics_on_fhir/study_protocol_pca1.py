import os
import re
import secrets

from fhir_constants import FHIR_SYSTEM_PRIMAERTUMOR
from loguru import logger
from mii_conditions_labs import PyRateQuery
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from study_protocol_pca_utils import (  # aggregate_local_csvs,; union_sort_pivot_join,
    flag_young_highrisk_cohort,
    plot_age_class,
    plot_diagnosis_year,
    plot_metastasis_loc,
    plot_therapies_metastasis_split,
    plot_therapy_combinations,
    read_merged_csvs,
    with_mapped_atc_column,
)
from utils import (
    IDENTIFYING_COLS,
    cast_study_dates,
    compute_age,
    deidentify,
    extract_conditions_patients_death,
    extract_gleason,
    extract_m_tnm,
    extract_metastasis,
    extract_n_tnm,
    extract_radiotherapies,
    extract_surgeries,
    extract_systemtherapies,
    extract_t_tnm,
    extract_uicc_tnm,
    group_ops,
    join_radiotherapies,
    map_gleason_sct_to_score,
    months_diff,
    normalize_array_columns,
    save_final_df,
    save_final_df_parquet,
)
from views import leistungszustand_ecog_karnofsky_view


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

        self.df_c61_conditions_patients_death_gleason_met: DataFrame | None = None
        self.df_c61_conditions_patients_death_gleason_met_clean: DataFrame | None = None
        # nachnutzbar für andere jobs bauen
        self.df_parents_radiotherapies: DataFrame | None = None
        self.df_children_bestrahlung: DataFrame | None = None
        self.df_radiotherapy_joined: DataFrame | None = None
        self.df_ops: DataFrame | None = None
        self.df_ops_grouped: DataFrame | None = None
        self.df_system_therapies: DataFrame | None = None
        self.year_min: int | None = None
        self.year_max: int | None = None

        self.output_dir = os.path.join(settings.results_directory_path, settings.study_name.value)

        os.makedirs(self.output_dir, exist_ok=True)

    def extract_from_obds(self, crypto_key) -> DataFrame:
        df = extract_conditions_patients_death(self.pc, self.data, self.settings, self.spark)
        df_c61_condition_patients_death = df.filter(F.col("icd10_code").startswith("C61"))
        logger.info(
            "df_c61_condition_patients_death count fruehere and primaertumor = {}",
            df_c61_condition_patients_death.count(),
        )

        # filter out fruehere tumorerkrankung - only primaerdiagnose
        df_c61_condition_patients_death = df_c61_condition_patients_death.filter(
            F.col("meta_profile").startswith(FHIR_SYSTEM_PRIMAERTUMOR)
        )
        logger.info(
            "df_c61_condition_patients_death count only primaertumor = {}",
            df_c61_condition_patients_death.count(),
        )

        df_gleason = extract_gleason(self.pc, self.data, self.settings, self.spark)
        logger.info("df_gleason count = {}", df_gleason.count())
        df_gleason.show(truncate=False)

        w = Window.partitionBy("observation_gleason_condition_resource_id").orderBy(
            F.col("gleason_date").asc()
        )

        df_gleason_first = (
            df_gleason.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .orderBy("gleason_date")
        )

        logger.info("df_gleason_first count = {}", df_gleason_first.count())
        df_gleason_first.show(truncate=False)

        # join conditions patients death + gleason
        df_c61_conditions_patients_death_gleason = (
            df_c61_condition_patients_death.alias("c")
            .join(
                df_gleason_first.alias("o"),
                F.col("c.condition_id") == F.col("o.observation_gleason_condition_resource_id"),
                "left",
            )
            .select("c.*", "o.*")
        )

        # map sct to score
        df_c61_conditions_patients_death_gleason = map_gleason_sct_to_score(
            df_c61_conditions_patients_death_gleason,
            gleason_sct_col="gleason_sct",
            out_col="gleason_score",
        )

        logger.info(
            "df_conditions_patients_death_gleason count = {}",
            df_c61_conditions_patients_death_gleason.count(),
        )
        df_c61_conditions_patients_death_gleason.show(truncate=False)

        df_metastasis = extract_metastasis(self.pc, self.data, self.settings, self.spark)

        logger.info("df_metastasis count = {}", df_metastasis.count())
        df_metastasis.show(truncate=False)

        df_c61_conditions_patients_death_gleason_met = (
            df_c61_conditions_patients_death_gleason.alias("c").join(
                df_metastasis.alias("m"),
                on="condition_id",
                how="left",
            )
        )

        self.df_c61_conditions_patients_death_gleason_met = (
            df_c61_conditions_patients_death_gleason_met
        )
        save_final_df(
            df_c61_conditions_patients_death_gleason_met,
            self.settings,
            suffix="c61_conditions_patients_death_gleason_met",
        )
        save_final_df(
            df_c61_conditions_patients_death_gleason_met,
            self.settings,
            suffix="c61_conditions_patients_death_gleason_met",
        )

        self.extract_save_therapies(
            df_c61_conditions_patients_death_gleason_met.select("condition_id", "asserted_date"),
            crypto_key,
        )

        self.extract_save_metastasis(
            df_c61_conditions_patients_death_gleason_met.select("condition_id", "asserted_date"),
            crypto_key,
        )

        self.extract_save_tnm(
            df_c61_conditions_patients_death_gleason_met.select("condition_id", "asserted_date"),
            crypto_key,
        )

        self.extract_save_leistungszustand_ecog_karnofsky(
            df_c61_conditions_patients_death_gleason_met.select("condition_id", "asserted_date"),
            crypto_key,
        )

    def run(self):
        logger.info("StudyProtocolPCa1 pipeline started")

        # crypto_key = secrets.token_hex(32)
        # DEV
        crypto_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

        # 1) Extract
        self.extract_from_obds(crypto_key)

        # 2) Prepare dates, age, cohort
        df_c61_conditions_patients_death_gleason_met = self.prepare(
            self.df_c61_conditions_patients_death_gleason_met
        )
        df_c61_conditions_patients_death_gleason_met.show()

        # 3) Clean + Jahr-Range
        df_c61_conditions_patients_death_gleason_met_clean = self.clean(
            df_c61_conditions_patients_death_gleason_met
        )
        logger.info(
            "df_c61_clean count = {}", df_c61_conditions_patients_death_gleason_met_clean.count()
        )
        self.year_min = df_c61_conditions_patients_death_gleason_met_clean.select(
            F.min(F.year("asserted_date"))
        ).first()[0]
        self.year_max = df_c61_conditions_patients_death_gleason_met_clean.select(
            F.max(F.year("asserted_date"))
        ).first()[0]
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        self.df_c61_conditions_patients_death_gleason_met_clean = (
            df_c61_conditions_patients_death_gleason_met_clean
        )
        save_final_df(
            df_c61_conditions_patients_death_gleason_met_clean,
            self.settings,
            suffix="c61_conditions_patients_death_gleason_met_clean",
        )
        save_final_df_parquet(
            df_c61_conditions_patients_death_gleason_met_clean,
            self.settings,
            suffix="c61_conditions_patients_death_gleason_met_clean",
        )
        df_c61_conditions_patients_death_gleason_met_clean_deidentified = deidentify(
            df_c61_conditions_patients_death_gleason_met_clean, IDENTIFYING_COLS, crypto_key
        )
        save_final_df(
            df_c61_conditions_patients_death_gleason_met_clean_deidentified,
            self.settings,
            suffix="c61_conditions_patients_death_gleason_met_clean_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_c61_conditions_patients_death_gleason_met_clean_deidentified,
            self.settings,
            suffix="c61_conditions_patients_death_gleason_met_clean_deidentified",
            deidentified=True,
        )

        # DEV
        # read csv here to pyspark
        # import pandas as pd
        # suffix = "c61_conditions_patients_death_gleason_met_clean"
        # csv_path = os.path.join(self.output_dir, f"df_{suffix}.csv")
        # df_c61_conditions_patients_death_gleason_met_clean = (
        #     self.spark.read.option("header", "true").option("sep", ";").csv(csv_path)
        # )
        # df_c61_conditions_patients_death_gleason_met_clean.show()

        # 4) Nebendiagnosen: extract mii conditions + labs for c61 pats
        pandas_df_pseudonyms_c61 = df_c61_conditions_patients_death_gleason_met_clean.toPandas()
        df_list_c61 = pandas_df_pseudonyms_c61["patient_resource_id"].drop_duplicates().dropna()

        mii_conditions_pandas = self.extract_mii_conditions(
            df_list_c61, suffix="", crypto_key=crypto_key
        )
        mii_conditions = self.spark.createDataFrame(mii_conditions_pandas)
        mii_conditions = mii_conditions.withColumnRenamed("condition_id", "condition_id_mii")
        mii_conditions = mii_conditions.withColumn(
            "diagnosis_onsetDateTime",
            F.to_date(F.col("diagnosis_onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        ).withColumn(
            "diagnosis_recordedDate",
            F.to_date(F.col("diagnosis_recordedDate"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )
        mii_conditions_asserted = df_c61_conditions_patients_death_gleason_met_clean.select(
            "condition_id", "asserted_date", "condition_patient_resource_id"
        ).join(
            mii_conditions,
            F.col("condition_patient_resource_id") == F.col("condition_patient_reference"),
            "left",
        )
        save_final_df(mii_conditions_asserted, self.settings, suffix="mii_conditions_asserted")
        save_final_df_parquet(
            mii_conditions_asserted, self.settings, suffix="mii_conditions_asserted"
        )
        mii_conditions_asserted_deidentified = deidentify(
            mii_conditions_asserted, IDENTIFYING_COLS, crypto_key
        )
        save_final_df(
            mii_conditions_asserted_deidentified,
            self.settings,
            suffix="mii_conditions_asserted_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            mii_conditions_asserted_deidentified,
            self.settings,
            suffix="mii_conditions_asserted_deidentified",
            deidentified=True,
        )

        # labs
        mii_labs_pandas = self.extract_mii_labs(df_list_c61, suffix="", crypto_key=crypto_key)

        # DEV
        # read csv to pandas here
        # suffix = "mii_labs"
        # csv_path = os.path.join(self.output_dir, f"df_{suffix}.csv")
        # mii_labs_pandas = pd.read_csv(csv_path, sep=";")

        # debug schema equivalent
        print(mii_labs_pandas.dtypes)
        mii_labs_pandas = mii_labs_pandas.replace(["NaN", "nan"], None)
        mii_labs_pandas = normalize_array_columns(mii_labs_pandas)
        mii_labs = self.spark.createDataFrame(mii_labs_pandas)
        for field in mii_labs.schema.fields:
            print(field.name, field.dataType)

        # DEV
        # read csv here to pyspark
        # suffix = "mii_labs"
        # csv_path = os.path.join(self.output_dir, f"df_{suffix}.csv")
        # # exchange sep to ; later here
        # mii_labs = self.spark.read.option("header", "true").option("sep", ",").csv(csv_path)
        # for field in mii_labs.schema.fields:
        #     print(field.name, field.dataType)

        # mii_labs = mii_labs.withColumn(
        #     "lab_dateTime", F.to_date(F.col("lab_dateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX")
        # )

        mii_labs = mii_labs.withColumn(
            "lab_dateTime", F.expr("try_to_timestamp(lab_dateTime, \"yyyy-MM-dd'T'HH:mm:ssXXX\")")
        )

        # optional
        mii_labs = mii_labs.withColumn("lab_dateTime", F.to_date("lab_dateTime"))

        mii_labs_asserted = df_c61_conditions_patients_death_gleason_met_clean.select(
            "condition_id", "asserted_date", "condition_patient_resource_id"
        ).join(
            mii_labs,
            F.col("condition_patient_resource_id") == F.col("observation_patient_reference"),
            "left",
        )
        save_final_df(mii_labs_asserted, self.settings, suffix="mii_labs_asserted")
        save_final_df_parquet(mii_labs_asserted, self.settings, suffix="mii_labs_asserted")
        mii_labs_asserted_deidentified = deidentify(mii_labs_asserted, IDENTIFYING_COLS, crypto_key)
        save_final_df(
            mii_labs_asserted_deidentified,
            self.settings,
            suffix="mii_labs_asserted_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            mii_labs_asserted_deidentified,
            self.settings,
            suffix="mii_labs_asserted_deidentified",
            deidentified=True,
        )

        # 5) therapy sequence all therapies - for REACTO
        # df_therapy_sequence = union_sort_pivot_join(
        #     df_c61_conditions_patients_death_gleason_clean,
        #     self.df_ops_grouped,
        #     self.df_system_therapies,
        #     self.df_radiotherapies_joined,
        # )
        # save_final_df(df_therapy_sequence, self.settings, suffix="df_therapy_sequence")
        # self.generate_sequence_csv(df_therapy_sequence)

        # 6) first line therapy - within 4 months of diagnosis
        # df_therapy_sequence_first_line_4_months = union_sort_pivot_join(
        #     df_c61_conditions_patients_death_gleason_met_clean,
        #     self.df_ops_grouped,
        #     self.df_system_therapies,
        #     self.df_radiotherapies_joined,
        #     first_line_months_threshold=4,
        # )
        # df_therapy_sequence_first_line_4_months = with_mapped_atc_column(
        #     df_therapy_sequence_first_line_4_months, self.spark
        # )
        # save_final_df(
        #     df_therapy_sequence_first_line_4_months,
        #     self.settings,
        #     suffix="therapy_sequence_first_line_4_months",
        # )

        # # 7) aggregate local csvs and save
        # aggregate_local_csvs(df_therapy_sequence_first_line_4_months, self.settings)

        logger.info("StudyProtocolPCa1 pipeline finished")

    def prepare(self, df: DataFrame) -> DataFrame:
        df = cast_study_dates(
            df,
            [
                "asserted_date",
                "recorded_date",
                "deceased_datetime",
                "date_death",
                "gleason_date",
                "metastasis_date",
            ],
        )
        df = compute_age(df)
        df = flag_young_highrisk_cohort(df, age_col="age_at_diagnosis", gleason_col="gleason_score")
        # calculate gleason and metastasis months diff
        df = months_diff(df, "gleason_date", "asserted_date")
        df = months_diff(df, "metastasis_date", "asserted_date")
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

    def extract_mii_conditions(self, df_list_c61, suffix, crypto_key):
        logger.info("start PyRate query for conditions + labs.")
        query = PyRateQuery(self.settings)
        mii_conditions = query.extract_conditions(df_list_c61, suffix, crypto_key)
        return mii_conditions

    def extract_mii_labs(self, df_list_c61, suffix, crypto_key):
        logger.info("start PyRate query for conditions + labs.")
        query = PyRateQuery(self.settings)
        mii_labs = query.extract_labs(df_list_c61, suffix, crypto_key)
        return mii_labs

    def merged_plots(self):
        # führe alle einzel csvs zu gesamtheitlichen BZKF csvs zusammen
        # process_csvs(self.spark)
        # read in all bzkf csvs
        df_dict = read_merged_csvs(self.spark)

        for key, df in df_dict.items():
            if "therapy_combinations_cohort" in key:
                df_pd = df.toPandas()
                plot_therapy_combinations(
                    df_pd,
                    "cohort",
                    self.settings,
                )
            elif "therapy_combinations_rest" in key:
                df_pd = df.toPandas()
                plot_therapy_combinations(
                    df_pd,
                    "rest",
                    self.settings,
                )
            elif "therapy_combinations_met" in key:
                df_pivot = (
                    df.groupBy("combo_label")
                    .pivot("metastatic_flag", ["metastatic", "non_metastatic"])
                    .sum("count")
                    .fillna(0)
                )
                df_pivot = df_pivot.toPandas()
                print(df_pivot)

                label = "cohort" if "cohort" in key else "rest"

                # extract year range automatically
                match = re.search(r"\d{4}-\d{4}", key)
                if match:
                    years = match.group().replace("-", "_")
                    label += f"_{years}"

                plot_therapies_metastasis_split(df_pivot, label, self.settings)
            elif "diagnosis_year" in key:
                df_pd = df.toPandas()
                plot_diagnosis_year(
                    df_pd,
                    "",
                    self.settings,
                )
            elif "age_class" in key:
                df_pd = df.toPandas()
                plot_age_class(
                    df_pd,
                    "",
                    self.settings,
                )
            elif "metastasis_loc" in key:
                df_pd = df.toPandas()
                plot_metastasis_loc(
                    df_pd,
                    "",
                    self.settings,
                )

    def extract_save_therapies(self, df_all_conditions, crypto_key):
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
        # right join to only keep system therapies for 1 or 2 malignancy conditions
        df_system_therapies = df_system_therapies.join(
            df_all_conditions,
            df_all_conditions.condition_id == df_system_therapies.reason_reference,
            "right",
        )
        save_final_df(
            df_system_therapies,
            self.settings,
            suffix="system_therapies",
        )
        df_system_therapies_deidentified = deidentify(
            df_system_therapies, IDENTIFYING_COLS, crypto_key
        )
        save_final_df(
            df_system_therapies_deidentified,
            self.settings,
            suffix="system_therapies_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_system_therapies_deidentified,
            self.settings,
            suffix="system_therapies_deidentified",
            deidentified=True,
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
            "right",
        )
        save_final_df(
            df_radiotherapies_joined,
            self.settings,
            suffix="radiotherapies_joined",
        )
        df_radiotherapies_joined_deidentified = deidentify(
            df_radiotherapies_joined, IDENTIFYING_COLS, crypto_key
        )
        save_final_df(
            df_radiotherapies_joined_deidentified,
            self.settings,
            suffix="radiotherapies_joined_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_radiotherapies_joined_deidentified,
            self.settings,
            suffix="radiotherapies_joined_deidentified",
            deidentified=True,
        )

        # surgery
        df_ops = extract_surgeries(self.pc, self.data, self.settings, self.spark)
        df_ops = cast_study_dates(
            df_ops,
            [
                "therapy_start_date",
            ],
        )
        df_ops = df_ops.join(
            df_all_conditions,
            df_all_conditions.condition_id == df_ops.reason_reference,
            "right",
        ).drop("meta_profile")
        save_final_df(
            df_ops,
            self.settings,
            suffix="ops",
        )
        save_final_df_parquet(df_ops, self.settings, suffix="ops")
        df_ops_deidentified = deidentify(df_ops, IDENTIFYING_COLS, crypto_key)
        save_final_df(
            df_ops_deidentified, self.settings, suffix="ops_deidentified", deidentified=True
        )
        save_final_df_parquet(
            df_ops_deidentified, self.settings, suffix="ops_deidentified", deidentified=True
        )

        df_ops_grouped = group_ops(df_ops)
        logger.info(f"df_ops_grouped.count() = : {df_ops_grouped.count()}")
        df_ops_grouped = df_ops_grouped.join(
            df_all_conditions,
            df_all_conditions.condition_id == df_ops_grouped.reason_reference,
            "right",
        )
        save_final_df(
            df_ops_grouped,
            self.settings,
            suffix="ops_grouped",
        )
        df_ops_grouped_deidentified = deidentify(df_ops_grouped, IDENTIFYING_COLS, crypto_key)
        save_final_df(
            df_ops_grouped_deidentified,
            self.settings,
            suffix="ops_grouped_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_ops_grouped_deidentified,
            self.settings,
            suffix="ops_grouped_deidentified",
            deidentified=True,
        )

    def extract_save_tnm(self, df_all_conditions, crypto_key):
        # extract t
        df_t_tnm = extract_t_tnm(self.pc, self.data, self.settings, self.spark)
        df_t_tnm = cast_study_dates(
            df_t_tnm,
            [
                "t_tnm_date",
            ],
        )

        df_t_tnm = df_t_tnm.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        df_t_tnm.show()

        save_final_df(df_t_tnm, self.settings, suffix="t_tnm")
        df_t_tnm_deidentified = deidentify(df_t_tnm, IDENTIFYING_COLS, crypto_key)
        save_final_df(
            df_t_tnm_deidentified, self.settings, suffix="t_tnm_deidentified", deidentified=True
        )
        save_final_df_parquet(
            df_t_tnm_deidentified, self.settings, suffix="t_tnm_deidentified", deidentified=True
        )

        # extract n
        df_n_tnm = extract_n_tnm(self.pc, self.data, self.settings, self.spark)
        df_n_tnm = cast_study_dates(
            df_n_tnm,
            [
                "n_tnm_date",
            ],
        )

        df_n_tnm = df_n_tnm.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        df_n_tnm.show()

        save_final_df(df_n_tnm, self.settings, suffix="n_tnm")
        df_n_tnm_deidentified = deidentify(df_n_tnm, IDENTIFYING_COLS, crypto_key)
        save_final_df(
            df_n_tnm_deidentified, self.settings, suffix="n_tnm_deidentified", deidentified=True
        )
        save_final_df_parquet(
            df_n_tnm_deidentified, self.settings, suffix="n_tnm_deidentified", deidentified=True
        )

        # extract m
        df_m_tnm = extract_m_tnm(self.pc, self.data, self.settings, self.spark)
        df_m_tnm = cast_study_dates(
            df_m_tnm,
            [
                "m_tnm_date",
            ],
        )

        df_m_tnm = df_m_tnm.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        df_m_tnm.show()

        save_final_df(df_m_tnm, self.settings, suffix="m_tnm")
        df_m_tnm_deidentified = deidentify(df_m_tnm, IDENTIFYING_COLS, crypto_key)
        df_m_tnm_deidentified.show()

        save_final_df(
            df_m_tnm_deidentified, self.settings, suffix="m_tnm_deidentified", deidentified=True
        )
        save_final_df_parquet(
            df_m_tnm_deidentified, self.settings, suffix="m_tnm_deidentified", deidentified=True
        )

        # extract uicc
        df_uicc_tnm = extract_uicc_tnm(self.pc, self.data, self.settings, self.spark)
        df_uicc_tnm = cast_study_dates(
            df_uicc_tnm,
            [
                "uicc_tnm_date",
            ],
        )

        df_uicc_tnm = df_uicc_tnm.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        df_uicc_tnm.show()

        save_final_df(df_uicc_tnm, self.settings, suffix="uicc_tnm")
        df_uicc_tnm_deidentified = deidentify(df_uicc_tnm, IDENTIFYING_COLS, crypto_key)
        df_uicc_tnm_deidentified.show()

        save_final_df(
            df_uicc_tnm_deidentified,
            self.settings,
            suffix="uicc_tnm_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_uicc_tnm_deidentified,
            self.settings,
            suffix="uicc_tnm_deidentified",
            deidentified=True,
        )

    def extract_save_leistungszustand_ecog_karnofsky(self, df_all_conditions, crypto_key):
        leistungszustand_ecog_karnofsky = leistungszustand_ecog_karnofsky_view(self.data)
        leistungszustand_ecog_karnofsky = cast_study_dates(
            leistungszustand_ecog_karnofsky,
            [
                "effective_dateTime",
            ],
        )

        leistungszustand_ecog_karnofsky = leistungszustand_ecog_karnofsky.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        leistungszustand_ecog_karnofsky.show()

        save_final_df(
            leistungszustand_ecog_karnofsky, self.settings, suffix="leistungszustand_ecog_karnofsky"
        )
        leistungszustand_ecog_karnofsky_deidentified = deidentify(
            leistungszustand_ecog_karnofsky, IDENTIFYING_COLS, crypto_key
        )
        leistungszustand_ecog_karnofsky_deidentified.show()

        save_final_df(
            leistungszustand_ecog_karnofsky_deidentified,
            self.settings,
            suffix="leistungszustand_ecog_karnofsky_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            leistungszustand_ecog_karnofsky_deidentified,
            self.settings,
            suffix="leistungszustand_ecog_karnofsky_deidentified",
            deidentified=True,
        )
        leistungszustand_ecog_karnofsky_deidentified = deidentify(
            leistungszustand_ecog_karnofsky, IDENTIFYING_COLS, crypto_key
        )
        leistungszustand_ecog_karnofsky_deidentified.show()

        save_final_df(
            leistungszustand_ecog_karnofsky_deidentified,
            self.settings,
            suffix="leistungszustand_ecog_karnofsky_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            leistungszustand_ecog_karnofsky_deidentified,
            self.settings,
            suffix="leistungszustand_ecog_karnofsky_deidentified",
            deidentified=True,
        )

    def extract_save_metastasis(self, df_all_conditions, crypto_key):
        df_metastasis = extract_metastasis(self.pc, self.data, self.settings, self.spark)
        logger.info(f"df_1_2_cond_id_asserted.count() = : {df_all_conditions.count()}")
        df_metastasis = df_metastasis.join(df_all_conditions, "condition_id", "left")
        logger.info(f"df_1_2_mals_metastasis.count() = : {df_metastasis.count()}")

        save_final_df(df_metastasis, self.settings, suffix="metastasis")
        save_final_df_parquet(df_metastasis, self.settings, suffix="metastasis")

        df_metastasis_deidentified = deidentify(df_metastasis, IDENTIFYING_COLS, crypto_key)

        save_final_df(
            df_metastasis_deidentified,
            self.settings,
            suffix="metastasis_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_metastasis_deidentified,
            self.settings,
            suffix="metastasis_deidentified",
            deidentified=True,
        )
