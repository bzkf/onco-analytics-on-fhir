import os
import secrets

from fhir_constants import FHIR_SYSTEM_PRIMAERTUMOR
from loguru import logger
from mii_conditions_labs import PyRateQuery
from pathling import PathlingContext
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from utils import (
    IDENTIFYING_COLS,
    cast_study_dates,
    compute_age,
    deidentify,
    extract_conditions_patients_death,
    extract_m_tnm,
    extract_metastasis,
    extract_n_tnm,
    extract_radiotherapies,
    extract_surgeries,
    extract_systemtherapies,
    extract_t_tnm,
    extract_uicc_tnm,
    extract_y_tnm,
    group_entity_or_parent,
    group_ops,
    join_radiotherapies,
    save_final_df,
    save_final_df_parquet,
)
from views import (
    grading_view,
    leistungszustand_ecog_karnofsky_view,
    progression_view,
    weitere_klassifikation_view,
)


class AllObdsPatients:
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

        self.year_min: int | None = None
        self.year_max: int | None = None

        # TO DO - decouple settings object where only output dir is needed
        self.output_dir = os.path.join(settings.results_directory_path, settings.study_name.value)

        os.makedirs(self.output_dir, exist_ok=True)

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
        logger.info("All oBDS pipeline started")

        crypto_key = secrets.token_hex(32)

        # Extract
        df_extract = self.extract()

        # Prepare
        df_prepare = self.prepare(df_extract)

        # Clean + Jahr-Range
        df_all_obds_clean = self.clean(df_prepare)
        self.year_min = df_all_obds_clean.select(F.min(F.year("asserted_date"))).first()[0]
        self.year_max = df_all_obds_clean.select(F.max(F.year("asserted_date"))).first()[0]

        save_final_df(df_all_obds_clean, self.settings, suffix="oBDS_primaerdiagnosen")
        save_final_df_parquet(df_all_obds_clean, self.settings, suffix="oBDS_primaerdiagnosen")

        # save all conditions
        logger.info(f"year range detected: {self.year_min} → {self.year_max}")
        df_all_obds_clean_deidentified = deidentify(df_all_obds_clean, IDENTIFYING_COLS, crypto_key)
        save_final_df(
            df_all_obds_clean_deidentified,
            self.settings,
            suffix="all_obds_clean_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            df_all_obds_clean_deidentified,
            self.settings,
            suffix="all_obds_clean_deidentified",
            deidentified=True,
        )

        # extract MII conditions
        pandas_df_clean = df_all_obds_clean.toPandas()
        patient_list = pandas_df_clean["condition_patient_resource_id"].dropna()
        patient_list.drop_duplicates(inplace=True)
        mii_conditions_all_obds_pats_pandas = self.extract_mii_conditions(
            patient_list, suffix="_2_mals", crypto_key=crypto_key
        )

        # TRANSFORM TO PYSPARK - parse dates
        mii_conditions_all_obds_pats = self.spark.createDataFrame(
            mii_conditions_all_obds_pats_pandas
        )
        mii_conditions_all_obds_pats = mii_conditions_all_obds_pats.withColumnRenamed(
            "condition_id", "condition_id_mii"
        )
        mii_conditions_all_obds_pats = mii_conditions_all_obds_pats.withColumn(
            "diagnosis_onsetDateTime",
            F.to_date(F.col("diagnosis_onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        ).withColumn(
            "diagnosis_recordedDate",
            F.to_date(F.col("diagnosis_recordedDate"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )

        mii_conditions_all_obds_pats_asserted = df_all_obds_clean.select(
            "condition_patient_resource_id", "asserted_date"
        ).join(
            mii_conditions_all_obds_pats,
            F.col("condition_patient_reference") == F.col("condition_patient_resource_id"),
            "left",
        )
        save_final_df(
            mii_conditions_all_obds_pats_asserted,
            self.settings,
            suffix="mii_conditions_2_mals_asserted",
        )
        save_final_df_parquet(
            mii_conditions_all_obds_pats_asserted,
            self.settings,
            suffix="mii_conditions_2_mals_asserted",
        )
        mii_conditions_all_obds_pats_asserted_deidentified = deidentify(
            mii_conditions_all_obds_pats_asserted, IDENTIFYING_COLS, crypto_key
        )
        save_final_df(
            mii_conditions_all_obds_pats_asserted_deidentified,
            self.settings,
            suffix="mii_conditions_all_obds_pats_asserted_deidentified",
        )
        save_final_df_parquet(
            mii_conditions_all_obds_pats_asserted_deidentified,
            self.settings,
            suffix="mii_conditions_all_obds_pats_asserted_deidentified",
        )

        self.extract_save_metastasis(df_all_obds_clean, crypto_key)

        self.extract_save_therapies(df_all_obds_clean, crypto_key)

        self.extract_save_tnm(df_all_obds_clean, crypto_key)

        self.extract_save_progressions(df_all_obds_clean, crypto_key)

        self.extract_save_gradings(df_all_obds_clean, crypto_key)

        # ! funktioniert nicht, to do: fix SQLSTATE: P0001
        # Expecting a collection with a single element but it has many
        # self.extract_save_weitere_klassifikationen(df_all_conditions, crypto_key)

        self.extract_save_leistungszustand_ecog_karnofsky(df_all_obds_clean, crypto_key)

        logger.info("All oBDS pipeline finished")

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

    def extract_mii_conditions(self, df_list, suffix, crypto_key):
        logger.info("start PyRate query for conditions.")
        query = PyRateQuery(self.settings)
        mii_condition_df = query.extract_conditions(df_list, suffix, crypto_key)
        return mii_condition_df

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
        save_final_df_parquet(df_system_therapies, self.settings, suffix="system_therapies")
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
        save_final_df_parquet(
            df_radiotherapies_joined, self.settings, suffix="radiotherapies_joined"
        )
        df_system_therapies_deidentified = deidentify(
            df_system_therapies, IDENTIFYING_COLS, crypto_key
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
        )
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

        # extract y tnm
        df_y_tnm = extract_y_tnm(self.pc, self.data, self.settings, self.spark)
        df_y_tnm = cast_study_dates(
            df_y_tnm,
            [
                "y_tnm_date",
            ],
        )

        df_y_tnm = df_y_tnm.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        df_y_tnm.show()

        save_final_df(df_y_tnm, self.settings, suffix="y_tnm")
        df_y_tnm_deidentified = deidentify(df_y_tnm, IDENTIFYING_COLS, crypto_key)
        df_y_tnm_deidentified.show()

        save_final_df(
            df_y_tnm_deidentified, self.settings, suffix="y_tnm_deidentified", deidentified=True
        )
        save_final_df_parquet(
            df_y_tnm_deidentified, self.settings, suffix="y_tnm_deidentified", deidentified=True
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

    def extract_save_progressions(self, df_all_conditions, crypto_key):
        progressions = progression_view(self.data)
        progressions = cast_study_dates(
            progressions,
            [
                "effective_dateTime",
            ],
        )

        progressions = progressions.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        progressions.show()

        save_final_df(progressions, self.settings, suffix="progressions")
        progressions_deidentified = deidentify(progressions, IDENTIFYING_COLS, crypto_key)
        progressions_deidentified.show()

        save_final_df(
            progressions_deidentified,
            self.settings,
            suffix="progressions_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            progressions_deidentified,
            self.settings,
            suffix="progressions_deidentified",
            deidentified=True,
        )

    def extract_save_gradings(self, df_all_conditions, crypto_key):
        gradings = grading_view(self.data)
        gradings = cast_study_dates(
            gradings,
            [
                "grading_date",
            ],
        )

        gradings = gradings.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        gradings.show()

        save_final_df(gradings, self.settings, suffix="gradings")
        gradings_deidentified = deidentify(gradings, IDENTIFYING_COLS, crypto_key)
        gradings_deidentified.show()

        save_final_df(
            gradings_deidentified, self.settings, suffix="gradings_deidentified", deidentified=True
        )
        save_final_df_parquet(
            gradings_deidentified, self.settings, suffix="gradings_deidentified", deidentified=True
        )

    def extract_save_weitere_klassifikationen(self, df_all_conditions, crypto_key):
        weitere_klassifikationen = weitere_klassifikation_view(self.data)
        weitere_klassifikationen = cast_study_dates(
            weitere_klassifikationen,
            [
                "weitere_klassifikation_date",
            ],
        )

        weitere_klassifikationen = weitere_klassifikationen.join(
            df_all_conditions,
            on="condition_id",
            how="right",
        )
        weitere_klassifikationen.show()

        save_final_df(weitere_klassifikationen, self.settings, suffix="weitere_klassifikationen")
        weitere_klassifikationen_deidentified = deidentify(
            weitere_klassifikationen, IDENTIFYING_COLS, crypto_key
        )
        weitere_klassifikationen_deidentified.show()

        save_final_df(
            weitere_klassifikationen_deidentified,
            self.settings,
            suffix="weitere_klassifikationen_deidentified",
            deidentified=True,
        )
        save_final_df_parquet(
            weitere_klassifikationen_deidentified,
            self.settings,
            suffix="weitere_klassifikationen_deidentified",
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
