import os
from enum import Enum
from pathlib import Path

import typed_settings as ts

HERE = os.path.abspath(os.path.dirname(__file__))


class StudyNames(Enum):
    EMBARK_RWD = "embark_rwd"
    STUDY_PROTOCOL_A = "study_protocol_a"
    STUDY_PROTOCOL_D = "study_protocol_d"
    STUDY_PROTOCOL_PCA1 = "study_protocol_pca1"
    STUDY_PROTOCOL_PCA1_MERGED_PLOTS = "study_protocol_pca1_merged_plots"
    STUDY_PROTOCOL_AML = "study_protocol_aml"
    AML = "aml"
    REACTO_COUNTS = "reacto_counts"
    DQ = "dq"
    ALL = "all"


@ts.settings
class SparkSettings:
    install_packages_and_exit: bool = False
    master: str = "local[*]"
    s3_endpoint: str = "localhost:9000"
    s3_connection_ssl_enabled: str = "false"
    warehouse_dir: str = os.path.join(HERE, "warehouse")
    checkpoint_dir: str = os.path.join(HERE, "spark-checkpoints")
    driver_memory: str = "4g"


@ts.settings
class FHIRServerSettings:
    base_url: str | None = None
    print_request_urls: bool = False
    chunk_size: int = 100
    page_count: int = 1000
    num_processes: int = 6
    user: str = ""
    password: str = ts.secret(default="")


@ts.settings
class AMLSettings:
    # absolute path to the input csv for cytostatic drug data
    csv_input_dir: str = "/home/onco-analytics-on-fhir/analytics-on-fhir/zenzy_data.csv"
    # column where the patient ids from input csv match column 'patient_mrn' in aml_all_patients.csv
    csv_patient_column: str = "KIS-Patienten-ID"
    use_cytostatics_data: bool = False


@ts.settings
class Settings:
    fhir: FHIRServerSettings
    spark: SparkSettings
    aml: AMLSettings
    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = ts.secret(default="miniopass")
    delta_database_path: str = ""
    fhir_bundles_path: str = os.path.join(HERE, "../tests/fixtures/fhir/")
    study_name: StudyNames = StudyNames.ALL
    results_directory_path: str = os.path.join(HERE, "results/")
    location: str = "BZKF"


converter = ts.converters.get_default_cattrs_converter()
converter.register_structure_hook(StudyNames, ts.converters.to_enum_by_value)

loaders = [
    *ts.default_loaders("analytics_on_fhir", env_prefix=""),
    ts.loaders.DotEnvLoader(
        prefix="",
        dotenv_path=Path(".env"),
    ),
]

settings = ts.load_settings(
    Settings,
    loaders=loaders,
    converter=converter,
)
