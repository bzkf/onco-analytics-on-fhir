from enum import Enum
import os

import typed_settings as ts

HERE = os.path.abspath(os.path.dirname(__file__))


class StudyNames(Enum):
    EMBARK_RWD = "embark_rwd"
    STUDY_PROTOCOL_A = "study_protocol_a"
    STUDY_PROTOCOL_D = "study_protocol_d"
    STUDY_PROTOCOL_PCA1 = "study_protocol_pca1"
    STUDY_PROTOCOL_AML = "study_protocol_aml"
    ALL = "all"


@ts.settings
class SparkSettings:
    install_packages_and_exit: bool = False
    master: str = "local[*]"
    s3_endpoint: str = "localhost:9000"
    s3_connection_ssl_enabled: str = "false"
    warehouse_dir: str = os.path.join(HERE, "warehouse")
    checkpoint_dir: str = ".spark/checkpoints/"
    driver_memory: str = "4g"


@ts.settings
class Settings:
    spark: SparkSettings
    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = ts.secret(default="miniopass")
    delta_database_path: str = ""
    fhir_bundles_path: str = os.path.join(HERE, "../tests/fixtures/fhir/")
    study_name: StudyNames = StudyNames.STUDY_PROTOCOL_A
    results_directory_path: str = os.path.join(HERE, "results/")
    location: str = "UKER"


settings = ts.load(Settings, appname="analytics_on_fhir", env_prefix="")
