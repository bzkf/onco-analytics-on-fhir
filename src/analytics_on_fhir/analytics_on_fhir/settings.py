import os

import typed_settings as ts

HERE = os.path.abspath(os.path.dirname(__file__))


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
class FHIRServerSettings:
    base_url: str = None

@ts.settings
class Settings:
    spark: SparkSettings = SparkSettings()
    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = ts.secret(default="miniopass")
    delta_database_path: str = ""
    fhir: FHIRServerSettings = FHIRServerSettings()
    fhir_bundles_path: str = os.path.join(HERE, "../tests/fixtures/fhir/")
    study_name: str = "uc_aml" # "study_protocol_a"  # "embark_rwd" # "study_protocol_d"
    results_directory_path: str = os.path.join(HERE, "results/")
    location: str = "UKER"


settings = ts.load(Settings, appname="analytics_on_fhir", env_prefix="")
