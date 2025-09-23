from setuptools import find_packages, setup

with open("src/obds_fhir_to_opal/requirements.in") as f:
    requirements = f.read().splitlines()


setup(
    name="obds_fhir_to_opal",
    version="0.1.0",
    description="Utilities for Analytics for Oncology Data on FHIR",
    author="Jasmin Ziegler",
    url="https://github.com/bzkf/onco-analytics-on-fhir",
    packages=find_packages(where="src"),
    install_requires=requirements,
    python_requires=">=3.10",
)
