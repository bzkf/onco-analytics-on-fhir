# onco-analytics-on-fhir

[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/bzkf/onco-analytics-on-fhir?label=openssf%20scorecard&style=flat)](https://scorecard.dev/viewer/?uri=github.com/bzkf/onco-analytics-on-fhir)

This software is used to transform oncological basic data set (oBDS) XML files from tumor documentation systems to HL7® FHIR® and to a tabular format.

## Modular Pipeline

![Figure Modular Pipeline](img/fig1.png)

### Citation

If you use this work, please cite:

Ziegler J, Erpenbeck MP, Fuchs T, Saibold A, Volkmer PC, Schmidt G, Eicher J, Pallaoro P, De Souza Falguera R, Aubele F, Hagedorn M, Vansovich E, Raffler J, Ringshandl S, Kerscher A, Maurer JK, Kühnel B, Schenkirsch G, Kampf M, Kapsner LA, Ghanbarian H, Spengler H, Soto-Rey I, Albashiti F, Hellwig D, Ertl M, Fette G, Kraska D, Boeker M, Prokosch HU, Gulden C
Bridging Data Silos in Oncology with Modular Software for Federated Analysis on Fast Healthcare Interoperability Resources: Multisite Implementation Study
J Med Internet Res 2025;27:e65681
[doi: 10.2196/65681](https://doi.org/10.2196/65681) PMID: 40233352 PMCID: 12041822

## Installation

### Installation: Docker Compose Setup

Please follow along here: [docker-compose/README.md](docker-compose/README.md)

### Installation: kubernetes setup

#### Prerequisites

- [helm](https://github.com/helm/helm)

#### Steps

##### Install K3S Cluster

```sh
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.26.1+k3s1 sh -

# optional: modify the kube config to allow running kubectl commands as a non-root user
# alternatively, prefix each kubectl and helm command with `sudo`
mkdir ~/.kube
sudo k3s kubectl config view --raw > ~/.kube/config
chmod 600 ~/.kube/config

kubectl get nodes

# create a namespace to isolate the installation
export ONCO_ANALYTICS_NAMESPACE_NAME=bzkf-onco-analytics
kubectl create namespace ${ONCO_ANALYTICS_NAMESPACE_NAME} --dry-run=client -o yaml | kubectl apply -f -
kubectl config set-context --current --namespace=${ONCO_ANALYTICS_NAMESPACE_NAME}
```

## Development

### Required tooling

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
sudo apt install openjdk-21-jre graphviz
```

### Install dependencies

```sh
uv sync
source .venv/bin/activate
```

### Format and auto-fix using Ruff

```sh
ruff format .
ruff check --fix .
```

### Run on CLI

```sh
python3 -m analytics_on_fhir.main
```

### Test

```sh
uv run pytest -vv --log-cli-level=20 --cov=analytics_on_fhir --cov-report=html --capture=no
```

#### Update snapshots

```sh
uv run pytest -vv --log-cli-level=20 --snapshot-update
```
