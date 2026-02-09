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

##### Air-gapped

Download the air-gapped installer and move it to the deployment machine:

<!-- x-release-please-start-version -->

```sh
curl -L -O https://github.com/bzkf/onco-analytics-on-fhir/releases/download/v2.13.0/air-gapped-installer.tgz
```

<!-- x-release-please-end -->

Run the following steps on the deployment machine.

Extract the archive:

```sh
tar xvzf ./air-gapped-installer.tgz
```

Prepare the images directory and k3s binary:

```sh
mkdir -p /var/lib/rancher/k3s/agent/images/
cp ./dist/air-gapped/k3s/k3s-airgap-images-amd64.tar /var/lib/rancher/k3s/agent/images/

cp ./dist/air-gapped/bin/k3s /usr/local/bin/k3s
```

Run the install script:

```sh
chmod +x ./dist/air-gapped/bin/install.sh
INSTALL_K3S_SKIP_DOWNLOAD=true ./dist/air-gapped/bin/install.sh
```

Run the script to import all required images:

```sh
chmod +x ./dist/air-gapped/bin/import-images-into-k3s.sh
IMAGE_FOLDER=./dist/air-gapped/images ./dist/air-gapped/bin/import-images-into-k3s.sh
```

##### Install Strimzi Operator and Kafka

<!-- x-release-please-start-version -->

```sh
helm upgrade --install --wait --timeout=10m --version=2.13.0 prerequisites oci://ghcr.io/bzkf/onco-analytics-on-fhir/charts/prerequisites

kubectl apply -f k8s/kafka-cluster.yaml
kubectl wait kafka/bzkf-dizbox-cluster --for=condition=Ready --timeout=300s

# Optionally install KafkaBridge
kubectl apply -f k8s/kafka-bridge.yaml
kubectl wait kafkabridge/bzkf-dizbox-bridge --for=condition=Ready --timeout=300s

kubectl get all -A
```

##### Install onco-analytics-on-fhir

```sh
helm upgrade --install --wait --timeout=10m --version=2.13.0 onco-analytics-on-fhir oci://ghcr.io/bzkf/onco-analytics-on-fhir/charts/onco-analytics-on-fhir

# test the installation
helm test onco-analytics-on-fhir

kubectl wait deployment/onco-analytics-on-fhir-stream-processors-obds-to-fhir --for=condition=Available --timeout=300s
kubectl wait deployment/onco-analytics-on-fhir-stream-processors-fhir-to-server --for=condition=Available --timeout=300s
```

<!-- x-release-please-end -->

## TODOs

- <https://docs.k3s.io/security/hardening-guide>
- set ACL for KafkaUsers to relevant topics
- hardening: change existing passwords; show how to add existing secrets via kubectl.
  `kubectl create secret generic --from-literal='GPAS__AUTH__BASIC__PASSWORD=test' gpas-basic-auth`
