# diz-in-a-box

[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/bzkf/diz-in-a-box/badge)](https://api.securityscorecards.dev/projects/github.com/bzkf/diz-in-a-box)

DIZ in a box.

## Installation

### Prerequisites

- [helm](https://github.com/helm/helm)

### Steps

#### Install K3S Cluster

```sh
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.26.1+k3s1 sh -

# optional: modify the kube config to allow running kubectl commands as a non-root user
# alternatively, prefix each kubectl and helm command with `sudo`
mkdir ~/.kube
sudo k3s kubectl config view --raw > ~/.kube/config
chmod 600 ~/.kube/config

kubectl get nodes

# create a namespace to isolate the installation
export DIZBOX_NAMESPACE_NAME=bzkf-dizbox
kubectl create namespace ${DIZBOX_NAMESPACE_NAME} --dry-run=client -o yaml | kubectl apply -f -
kubectl config set-context --current --namespace=${DIZBOX_NAMESPACE_NAME}
```

##### Air-gapped

Download the air-gapped installer and move it to the deployment machine:

<!-- x-release-please-start-version -->

```sh
curl -L -O https://github.com/bzkf/diz-in-a-box/releases/download/v1.5.5/air-gapped-installer.tgz
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

#### Install Strimzi Operator and Kafka

<!-- x-release-please-start-version -->

```sh
helm upgrade --install --wait --timeout=10m --version=1.5.5 prerequisites oci://ghcr.io/bzkf/diz-in-a-box/charts/prerequisites

kubectl apply -f k8s/kafka-cluster.yaml
kubectl wait kafka/bzkf-dizbox-cluster --for=condition=Ready --timeout=300s

# Optionally install KafkaBridge
kubectl apply -f k8s/kafka-bridge.yaml
kubectl wait kafkabridge/bzkf-dizbox-bridge --for=condition=Ready --timeout=300s

kubectl get all -A
```

#### Install DIZ-in-a-box

```sh
helm upgrade --install --wait --timeout=10m --version=1.5.5 diz-in-a-box oci://ghcr.io/bzkf/diz-in-a-box/charts/diz-in-a-box

# test the installation
helm test diz-in-a-box

kubectl wait deployment/diz-in-a-box-stream-processors-onkoadt-to-fhir --for=condition=Available --timeout=300s
kubectl wait deployment/diz-in-a-box-stream-processors-fhir-to-server --for=condition=Available --timeout=300s
```

<!-- x-release-please-end -->

## TODOs

- <https://docs.k3s.io/security/hardening-guide>
- set ACL for KafkaUsers to relevant topics
- hardening: change existing passwords; show how to add existing secrets via kubectl.
  `kubectl create secret generic --from-literal='GPAS__AUTH__BASIC__PASSWORD=test' gpas-basic-auth`
