# diz-in-a-box for onkoadt-to-fhir delivery

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

#### Install Strimzi Operator and Kafka

```sh
helm repo add strimzi https://strimzi.io/charts/
helm upgrade --install --wait -f k8s/strimzi-values.yaml strimzi-kafka-operator strimzi/strimzi-kafka-operator

kubectl apply -f k8s/kafka-cluster.yaml
kubectl wait kafka/bzkf-dizbox-cluster --for=condition=Ready --timeout=300s

kubectl get all -A
```

#### Install DIZ-in-a-box

<!-- x-release-please-start-version -->

```sh
helm upgrade --install --wait --timeout=10m --version=1.0.0 diz-in-a-box oci://ghcr.io/bzkf/diz-in-a-box/charts/diz-in-a-box

# test the installation
helm test diz-in-a-box

kubectl wait deployment/diz-in-a-box-stream-processors-onkoadt-to-fhir --for=condition=Available --timeout=300s
kubectl wait deployment/diz-in-a-box-stream-processors-fhir-to-server --for=condition=Available --timeout=300s
```

<!-- x-release-please-end -->

## TODOs

- <https://docs.k3s.io/security/hardening-guide>
- set ACL for KafkaUsers to relevant topics
