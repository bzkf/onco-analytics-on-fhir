#!/bin/bash
set -euox pipefail

SHOULD_CREATE_K3S_AIR_GAPPED_INSTALLER=${SHOULD_CREATE_K3S_AIR_GAPPED_INSTALLER:-"0"}

if [ "$SHOULD_CREATE_K3S_AIR_GAPPED_INSTALLER" = "1" ]; then
  # via <https://duncanlock.net/blog/2021/06/15/good-simple-bash-slugify-function/>
  function slugify() {
    iconv -t ascii//TRANSLIT |
      tr -d "'" |
      sed -E 's/[^a-zA-Z0-9]+/-/g' |
      sed -E 's/^-+|-+$//g' |
      tr "[:upper:]" "[:lower:]"
  }

  AIR_GAPPED_INSTALL_DIR=${AIR_GAPPED_INSTALL_DIR:-"./dist/air-gapped"}

  mkdir -p "$AIR_GAPPED_INSTALL_DIR"
  mkdir -p "$AIR_GAPPED_INSTALL_DIR/bin"
  mkdir -p "$AIR_GAPPED_INSTALL_DIR/k3s"
  mkdir -p "$AIR_GAPPED_INSTALL_DIR/images"

  curl -L "https://get.helm.sh/helm-v3.11.3-linux-amd64.tar.gz" | tar xz
  mv linux-amd64/helm "$AIR_GAPPED_INSTALL_DIR/bin/helm"

  curl -L -o "$AIR_GAPPED_INSTALL_DIR/bin/kubectl" "https://storage.googleapis.com/kubernetes-release/release/v1.26.0/bin/linux/amd64/kubectl"
  curl -L -o "$AIR_GAPPED_INSTALL_DIR/k3s/k3s-airgap-images-amd64.tar" "https://github.com/k3s-io/k3s/releases/download/v1.26.3%2Bk3s1/k3s-airgap-images-amd64.tar"
  curl -L -o "$AIR_GAPPED_INSTALL_DIR/bin/k3s" "https://github.com/k3s-io/k3s/releases/download/v1.26.3%2Bk3s1/k3s"
  curl -L -o "$AIR_GAPPED_INSTALL_DIR/bin/install.sh" "https://get.k3s.io/"

  helm repo add miracum https://miracum.github.io/charts
  helm repo add akhq https://akhq.io/
  helm repo add hapi-fhir-jpaserver-starter https://hapifhir.github.io/hapi-fhir-jpaserver-starter
  helm repo add strimzi https://strimzi.io/charts/

  helm dependency build charts/prerequisites/
  helm dependency build charts/diz-in-a-box/

  prereq_images_string=$(helm template charts/prerequisites/ | yq -N '..|.image? | select(.)' | sort -u)
  diz_in_a_box_images_string=$(helm template charts/diz-in-a-box/ | yq -N '..|.image? | select(.)' | sort -u)

  readarray -t prereq_images <<<"$prereq_images_string"
  readarray -t diz_in_a_box_images <<<"$diz_in_a_box_images_string"

  for image in "${diz_in_a_box_images[@]}"; do
    image_slug=$(echo "$image" | slugify)
    file_name="$image_slug.tar"

    echo "Saving image $image as $AIR_GAPPED_INSTALL_DIR/images/$file_name"
    docker pull "$image"
    docker save "$image" -o "$AIR_GAPPED_INSTALL_DIR/images/$file_name"
  done

  cp ./import-images-into-k3s.sh "$AIR_GAPPED_INSTALL_DIR/bin/import-images-into-k3s.sh"
  tar -zcvf air-gapped-installer.tgz "$AIR_GAPPED_INSTALL_DIR"

  AIR_GAPPED_PREREQUISITES_INSTALL_DIR=${AIR_GAPPED_PREREQUISITES_INSTALL_DIR:-"./dist/air-gapped-prerequisites"}
  mkdir -p "$AIR_GAPPED_PREREQUISITES_INSTALL_DIR"
  mkdir -p "$AIR_GAPPED_PREREQUISITES_INSTALL_DIR/images"
  mkdir -p "$AIR_GAPPED_PREREQUISITES_INSTALL_DIR/bin"

  images=("quay.io/strimzi/kafka-bridge:0.25.0@sha256:98eb7542e3ff4e10043040acff7a90aa5fd87ff1ea0cac8491f66b1bbdf072dd" "quay.io/strimzi/kafka:0.34.0-kafka-3.4.0@sha256:d87417992eb9118d2395c9950c445f51c4d70f9903fd5eebd4eb7570310e27f9")
  images+=("${prereq_images[@]}")

  for image in "${images[@]}"; do
    image_slug=$(echo "$image" | slugify)
    file_name="$image_slug.tar"

    echo "Saving image $image as $AIR_GAPPED_PREREQUISITES_INSTALL_DIR/images/$file_name"
    docker pull "$image"
    docker save "$image" -o "$AIR_GAPPED_PREREQUISITES_INSTALL_DIR/images/$file_name"
  done

  cp ./import-images-into-k3s.sh "$AIR_GAPPED_PREREQUISITES_INSTALL_DIR/bin/import-images-into-k3s.sh"
  tar -zcvf air-gapped-prerequisites-installer.tgz "$AIR_GAPPED_PREREQUISITES_INSTALL_DIR"
fi

# compose-based air-gapped installer

AIR_GAPPED_COMPOSE_INSTALL_DIR=${AIR_GAPPED_COMPOSE_INSTALL_DIR:-"./dist/compose-air-gapped"}
mkdir -p "$AIR_GAPPED_COMPOSE_INSTALL_DIR"

# generate save-images.sh
docker compose --profile=kafka-connect \
  -f docker-compose/compose.full.yaml \
  -f docker-compose/compose.onkoadt-to-fhir.yaml \
  -f docker-compose/compose.decompose-xmls.yaml \
  config -o docker-compose/compose.normalized.yaml

docker run \
  --env SENZING_DOCKER_COMPOSE_FILE=/data/compose.normalized.yaml \
  --env SENZING_OUTPUT_FILE=/data/save-images.sh \
  --env SENZING_SUBCOMMAND=create-save-images \
  --rm \
  --volume "${PWD}/docker-compose:/data" \
  --user "${UID}" \
  docker.io/senzing/docker-compose-air-gapper:1.0.4@sha256:f519089580c5422c02100042965f14ac2bb7bab5c3321e8a668b4f4b6b03902a

chmod +x ./docker-compose/save-images.sh

# finally run the generated script to download the images
MY_HOME="${AIR_GAPPED_COMPOSE_INSTALL_DIR}" ./docker-compose/save-images.sh

find "$AIR_GAPPED_COMPOSE_INSTALL_DIR" -name "docker-compose-air-gapper-*.tgz" -exec mv '{}' "./docker-compose/" \;

tar -zcvf compose-air-gapped-installer.tgz "./docker-compose"
