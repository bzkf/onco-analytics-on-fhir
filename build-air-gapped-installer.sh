#!/bin/bash
set -euox pipefail

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

images=("quay.io/strimzi/kafka-bridge:0.25.0" "quay.io/strimzi/kafka:0.34.0-kafka-3.4.0")
images+=("${prereq_images[@]}")
images+=("${diz_in_a_box_images[@]}")

for image in "${images[@]}"; do
  image_slug=$(echo "$image" | slugify)
  file_name="$image_slug.tar"

  echo "Saving image $image as $AIR_GAPPED_INSTALL_DIR/images/$file_name"
  docker pull "$image"
  docker save "$image" -o "$AIR_GAPPED_INSTALL_DIR/images/$file_name"
done

cp ./import-images-into-k3s.sh "$AIR_GAPPED_INSTALL_DIR/bin/import-images-into-k3s.sh"

MY_HOME="${AIR_GAPPED_INSTALL_DIR}" ./docker-compose/save-images.sh

cp -r ./docker-compose/ "$AIR_GAPPED_INSTALL_DIR"

tar -zcvf air-gapped-installer.tgz "$AIR_GAPPED_INSTALL_DIR"
