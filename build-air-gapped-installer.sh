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

AIRGAPPED_DIST_DIR=${AIRGAPPED_DIST_DIR:-"./dist/airgapped"}

mkdir -p "$AIRGAPPED_DIST_DIR"

curl -L -o "$AIRGAPPED_DIST_DIR/k3s-airgap-images-amd64.tar" "https://github.com/k3s-io/k3s/releases/download/v1.26.3%2Bk3s1/k3s-airgap-images-amd64.tar"
curl -L -o "$AIRGAPPED_DIST_DIR/k3s" "https://github.com/k3s-io/k3s/releases/download/v1.26.3%2Bk3s1/k3s"
curl -L -o "$AIRGAPPED_DIST_DIR/install.sh" "https://get.k3s.io/"

images=("quay.io/strimzi/kafka-bridge:0.25.0" "quay.io/strimzi/kafka:0.34.0-kafka-3.4.0")

prereq_images_string=$(helm template charts/prerequisites/ | yq -N '..|.image? | select(.)' | sort -u)
diz_in_a_box_images_string=$(helm template charts/diz-in-a-box/ | yq -N '..|.image? | select(.)' | sort -u)

readarray -t prereq_images <<<"$prereq_images_string"
readarray -t diz_in_a_box_images <<<"$diz_in_a_box_images_string"

images+=("${prereq_images[@]}")
images+=("${diz_in_a_box_images[@]}")

for image in "${diz_in_a_box_images[@]}"; do
    image_slug=$(echo $image | slugify)
    file_name="$image_slug.tar"

    echo "Saving image $image as $AIRGAPPED_DIST_DIR/$file_name"
    crane pull "$image" "$AIRGAPPED_DIST_DIR/$file_name"
done
