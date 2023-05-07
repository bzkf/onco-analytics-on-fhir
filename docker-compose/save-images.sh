#!/usr/bin/env bash
# shellcheck disable=all
# the above directive will have to be added manually everytime until
# <https://github.com/koalaman/shellcheck/issues/2411> is resolved or
# the "issues" are fixed upstream

# The save-images.sh script takes 1 input:
#  - DOCKER_IMAGE_NAMES
# Given that input, the docker images are downloaded, saved, and compressed into a single file.

# Enumerate docker images to be processed.

DOCKER_IMAGE_NAMES=(
  "docker.io/tchiotludo/akhq:0.24.0@sha256:6ccf8323ae6e93a893107f857cd9f7210add3569743b2c8528c6567967cc636f"
  "docker.io/library/postgres:15.1@sha256:10d6e725f9b2f5531617d93164f4fc85b1739e04cab62cbfbfb81ccd866513b8"
  "ghcr.io/miracum/fhir-gateway:v3.10.9@sha256:f4bae212ee8c137c522df7610b931d648ee7a5dd237f842432ae092fc3adbd0a"
  "ghcr.io/miracum/fhir-pseudonymizer:v2.17.0@sha256:431a7f4516c1f641966c507941b87a997441665656572a1c00c44376ef08fe24"
  "docker.io/hapiproject/hapi:v6.4.0@sha256:638b8df98fcfc074404520700923c07d313f62c5dcad49ca569cdb8041c7cc57"
  "docker.io/bitnami/kafka:3.4.0@sha256:f091afcc2a1db46f88dbc87839e7533adab2f3e8b95a0dbe0505a7bc8fe06262"
  "harbor.miracum.org/streams-ume/onkoadt-to-fhir:v1.10.0@sha256:e584922155db6e8c281a9b7fc3728701d0f08f0713ebd35c9931492a761c003a"
  "docker.io/library/traefik:v2.10.1@sha256:2a1c328e239fa9cb3891a98c4c1beee300bf467fc7add316632e82de24529b49"
)

# Make output variables.

MY_HOME=${MY_HOME:-~}
OUTPUT_DATE=$(date +%s)
OUTPUT_DATE_HUMAN=$(date --rfc-3339=seconds)
OUTPUT_FILE=${OUTPUT_FILE:-${MY_HOME}/docker-compose-air-gapper-${OUTPUT_DATE}.tgz}
OUTPUT_DIR_NAME=docker-compose-air-gapper-${OUTPUT_DATE}
OUTPUT_DIR=${MY_HOME}/${OUTPUT_DIR_NAME}
OUTPUT_IMAGES_DIR=${OUTPUT_DIR}/images
OUTPUT_LOAD_REPOSITORY_SCRIPT=${OUTPUT_DIR}/load-images.sh

# Make output directories.

mkdir ${OUTPUT_DIR}
mkdir ${OUTPUT_IMAGES_DIR}

# Define return codes.

OK=0
NOT_OK=1

# Create preamble to OUTPUT_LOAD_REPOSITORY_SCRIPT.

cat <<EOT >${OUTPUT_LOAD_REPOSITORY_SCRIPT}
#!/usr/bin/env bash

# 'load-images.sh' uses 'docker load' to import images into local registry.
# Created on ${OUTPUT_DATE_HUMAN}

EOT

chmod +x ${OUTPUT_LOAD_REPOSITORY_SCRIPT}

# Save Docker images and scripts to output directory.

for DOCKER_IMAGE_NAME in ${DOCKER_IMAGE_NAMES[@]}; do

  # Pull docker image.

  echo "Pulling ${DOCKER_IMAGE_NAME} from DockerHub."
  docker pull ${DOCKER_IMAGE_NAME}

  # Do a "docker save" to make a file from docker image.

  DOCKER_OUTPUT_FILENAME=$(echo ${DOCKER_IMAGE_NAME} | tr "/:" "--")-${OUTPUT_DATE}.tar
  echo "Creating ${OUTPUT_IMAGES_DIR}/${DOCKER_OUTPUT_FILENAME}"
  docker save ${DOCKER_IMAGE_NAME} --output ${OUTPUT_IMAGES_DIR}/${DOCKER_OUTPUT_FILENAME}

  # Add commands to OUTPUT_LOAD_REPOSITORY_SCRIPT to load file into local repository.

  echo "docker load --input images/${DOCKER_OUTPUT_FILENAME}" >>${OUTPUT_LOAD_REPOSITORY_SCRIPT}

done

# Compress results.

tar -zcvf ${OUTPUT_FILE} --directory ${MY_HOME} ${OUTPUT_DIR_NAME}

# Epilog.

echo "Done."
echo "    Output file: ${OUTPUT_FILE}"
echo "    Which is a compressed version of ${OUTPUT_DIR}"

exit ${OK}
