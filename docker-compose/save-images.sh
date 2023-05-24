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
  "docker.io/tchiotludo/akhq:0.24.0"
  "docker.io/library/postgres:15.3"
  "ghcr.io/miracum/fhir-gateway:v3.10.9"
  "ghcr.io/miracum/fhir-pseudonymizer:v2.17.0"
  "docker.io/hapiproject/hapi:v6.4.4"
  "ghcr.io/miracum/kafka-fhir-to-server:v1.2.3"
  "docker.io/bitnami/kafka:3.4.0"
  "docker.io/cricketeerone/apache-kafka-connect:3.4.0"
  "ghcr.io/miracum/onkoadt-to-fhir:v1.10.5"
  "docker.io/library/traefik:v2.10.1"
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

cat <<EOT > ${OUTPUT_LOAD_REPOSITORY_SCRIPT}
#!/usr/bin/env bash

# 'load-images.sh' uses 'docker load' to import images into local registry.
# Created on ${OUTPUT_DATE_HUMAN}

EOT

chmod +x ${OUTPUT_LOAD_REPOSITORY_SCRIPT}

# Save Docker images and scripts to output directory.

for DOCKER_IMAGE_NAME in ${DOCKER_IMAGE_NAMES[@]};
do

  # Pull docker image.

  echo "Pulling ${DOCKER_IMAGE_NAME} from DockerHub."
  docker pull ${DOCKER_IMAGE_NAME}

  # Do a "docker save" to make a file from docker image.

  DOCKER_OUTPUT_FILENAME=$(echo ${DOCKER_IMAGE_NAME} | tr "/:" "--")-${OUTPUT_DATE}.tar
  echo "Creating ${OUTPUT_IMAGES_DIR}/${DOCKER_OUTPUT_FILENAME}"
  docker save ${DOCKER_IMAGE_NAME} --output ${OUTPUT_IMAGES_DIR}/${DOCKER_OUTPUT_FILENAME}

  # Add commands to OUTPUT_LOAD_REPOSITORY_SCRIPT to load file into local repository.

  echo "docker load --input images/${DOCKER_OUTPUT_FILENAME}" >> ${OUTPUT_LOAD_REPOSITORY_SCRIPT}

done

# Compress results.

tar -zcvf ${OUTPUT_FILE} --directory ${MY_HOME} ${OUTPUT_DIR_NAME}

# Epilog.

echo "Done."
echo "    Output file: ${OUTPUT_FILE}"
echo "    Which is a compressed version of ${OUTPUT_DIR}"

exit ${OK}
