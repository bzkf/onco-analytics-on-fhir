#!/bin/bash
set -euox pipefail

if [ -z "$IMAGE_FOLDER" ]; then
  IMAGE_FOLDER="../images/"
fi

# Check if folder exists
if [ ! -d "$IMAGE_FOLDER" ]; then
  echo "Error: $IMAGE_FOLDER does not exist."
  exit 1
fi

# Iterate over all tar files in the folder
for file in "$IMAGE_FOLDER"/*.tar; do
  echo "Importing image from $file"
  k3s ctr images import "$file" --digests=true
done
