#!/bin/bash

cd "$(dirname "$0")"

while getopts ":p:i:t:" opt; do
  case $opt in
    p) PROJECT_ID="$OPTARG"
    ;;
    i) IMAGE_NAME="$OPTARG"
    ;;
    t) TAG_NAME="$OPTARG"
    ;;
  esac
done

if [ -z "$PROJECT_ID" ]; then
	PROJECT_ID=$(gcloud config config-helper --format "value(configuration.properties.core.project)")
fi

if [ -z "$IMAGE_NAME" ]; then
	pwd=$(pwd)
	IMAGE_NAME=${pwd##*/}  # Default image name is the component folder name
fi

if [ -z "$TAG_NAME" ]; then
	TAG_NAME="latest"  # Default
fi

full_image_name=gcr.io/${PROJECT_ID}/${IMAGE_NAME}:${TAG_NAME}

printf "PROJECT: %s\n" "$PROJECT_ID"
printf "IMAGE: %s\n" "$IMAGE_NAME"
printf "TAG: %s\n\n" "$TAG_NAME"

docker build -t "${full_image_name}" .
docker push "$full_image_name"

echo $full_image_name
