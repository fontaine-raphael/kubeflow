#!/bin/bash

cd "$(dirname "$0")"

python ./src/pre_processing.py \
    --project "kubeflow-2020" \
    --staging-bucket "gs://kubeflow-2020-nyc/staging"
