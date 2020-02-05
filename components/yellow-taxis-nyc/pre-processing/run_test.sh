#!/bin/bash

SECRET=$HOME/.secret/kubeflow-xyz-17f188982cf6.json

if [ -f "$SECRET" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS=$SECRET
fi

cd "$(dirname "$0")"

python ./src/pre_processing.py \
    --project "kubeflow-xyz" \
    --staging-bucket "gs://yellow-taxi-nyc/staging"
