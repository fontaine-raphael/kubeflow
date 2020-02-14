#!/bin/bash

SECRET=$HOME/.secret/kubeflow-xyz-17f188982cf6.json

if [ -f "$SECRET" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS=$SECRET
fi

cd "$(dirname "$0")"

python ./src/extract.py \
    --project "kubeflow-xyz" \
    --dataset "yellow_taxi" \
    --bucket "gs://yellow-taxi-nyc" \
    --start-date "2015-01-01" \
    --end-date "2015-01-05" \
    --staging-bucket "output/staging_bucket"
