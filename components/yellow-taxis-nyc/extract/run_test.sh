#!/bin/bash

cd "$(dirname "$0")"

python ./src/extract.py \
    --project "kubeflow-2020" \
    --dataset "kubeflow" \
    --bucket "gs://kubeflow-2020-nyc" \
    --start-date "2015-01-01" \
    --end-date "2015-01-01" \
    --staging-bucket "output"
