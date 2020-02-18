import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
import kfp.gcp as gcp
import os


extract_path = '/Users/fontaine/projects/kubeflow/components/yellow-taxis-nyc/extract'
preprocessing_path = '/Users/fontaine/projects/kubeflow/components/yellow-taxis-nyc/pre-processing'

extract_op = comp.load_component_from_file(os.path.join(extract_path, 'component.yaml'))
preprocessing_op = comp.load_component_from_file(os.path.join(preprocessing_path, 'component.yaml'))

@dsl.pipeline(name='NYC Yellow Taxi Fare Predict', description='Pipeline to predict the fare amount of NYC Yellow Cab.')
def nyc_taxi_pipeline(
    project='kubeflow-xyz',
    dataset='yellow_taxi',
    bucket='gs://yellow-taxi-nyc',
    start_date='2015-01-01',
    end_date='2015-01-05'
):
    extract = extract_op(
        project=project,
        dataset=dataset,
        bucket=bucket,
        start_date=start_date,
        end_date=end_date
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))

    preprocessing = preprocessing_op(
        project=project,
        staging_bucket=extract.outputs['staging_bucket']
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))

# Compile
pipeline_func = nyc_taxi_pipeline
pipeline_filename = pipeline_func.__name__ + ".tar.gz"
compiler.Compiler().compile(pipeline_func, pipeline_filename)
print(pipeline_filename)
