import re

from google.cloud import bigquery
from google.cloud import storage


def get_client(project):
    client = bigquery.Client(
        project=project
    )

    return client


def extract_from_query(client, table_ref, query_string, query_job_config, bucket, prefix, file_name):
    query_job = client.query(
        query_string,
        job_config=query_job_config
    )

    query_job.result()

    extract_table(client, table_ref, bucket, prefix, file_name, delete=True)


def extract_table(client, table_ref, bucket, prefix, file_name, delete=False):
    full_file_name = '/' + prefix + '/' + file_name
    destination_uri = bucket + full_file_name

    clean_bucket(client.project, re.search('(?![gs://]).*', bucket).group(), prefix)

    extract_job = client.extract_table(
        source=table_ref,
        destination_uris=destination_uri
    )

    extract_job.result()

    if delete:
        delete_table(client, table_ref)


def clean_bucket(project, bucket, prefix):
    client = storage.Client(project=project)

    blobs = client.list_blobs(bucket, prefix=prefix)
    list_blobs = list(blobs)

    if list_blobs:
        bucket = client.get_bucket(bucket)
        bucket.delete_blobs(list_blobs)


def delete_table(client, table_ref):
    client.delete_table(
        table_ref
    )
