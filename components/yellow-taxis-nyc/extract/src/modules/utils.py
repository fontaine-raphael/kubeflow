import re

from google.cloud import bigquery
from google.cloud import storage


def get_client(project):
    """Establishes the connection for using the BigQuery API."""
    try:
        client = bigquery.Client(
            project=project
        )
    except Exception as e:  # google.auth.exceptions.DefaultCredentialsError
        print('Error when trying to connect to the project: %s\n\n%s' % (project, type(e)))

    return client


def extract_from_query(client, table_ref, query_string, query_job_config, bucket, prefix, file_name, delete_after=False):
    """Run a custom query and then export the result to a bucket."""
    try:
        query_job = client.query(
            query_string,
            job_config=query_job_config
        )
    except Exception as e:
        print('Error when trying to run a query\n%s' % type(e))

    query_job.result()  # Waiting for the end of query

    # Calling the function that actually extracts the table
    extract_table(client, table_ref, bucket, prefix, file_name, delete_after)


def extract_table(client, table_ref, bucket, prefix, file_name, delete_after=False):
    """Clean the target bucket and later export the contents of the table."""
    full_file_name = '/' + prefix + '/' + file_name
    destination_uri = bucket + full_file_name

    # Calling the function that clears past blobs from the bucket
    clean_bucket(client.project, re.search('(?![gs://]).*', bucket).group(), prefix)

    try:
        extract_job = client.extract_table(
            source=table_ref,
            destination_uris=destination_uri
        )
    except Exception as e:
        print('Error when trying to extract the following table %s\n%s' % (table_ref.path, type(e)))

    extract_job.result()  # Waiting for the end of extract job

    # By default, the target table is not deleted after exporting the content
    if delete_after:
        delete_table(client, table_ref)


def clean_bucket(project, bucket, prefix):
    """Clean the target bucket."""
    client = storage.Client(project=project)

    blobs = client.list_blobs(bucket, prefix=prefix)
    list_blobs = list(blobs)

    # If the bucket has any blob, it will be deleted
    if list_blobs:
        bucket = client.get_bucket(bucket)
        bucket.delete_blobs(list_blobs)


def delete_table(client, table_ref):
    """Delete target table."""
    try:
        client.delete_table(
            table_ref
        )
    except Exception as e:
        print('Error when trying to delete the following table %s\n%s' % (table_ref.path, type(e)))
