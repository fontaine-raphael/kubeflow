import logging
import re

from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from pytz import timezone, utc


def get_client(project):
    """Establishes the connection for using the BigQuery API."""
    try:
        logging.info(f'Connecting to the BigQuery API through the {project} Project')

        client = bigquery.Client(
            project=project
        )
    except:
        logging.fatal(f'Error when trying to connect the BigQuery API to Project {project}')
        raise

    return client


def extract_from_query(client, table_ref, query_string, query_job_config, bucket, prefix, file_name, delete_after=False):
    """Run a custom query and then export the result to a bucket."""
    try:
        logging.info(f'Running the query with target table: {table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}')
        print(query_string)

        query_job = client.query(
            query_string,
            job_config=query_job_config
        )

        query_result = query_job.result()  # Waiting for the end of query job

        mib = (query_job.total_bytes_billed / 2**20)
        gib = (query_job.total_bytes_billed / 2**30)

        logging.info('Query finished')
        logging.info(f'Total lines reached {query_result.total_rows}')
        logging.info(f'Total bytes billed: {query_job.total_bytes_billed} (MiB: {mib:.3f}) (GiB: {gib:.3f})')
    except:
        logging.fatal('Error when trying to run the query')
        raise

    # Calling the function that actually extracts the table
    extract_table(client, table_ref, bucket, prefix, file_name, delete_after)


def extract_table(client, table_ref, bucket, prefix, file_name, delete_after=False):
    """Clean the target bucket and later export the contents of the table."""
    full_file_name = '/' + prefix + '/' + file_name
    destination_uri = bucket + full_file_name

    # Calling the function that clears past blobs from the bucket
    clean_bucket(client.project, re.search('(?![gs://]).*', bucket).group(), prefix)

    try:
        logging.info(f'Starting the content extraction of table {table_ref.table_id} to {bucket}/{prefix}')

        extract_job = client.extract_table(
            source=table_ref,
            destination_uris=destination_uri
        )

        logging.info(f'{extract_job.destination_uri_file_counts} chunk file(s) to extract')

        extract_job.result()  # Waiting for the end of extract job
    except:
        logging.fatal('Error when trying to extract')
        raise

    # By default, the target table is not deleted after exporting the content
    if delete_after:
        delete_table(client, table_ref)


def clean_bucket(project, bucket, prefix):
    """Clean the target bucket."""
    try:
        client = storage.Client(project=project)

        blobs = client.list_blobs(bucket, prefix=prefix)
        list_blobs = list(blobs)
    except:
        logging.fatal('Error when trying to list blobs in bucket {bucket}')
        raise

    # If the bucket has any blob, it will be deleted
    if list_blobs:
        bucket = client.get_bucket(bucket)
        bucket.delete_blobs(list_blobs)


def delete_table(client, table_ref):
    """Delete target table."""
    try:
        logging.info(f'Deleting the table {table_ref.table_id}')

        client.delete_table(
            table_ref
        )
    except:
        logging.fatal('Error when trying to delete the table {table_ref.path}')
        raise


def custom_tmz(*args):
    """Return a custom time zone."""
    utc_dt = utc.localize(datetime.utcnow())
    my_tz = timezone("America/Sao_Paulo")
    converted = utc_dt.astimezone(my_tz)

    return converted.timetuple()
