import logging
import os

from datetime import datetime
from google.cloud import storage
from pytz import timezone, utc


def get_client(project):
    """Establishes the connection for using the Storage API."""
    try:
        logging.info(f'Connecting to the Storage API through the {project} Project')

        client = storage.Client(
            project=project
        )
    except:
        logging.fatal(f'Error when trying to connect the Storage API to Project {project}')
        raise

    return client


def get_bucket(client, bucket):
    """Retrieve the bucket reference."""
    try:
        logging.info(f'Getting the bucket gs://{bucket}')

        bucket = client.get_bucket(bucket)
    except:
        logging.fatal(f'Error when trying to retrieve the {bucket} bucket')
        raise

    return bucket


def list_blobs(client, bucket, prefix):
    """Returns a list containing all the blobs in the bucket."""
    try:
        logging.info(f'Listing blob(s) in the bucket gs://{bucket.name}')

        blobs = client.list_blobs(bucket, prefix=prefix)

        blob_name = []

        for blob in blobs:
            blob_name.append(blob.name)
            print(f' - {blob.name}')
    except:
        logging.fatal(f'Error when trying to list blobs in bucket {bucket.name}/{prefix}')
        raise

    return blob_name


def download_blob(bucket, blob_name):
    """Download a blob to a local file."""
    blob = bucket.blob(blob_name)
    file_name = blob_name.split('/')[-1]

    try:
        logging.info(f'Downloading blob {blob_name}...')

        blob.download_to_filename(file_name)
    except:
        logging.fatal(f'Error when trying to download the following blob: {blob_name}')
        raise
    
    file_name = './' + file_name

    return file_name


def upload_blob(bucket, source_file_name, destination_blob_name):
    """Load the blob into the bucket after applying the transformations."""
    prefix = 'pre-processing'
    destination_blob_name = prefix + '/' + destination_blob_name

    blob = bucket.blob(destination_blob_name)

    try:
        logging.info(f'Uploading blob {blob.name}...')
        blob.upload_from_filename(source_file_name)
    except:
        logging.fatal(f'Error when trying to upload the blob {blob.name} to the bucket {bucket.name}')
        raise

    os.remove(source_file_name)  # Deletes the file after upload


def custom_tmz(*args):
    """Return a custom time zone."""
    utc_dt = utc.localize(datetime.utcnow())
    my_tz = timezone("America/Sao_Paulo")
    converted = utc_dt.astimezone(my_tz)

    return converted.timetuple()
