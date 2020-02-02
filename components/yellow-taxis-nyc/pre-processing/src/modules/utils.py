# import gcsfs
import os

from google.cloud import storage


def get_client(project):
    """Establishes the connection for using the Storage API."""
    try:
        client = storage.Client(
            project=project
        )
    except Exception as e:
        print('Error when trying to connect to the project: %s\n\n%s' % (project, type(e)))

    return client


def get_bucket(client, bucket):
    """Retrieve the bucket reference."""
    try:
        bucket = client.get_bucket(bucket)
    except Exception as e:  # google.cloud.exceptions.NotFound
        print('Error when trying to retrieve the following bucket %s\n%s' % (bucket.name, type(e)))

    return bucket


def list_blobs(client, bucket, prefix):
    """Returns a list containing all the blobs in the bucket."""
    try:
        blobs = client.list_blobs(bucket, prefix=prefix)
    except Exception as e:
        print('Error when trying to list the blobs in the following bucket %s\n%s' % (bucket.name, type(e)))

    blob_name = []

    for blob in blobs:
        blob_name.append(blob.name)

    return blob_name


def download_blob(bucket, blob_name):
    """Download a blob to a local file."""
    blob = bucket.blob(blob_name)
    file_name = blob_name.split('/')[-1]

    try:
        blob.download_to_filename(file_name)
    except Exception as e:
        print('Error when trying to download the following blob %s\n%s' % (blob.name, type(e)))
    
    file_name = './' + file_name

    return file_name


def upload_blob(bucket, source_file_name, destination_blob_name):
    """Load the blob into the bucket after applying the transformations."""
    prefix = 'pre-processing'
    destination_blob_name = prefix + '/' + destination_blob_name

    blob = bucket.blob(destination_blob_name)

    try:
        blob.upload_from_filename(source_file_name)
    except Exception as e:
        print('Error when trying to upload the following blob %s to the following bucket %s\n%s' % (blob, bucket.name, type(e)))

    os.remove(source_file_name)  # Deletes the file after upload
