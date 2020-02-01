from google.cloud import storage


def get_client(project):
    client = storage.Client(
        project=project
    )

    return client


def list_blobs(client, bucket):
    bucket = bucket.split('/')[2]
    prefix = ['/'.join(x for ind, x in enumerate(bucket.split('/')[3:]))][0]

    blobs = client.list_blobs(bucket, prefix=prefix)
    blob_name = []

    for blob in blobs:
        blob_name.append(blob.name)

    return blob_name
