import argparse

from modules import utils


# Defining and parsing the command-line arguments
parser = argparse.ArgumentParser(description='The pre-process step is responsible for applying the transformation rules.')
parser.add_argument('--project', type=str, help='Destination project of files. Billing project.')
# Output of extract step
parser.add_argument('--staging-bucket', type=str, help='Staging bucket that contains the files.')
args = parser.parse_args()

bucket = args.staging_bucket.split('/')[2]
prefix = ['/'.join(x for ind, x in enumerate(args.staging_bucket.split('/')[3:]))][0]

client = utils.get_client(args.project)
bucket = utils.get_bucket(client, bucket)

blobs = utils.list_blobs(client, bucket, prefix)
file_name = utils.download_blob(bucket, blobs[0])

utils.upload_blob(bucket, file_name, file_name.split('/')[-1])
