import argparse

from models import utils


# Defining and parsing the command-line arguments
parser = argparse.ArgumentParser(description='Preprocess step.')
parser.add_argument('--project', type=str, help='Destination project of files. Billing project.')
parser.add_argument('--staging-bucket', type=str, help='Staging bucket that contains the files.')
args = parser.parse_args()

client = utils.get_client(args.project)

blobs = utils.list_blobs(client, args.staging_bucket)

for blob in blobs:
    print(blob)
