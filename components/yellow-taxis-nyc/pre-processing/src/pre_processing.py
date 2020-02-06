import argparse
import logging

from modules import transformations
from modules import utils


# Defining and parsing the command-line arguments
parser = argparse.ArgumentParser(description='The pre-process step is responsible for applying the transformation rules.')
parser.add_argument('--project', type=str, help='Destination project of files. Billing project.')
# Input from extract step
parser.add_argument('--staging-bucket', type=str, help='Staging bucket that contains the files.')
args = parser.parse_args()

# Configuring logging
log = logging.getLogger()  # 'root' Looger

console = logging.StreamHandler()

format_str = '%(asctime)s\t%(levelname)s -- %(filename)s %(funcName)s:%(lineno)s -- %(message)s'
logging.Formatter.converter = utils.custom_tmz

console.setFormatter(logging.Formatter(format_str))

log.addHandler(console)  # Prints to console
log.setLevel(logging.INFO)  # Anything INFO or above


def main():
    """The pre-process step is responsible for applying the transformation rules."""
    bucket = args.staging_bucket.split('/')[2]
    prefix = ['/'.join(x for ind, x in enumerate(args.staging_bucket.split('/')[3:]))][0]

    client = utils.get_client(args.project)
    bucket = utils.get_bucket(client, bucket)

    blobs = utils.list_blobs(client, bucket, prefix)

    # Core transformation program transformation
    for blob in blobs:
        transformations.entrypoint(args.project, bucket.id, blob)


if __name__ == '__main__':
    print('-'*42 + ' Start of work ' + '-'*42)
    main()
    print('-'*43 + ' End of work ' + '-'*43)
