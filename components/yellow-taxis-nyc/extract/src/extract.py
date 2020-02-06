import argparse
import logging

from google.cloud import bigquery
from modules import utils
from pathlib import Path


# Defining and parsing the command-line arguments
parser = argparse.ArgumentParser(description='Saves the result of a query to a table and later exports it to a bucket.')
parser.add_argument('--project', type=str, help='Destination project of table and files exported. Billing project.')
parser.add_argument('--dataset', type=str, default=100, help='Dataset ouput.')
parser.add_argument('--bucket', type=str, help='Desired bucket for exported files.')
parser.add_argument('--start-date', type=str, help='Start date for boarding taxi rides.')
parser.add_argument('--end-date', type=str, help='End date for boarding taxi rides.')
# Output
parser.add_argument('--staging-bucket', type=str, help='Staging bucket.')
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
    """Saves the result of a query to a table and later exports it to a bucket."""
    client = utils.get_client(args.project)

    dataset_ref = client.dataset(args.dataset)
    table_ref = dataset_ref.table('trips_{}_{}'.format(args.start_date, args.end_date).replace('-', ''))

    query_string = """
        SELECT
            pickup_datetime, dropoff_datetime,
            pickup_longitude, pickup_latitude,
            dropoff_longitude, dropoff_latitude,
            passenger_count, trip_distance, fare_amount, mta_tax, tip_amount, tolls_amount, total_amount
        FROM
            --`nyc-tlc.yellow.january_trips`
            `kubeflow-xyz.yellow_taxi.january_trips`
        WHERE 
            pickup_datetime BETWEEN @start_date AND @end_date
        AND dropoff_datetime BETWEEN @start_date AND @end_date
    """

    query_job_config = bigquery.QueryJobConfig(
        destination=table_ref,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        query_parameters=[
            bigquery.ScalarQueryParameter(name='start_date', type_='STRING', value=args.start_date),
            bigquery.ScalarQueryParameter(name='end_date', type_='STRING', value=args.end_date)
        ]
    )

    prefix = 'staging'
    file_name = 'trips-*.csv'

    utils.extract_from_query(client, table_ref, query_string, query_job_config, args.bucket, prefix, file_name, delete_after=False)

    # Component Output(s)
    staging_bucket = args.bucket + '/' + prefix

    Path(args.staging_bucket).parent.mkdir(parents=True, exist_ok=True)
    Path(args.staging_bucket).write_text(staging_bucket)

    with open(args.staging_bucket, 'r') as f:
        content = str(f.read())
        
        print('-'*45 + ' Outputs ' + '-'*45)
        print(f' - staging-bucket: {content}')


if __name__ == '__main__':
    print('-'*42 + ' Start of work ' + '-'*42)
    main()
    print('-'*43 + ' End of work ' + '-'*43)
