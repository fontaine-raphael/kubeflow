import gcsfs
import logging
import pandas as pd

from pathlib import Path

def entrypoint(project, bucket, blob):
    """Orchestration for the workflow of the transformation steps."""
    df = get_dataframe(project, bucket, blob)
    
    df = pipeline(df)

    source_file_name = save_dataframe(df, blob)

    return source_file_name


def get_dataframe(project, bucket, blob):
    """Download the blob to a pandas dataframe."""
    try:
        logging.info(f'Creating the pandas dataframe for the blob {blob}')

        fs = gcsfs.GCSFileSystem(project=project)
        file = bucket + '/' + blob

        with fs.open(file) as f:
            df = pd.read_csv(f)

    except:
        logging.fatal(f'Error when try to create the dataframe')
        raise

    return df


def pipeline(df):
    """Transformation Pipeline."""
    print('-'*41 + ' Pipeline-Start ' + '-'*41)

    try:
        logging.info('[1] - Trip distance greater than 0...')
        df = df[df['trip_distance'] > 0]

        logging.info('[2] - Fare amount between $6 and $200...')
        df = df[(df['fare_amount'] >= 6) & (df['fare_amount'] <= 200)]

        logging.info('[3] - Pickup Longitude > -75 and < -73...')
        df = df[(df['pickup_longitude'] > -75) & (df['pickup_longitude'] < -73)]

        logging.info('[4] - Dropoof Longitude > -75 and < -73...')
        df = df[(df['dropoff_longitude'] > -75) & (df['dropoff_longitude'] < -73)]

        logging.info('[5] - Pickup Latitude > 40 and < 42...')
        df = df[(df['pickup_latitude'] > 40) & (df['pickup_latitude'] < 42)]

        logging.info('[6] - Dropoof Latitude > 40 and < 42...')
        df = df[(df['dropoff_latitude'] > 40) & (df['dropoff_latitude'] < 42)]
    except:
        logging.fatal('Error when trying to run the transformation processes')
        raise

    print('-'*42 + ' Pipeline-End ' + '-'*42)

    return df


def save_dataframe(df, blob):
    """Saves the dataframe to a temporary local file."""
    tmp_folder = 'tmp'
    file_name = blob.split('/')[-1]

    Path(tmp_folder).mkdir(parents=True, exist_ok=True)

    full_file_name = './' + tmp_folder + '/' + file_name

    try:
        logging.info(f'Saving the dataframe to the file {full_file_name}')
        df.to_csv(full_file_name)

        del df
    except:
        logging.critical(f'Error when trying to save the file {full_file_name}')
        raise

    return full_file_name
