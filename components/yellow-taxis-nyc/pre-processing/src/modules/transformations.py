import gcsfs
import logging
import pandas as pd


def entrypoint(project, bucket, blob):
    """Orchestration for the workflow of the transformation steps."""
    df = get_dataframe(project, bucket, blob)
    print(df.describe())
    del df


def get_dataframe(project, bucket, blob):
    """Download the blob to a pandas dataframe."""
    try:
        logging.info(f'Creating the pandas dataframe for the blob {blob}')

        fs = gcsfs.GCSFileSystem(project=project)
        file = bucket + '/' + blob

        with fs.open(file) as f:
            df = pd.read_csv(f, nrows=100)

    except:
        logging.fatal(f'Error when try to create the dataframe')
        raise

    return df
