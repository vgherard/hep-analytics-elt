from sqlalchemy import create_engine

def bigquery_engine(project_id, credentials_path, dataset = 'main'):
    """
    Thin wrapper around sqlalchemy.create_engine().
    :param project_id: Google Cloud project name.
    :param credentials: path to Service Account credentials JSON.
    """
    uri = f'bigquery://{project_id}/{dataset}'
    eng = create_engine(uri, credentials_path = credentials_path)
    return eng

default_engine = bigquery_engine