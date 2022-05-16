from sqlalchemy import create_engine
from yaml import safe_load as load_yaml
from os.path import expanduser

def postgres_engine():
    """
    Thin wrapper around sqlalchemy.create_engine().
    """

    db_conn_path = expanduser("~/.hepanalytics/db_connection.yml")
    db_conn = load_yaml(open(db_conn_path))
    uri = "postgresql+psycopg2://" +\
          db_conn["user"] + ":" + db_conn["password"] +\
          "@" + db_conn["host"] + ":" + str(db_conn["port"]) + "/" + db_conn["dbname"]
    eng = create_engine(uri)
    return eng

default_engine = postgres_engine