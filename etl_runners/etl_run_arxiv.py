from etl.arxiv_etl import ArxivETL
from datetime import date, timedelta


# TODO: read BigQuery credentials from env variable or from system configuration file

N_DAYS = 7

end_date = date.today()
start_date = end_date - timedelta(days = N_DAYS)

ArxivETL.run(start_date = start_date, end_date = end_date)