from etl.arxiv_etl import ArxivETL
from datetime import date, timedelta

N_DAYS = 7

end_date = date.today()
start_date = end_date - timedelta(days = N_DAYS)

ArxivETL.run(start_date = start_date, end_date = end_date)