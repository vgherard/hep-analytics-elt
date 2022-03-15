import os

from sickle import Sickle
from requests.exceptions import HTTPError
from time import sleep

import pandas as pd
import sqlite3

from etl_runners.etl import ETL


OAI_BASE_URL = "http://export.arxiv.org/oai2"

# TODO: read path from env variable
SQLITE_DB = os.path.abspath("./db/raw.sqlite")

class ArxivETL(ETL):
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__()

    def _get_oai_iterator(self, resumptionToken = None):
        if resumptionToken:
            args = {'resumptionToken': resumptionToken}
        else:
            args = {
                'from': self.start_date
                , 'until': self.end_date
                , 'metadataPrefix': 'arXivRaw'
                # TODO: double-check this is actually filtering deleted records
                , 'ignore_deleted': True
                # TODO: choose set
                , 'set': 'physics:hep-ph'
            }
        it = self.oai_con.ListRecords(**args)
        return it

    def setup(self):
        self.oai_con = Sickle(OAI_BASE_URL)
        self.db_con = sqlite3.connect(SQLITE_DB)

    def extract(self):
        it = self._get_oai_iterator()
        self.data = []
        while True:
            try:
                self.data.append(it.next().get_metadata())
            except AttributeError:
                print("There was an AttributeError")
            except HTTPError:
                print('There was an HTTPError. Going to sleep for 30 seconds...')
                sleep(30)
                it = self._get_oai_iterator(resumptionToken = it.resumption_token.token)
            except StopIteration:
                break

    def transform(self):
        self.data = [
            {field: value[0] for field, value in el.items()} | {'updated_date': el['date'][-1]}
            for el in self.data
        ]
        self.data = pd.DataFrame(self.data)
        self.data = self.data[[
            "id"
            , "date"
            , "updated_date"
            , "submitter"
            , "authors"
            , "title"
            , "abstract"
            , "categories"
            , "comments"
            , "journal-ref"
            , "report-no"
            , "license"
            , "doi"
        ]]
        self.data = self.data.rename(columns = {'journal-ref': 'journal_ref', 'report-no': 'report_no'})

    def load(self):
        # TODO: Print number of rows inserted (return value of to_sql())
        self.data.to_sql(
            name = "arxiv_dump"
            , con = self.db_con
            , if_exists = "replace"
            , index = False
        )
        self.db_con.execute(
            """
            INSERT OR REPLACE INTO arxiv_raw
            SELECT * FROM arxiv_dump
            WHERE 1 = 1
            ON CONFLICT(id) DO UPDATE SET
                updated_date = excluded.updated_date
                , submitter = excluded.submitter
                , authors = excluded.authors
                , title = excluded.title
                , abstract = excluded.abstract
                , categories = excluded.categories
                , comments = excluded.comments
                , journal_ref = excluded.journal_ref
                , report_no = excluded.report_no
                , license = excluded.license
                , doi = excluded.doi
            """
          )
        self.db_con.execute("DROP TABLE arxiv_dump")
        self.db_con.commit()

    def cleanup(self):
        self.db_con.close()

