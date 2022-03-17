import os
from time import sleep

from requests import get as http_get
from feedparser import parse as parse_feed


import pandas as pd
import sqlite3

from .etl import ETL


ARXIV_API_ENDPOINT = "http://export.arxiv.org/api/query"

# TODO: read path from env variable
SQLITE_DB = os.path.abspath("./db/raw.sqlite")

class ArxivETL(ETL):
    def __init__(self, start_date, end_date):
        self.start_date = start_date.strftime('%Y%m%d')
        self.end_date = end_date.strftime('%Y%m%d')
        super().__init__()

    def _arxiv_api_request(self):
        search_query = 'cat:hep-ph' + " AND submittedDate:[" + self.start_date + " TO " + self.end_date + "]"
        return http_get(
            ARXIV_API_ENDPOINT
            , params = {'search_query': search_query, 'max_results': 1000}
            )

    def setup(self):
        self.db_con = sqlite3.connect(SQLITE_DB)

    def extract(self):
        response = self._arxiv_api_request()
        feed = parse_feed(response.content)
        self.data = pd.DataFrame(feed.entries)

    def transform(self):
        self.data = self.data[[
            'id'
            , 'published'
            , 'updated'
            , 'authors'
            , 'author'
            , 'title'
            , 'summary'
            , 'link'
            , 'arxiv_doi'
            , 'arxiv_comment'
            , 'arxiv_journal_ref'
            , 'tags'
        ]].rename(columns = {
            'author': 'submitter'
        })
        self.data['tags'] = [
            '|'.join( [tag['term'] for tag in tag_list] )
            for tag_list in self.data['tags']
            ]
        self.data['authors'] = [
            '|'.join( [author['name'] for author in authors_list] )
            for authors_list in self.data['authors']
            ]

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
                id = excluded.id 
                , published = excluded.published
                , updated = excluded.updated
                , authors = excluded.authors
                , submitter = excluded.submitter
                , title = excluded.title
                , summary = excluded.summary
                , link = excluded.link
                , arxiv_doi = excluded.arxiv_doi
                , arxiv_comment = excluded.arxiv_comment
                , arxiv_journal_ref = excluded.arxiv_journal_ref
                , tags = excluded.tags
            """
          )
        self.db_con.execute("DROP TABLE arxiv_dump")
        self.db_con.commit()

    def cleanup(self):
        self.db_con.close()

