# TODO: Setup resumption if API request fails?
from time import sleep

from requests import get as http_get
from feedparser import parse as parse_feed

import pandas as pd

from .etl import ETL

ARXIV_API_ENDPOINT = "http://export.arxiv.org/api/query"
ARXIV_FEED_COLUMNS = [
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
    ]

class ArxivETL(ETL):
    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date.strftime('%Y%m%d')
        self.end_date = end_date.strftime('%Y%m%d')

    def _arxiv_api_request(self):
        search_query = 'cat:hep-ph' + " AND submittedDate:[" + self.start_date + " TO " + self.end_date + "]"
        return http_get(
            ARXIV_API_ENDPOINT
            , params = {'search_query': search_query, 'max_results': 1000}
            )

    def setup(self):
        pass

    def extract(self):
        response = self._arxiv_api_request()
        feed = parse_feed(response.content)
        self.data = pd.DataFrame(feed.entries)

    def transform(self):
        # TODO: append columns with NULLs if the arxiv feed does not return all variables
        for col_name in ARXIV_FEED_COLUMNS:
            if col_name not in self.data:
                self.data[col_name] = None
        self.data = self.data[ARXIV_FEED_COLUMNS]
        self.data = self.data.rename(columns = {'author': 'submitter'})
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
            , con = self._eng
            , schema = 'raw'
            , if_exists = "replace"
            , index = False
        )

        # TODO: either the above solution, or create a wrapper for this upsert query!
        on_conflict_updates = ", ".join([col_name + " = EXCLUDED." + col_name for col_name in ARXIV_FEED_COLUMNS ])
        self._eng.execute(
            """
            INSERT INTO raw.arxiv
            ( SELECT * FROM raw.arxiv_dump )
            ON CONFLICT (id) DO UPDATE
            SET 
            """ + on_conflict_updates
          )

    def cleanup(self):
        pass

