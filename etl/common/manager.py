import logging
import time

import backoff
from elasticsearch import exceptions
from psycopg2 import OperationalError

from etl.postgres_to_es.extract import PostgresExtractor
from etl.postgres_to_es.load import ElasticsearchLoader
from etl.settings.const import SLEEP
from etl.settings.es import INDEX


class ETLManager:
    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, OperationalError, exceptions.ConnectionError),
        max_tries=10,
    )
    def daemonize(self):
        pg_extractor = PostgresExtractor()
        es_loader = ElasticsearchLoader()
        es_loader.create_index(INDEX)

        while True:
            for ids in [
                pg_extractor.get_ids_film_work_by_person(),
                pg_extractor.get_ids_film_work_by_genre(),
                pg_extractor.get_modified_film_works(),
            ]:
                data = pg_extractor.extract_movies(ids)

                es_loader.bulk_load(data)

            time.sleep(SLEEP)

    def run(self):
        try:
            self.daemonize()
        except Exception:
            logging.error("Daemon Error", exc_info=True)
