import logging
import time

import click
import psycopg2
from elasticsearch import Elasticsearch
from psycopg2._psycopg import connection as _connection
from psycopg2.extras import DictCursor

from etl.postgres_to_es.extract import PostgresExtractor
from etl.postgres_to_es.load import ElasticsearchLoader
from etl.settings.const import SLEEP
from etl.settings.es import ES_DSL, INDEX
from etl.settings.pg import PG_DSL


class ETLManager:
    def __init__(self, pg_conn: _connection, es_conn: Elasticsearch):
        self.pg_conn = pg_conn
        self.es_conn = es_conn

    def daemonize(self):
        pg = PostgresExtractor(self.pg_conn)
        es = ElasticsearchLoader(self.es_conn)
        es.create_index(INDEX)

        while True:
            for ids in [
                pg.get_modified_film_works_ids_by_person(),
                pg.get_modified_film_works_ids_by_genre(),
                pg.get_modified_film_works(),
            ]:
                data = pg.extract_movies(ids)

                es.bulk_load(data)

            time.sleep(SLEEP)

    def run(self):
        try:
            self.daemonize()
        except Exception:
            logging.error("Daemon Error", exc_info=True)


@click.group()
def cli():
    pass


@cli.command("run")
def run():
    with Elasticsearch(**ES_DSL) as es_conn, psycopg2.connect(
        **PG_DSL, cursor_factory=DictCursor
    ) as pg_conn:
        daemon = ETLManager(pg_conn, es_conn)
        daemon.run()


if __name__ == "__main__":
    cli()
