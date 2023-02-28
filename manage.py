import logging
import time

import click

from etl.postgres_to_es.extract import PostgresExtractor
from etl.postgres_to_es.load import ElasticsearchLoader
from etl.settings.const import SLEEP
from etl.settings.es import INDEX


class ETLManager:
    def daemonize(self):
        pg = PostgresExtractor()
        es = ElasticsearchLoader()
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
    daemon = ETLManager()
    daemon.run()


if __name__ == "__main__":
    cli()
