import logging
from typing import Iterator, List

from elasticsearch.helpers import bulk

from etl.common.resources import ResourcesMixin
from etl.models.film_work import FilmWorkLoad
from etl.postgres_to_es.transform import DataTransform
from etl.settings.const import CHANK


class ElasticsearchLoader(ResourcesMixin):
    def __init__(self):
        self.trans = DataTransform()

    def create_index(self, index: str, settings: dict, mappings: dict) -> None:
        if not self.get_es_client().indices.exists(index=index):
            self.get_es_client().indices.create(
                index=index, settings=settings, mappings=mappings
            )

    def gen_data(self, data: Iterator[List[FilmWorkLoad]]) -> Iterator[dict]:
        for chank_data in data:
            for row in chank_data:
                yield self.trans.get_action(row=row)

    def bulk_load(self, data: Iterator[List[FilmWorkLoad]]) -> None:
        processed, errors = bulk(
            client=self.get_es_client(),
            actions=self.gen_data(data),
            chunk_size=CHANK,
        )
        if errors:
            logging.error("Migration failed: %s", errors)
        print(processed, errors)
