import logging

from elasticsearch.helpers import bulk

from etl.common.resources import ResourcesMixin
from etl.postgres_to_es.transform import DataTransform
from etl.settings.const import CHANK
from etl.settings.es import INDEX, MAPPINGS, SETTINGS


class ElasticsearchLoader(ResourcesMixin):
    def __init__(self):
        self.trans = DataTransform()

    def create_index(self, index):
        if not self.get_es_client().indices.exists(index=index):
            self.get_es_client().indices.create(
                index=INDEX, settings=SETTINGS, mappings=MAPPINGS
            )

    def gen_data(self, data):
        for chank_data in data:
            for row in chank_data:
                yield self.trans.get_action(row=row)

    def bulk_load(self, data):
        processed, errors = bulk(
            client=self.get_es_client(),
            actions=self.gen_data(data),
            chunk_size=CHANK,
        )
        if errors:
            logging.error("Migration failed: %s", errors)
        print(processed, errors)
