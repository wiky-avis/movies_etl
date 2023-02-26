import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from etl.common.decorators import backoff
from etl.postgres_to_es.transform import DataTransform
from etl.settings.const import CHANK
from etl.settings.es import INDEX, MAPPINGS, SETTINGS


class ElasticsearchLoader:
    def __init__(self, conn: Elasticsearch):
        self.es_conn = conn
        self.trans = DataTransform()

    def create_index(self, index):
        if not self.es_conn.indices.exists(index=index):
            self.es_conn.indices.create(
                index=INDEX, settings=SETTINGS, mappings=MAPPINGS
            )

    def gen_data(self, data):
        for chank_data in data:
            for row in chank_data:
                yield self.trans.get_action(row=row)

    @backoff()
    def bulk_load(self, data):
        processed, errors = bulk(
            client=self.es_conn, actions=self.gen_data(data), chunk_size=CHANK
        )
        if errors:
            logging.error("Migration failed: %s", errors)
        print(processed, errors)
