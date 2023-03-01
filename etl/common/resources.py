from dataclasses import dataclass
from typing import Optional

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2._psycopg import connection as _connection
from psycopg2.extras import DictCursor

from etl.common.storage import JsonFileStorage, State
from etl.settings.const import FILE_NAME
from etl.settings.es import ES_DSL
from etl.settings.pg import PG_DSL


@dataclass
class Resources:
    es_client: Optional[Elasticsearch] = None
    pg_conn: Optional[_connection] = None
    state: Optional[State] = None


class ResourcesMixin:
    @property
    def resources(self) -> Resources:
        return Resources(
            es_client=Elasticsearch(**ES_DSL),
            pg_conn=psycopg2.connect(**PG_DSL, cursor_factory=DictCursor),
            state=State(JsonFileStorage(FILE_NAME)),
        )

    def get_es_client(self):
        return self.resources.es_client

    def get_pg_conn(self) -> _connection:
        return self.resources.pg_conn
