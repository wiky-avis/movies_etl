from typing import List

from etl.models.film_work import FilmWorkLoad
from etl.models.person import Person, PersonRole
from etl.settings.es import INDEX


class DataTransform:
    def get_name_persons(self, row: FilmWorkLoad, role: str) -> List[str]:
        return [
            i.get("person_name")
            for i in row.persons
            if i.get("person_role") == role
        ]

    def get_persons(self, row: FilmWorkLoad, role: str) -> List[dict]:
        return [
            dict(Person(**i))
            for i in row.persons
            if i.get("person_role") == role
        ]

    def get_doc(self, row: FilmWorkLoad) -> dict:
        return {
            "id": str(row.id),
            "title": row.title,
            "imdb_rating": row.imdb_rating,
            "genre": row.genre,
            "description": row.description,
            "director": self.get_name_persons(row, PersonRole.DIRECTOR),
            "actors_names": self.get_name_persons(row, PersonRole.ACTOR),
            "writers_names": self.get_name_persons(row, PersonRole.WRITER),
            "actors": self.get_persons(row, PersonRole.ACTOR),
            "writers": self.get_persons(row, PersonRole.WRITER),
        }

    def get_action(self, row: FilmWorkLoad) -> dict:
        return {
            "_op_type": "index",
            "_index": INDEX,
            "_id": str(row.id),
            "_source": self.get_doc(row),
        }
