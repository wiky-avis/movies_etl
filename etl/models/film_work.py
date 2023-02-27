from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class TableName(str, Enum):
    FILM_WORK = "film_work"
    PERSON = "person"
    GENRE = "genre"
    GENRE_FILM_WORK = "genre_film_work"
    PERSON_FILM_WORK = "person_film_work"


class FilmWorkLoad(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float] = Field(default=0.0, alias="rating")
    genre: Optional[List] = Field(alias="genres")
    description: Optional[str] = None
    persons: Optional[list[dict]] = []
