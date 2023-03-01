from typing import List, Optional

from pydantic import BaseModel, Field


class FilmWorkLoad(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float] = Field(default=0.0, alias="rating")
    genre: Optional[List] = Field(alias="genres")
    description: Optional[str] = None
    persons: Optional[list[dict]] = []
