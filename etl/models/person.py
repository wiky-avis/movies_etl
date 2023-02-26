from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class PersonRole(str, Enum):
    DIRECTOR = "director"
    WRITER = "writer"
    ACTOR = "actor"


class Person(BaseModel):
    id: str = Field(alias="person_id")
    name: Optional[str] = Field(alias="person_name")
