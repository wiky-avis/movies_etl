import logging
from typing import Generator, Iterator, List

import psycopg2

from etl.common.resources import ResourcesMixin
from etl.models.film_work import FilmWorkLoad
from etl.postgres_to_es.sql import (
    GET_FILM_WORKS,
    GET_FILM_WORKS_IDS,
    GET_GENRES,
    GET_MOVIES,
    GET_PERSONS,
)
from etl.settings.const import CHANK, DEFAULT_DATE


class PostgresExtractor(ResourcesMixin):
    def get_modified_persons(self) -> Iterator[Generator[int, None, None]]:
        modified_dt = self.resources.state.get_state("persons") or DEFAULT_DATE
        with self.get_pg_conn().cursor() as cursor:
            try:
                cursor.execute(GET_PERSONS, (modified_dt,))
            except psycopg2.Error:
                logging.error("Failed to load persons ids", exc_info=True)
                return

            while True:
                records = cursor.fetchmany(CHANK)
                if not records:
                    break

                modified_dt = records[-1][1]
                self.resources.state.set_state(
                    key="persons", value=modified_dt
                )

                yield (record[0] for record in records)

    def get_modified_genres(self) -> Iterator[Generator[int, None, None]]:
        modified_dt = self.resources.state.get_state("genres") or DEFAULT_DATE
        with self.get_pg_conn().cursor() as cursor:
            try:
                cursor.execute(GET_GENRES, (modified_dt,))
            except psycopg2.Error:
                logging.error("Failed to load genres ids", exc_info=True)
                return

            while True:
                records = cursor.fetchmany(CHANK)
                if not records:
                    break

                modified_dt = records[-1][1]
                self.resources.state.set_state(key="genres", value=modified_dt)

                yield (record[0] for record in records)

    def get_ids_film_works(self) -> Iterator[Generator[int, None, None]]:
        modified_dt = (
            self.resources.state.get_state("film_works") or DEFAULT_DATE
        )
        with self.get_pg_conn().cursor() as cursor:
            try:
                cursor.execute(GET_MOVIES, (modified_dt,))
            except psycopg2.Error:
                logging.error("Failed to load film_works ids", exc_info=True)
                return

            while True:
                records = cursor.fetchmany(CHANK)
                if not records:
                    break

                modified_dt = records[-1][1]
                self.resources.state.set_state(
                    key="film_works", value=modified_dt
                )

                yield (record[0] for record in records)

    def get_ids_film_work_by_person(
        self,
    ) -> Iterator[Generator[int, None, None]]:
        table_name, colum, data = (
            "person_film_work",
            "person_id",
            self.get_modified_persons(),
        )
        ids = tuple(row for chank_data in data for row in chank_data)
        if not ids:
            return

        with self.get_pg_conn().cursor() as cursor:
            try:
                cursor.execute(
                    GET_FILM_WORKS_IDS
                    % {"table": table_name, "colum": colum, "ids": ids}
                )
            except psycopg2.Error:
                logging.error(
                    "Failed to load film_works by person ids", exc_info=True
                )
                return

            while True:
                records = cursor.fetchmany(CHANK)
                if not records:
                    break

                yield (record[0] for record in records)

    def get_ids_film_work_by_genre(
        self,
    ) -> Iterator[Generator[int, None, None]]:
        table_name, colum, data = (
            "genre_film_work",
            "genre_id",
            self.get_modified_genres(),
        )
        ids = tuple(row for chank_data in data for row in chank_data)
        if not ids:
            return

        with self.get_pg_conn().cursor() as cursor:
            try:
                cursor.execute(
                    GET_FILM_WORKS_IDS
                    % {"table": table_name, "colum": colum, "ids": ids}
                )
            except psycopg2.Error:
                logging.error(
                    "Failed to load film_works by genre ids", exc_info=True
                )
                return

            while True:
                records = cursor.fetchmany(CHANK)
                if not records:
                    break

                yield (record[0] for record in records)

    def extract_movies(
        self, data: Iterator[Generator[int, None, None]]
    ) -> Iterator[List[FilmWorkLoad]]:
        ids = tuple(row for chank_data in data for row in chank_data)
        if not ids:
            return []

        with self.get_pg_conn().cursor() as cursor:
            try:
                cursor.execute(GET_FILM_WORKS % {"ids": ids})
            except psycopg2.Error:
                logging.error("Failed to load film_works", exc_info=True)
                return []

            records = cursor.fetchall()
            if not records:
                return []

            yield (FilmWorkLoad(**record) for record in records)
